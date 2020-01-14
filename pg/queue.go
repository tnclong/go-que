package pg

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tnclong/go-que"
)

func New(db *sql.DB, qe string) (que.Queue, error) {
	if db == nil {
		return nil, errors.New("db must not be nil")
	}
	err := que.CheckQueue(qe)
	if err != nil {
		return nil, err
	}
	return &queue{
		queue: qe,
		db:    db,
		ids:   make(map[int64]bool),
	}, nil
}

type queue struct {
	queue  string
	db     *sql.DB
	conn   *sql.Conn
	ids    map[int64]bool
	closed bool
}

func (q *queue) newErrAlreadyClosed() error {
	return q.newErrQueue(que.ErrQueueAlreadyClosed)
}

func (q *queue) newErrQueue(err error) error {
	return &que.ErrQueue{Queue: q.queue, Err: err}
}

func (q *queue) Queue() string {
	return q.queue
}

func (q *queue) Close() error {
	if q.closed {
		return q.newErrAlreadyClosed()
	}

	q.closed = true
	if q.conn != nil {
		err := q.conn.Raw(func(driverConn interface{}) error {
			return driver.ErrBadConn
		})
		if err != nil && err != driver.ErrBadConn {
			return err
		}
		if err == nil {
			return errors.New("try to release connection failed by returns driver.ErrBadConn")
		}
		q.conn = nil
	}
	return nil
}

const insertJob = `INSERT INTO goque_jobs(queue, run_at, args)
VALUES ($1::text, $2::timestamptz, $3::jsonb)
RETURNING id`

var emptyArgs = []byte{'[', ']'}

func (q *queue) Enqueue(ctx context.Context, tx *sql.Tx, runAt time.Time, args ...interface{}) (id int64, err error) {
	if q.closed {
		return 0, q.newErrAlreadyClosed()
	}

	var argsBytes []byte
	if len(args) > 0 {
		var buf bytes.Buffer
		encoder := json.NewEncoder(&buf)
		encoder.SetEscapeHTML(false)
		err = encoder.Encode(args)
		if err != nil {
			return 0, err
		}
		argsBytes = buf.Bytes()
	} else {
		argsBytes = emptyArgs
	}

	row := q.queryRow(tx)(ctx, insertJob, q.queue, runAt, argsBytes)
	err = row.Scan(&id)
	return id, err
}

func (q *queue) holdConn() error {
	var err error
	if q.conn == nil {
		q.conn, err = q.db.Conn(context.Background())
	}
	return err
}

const lockJobs = `WITH RECURSIVE jobs AS (
    SELECT (jb_t.jb).*, locks.locked, locks.remaining
    FROM (
             SELECT goque_jobs AS jb
             FROM goque_jobs
             where queue = $1::text
               AND NOT id = ANY ($2::bigint[])
               AND run_at <= now()
               AND done_at IS NULL
               AND expired_at IS NULL
             ORDER BY run_at, id
             LIMIT 1
         ) AS jb_t
             JOIN LATERAL (SELECT *
                           FROM goque_lock_and_decrease_remaining($3, jb_t.jb)) AS locks ON TRUE
    UNION ALL
    (
        SELECT (jb_t.jb).*, locks.locked, locks.remaining
        FROM (
                 SELECT remaining,
                        (
                            SELECT goque_jobs
                            FROM goque_jobs
                            WHERE queue = $1::text
                              AND NOT id = ANY ($2::bigint[])
                              AND run_at <= now()
                              AND done_at IS NULL
                              AND expired_at IS NULL
                              AND (run_at, id) >
                                  (jobs.run_at, jobs.id)
                            ORDER BY run_at, id
                            LIMIT 1
                        ) AS jb
                 FROM jobs
                 WHERE jobs.remaining != 0
                 LIMIT 1
             ) AS jb_t
                 JOIN LATERAL (SELECT *
                               FROM goque_lock_and_decrease_remaining(jb_t.remaining, jb_t.jb)) AS locks
                      ON TRUE
    )
)
SELECT *
FROM jobs
WHERE locked`

func (q *queue) Lock(ctx context.Context, size int) ([]que.Job, error) {
	if q.closed {
		return nil, q.newErrAlreadyClosed()
	}
	if err := q.holdConn(); err != nil {
		return nil, err
	}

	var jobs []que.Job
	rows, err := q.conn.QueryContext(ctx, lockJobs, q.queue, q.idsAsString(), size)
	if err != nil {
		return nil, err
	}
	var doneAt, exipredAt sql.NullTime
	var locked bool
	var remaining int
	defer rows.Close()
	for rows.Next() {
		var jb job
		err = rows.Scan(
			&jb.id, &jb.queue, &jb.args, &jb.runAt, &doneAt, &exipredAt,
			&jb.retryCount, &jb.lastErrMsg, &jb.lastErrStack,
			&locked, &remaining,
		)
		if err != nil {
			return nil, err
		}
		if jb.queue != q.queue {
			panic(fmt.Errorf("queue %s get a job(%v) from another queue %s", q.queue, jb.id, jb.queue))
		}
		if doneAt.Valid {
			panic(fmt.Errorf("queue %s get a job(%v) has done_at=%s", q.queue, jb.id, doneAt.Time.String()))
		}
		if exipredAt.Valid {
			panic(fmt.Errorf("queue %s get a job(%v) has expired_at=%s", q.queue, jb.id, doneAt.Time.String()))
		}
		if !locked {
			panic(fmt.Errorf("queue %s get a job(%v) has locked=%v", q.queue, jb.id, locked))
		}
		jb.db = q.db
		q.ids[jb.id] = true
		jobs = append(jobs, &jb)
	}
	return jobs, nil
}

func (q *queue) idsAsString() string {
	if len(q.ids) > 0 {
		b := make([]byte, 1, 1+len(q.ids)*2)
		b[0] = '{'
		for id := range q.ids {
			b = strconv.AppendInt(b, id, 10)
			b = append(b, ',')
		}
		b[len(b)-1] = '}'
		return string(b)
	}
	return "{}"
}

func (q *queue) Unlock(ctx context.Context, ids []int64) error {
	if q.closed {
		return q.newErrAlreadyClosed()
	}

	if len(ids) == 0 {
		return nil
	}

	var notExists []int64
	for _, id := range ids {
		if !q.ids[id] {
			notExists = append(notExists, id)
		}
	}
	if len(notExists) > 0 {
		return q.newErrQueue(&que.ErrUnlock{IDs: notExists, Err: que.ErrNotLockedJobsInLocal})
	}

	var b strings.Builder
	b.WriteString("SELECT pg_advisory_unlock(v.i) FROM (VALUES (")
	b.WriteString(strconv.FormatInt(ids[0], 10))
	for _, id := range ids[1:] {
		b.WriteString("), (")
		b.WriteString(strconv.FormatInt(id, 10))
	}
	b.WriteString(")) v (i)")
	rows, err := q.conn.QueryContext(ctx, b.String())
	if err != nil {
		return err
	}
	defer rows.Close()
	var notLocked []int64
	var i int
	for rows.Next() {
		var unlocked bool
		err = rows.Scan(&unlocked)
		if err != nil {
			return err
		}

		delete(q.ids, ids[i])
		if !unlocked {
			notLocked = append(notLocked, ids[i])
		}
		i++
	}
	if len(notLocked) > 0 {
		return q.newErrQueue(&que.ErrUnlock{IDs: notLocked, Err: que.ErrNotLockedJobsInDB})
	}
	return nil
}

func (q *queue) queryRow(tx *sql.Tx) func(context.Context, string, ...interface{}) *sql.Row {
	if tx != nil {
		return tx.QueryRowContext
	}
	return q.db.QueryRowContext
}
