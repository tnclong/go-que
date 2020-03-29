package pg

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tnclong/go-que"
)

type mutex struct {
	db *sql.DB

	mu     sync.Mutex
	conn   *sql.Conn
	active time.Time
	cancel context.CancelFunc
	ids    map[int64]bool
	err    error
}

type releaseConn func(error)

func (m *mutex) grabConn(ctx context.Context) (conn *sql.Conn, rc releaseConn, err error) {
	m.mu.Lock()
	if m.err != nil {
		m.mu.Unlock()
		return nil, nil, &que.ErrQueue{Err: que.ErrBadMutex}
	}

	if m.conn != nil {
		m.active = time.Now()
	} else {
		m.conn, err = m.db.Conn(ctx)
		if err != nil {
			m.mu.Unlock()
			return nil, nil, err
		}

		var cctx context.Context
		cctx, m.cancel = context.WithCancel(context.Background())
		m.active = time.Now()
		go m.closeIdleConn(cctx)
	}

	return m.conn, m.unlockCondReleaseConn, nil
}

func (m *mutex) unlockCondReleaseConn(err error) {
	defer m.mu.Unlock()
	if err == nil {
		return
	}

	m.err = err
	m.cancel()
	m.cancel = nil
	if err != driver.ErrBadConn {
		m.closeConn(m.conn)
	}
	m.conn = nil
	m.ids = nil
}

var maxIdle = 10 * time.Second

func (m *mutex) closeIdleConn(ctx context.Context) {
	ticker := time.NewTicker(maxIdle)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if m.tryCloseIdleConn() {
				return
			}
		}
	}
}

func (m *mutex) tryCloseIdleConn() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.ids) != 0 {
		return false
	}
	if m.active.Add(maxIdle).After(time.Now()) {
		return false
	}
	if m.err != nil {
		return true
	}

	m.cancel()
	m.cancel = nil
	m.conn.Close()
	m.conn = nil
	return true
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

func (m *mutex) Lock(ctx context.Context, queue string, count int) (jobs []que.Job, err error) {
	if count <= 0 {
		return nil, nil
	}

	var conn *sql.Conn
	var release releaseConn
	conn, release, err = m.grabConn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		release(err)
	}()

	var rows *sql.Rows
	rows, err = conn.QueryContext(ctx, lockJobs, queue, m.idsAsString(), count)
	if err != nil {
		return nil, err
	}
	var doneAt, exipredAt sql.NullTime
	var locked bool
	var remaining int
	defer rows.Close()
	for rows.Next() {
		var jb job
		rp := (*jsonRetryPolicy)(&jb.plan.RetryPolicy)
		var uniqueID sql.NullString
		err = rows.Scan(
			&jb.id, &jb.plan.Queue, &jb.plan.Args, &jb.plan.RunAt, rp, &doneAt, &exipredAt,
			&jb.retryCount, &jb.lastErrMsg, &jb.lastErrStack,
			&uniqueID, &jb.plan.UniqueLifecycle,
			&locked, &remaining,
		)
		if err != nil {
			return nil, err
		}
		if doneAt.Valid {
			err = fmt.Errorf("get a job(%v) has done_at=%s", jb.id, doneAt.Time.String())
			panic(err)
		}
		if exipredAt.Valid {
			err = fmt.Errorf("get a job(%v) has expired_at=%s", jb.id, doneAt.Time.String())
			panic(err)
		}
		if !locked {
			err = fmt.Errorf("get a job(%v) has locked=%v", jb.id, locked)
			panic(err)
		}
		jb.db = m.db
		if uniqueID.Valid {
			jb.plan.UniqueID = &uniqueID.String
		}
		m.ids[jb.id] = true
		jobs = append(jobs, &jb)
	}
	return jobs, nil
}

func (m *mutex) idsAsString() string {
	if len(m.ids) > 0 {
		b := make([]byte, 1, 1+len(m.ids)*2)
		b[0] = '{'
		for id := range m.ids {
			b = strconv.AppendInt(b, id, 10)
			b = append(b, ',')
		}
		b[len(b)-1] = '}'
		return string(b)
	}
	return "{}"
}

func (m *mutex) Unlock(ctx context.Context, ids []int64) (err error) {
	if len(ids) == 0 {
		return nil
	}

	var conn *sql.Conn
	var release releaseConn
	conn, release, err = m.grabConn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		release(err)
	}()

	var notExists []int64
	for _, id := range ids {
		if !m.ids[id] {
			notExists = append(notExists, id)
		}
	}
	if len(notExists) > 0 {
		return &que.ErrQueue{Err: que.ErrUnlockedJobs}
	}

	var b strings.Builder
	b.WriteString("SELECT pg_advisory_unlock(v.i) FROM (VALUES (")
	b.WriteString(strconv.FormatInt(ids[0], 10))
	for _, id := range ids[1:] {
		b.WriteString("), (")
		b.WriteString(strconv.FormatInt(id, 10))
	}
	b.WriteString(")) v (i)")

	var rows *sql.Rows
	rows, err = conn.QueryContext(ctx, b.String())
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

		delete(m.ids, ids[i])
		if !unlocked {
			notLocked = append(notLocked, ids[i])
		}
		i++
	}
	if len(notLocked) > 0 {
		return &que.ErrQueue{Err: que.ErrUnlockedJobs}
	}
	return nil
}

func (m *mutex) closeConn(conn *sql.Conn) {
	conn.Raw(func(driverConn interface{}) error {
		return driver.ErrBadConn
	})
}

func (q *queue) query(tx *sql.Tx) func(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if tx != nil {
		return tx.QueryContext
	}
	return q.db.QueryContext
}

type jsonRetryPolicy que.RetryPolicy

func (rp jsonRetryPolicy) Value() (driver.Value, error) {
	return json.Marshal(rp)
}

func (rp *jsonRetryPolicy) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("jsonRetryPolicy: type assertion to []byte failed")
	}

	return json.Unmarshal(b, &rp)
}
