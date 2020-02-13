package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/tnclong/go-que"
)

func New(db *sql.DB) (que.Queue, error) {
	if db == nil {
		return nil, errors.New("db must not be nil")
	}
	return &queue{db: db}, nil
}

type queue struct {
	db *sql.DB
}

var emptyArgs = []byte{'[', ']'}

func (q *queue) Enqueue(ctx context.Context, tx *sql.Tx, plans ...que.Plan) (ids []int64, err error) {
	if len(plans) == 0 {
		return nil, nil
	}

	const values = "($%d::text, $%d::timestamptz, $%d::jsonb, $%d::jsonb)"
	args := make([]interface{}, 0, 4*len(plans))
	var b strings.Builder
	b.WriteString("INSERT INTO goque_jobs(queue, run_at, args, retry_policy) VALUES ")
	n := (len(values)+2)*len(plans) + 13
	b.Grow(n)
	fmt.Fprintf(&b, values, 1, 2, 3, 4)
	plan := plans[0]
	if len(plan.Args) == 0 {
		plan.Args = emptyArgs
	}
	args = append(args, plan.Queue, plan.RunAt, plan.Args, jsonRetryPolicy(plan.RetryPolicy))
	i := 5
	for _, plan = range plans[1:] {
		b.WriteString(", ")
		fmt.Fprintf(&b, values, i, i+1, i+2, i+3)
		i += 4
		if len(plan.Args) == 0 {
			plan.Args = emptyArgs
		}
		args = append(args, plan.Queue, plan.RunAt, plan.Args, jsonRetryPolicy(plan.RetryPolicy))
	}
	b.WriteString(" RETURNING id")
	rows, err := q.query(tx)(ctx, b.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids = make([]int64, 0, len(plans))
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, err
}

func (q *queue) Mutex() que.Mutex {
	return &mutex{db: q.db, ids: make(map[int64]bool)}
}
