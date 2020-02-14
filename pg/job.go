package pg

import (
	"context"
	"database/sql"
	"time"

	"github.com/tnclong/go-que"
)

type job struct {
	db *sql.DB
	tx *sql.Tx

	id   int64
	plan que.Plan

	retryCount   int32
	lastErrMsg   sql.NullString
	lastErrStack sql.NullString
}

func (j *job) ID() int64 {
	return j.id
}

func (j *job) Plan() que.Plan {
	return j.plan
}

func (j *job) RetryCount() int32 {
	return j.retryCount
}

func (j *job) LastErrMsg() *string {
	if j.lastErrMsg.Valid {
		return &j.lastErrMsg.String
	}
	return nil
}

func (j *job) LastErrStack() *string {
	if j.lastErrStack.Valid {
		return &j.lastErrStack.String
	}
	return nil
}

func (j *job) In(tx *sql.Tx) {
	j.tx = tx
}

const doneJob = `UPDATE goque_jobs
SET done_at = now()
WHERE id = $1::bigint`

func (j *job) Done(ctx context.Context) error {
	_, err := j.exec(j.tx)(ctx, doneJob, j.id)
	return err
}

const destroyJob = `DELETE
FROM goque_jobs
WHERE id = $1::bigint`

func (j *job) Destroy(ctx context.Context) error {
	if j.plan.UniqueLifecycle > que.Ignore {
		return j.Done(ctx)
	}
	_, err := j.exec(j.tx)(ctx, destroyJob, j.id)
	return err
}

const expireJob = `UPDATE goque_jobs
SET retry_count = retry_count + 1,
    expired_at  = now()
WHERE id = $1::bigint`

const expireUniqueIDJob = `UPDATE goque_jobs
SET retry_count = retry_count + 1,
	expired_at  = now(),
	unique_id = null
WHERE id = $1::bigint`

func (j *job) Expire(ctx context.Context) error {
	var execSQL string
	if j.plan.UniqueLifecycle == que.Done {
		execSQL = expireUniqueIDJob
	} else {
		execSQL = expireJob
	}
	_, err := j.exec(j.tx)(ctx, execSQL, j.id)
	return err
}

const retryJob = `UPDATE goque_jobs
SET retry_count        = retry_count + 1,
    run_at             = now() + $1::float * '1 second'::interval,
    last_err_msg       = left($2::text, 512),
    last_err_stack = left($3::text, 8192)
WHERE id = $4::bigint`

func (j *job) RetryAfter(ctx context.Context, interval time.Duration, cerr error) error {
	args := make([]interface{}, 4)
	args[0] = interval.Seconds()
	if cerr != nil {
		args[1] = cerr.Error()
		args[2] = que.Stack(4)
	}
	args[3] = j.id
	_, err := j.exec(j.tx)(ctx, retryJob, args...)
	return err
}

func (j *job) RetryInPlan(ctx context.Context, cerr error) error {
	nextInterval, ok := j.plan.RetryPolicy.NextInterval(j.retryCount)
	if ok {
		return j.RetryAfter(context.Background(), nextInterval, cerr)
	}
	return j.Expire(context.Background())
}

func (j *job) exec(tx *sql.Tx) func(context.Context, string, ...interface{}) (sql.Result, error) {
	if tx != nil {
		return tx.ExecContext
	}
	return j.db.ExecContext
}
