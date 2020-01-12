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

	id    int64
	queue string
	args  []byte

	runAt time.Time

	retryCount   int
	lastErrMsg   sql.NullString
	lastErrStack sql.NullString
}

func (j *job) ID() int64 {
	return j.id
}

func (j *job) Queue() string {
	return j.queue
}

func (j *job) Args() []byte {
	return j.args
}

func (j *job) RunAt() time.Time {
	return j.runAt
}

func (j *job) RetryCount() int {
	return j.retryCount
}

func (j *job) LastErrMsg() string {
	return j.lastErrMsg.String
}

func (j *job) LastErrStack() string {
	return j.lastErrStack.String
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
	_, err := j.exec(j.tx)(ctx, destroyJob, j.id)
	return err
}

const expireJob = `UPDATE goque_jobs
SET retry_count = retry_count + 1,
    expired_at  = now()
WHERE id = $1::bigint`

func (j *job) Expire(ctx context.Context) error {
	_, err := j.exec(j.tx)(ctx, expireJob, j.id)
	return err
}

const retryJob = `UPDATE goque_jobs
SET retry_count        = retry_count + 1,
    run_at             = now() + $1::float * '1 second'::interval,
    last_err_msg       = left($2::text, 512),
    last_err_stack = left($3::text, 8192)
WHERE id = $4::bigint`

func (j *job) RetryIn(ctx context.Context, interval time.Duration, cerr error) error {
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

func (j *job) exec(tx *sql.Tx) func(context.Context, string, ...interface{}) (sql.Result, error) {
	if tx != nil {
		return tx.ExecContext
	}
	return j.db.ExecContext
}
