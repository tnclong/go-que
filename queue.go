package que

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Job represents a job in queue.
type Job interface {
	// ID returns unique identity of job.
	ID() int64
	// Queue returns a string express of queue.
	Queue() string
	// Args returns json array encoded of user special arguments.
	// Caller uses encoding/json to get real values of args.
	Args() []byte
	RunAt() time.Time

	// RetryCount returns current retry count.
	RetryCount() int
	LastErrMsg() string
	LastErrStack() string

	// In sets underline method in tx.
	// It is great benefit, we use database as a queue.
	In(tx *sql.Tx)
	// Done marks job as done.
	Done(ctx context.Context) error
	// Destroy removes job from database.
	Destroy(ctx context.Context) error
	// Expire marks job as expired.
	Expire(ctx context.Context) error
	// RetryIn retries perform job after interval.
	RetryIn(ctx context.Context, interval time.Duration, cerr error) error
}

// Queue is a abstract interface.
// It describes a set of methods that a database queue should implements.
type Queue interface {
	// Queue returns a string express of queue.
	Queue() string

	// Enqueue saves a new job to queue(database).
	//
	// If tx is not nil, saves the new job in tx.
	//
	// runAt represents when job will be performed.
	// args must be accepted by json.Marshal.
	// args is a array even if only special zero/one argument.
	//
	// id is unique identity of saved job.
	Enqueue(ctx context.Context, tx *sql.Tx, runAt time.Time, args ...interface{}) (id int64, err error)

	// Lock locks a set of jobs in database.
	// These jobs unable to lock by other queues until database connection break(closed)
	// or unlock them use Unlock.
	// Lock not guarantee get count of jobs.
	Lock(ctx context.Context, count int) ([]Job, error)
	// Unlock unlocks jobs by given ids.
	// - ErrNotLockedJobsInLocal
	//    When unlock id not in local map.
	// - ErrNotLockedJobsInDB
	//    When unlock ids not locked in database.
	Unlock(ctx context.Context, ids []int64) error

	// Close close queue and holded database connection.
	// Close releases all locks that obtain by call Lock method.
	// After Close returns, above method except Queue() returns ErrQueueAlreadyClosed.
	Close() error
}

// CheckQueue checks queue is valid.
// The length of queue must in [1,100].
func CheckQueue(queue string) error {
	if queue == "" {
		return &ErrQueue{Queue: queue, Err: errors.New("queue must not be empty string")}
	}
	if len(queue) > 100 {
		return &ErrQueue{Queue: queue, Err: errors.New("the length of queue greater than 100")}
	}
	return nil
}
