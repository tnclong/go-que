package que

import (
	"context"
	"database/sql"
	"time"
)

// Job represents a job in queue.
type Job interface {
	// ID returns unique identity of job.
	ID() int64
	// Plan returns a copy of plan.
	Plan() Plan
	// RetryCount returns current retry count.
	RetryCount() int32
	LastErrMsg() *string
	LastErrStack() *string

	// In sets underline method in tx.
	// It is great benefit, we use database as a queue.
	In(tx *sql.Tx)
	// Done marks job as done.
	Done(ctx context.Context) error
	// Destroy removes job from database.
	Destroy(ctx context.Context) error
	// Expire marks job as expired.
	Expire(ctx context.Context) error
	// RetryAfter retries perform job after interval.
	RetryAfter(ctx context.Context, interval time.Duration, cerr error) error
	// RetryInPlan retries according to RetryPolicy of Plan.
	// It executes Expire function when RetryCount() >= MaxRetryCount.
	RetryInPlan(ctx context.Context, cerr error) error
}

// UniqueLifecycle controls if and when job need unique in database.
// The unique is queue level. No potential impact between different queues.
// If UniqueLifecycle is Always or Done, auto override Destory() method to Done().
type UniqueLifecycle uint8

const (
	// Ignore ignore UniqueID and set the UniqueID to nil.
	Ignore UniqueLifecycle = iota
	// Always must set a UniqueID and it's value is unique after enqueue.
	Always
	// Done keeps job as unique until expired.
	Done
)

// Plan is a series of parameters structure a new job.
type Plan struct {
	// Queue is a string express of queue.
	Queue string

	// Args is json array encoded of user specified arguments.
	// args is a array even if only special zero/one argument.
	// Uses helper method Args() for convenience.
	//    que.Args(1, "2")
	Args []byte
	// RunAt represents when job will be performed.
	RunAt time.Time

	// RetryPolicy guides retry behavior when perform job failed.
	RetryPolicy RetryPolicy

	// UniqueID is optinal value when UniqueLifecycle is Ignore.
	// UniqueID must set a value when UniqueLifecycle is Always or Done.
	UniqueID        *string
	UniqueLifecycle UniqueLifecycle
}

// Queue is a abstract interface.
// It describes a set of methods that a database queue should implements.
type Queue interface {
	// Enqueue saves a set of jobs according to plans.
	//
	// If tx is not nil, saves new jobs in tx.
	//
	// ids are unique identities of saved jobs.
	Enqueue(ctx context.Context, tx *sql.Tx, plans ...Plan) (ids []int64, err error)

	Mutex() Mutex
}

// Mutex controls locks of performable jobs in database.
// Mutex must Release after none use it.
type Mutex interface {
	// Lock locks a set of jobs in database.
	// These jobs unable to lock by other mutex until database connection break(closed)
	// or unlock them use Unlock.
	// Lock not guarantee get count of jobs.
	Lock(ctx context.Context, queue string, count int) ([]Job, error)
	// Unlock unlocks jobs by given ids.
	// - ErrNotLockedJobsInLocal
	//    When unlock id not in local map.
	// - ErrNotLockedJobsInDB
	//    When unlock ids not locked in database.
	Unlock(ctx context.Context, ids []int64) error

	// Release closes holded database connection and
	// releases all locks that obtain by call Lock method.
	Release() error
}
