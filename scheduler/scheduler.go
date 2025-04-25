package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/tnclong/go-que"
)

// Scheduler enqueue jobs according to provided schedule.
// Prepare must be called in application initializtion.
// Scheduler with same Queue can run in different machines.
type Scheduler struct {
	DB *sql.DB
	// Queue is used by scheduler ennqueue self.
	Queue string
	// Enqueue saves a set of jobs according to plans.
	Enqueue func(context.Context, *sql.Tx, ...que.Plan) ([]int64, error)
	// Provider privide a list of named schedule.
	// Scheduler enqueues new plans according to privided schedule.
	Provider Provider
	// Derivations is list of Derivation.
	// The key of provided Schedule corresponding to the key of Derivations.
	Derivations map[string]Derivation
}

// Derivation derives new plans from scheduled plans.
// Derived plans will replace scheduled plans.
type Derivation interface {
	Derive(context.Context, *sql.Tx, []que.Plan) ([]que.Plan, error)
}

// DerivationFunc wraps a function as Derivation.
type DerivationFunc func(context.Context, *sql.Tx, []que.Plan) ([]que.Plan, error)

// Derive implements Derive of Derivation.
func (f DerivationFunc) Derive(ctx context.Context, tx *sql.Tx, plans []que.Plan) ([]que.Plan, error) {
	return f(ctx, tx, plans)
}

var retryPolicy = que.RetryPolicy{
	InitialInterval: 10 * time.Second,

	MaxInterval:            2 * time.Minute,
	NextIntervalMultiplier: 1.5,
	IntervalRandomPercent:  0,
	MaxRetryCount:          1 << 24,
}

var (
	uniqueID        = "scheduler.enqueue.self.unique"
	uniqueLifecycle = que.Lockable
)

// Prepare auto schedules self in given queue at first.
// Prepare must be called in application initializtion.
func (sc *Scheduler) Prepare(ctx context.Context) error {
	_, err := sc.Enqueue(ctx, nil, que.Plan{
		Queue:           sc.Queue,
		Args:            que.Args(),
		RunAt:           nowFunc(),
		RetryPolicy:     retryPolicy,
		UniqueID:        &uniqueID,
		UniqueLifecycle: uniqueLifecycle,
	})
	if err != nil {
		if errors.Is(err, que.ErrViolateUniqueConstraint) {
			err = nil
		}
	} else {
		log.Print(sc.sprintf("prepare enqueue self sucess"))
	}
	return err
}

// Perform is Perform function of que.WorkerOptions.
func (sc *Scheduler) Perform(ctx context.Context, job que.Job) error {
	plan := job.Plan()
	args, err := decodeArgs(plan.Args)
	if err != nil {
		log.Panic(sc.sprintf("decode args failed with err: %v"), err)
	}
	schedule, err := sc.Provider.Provide()
	if err != nil {
		log.Panic(sc.sprintf("provide schedule failed with err: %v", err))
	}
	if len(schedule) == 0 {
		log.Print(sc.sprintf("provide zero length of schedule"))
	}
	err = ValidateSchedule(schedule)
	if err != nil {
		log.Panic(sc.sprintf("provide invalid schedule: %v", err))
	}
	now := nowFunc()
	namePlans := calculate(schedule, now, args)
	nameIDs := make(map[string][]int64)
	sc.inTx(ctx, func(tx *sql.Tx) {
		var err error
		var ids []int64
		var dn Derivation
		for name, plans := range namePlans {
			dn = sc.Derivations[name]
			if dn != nil {
				plans, err = dn.Derive(ctx, tx, plans)
				if err != nil {
					log.Panic(sc.sprintf("derive plans of %q with err: %v", name, err))
				}
				if len(plans) == 0 {
					log.Printf(sc.sprintf("derive zero plan of %q", name))
					continue
				}
			}

			ids, err = sc.Enqueue(ctx, tx, plans...)
			if err != nil {
				log.Panic(sc.sprintf("enqueue plans of %q with err: %v", name, err))
			}
			nameIDs[name] = ids
		}

		job.In(tx)
		err = job.Destroy(ctx)
		if err != nil {
			log.Panic(sc.sprintf("destroy old self with err: %v", err))
		}
		nameIDs[sc.Queue] = sc.enqueueSelfAgain(ctx, tx, now, schedule)
	})

	log.Print(sc.sprintf("last ran at %s", args.lastRunAt.String()))
	for name, ids := range nameIDs {
		log.Print(sc.sprintf("enqueue plans of %q ids %v", name, ids))
	}

	return nil
}

func (sc *Scheduler) inTx(ctx context.Context, fn func(tx *sql.Tx)) {
	var tx *sql.Tx
	var err error
	tx, err = sc.DB.BeginTx(ctx, nil)
	if err != nil {
		log.Panic(sc.sprintf("begin tx with err: %v", err))
	}

	var calledFn bool
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%+v", r)
		}
		if !calledFn || err != nil {
			if err2 := tx.Rollback(); err2 != nil {
				log.Panic(sc.sprintf("tx rollback with another err: %v", err))
			}
			panic(err)
		} else {
			err = tx.Commit()
			if err != nil {
				log.Panic(sc.sprintf("tx commit with err: %v", err))
			}
		}
	}()
	fn(tx)
	calledFn = true
}

func (sc *Scheduler) enqueueSelfAgain(ctx context.Context, tx *sql.Tx, now time.Time, schedule Schedule) []int64 {
	runAt := now.Truncate(time.Minute).Add(time.Minute)
	names := make([]string, 0, len(schedule))
	for name := range schedule {
		names = append(names, name)
	}
	ids, err := sc.Enqueue(ctx, tx, que.Plan{
		Queue:           sc.Queue,
		Args:            que.Args(now, names),
		RunAt:           runAt,
		RetryPolicy:     retryPolicy,
		UniqueID:        &uniqueID,
		UniqueLifecycle: uniqueLifecycle,
	})
	if err != nil {
		log.Panic(sc.sprintf("enqueue self again with err: ", err))
	}
	return ids
}

func (sc *Scheduler) sprintf(format string, a ...interface{}) string {
	return fmt.Sprintf("scheduler "+sc.Queue+" "+format, a...)
}

type args struct {
	lastRunAt time.Time
	names     []string
}

func (a args) contains(name string) bool {
	for _, n := range a.names {
		if n == name {
			return true
		}
	}
	return false
}

func calculate(schedule Schedule, now time.Time, args args) map[string][]que.Plan {
	namePlans := make(map[string][]que.Plan)
	for name, item := range schedule {
		initiateTimeSet := item.InitiateTime != nil && !item.InitiateTime.IsZero()
		if !args.contains(name) && !initiateTimeSet {
			continue
		}

		cronSc, _ := parseCron(item.Cron)
		lastTime := args.lastRunAt
		if initiateTimeSet {
			// Use InitiateTime-1ns when it's later than lastRunAt to include exact cron points
			prevTime := item.InitiateTime.Add(-1 * time.Nanosecond)
			if prevTime.After(lastTime) {
				lastTime = prevTime
			}
		}
		var nextTimes []time.Time
		for {
			nextTime := cronSc.Next(lastTime)
			if nextTime.After(now) {
				break
			}
			nextTimes = append(nextTimes, nextTime)
			lastTime = nextTime
		}
		if len(nextTimes) == 0 {
			continue
		}
		if item.RecoveryPolicy == Ignore {
			nextTimes = nextTimes[len(nextTimes)-1:]
		}

		plans := make([]que.Plan, 0, len(nextTimes))
		for _, runAt := range nextTimes {
			plan := que.Plan{
				Queue:       item.Queue,
				Args:        []byte(item.Args),
				RunAt:       runAt,
				RetryPolicy: item.RetryPolicy,
			}
			plans = append(plans, plan)
		}
		namePlans[name] = plans
	}
	return namePlans
}

func decodeArgs(data []byte) (a args, err error) {
	var count int
	count, err = que.ParseArgs(data, &a.lastRunAt, &a.names)
	if err != nil {
		return
	}
	if count == 0 {
		return args{lastRunAt: nowFunc()}, nil
	}
	return a, nil
}

// nowFunc returns the current time; it's overridden in tests.
var nowFunc = time.Now
