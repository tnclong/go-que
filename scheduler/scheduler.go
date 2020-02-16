package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
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
	Queue    string
	Enqueue  func(context.Context, *sql.Tx, ...que.Plan) ([]int64, error)
	Provider Provider
}

var retryPolicy = que.RetryPolicy{
	InitialInterval: 10 * time.Second,

	MaxInterval:            2 * time.Minute,
	NextIntervalMultiplier: 1.5,
	IntervalRandomPercent:  0,
	MaxRetryCount:          1 << 24,
}

var uniqueID = "scheduler.enqueue.self.unique"
var uniqueLifecycle = que.Lockable

// Prepare auto schedules self in given queue at first.
// Prepare must be called in application initializtion.
func (sc *Scheduler) Prepare(ctx context.Context) error {
	_, err := sc.Enqueue(context.Background(), nil, que.Plan{
		Queue:           sc.Queue,
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
		log.Print(sc.sprintf("prepare enqueue self sucess", sc.Queue))
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
	err = ValidateSchedule(schedule)
	if err != nil {
		log.Panic(sc.sprintf("provide invalid schedule: %v", err))
	}
	now := nowFunc()
	plans := calculate(schedule, now, args)
	var planIDs []int64
	sc.inTx(ctx, func(tx *sql.Tx) {
		var err error
		planIDs, err = sc.Enqueue(ctx, tx, plans...)
		if err != nil {
			log.Panic(sc.sprintf("enqueue jobs with err: %v", err))
		}
		sc.enqueueSelfAgain(ctx, tx, now, schedule)
		job.In(tx)
		err = job.Destroy(ctx)
		if err != nil {
			log.Panic(sc.sprintf("destroy old self with err: %v", err))
		}
	})

	log.Print(sc.sprintf("last ran at %s", args.lastRunAt.String()))
	for i, plan := range plans {
		log.Print(sc.sprintf("enqueue %s job %d", plan.Queue, planIDs[i]))
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

func (sc *Scheduler) enqueueSelfAgain(ctx context.Context, tx *sql.Tx, now time.Time, schedule Schedule) {
	runAt := now.Truncate(time.Minute).Add(time.Minute)
	args := make([]interface{}, 0, len(schedule)+1)
	args = append(args, now)
	for name := range schedule {
		args = append(args, name)
	}
	_, err := sc.Enqueue(ctx, tx, que.Plan{
		Queue:           sc.Queue,
		Args:            que.Args(args...),
		RunAt:           runAt,
		RetryPolicy:     retryPolicy,
		UniqueID:        &uniqueID,
		UniqueLifecycle: uniqueLifecycle,
	})
	if err != nil {
		panic(sc.sprintf("enqueue self again with err: ", err))
	}
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

func calculate(schedule Schedule, now time.Time, args args) []que.Plan {
	var plans []que.Plan
	for name, item := range schedule {
		if !args.contains(name) {
			continue
		}

		cronSc, _ := parseCron(item.Cron)
		lastTime := args.lastRunAt
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

		for _, runAt := range nextTimes {
			plan := que.Plan{
				Queue: item.Queue,
				Args:  []byte(item.Args),
				RunAt: runAt,
			}
			plans = append(plans, plan)
		}
	}
	return plans
}

func decodeArgs(data []byte) (a args, err error) {
	err = json.Unmarshal(data, &a.names)
	if err != nil {
		return
	}
	if len(a.names) == 0 {
		return args{lastRunAt: nowFunc()}, nil
	}
	a.lastRunAt, err = time.Parse(time.RFC3339, a.names[0])
	if err != nil {
		return
	}
	a.names = a.names[1:]
	return a, nil
}

// nowFunc returns the current time; it's overridden in tests.
var nowFunc = time.Now
