package que_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/tnclong/go-que"
	"github.com/tnclong/go-que/pg"
)

func TestEnqueueLockUnlock(t *testing.T) {
	q := newQueue()
	mu := q.Mutex()
	qs := randQueue()

	type user struct {
		Name string
	}
	runAt := time.Now()
	amap := map[string]int{"1": 1}
	astruct := user{Name: "name"}
	plan := que.Plan{
		Queue: qs,
		Args: que.Args(
			1, 2.0, math.MaxInt64, "text", true,
			runAt, amap, astruct,
		),
		RunAt: runAt,
		RetryPolicy: que.RetryPolicy{
			InitialInterval:        10 * time.Second,
			MaxInterval:            20 * time.Second,
			NextIntervalMultiplier: 2,
			IntervalRandomPercent:  8,
			MaxRetryCount:          3,
		},
	}
	ids, err := q.Enqueue(
		context.Background(), nil,
		plan,
	)
	if err != nil {
		t.Fatalf("Enqueue get err: %v", err)
	}
	if len(ids) == 0 || ids[0] <= 0 {
		t.Fatalf("want id greater than zero but get %d", ids)
	}
	id := ids[0]

	jobs, err := mu.Lock(context.Background(), qs, 1)
	if err != nil {
		t.Fatalf("Lock get err: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("want length of jobs is 1 but get %d", len(jobs))
	}
	job := jobs[0]
	if job.ID() != id {
		t.Fatalf("want id is %d but get %d", id, job.ID())
	}

	actualPlan := job.Plan()
	if !plan.RunAt.Equal(actualPlan.RunAt) {
		t.Fatalf("want run at is %s but get %s", plan.RunAt.String(), actualPlan.RunAt.String())
	}
	plan.RunAt = actualPlan.RunAt
	actualPlan.Args = bytes.ReplaceAll(actualPlan.Args, []byte{' '}, nil)
	if !reflect.DeepEqual(plan, actualPlan) {
		t.Errorf("want plan is %#v but get %#v", plan, actualPlan)
		t.Fatalf("want args is |%s| but get |%s|", plan.Args, actualPlan.Args)
	}

	if job.RetryCount() != 0 {
		t.Fatalf("want retry count is 0 but get %d", job.RetryCount())
	}
	if job.LastErrMsg() != nil {
		t.Fatalf("want last err msg is nil but get %q", *job.LastErrMsg())
	}
	if job.LastErrStack() != nil {
		t.Fatalf("want last err stack is nil but get %q", *job.LastErrStack())
	}

	jobs2, err := mu.Lock(context.Background(), qs, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs2) != 0 {
		t.Fatalf("want length of job2 is zero but get %d", len(jobs2))
	}

	err = mu.Unlock(context.Background(), []int64{id})
	if err != nil {
		t.Fatal(err)
	}
}

func TestErrUnlockedJobs(t *testing.T) {
	mu := newQueue().Mutex()
	var id int64 = 100000
	err := mu.Unlock(context.Background(), []int64{id})
	if !errors.Is(err, que.ErrUnlockedJobs) {
		t.Fatalf("want err is %v but get %v", que.ErrUnlockedJobs, err)
	}
	var errQue *que.ErrQueue
	if !errors.As(err, &errQue) {
		t.Fatalf("want %T but get %T", errQue, err)
	}

	qs := randQueue()
	_, err = mu.Lock(context.Background(), qs, 1)
	if !errors.Is(err, que.ErrBadMutex) {
		t.Fatalf("want err %v but get %v", que.ErrBadMutex, err)
	}
}

func TestLockSuccessAfterUnlock(t *testing.T) {
	q := newQueue()
	mu := q.Mutex()
	qs := randQueue()
	id, err := q.Enqueue(context.Background(), nil, que.Plan{
		Queue: qs,
		Args:  []byte{},
		RunAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs1, err := mu.Lock(context.Background(), qs, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs1) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs1))
	}

	err = mu.Unlock(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}

	jobs2, err := mu.Lock(context.Background(), qs, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs2) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs2))
	}
}

func TestSameQueueDiffMutex(t *testing.T) {
	q := newQueue()

	mu1 := q.Mutex()
	qs1 := randQueue()
	id, err := q.Enqueue(context.Background(), nil, que.Plan{
		Queue: qs1,
		Args:  que.Args(),
		RunAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs1, err := mu1.Lock(context.Background(), qs1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs1) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs1))
	}

	// mu1 locked
	// mu2 get nothinng
	mu2 := q.Mutex()
	jobs2, err := mu2.Lock(context.Background(), qs1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs2) != 0 {
		t.Fatalf("want lock 0 job but get %d", len(jobs2))
	}

	// mu1 unlock
	err = mu1.Unlock(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}

	// mu1 unlock, mu2 get job
	jobs3, err := mu2.Lock(context.Background(), qs1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs3) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs3))
	}

	// q enqueue another job
	_, err = q.Enqueue(context.Background(), nil, que.Plan{
		Queue: qs1,
		Args:  que.Args(),
		RunAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	// q1 enqueue another job, mu2 get another job.
	jobs4, err := mu2.Lock(context.Background(), qs1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs4) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs3))
	}
}

func TestDiffQueue(t *testing.T) {
	q := newQueue()

	mu := q.Mutex()
	qs1 := randQueue()
	_, err := q.Enqueue(context.Background(), nil, que.Plan{
		Queue: qs1,
		Args:  que.Args(),
		RunAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// qs2 get nothinng
	qs2 := randQueue()
	jobs2, err := mu.Lock(context.Background(), qs2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs2) != 0 {
		t.Fatalf("want lock 0 job but get %d", len(jobs2))
	}
}

func testResolve(t *testing.T, resolve func(jb que.Job) error) {
	q := newQueue()
	mu := q.Mutex()
	qs := randQueue()
	_, err := q.Enqueue(context.Background(), nil, que.Plan{
		Queue: qs,
		Args:  que.Args(),
		RunAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs1, err := mu.Lock(context.Background(), qs, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs1) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs1))
	}
	jb1 := jobs1[0]
	err = resolve(jb1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = q.Enqueue(context.Background(), nil, que.Plan{
		Queue: qs,
		Args:  que.Args(),
		RunAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs2, err := mu.Lock(context.Background(), qs, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs2) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs2))
	}
	jb2 := jobs2[0]
	dbTx(t, false, func(tx *sql.Tx) {
		jb2.In(tx)
		err2 := resolve(jb2)
		if err2 != nil {
			t.Fatal(err2)
		}
	})

	_, err = q.Enqueue(context.Background(), nil, que.Plan{
		Queue: qs,
		RunAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs3, err := mu.Lock(context.Background(), qs, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs3) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs3))
	}
	jb3 := jobs3[0]
	dbTx(t, true, func(tx *sql.Tx) {
		jb3.In(tx)
		err3 := resolve(jb3)
		if err3 != nil {
			t.Fatal(err3)
		}
	})

	err = mu.Unlock(context.Background(), []int64{jb1.ID(), jb2.ID(), jb3.ID()})
	if err != nil {
		t.Fatal(err)
	}

	jobs4, err := mu.Lock(context.Background(), qs, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs4) != 1 {
		t.Fatalf("want lock 1 rollback job(%d)", jb3.ID())
	}
	jb4 := jobs4[0]
	if jb4.ID() != jb3.ID() {
		t.Fatalf("rollback job(%d) should lock but get %v", jb3.ID(), jb4.ID())
	}
}

func TestDone(t *testing.T) {
	testResolve(t, func(jb que.Job) error {
		return jb.Done(context.Background())
	})
}
func TestDestroy(t *testing.T) {
	testResolve(t, func(jb que.Job) error {
		return jb.Destroy(context.Background())
	})
}
func TestExpire(t *testing.T) {
	testResolve(t, func(jb que.Job) error {
		return jb.Expire(context.Background(), nil)
	})
}

func TestRetryAfter(t *testing.T) {
	testResolve(t, func(jb que.Job) error {
		return jb.RetryAfter(context.Background(), 10*time.Second, nil)
	})
}

func TestRetryAfterThenWaitUntilGet(t *testing.T) {
	q := newQueue()
	mu := q.Mutex()
	qs := randQueue()
	_, err := q.Enqueue(context.Background(), nil, que.Plan{
		Queue: qs,
		Args:  que.Args(),
		RunAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs1, err := mu.Lock(context.Background(), qs, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs1) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs1))
	}
	jb1 := jobs1[0]

	err = jb1.RetryAfter(context.Background(), 1*time.Second, errors.New("test retry"))
	if err != nil {
		t.Fatal(err)
	}
	err = mu.Unlock(context.Background(), []int64{jb1.ID()})
	if err != nil {
		t.Fatal(err)
	}
	jobs2, err := mu.Lock(context.Background(), qs, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs2) != 0 {
		t.Fatalf("want lock 0 job but get %d", len(jobs2))
	}

	time.Sleep(time.Duration(1.1 * float64(time.Second)))
	jobs3, err := mu.Lock(context.Background(), qs, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs3) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs3))
	}
	jb3 := jobs3[0]
	if jb3.ID() != jb1.ID() {
		t.Fatalf("want job(%d) but get %d", jb1.ID(), jb3.ID())
	}
	if jb3.RetryCount() != 1 {
		t.Fatalf("want retry count is 1 but get %d", jb3.RetryCount())
	}
	if *jb3.LastErrMsg() != "test retry" {
		t.Fatalf("want LastErrMsg is %q but get %q", "test retry", *jb3.LastErrMsg())
	}
	if !strings.Contains(*jb3.LastErrStack(), "queue_test.go") {
		t.Fatalf("want last err stack contains %s but get %s", "queue_test.go", *jb3.LastErrStack())
	}
}

func enqueue(q que.Queue, qs string, uid *string, ulf que.UniqueLifecycle) error {
	_, err := q.Enqueue(context.Background(), nil, que.Plan{
		Queue:           qs,
		RunAt:           time.Now(),
		UniqueID:        uid,
		UniqueLifecycle: ulf,
	})
	return err
}

func TestUniqueIgnore(t *testing.T) {
	q := newQueue()
	qs := randQueue()

	if err := enqueue(q, qs, &qs, que.Ignore); err != nil {
		t.Fatal(err)
	}
	if err := enqueue(q, qs, &qs, que.Ignore); err != nil {
		t.Fatal(err)
	}
}

func testUnique(t *testing.T, ulf que.UniqueLifecycle, unique bool, resolve func(job que.Job)) {
	q := newQueue()
	qs := randQueue()
	mu := q.Mutex()

	if err := enqueue(q, qs, &qs, ulf); err != nil {
		t.Fatal(err)
	}
	err := enqueue(q, qs, &qs, ulf)
	if err == nil || !errors.Is(err, que.ErrViolateUniqueConstraint) {
		t.Fatalf("want err is que.ErrViolateUniqueConstraint but get %v", err)
	}
	jobs, err := mu.Lock(context.Background(), qs, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("want length of jobs is 1 but get %d", len(jobs))
	}
	resolve(jobs[0])
	err = enqueue(q, qs, &qs, ulf)
	if unique {
		if err == nil || !errors.Is(err, que.ErrViolateUniqueConstraint) {
			t.Fatalf("want err is que.ErrViolateUniqueConstraint but get %v", err)
		}
	} else {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func testUniqueAlways(t *testing.T, resolve func(job que.Job)) {
	testUnique(t, que.Always, true, resolve)
}

func TestUniqueAlwaysDestroy(t *testing.T) {
	testUniqueAlways(t, func(job que.Job) {
		if err := job.Destroy(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUniqueAlwaysDone(t *testing.T) {
	testUniqueAlways(t, func(job que.Job) {
		if err := job.Done(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUniqueAlwaysExpire(t *testing.T) {
	testUniqueAlways(t, func(job que.Job) {
		if err := job.Expire(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUniqueAlwaysRetryAfter(t *testing.T) {
	testUniqueAlways(t, func(job que.Job) {
		if err := job.RetryAfter(context.Background(), 10*time.Second, nil); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUniqueAlwaysRetryInPlan(t *testing.T) {
	testUniqueAlways(t, func(job que.Job) {
		if err := job.RetryInPlan(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
	})
}

func testUniqueDone(t *testing.T, unique bool, resolve func(job que.Job)) {
	testUnique(t, que.Done, unique, resolve)
}

func TestUniqueDoneDestroy(t *testing.T) {
	testUniqueDone(t, true, func(job que.Job) {
		if err := job.Destroy(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUniqueDoneDone(t *testing.T) {
	testUniqueDone(t, true, func(job que.Job) {
		if err := job.Done(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUniqueDoneRetryAfter(t *testing.T) {
	testUniqueDone(t, true, func(job que.Job) {
		if err := job.RetryAfter(context.Background(), 10*time.Second, nil); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUniqueDoneExpire(t *testing.T) {
	testUniqueDone(t, false, func(job que.Job) {
		if err := job.Expire(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
	})
}

func testUniqueLockable(t *testing.T, unique bool, resolve func(job que.Job)) {
	testUnique(t, que.Lockable, unique, resolve)
}

func TestUniqueLockableRetryAfter(t *testing.T) {
	testUniqueLockable(t, true, func(job que.Job) {
		if err := job.RetryAfter(context.Background(), 10*time.Second, nil); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUniqueLockableDone(t *testing.T) {
	testUniqueLockable(t, false, func(job que.Job) {
		if err := job.Done(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUniqueLockableDestroy(t *testing.T) {
	testUniqueLockable(t, false, func(job que.Job) {
		if err := job.Destroy(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUniqueLockableExpire(t *testing.T) {
	testUniqueLockable(t, false, func(job que.Job) {
		if err := job.Expire(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUniqueLockableRetryInPlan(t *testing.T) {
	testUniqueLockable(t, false, func(job que.Job) {
		if err := job.RetryInPlan(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
	})
}

func TestParallelLock(t *testing.T) {
	q := newQueue()
	qs := randQueue()

	var wantCount = 1 << 8
	var waitGroup sync.WaitGroup
	var goJobs [5][]que.Job
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < len(goJobs); i++ {
		go func(i int) {
			mu := q.Mutex()
			waitGroup.Add(1)
			for {
				select {
				case <-ctx.Done():
					waitGroup.Done()
					return
				default:
					jobs, err := mu.Lock(context.Background(), qs, 1<<4)
					if err != nil {
						t.Error(err)
					}
					goJobs[i] = append(goJobs[i], jobs...)
					var ids []int64
					for _, job := range jobs {
						ids = append(ids, job.ID())
						if err = job.Done(context.Background()); err != nil {
							t.Error(err)
						}
					}
					err = mu.Unlock(context.Background(), ids)
					if err != nil {
						t.Error(err)
					}
				}
			}
		}(i)
	}

	for i := 0; i < wantCount; i++ {
		_, err := q.Enqueue(context.Background(), nil, que.Plan{
			Queue: qs,
			Args:  que.Args(),
			RunAt: time.Now(),
		})
		if err != nil {
			t.Error(err)
		}
	}

	time.Sleep(1 * time.Second)
	cancel()
	waitGroup.Wait()
	var lockCount int
	for i := 0; i < len(goJobs); i++ {
		lockCount += len(goJobs[i])
	}
	if lockCount != wantCount {
		t.Fatalf("want lock count is %d but get %d", wantCount, lockCount)
	}
}

func dbTx(t *testing.T, rollback bool, f func(*sql.Tx)) {
	tx, err := _db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if rollback {
			err2 := tx.Rollback()
			if err != nil {
				t.Fatal(err2)
			}
		} else {
			err2 := tx.Commit()
			if err2 != nil {
				t.Fatal(err2)
			}
		}
	}()
	f(tx)
}

var dbOnce sync.Once
var _db *sql.DB
var _driver string

func newQueue() que.Queue {
	dbOnce.Do(func() {
		_driver = os.Getenv("QUE_DB_DRIVER")
		source := os.Getenv("QUE_DB_SOURCE")
		switch _driver {
		case "postgres":
			var err error
			_db, err = sql.Open(_driver, source)
			if err != nil {
				panic(err)
			}
			err = _db.Ping()
			if err != nil {
				panic(err)
			}
		default:
			panic(fmt.Sprintf("unsupported driver: %q", _driver))
		}
	})

	switch _driver {
	case "postgres":
		q, err := pg.New(_db)
		if err != nil {
			panic(err)
		}
		return q
	default:
		panic("already check supported driver in dbOnce")
	}
}

func randQueue() string {
	const bufLen = 10
	var buf [bufLen]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		panic(err)
	}
	var q [3 + 2*bufLen]byte
	q[0] = 't'
	q[1] = 't'
	q[2] = '-'
	hex.Encode(q[3:], buf[:])
	return string(q[:])
}
