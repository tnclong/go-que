package que_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
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

func TestQueue_Queue(t *testing.T) {
	queue := "tt-queue"
	q := newQueue(t, queue)
	if q.Queue() != queue {
		t.Fatalf("want queue is %s but get %s", queue, q.Queue())
	}
	err := q.Close()
	if err != nil {
		t.Fatalf("want Close() returns nil but get err %v", err)
	}
}

func TestQueue_EnqueueLockUnlock(t *testing.T) {
	q := newQueue(t, "")

	type user struct {
		Name string
	}
	runAt := time.Now()
	amap := map[string]int{"1": 1}
	astruct := user{Name: "name"}
	id, err := q.Enqueue(
		context.Background(), nil, runAt,
		1, 2.0, math.MaxInt64, "text", true,
		runAt, amap, astruct,
	)
	if err != nil {
		t.Fatalf("Enqueue get err: %v", err)
	}
	if id <= 0 {
		t.Fatalf("want id greater than zero but get %d", id)
	}

	jobs, err := q.Lock(context.Background(), 1)
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
	if job.Queue() != q.Queue() {
		t.Fatalf("want queue is %s but get %s", job.Queue(), q.Queue())
	}
	if !job.RunAt().Equal(runAt) {
		t.Fatalf("want run at %s but get %s", job.RunAt().String(), runAt.String())
	}
	if job.RetryCount() != 0 {
		t.Fatalf("want retry count is 0 but get %d", job.RetryCount())
	}
	if job.LastErrMsg() != "" {
		t.Fatalf("want last err msg is %q but get %q", "", job.LastErrMsg())
	}
	if job.LastErrStack() != "" {
		t.Fatalf("want last err stack is %q but get %q", "", job.LastErrStack())
	}

	argsBytes := job.Args()
	enc := json.NewDecoder(bytes.NewReader(argsBytes))
	tok, err := enc.Token()
	if err != nil {
		t.Fatal(err)
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		t.Fatalf("want a json.Delim but get %T(%v)", tok, tok)
	}
	if delim != '[' {
		t.Fatalf("want %c but get %c", '[', delim)
	}
	var intv int // 0
	if err = enc.Decode(&intv); err != nil {
		t.Fatal(err)
	}
	if intv != 1 {
		t.Fatalf("want 1 but get %d", intv)
	}
	var float64v float64 // 1
	if err = enc.Decode(&float64v); err != nil {
		t.Fatal(err)
	}
	if float64v != 2.0 {
		t.Fatalf("want 2.0 but get %f", float64v)
	}
	var int64v int64 // 2
	if err = enc.Decode(&int64v); err != nil {
		t.Fatal(err)
	}
	if int64v != math.MaxInt64 {
		t.Fatalf("want %d but get %d", math.MaxInt64, int64v)
	}
	var stringv string // 3
	if err = enc.Decode(&stringv); err != nil {
		t.Fatal(err)
	}
	if stringv != "text" {
		t.Fatalf("want %s but get %s", "test", stringv)
	}
	var boolv bool // 4
	if err = enc.Decode(&boolv); err != nil {
		t.Fatal(err)
	}
	if boolv != true {
		t.Fatalf("want true but get %v", boolv)
	}
	var timev time.Time // 5
	if err = enc.Decode(&timev); err != nil {
		t.Fatal(err)
	}
	if !timev.Equal(runAt) {
		t.Fatalf("want %s but get %s", runAt.String(), timev.String())
	}
	var mapv map[string]int // 6
	if err = enc.Decode(&mapv); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mapv, amap) {
		t.Fatalf("want %#v but get %#v", amap, mapv)
	}
	var structv user // 7
	if err = enc.Decode(&structv); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(structv, astruct) {
		t.Fatalf("want %#v but get %#v", astruct, structv)
	}
	tok, err = enc.Token()
	if err != nil {
		t.Fatal(err)
	}
	delim, ok = tok.(json.Delim)
	if !ok {
		t.Fatalf("want a json.Delim but get %T(%v)", tok, tok)
	}
	if delim != ']' {
		t.Fatalf("want %c but get %c", ']', delim)
	}
	tok, err = enc.Token()
	if tok != nil || err != io.EOF {
		t.Fatalf("want <nil>, EOF but get %v, %v", tok, err)
	}

	jobs2, err := q.Lock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs2) != 0 {
		t.Fatalf("want length of job2 is zero but get %d", len(jobs2))
	}

	err = q.Unlock(context.Background(), []int64{id})
	if err != nil {
		t.Fatal(err)
	}

	err = q.Close()
	if err != nil {
		t.Fatalf("want Close() returns nil but get err %v", err)
	}
}

func TestQueue_ErrNotLockedJobsInLocal(t *testing.T) {
	q := newQueue(t, "")
	var id int64 = 100000
	err := q.Unlock(context.Background(), []int64{id})
	if !errors.Is(err, que.ErrNotLockedJobsInLocal) {
		t.Fatalf("want err is %v but get %v", que.ErrNotLockedJobsInLocal, err)
	}
	var errQue *que.ErrQueue
	if !errors.As(err, &errQue) {
		t.Fatalf("want %T but get %T", errQue, err)
	}
	if errQue.Queue != q.Queue() {
		t.Fatalf("want queue is %s but get %s", q.Queue(), errQue.Queue)
	}
	var errUnlock *que.ErrUnlock
	if !errors.As(errQue.Err, &errUnlock) {
		t.Fatalf("want %T but get %T", errUnlock, errQue.Err)
	}
	if len(errUnlock.IDs) != 1 {
		t.Fatalf("want length of id is 1 but get %d", len(errUnlock.IDs))
	}
	aid := errUnlock.IDs[0]
	if aid != id {
		t.Fatalf("want id is %d but get %d", id, aid)
	}

	err = q.Close()
	if err != nil {
		t.Fatalf("want Close() returns nil but get err %v", err)
	}
}

func TestQueue_LockSuccessAfterUnlock(t *testing.T) {
	q := newQueue(t, "")
	id, err := q.Enqueue(context.Background(), nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	jobs1, err := q.Lock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs1) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs1))
	}
	err = q.Unlock(context.Background(), []int64{id})
	if err != nil {
		t.Fatal(err)
	}
	jobs2, err := q.Lock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs2) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs2))
	}

	err = q.Close()
	if err != nil {
		t.Fatalf("want Close() returns nil but get err %v", err)
	}
}

func TestQueue_SameQueueDiffInstance(t *testing.T) {
	// q1 locked
	q1 := newQueue(t, "")
	id, err := q1.Enqueue(context.Background(), nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	jobs1, err := q1.Lock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs1) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs1))
	}

	// q1 locked
	// q2 get nothinng
	q2 := newQueue(t, q1.Queue())
	jobs2, err := q2.Lock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs2) != 0 {
		t.Fatalf("want lock 0 job but get %d", len(jobs2))
	}

	// q1 unlock
	err = q1.Unlock(context.Background(), []int64{id})
	if err != nil {
		t.Fatal(err)
	}

	// q1 unlock, q2 get job
	jobs3, err := q2.Lock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs3) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs3))
	}

	// q1 enqueue another job
	_, err = q1.Enqueue(context.Background(), nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// q1 enqueue another job, q2 get another job.
	jobs4, err := q2.Lock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs4) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs3))
	}

	err = q1.Close()
	if err != nil {
		t.Fatalf("want Close() returns nil but get err %v", err)
	}
	err = q2.Close()
	if err != nil {
		t.Fatalf("want Close() returns nil but get err %v", err)
	}
}

func TestQueue_DiffQueue(t *testing.T) {
	// q1 enqueue
	q1 := newQueue(t, "")
	_, err := q1.Enqueue(context.Background(), nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// q2 get nothinng
	q2 := newQueue(t, "")
	jobs2, err := q2.Lock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs2) != 0 {
		t.Fatalf("want lock 0 job but get %d", len(jobs2))
	}

	err = q1.Close()
	if err != nil {
		t.Fatalf("want Close() returns nil but get err %v", err)
	}
	err = q2.Close()
	if err != nil {
		t.Fatalf("want Close() returns nil but get err %v", err)
	}
}

func TestQueue_Close(t *testing.T) {
	q1 := newQueue(t, "")
	err := q1.Close()
	if err != nil {
		t.Fatalf("want Close() returns nil but get err %v", err)
	}

	q1.Queue()

	_, err = q1.Enqueue(context.Background(), nil, time.Now())
	if !errors.Is(err, que.ErrQueueAlreadyClosed) {
		t.Fatalf("want err %v but get %v", que.ErrQueueAlreadyClosed, err)
	}

	_, err = q1.Lock(context.Background(), 1)
	if !errors.Is(err, que.ErrQueueAlreadyClosed) {
		t.Fatalf("want err %v but get %v", que.ErrQueueAlreadyClosed, err)
	}

	err = q1.Close()
	if !errors.Is(err, que.ErrQueueAlreadyClosed) {
		t.Fatalf("want err %v but get %v", que.ErrQueueAlreadyClosed, err)
	}
}

func TestQueue_LockSucessAfterClose(t *testing.T) {
	// q1 locked
	q1 := newQueue(t, "")
	_, err := q1.Enqueue(context.Background(), nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	jobs1, err := q1.Lock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs1) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs1))
	}
	err = q1.Close()
	if err != nil {
		t.Fatalf("want Close() returns nil but get err %v", err)
	}

	q2 := newQueue(t, q1.Queue())
	jobs2, err := q2.Lock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs2) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs2))
	}
	err = q2.Close()
	if err != nil {
		t.Fatalf("want Close() returns nil but get err %v", err)
	}
}

func testQueue_Resolve(t *testing.T, q que.Queue, resolve func(jb que.Job) error) {
	_, err := q.Enqueue(context.Background(), nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	jobs1, err := q.Lock(context.Background(), 1)
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

	_, err = q.Enqueue(context.Background(), nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	jobs2, err := q.Lock(context.Background(), 1)
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

	_, err = q.Enqueue(context.Background(), nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	jobs3, err := q.Lock(context.Background(), 1)
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

	err = q.Unlock(context.Background(), []int64{jb1.ID(), jb2.ID(), jb3.ID()})
	if err != nil {
		t.Fatal(err)
	}

	jobs4, err := q.Lock(context.Background(), 1)
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

	err = q.Close()
	if err != nil {
		t.Fatalf("want Close() returns nil but get err %v", err)
	}
}

func TestQueue_Done(t *testing.T) {
	testQueue_Resolve(t, newQueue(t, ""), func(jb que.Job) error {
		return jb.Done(context.Background())
	})
}
func TestQueue_Destroy(t *testing.T) {
	testQueue_Resolve(t, newQueue(t, ""), func(jb que.Job) error {
		return jb.Destroy(context.Background())
	})
}
func TestQueue_Expire(t *testing.T) {
	testQueue_Resolve(t, newQueue(t, ""), func(jb que.Job) error {
		return jb.Expire(context.Background())
	})
}

func TestQueue_RetryIn(t *testing.T) {
	testQueue_Resolve(t, newQueue(t, ""), func(jb que.Job) error {
		return jb.RetryIn(context.Background(), 10*time.Second, nil)
	})
}

func TestQueue_RetryInLock(t *testing.T) {
	q := newQueue(t, "")
	_, err := q.Enqueue(context.Background(), nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	jobs1, err := q.Lock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs1) != 1 {
		t.Fatalf("want lock 1 job but get %d", len(jobs1))
	}
	jb1 := jobs1[0]

	err = jb1.RetryIn(context.Background(), 1*time.Second, errors.New("test retry"))
	if err != nil {
		t.Fatal(err)
	}
	err = q.Unlock(context.Background(), []int64{jb1.ID()})
	if err != nil {
		t.Fatal(err)
	}
	jobs2, err := q.Lock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs2) != 0 {
		t.Fatalf("want lock 0 job but get %d", len(jobs2))
	}

	time.Sleep(1 * time.Second)
	jobs3, err := q.Lock(context.Background(), 1)
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
	if jb3.LastErrMsg() != "test retry" {
		t.Fatalf("want LastErrMsg is %q but get %q", "test retry", jb3.LastErrMsg())
	}
	if !strings.Contains(jb3.LastErrStack(), "queue_test.go") {
		t.Fatalf("want last err stack contains %s but get %s", "queue_test.go", jb3.LastErrStack())
	}

	err = q.Close()
	if err != nil {
		t.Fatalf("want Close() returns nil but get err %v", err)
	}
}

func TestCheckQueue(t *testing.T) {
	var testCases = []struct {
		queue string
		err   string
	}{
		{
			queue: "",
			err:   ": queue must not be empty string",
		},
		{
			queue: strings.Repeat("1", 101),
			err:   strings.Repeat("1", 101) + ": the length of queue greater than 100",
		},
		{
			queue: "check-queue",
			err:   "",
		},
	}
	for _, tc := range testCases {
		err := que.CheckQueue(tc.queue)
		if err == nil {
			if tc.err != "" {
				t.Fatalf("queue(%q): want err %q but get %v", tc.queue, tc.err, err)
			}
			continue
		}
		if err.Error() != tc.err {
			t.Fatalf("queue(%q): want err %q but get %q", tc.queue, tc.err, err.Error())
		}
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

func newQueue(t *testing.T, queue string) que.Queue {
	dbOnce.Do(func() {
		_driver = os.Getenv("QUE_DB_DRIVER")
		source := os.Getenv("QUE_DB_SOURCE")
		switch _driver {
		case "postgres":
			var err error
			_db, err = sql.Open(_driver, source)
			if err != nil {
				t.Fatal(err)
			}
			err = _db.Ping()
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unsupported driver: %q", _driver)
		}
	})

	if queue == "" {
		queue = randQueue(t)
		t.Log("queue:", queue)
	}
	switch _driver {
	case "postgres":
		q, err := pg.New(_db, queue)
		if err != nil {
			t.Fatal(err)
		}
		return q
	default:
		panic("already check supported driver in dbOnce")
	}
}

func randQueue(t *testing.T) string {
	const bufLen = 10
	var buf [bufLen]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		t.Fatalf("rand queue with err: %v", err)
	}
	var q [3 + 2*bufLen]byte
	q[0] = 't'
	q[1] = 't'
	q[2] = '-'
	hex.Encode(q[3:], buf[:])
	return string(q[:])
}
