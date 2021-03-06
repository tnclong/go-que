package que

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// NewWorker creates a worker instance by given opts.
// The Queue, Mutex and Perform of opts are required.
// Other options is set to a default value if they are invalid.
func NewWorker(opts WorkerOptions) (*Worker, error) {
	if opts.Queue == "" {
		return nil, errors.New("queue must not be empty string")
	}
	if opts.Mutex == nil {
		return nil, errors.New("mutex must not be nil")
	}
	if opts.Perform == nil {
		return nil, errors.New("perform must not be nil")
	}
	if opts.MaxLockPerSecond <= 0 {
		opts.MaxLockPerSecond = 1
	}
	if opts.MaxPerformPerSecond <= 0 {
		opts.MaxPerformPerSecond = 1
	}
	if opts.MaxConcurrentPerformCount <= 0 {
		opts.MaxConcurrentPerformCount = 1
	}
	if opts.MaxBufferJobsCount < 0 {
		opts.MaxBufferJobsCount = 0
	}
	performContext := opts.PerformContext
	if performContext == nil {
		performContext = context.Background()
	}
	var count = opts.MaxConcurrentPerformCount + opts.MaxBufferJobsCount

	return &Worker{
		queue:                     opts.Queue,
		mutex:                     opts.Mutex,
		performContext:            performContext,
		perform:                   opts.Perform,
		maxConcurrentPerformCount: opts.MaxConcurrentPerformCount,
		lockLimiter:               newRateLimiter(opts.MaxLockPerSecond),
		jobC:                      make(chan Job, count),
		performLimiter:            newRateLimiter(opts.MaxPerformPerSecond),
		processed:                 make([]int64, 0, count),
	}, nil
}

func newRateLimiter(perSecond float64) *rate.Limiter {
	return rate.NewLimiter(rate.Limit(perSecond), 1)
}

// WorkerOptions is used for new a worker.
type WorkerOptions struct {
	// A queue instance must only or not assign to a worker.
	// When work stoped, inner queue is closed.
	Queue string
	Mutex Mutex
	// MaxLockPerSecond is maximum frequency calls Lock() of Queue.
	// Lower number uses lower database cpu.
	MaxLockPerSecond float64
	// MaxBufferJobsCount is maximum of jobs in chan that can't find
	// a goroutine execute it.
	MaxBufferJobsCount int

	// Perform is user special logic function.
	// context.Context is PerformContext.
	//
	// One of Done, Destroy and Expire method of Job must be called
	// in Perform when execute successfully, if not, Perform will be executed forever.
	//
	// When Perform panic or returns an error, RetryIn will be auto executed according to PerformRetryPolicy.
	// When PerformRetryPolicy says stop retry, Expire will be executed.
	Perform        func(context.Context, Job) error
	PerformContext context.Context
	// MaxPerformPerSecond is maximum frequency of Perform execution.
	MaxPerformPerSecond float64
	// MaxConcurrentPerformCount is maximum goroutine of Perform execution.
	MaxConcurrentPerformCount int
}

// Worker locks jobs from Queue and executes Perform method according to given WorkerOptions.
//
// Run a worker:
//
//    mutex := q.Mutex()
//    w, err := que.NewWorker(que.WorkerOptions{
//        Queue: "order.mail.register",
//        Mutex: mutex,
//        Perform: func(ctx context.Context, job que.Job) error {
//            plan := job.Plan()
//            args := decode(plan.Args)
//            fmt.Println(args)
//            // one of Done, Destroy, Expire, RetryAfter and RetryInPlan must be called.
//            return job.Done(ctx)
//        },
//    })
//    if err := w.Run(); err != nil {
//        log.Println(err)
//    }
//
// Reasonable quit worker execution:
//
//    if err := w.Stop(context.Background()); err != nil {
//        log.Println(err)
//    }
type Worker struct {
	queue string
	mutex Mutex

	perform        func(context.Context, Job) error
	performContext context.Context

	maxConcurrentPerformCount int
	lockLimiter               *rate.Limiter

	jobC               chan Job
	performLimiter     *rate.Limiter
	performRetryPolicy RetryPolicy

	ongoing int32

	mux       sync.Mutex
	processed []int64

	stopped int32
}

// Run acutal starts goroutine to execute Perform method and locks jobs.
// Run blocked current caller's goroutine and returns error when get unsolved error.
// When Run() returned, any call to it get ErrWorkerStoped.
func (w *Worker) Run() error {
	if w.isStopped() {
		return ErrWorkerStoped
	}

	for i := 0; i < w.maxConcurrentPerformCount; i++ {
		go w.work()
	}
	return w.lock()
}

func (w *Worker) lock() (err error) {
	defer func() {
		if !w.isStopped() {
			w.Stop(context.Background())
		}
	}()
	defer close(w.jobC)
	var jobs []Job
	for {
		w.lockLimiter.Wait(context.Background())
		if w.isStopped() {
			return ErrWorkerStoped
		}
		ongoingCount := int(atomic.LoadInt32(&w.ongoing))
		lockCount := cap(w.jobC) - len(w.jobC) - ongoingCount
		if lockCount > 0 {
			jobs, err = w.mutex.Lock(context.Background(), w.queue, lockCount)
			if err != nil {
				return err
			}
			for _, job := range jobs {
				atomic.AddInt32(&w.ongoing, 1)
				w.jobC <- job
			}
		}
		err = w.unlockProcessed()
		if err != nil {
			return err
		}
	}
}

func (w *Worker) work() {
	for {
		job, ok := <-w.jobC
		if !ok {
			return
		}
		if w.isStopped() {
			w.asProcessed(job.ID())
			continue
		}

		w.performLimiter.Wait(context.Background())
		w.tryPerform(job)
	}
}

func (w *Worker) tryPerform(job Job) {
	defer w.asProcessed(job.ID())

	var err error
	defer func() {
		e := recover()
		if e != nil {
			err = fmt.Errorf("%v", e)
		}
		if err != nil {
			err2 := w.handleErr(job, err)
			if err2 != nil {
				log.Printf(
					"que: perform(job(%v)) with err %v but handle err with a new err %v",
					job.ID(), err, err2,
				)
			}
		}
	}()
	err = w.perform(w.performContext, job)
}

func (w *Worker) handleErr(job Job, cerr error) error {
	return job.RetryInPlan(context.Background(), cerr)
}

func (w *Worker) asProcessed(id int64) {
	w.mux.Lock()
	w.processed = append(w.processed, id)
	w.mux.Unlock()
	atomic.AddInt32(&w.ongoing, -1)
}

func (w *Worker) unlockProcessed() error {
	var processed []int64
	w.mux.Lock()
	if len(w.processed) > 0 {
		processed = w.processed
		w.processed = make([]int64, 0, cap(processed))
	}
	w.mux.Unlock()
	var err error
	if len(processed) > 0 {
		err = w.mutex.Unlock(context.Background(), processed)
	}
	return err
}

func (w *Worker) isStopped() bool {
	return atomic.LoadInt32(&w.stopped) == 1
}

// Stop stops worker execution.
// It blocks until ctx.Done() or all processing jobs done.
func (w *Worker) Stop(ctx context.Context) error {
	atomic.StoreInt32(&w.stopped, 1)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
wait:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if atomic.LoadInt32(&w.ongoing) == 0 {
				break wait
			}
		}
	}

	return w.unlockProcessed()
}
