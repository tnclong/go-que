package que_test

import (
	"context"
	"fmt"
	"time"

	"github.com/tnclong/go-que"
)

func ExampleWorker() {
	q := newQueue()
	mutex := q.Mutex()
	qs := randQueue()

	performDone := make(chan struct{}, 1)
	var performCount int
	worker, err := que.NewWorker(que.WorkerOptions{
		Queue:              qs,
		Mutex:              mutex,
		MaxLockPerSecond:   10,
		MaxBufferJobsCount: 0,

		Perform: func(ctx context.Context, job que.Job) error {
			performCount++
			retryCount := job.RetryCount()
			if retryCount < 2 { // panic until perform 3rd times.
				fmt.Printf("performCount: %d; retryCount: %d; panic\n", performCount, retryCount)
				panic(performCount)
			}
			defer func() {
				performDone <- struct{}{}
			}()
			fmt.Printf("performCount: %d; retryCount: %d; Done\n", performCount, retryCount)
			return job.Done(ctx)
		},
		MaxPerformPerSecond:       2,
		MaxConcurrentPerformCount: 1,
	})
	if err != nil {
		panic(err)
	}
	go func() {
		err := worker.Run()
		fmt.Println("Run():", err.Error())
	}()

	_, err = q.Enqueue(context.Background(), nil, que.Plan{
		Queue: qs,
		Args:  que.Args(1, 2, 3),
		RunAt: time.Now(),
		RetryPolicy: que.RetryPolicy{
			InitialInterval:        1 * time.Second,
			NextIntervalMultiplier: 1,
			IntervalRandomPercent:  0,
			// MaxRetryCount greater than panic count.
			MaxRetryCount: 3,
		},
	})
	if err != nil {
		panic(err)
	}

	<-performDone
	err = worker.Stop(context.Background())
	fmt.Println("Stop():", err)
	fmt.Println("Release():", mutex.Release())
	// Output:
	// performCount: 1; retryCount: 0; panic
	// performCount: 2; retryCount: 1; panic
	// performCount: 3; retryCount: 2; Done
	// Run(): worker stoped
	// Stop(): <nil>
	// Release(): <nil>
}
