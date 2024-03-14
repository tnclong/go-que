package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/tnclong/go-que"
	"github.com/tnclong/go-que/pg"
)

func main() {
	db, err := sql.Open("postgres", "postgres://myuser:mypassword@127.0.0.1:5432/mydb?sslmode=disable")
	if err != nil {
		panic(err)
	}

	db.SetConnMaxLifetime(100 * time.Second)
	db.SetMaxOpenConns(80)
	db.SetMaxIdleConns(50)

	q, err := pg.New(db, "queue1")
	if err != nil {
		panic(err)
	}

	worker, err := que.NewWorker(que.WorkerOptions{
		Queue:              q,
		MaxLockPerSecond:   20,
		MaxBufferJobsCount: 200,
		Perform: func(ctx context.Context, job que.Job) error {
			args := job.Args()
			fmt.Println("started job id:", job.ID(), "args:", string(args))
			time.Sleep(1 * time.Second)
			err := job.Done(ctx)
			fmt.Println("end job id:", job.ID(), "args:", string(args))
			return err
		},
		MaxPerformPerSecond:       2000,
		MaxConcurrentPerformCount: 100,
	})

	if err != nil {
		panic(err)
	}

	err = worker.Run()

	if err != nil {
		panic(err)
	}

}
