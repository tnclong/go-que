package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
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

	for i := 0; i < 100; i++ {
		id, err := q.Enqueue(context.Background(), nil, time.Now(), "hello", i)
		fmt.Println(id, err)
	}

}
