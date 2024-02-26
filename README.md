## Que
#### A golang background job queue that uses optimized database lock for reliability and speed.

There are benefits of using Que to route jobs:

  * **Transaction** - Controls job along with other changes to your database optional in a transaction.
  * **Simplify Architecture** - If you're already using database, a separate queue is another moving part that can break.
  * **Safety** - If a golang process dies, the jobs it's working won't be lost, or left in a locked or ambiguous state - they immediately become available for any other worker to pick up according to RetryPolicy.
  * **Concurrency** - Workers don't block each other. This allows for high throughput with a large number of workers.
  * **Uniqueness** - Controls the uniqueness of jobs in same queue.
  * **Scheduler** - Schedule jobs uses cron expression.
  * **Customization** - Database let you easy to customizate queue suit for your business requirement.

## Install

```bash
go get github.com/theplant/go-que
```

## Doc

https://godoc.org/github.com/theplant/go-que

## Quickstart

Start databases:

```bash
docker-compose up
```

If you use PostgreSQL:

```bash
# Migrate database by your favorite tool on production
docker exec -i go-que_postgres_1 psql -U myuser -d mydb < pg/schema.sql
source pg_env
```

Install dependences:

```bash
go get -v ./...
```

Run test:

```bash
go test ./...
```

Example:

example_test.go

## Benchmark

The purpose of benchmark:

  - Guide performace improvement.
  - Obtain a real WorkerOptions on production with accepted database resource wastage.

Run bm:

```bash
cd bm
# Adjust bm.yaml to simulate your scene.
go run main.go -f bm.yaml
```

Output:

```
using queue "bm-e8b62c5d67bbb33e6771"
                      +-------------------------------------------------
                      |                Duration (ms)                   |
+---------+-----------+------------------------------------------------+
| Method  |    RPS    |  avg   min   p25   p50   p75   p90   p99   max |
+---------+-----------+------------------------------------------------+
| Enqueue |   854.7   |   12     2     6     8    14    23    53   144 |
| Perform |   847.7   |  241    19   105   168   311   566   786   877 |
```
