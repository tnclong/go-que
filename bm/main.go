package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"sort"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/tnclong/go-que"
	"github.com/tnclong/go-que/pg"
	"gopkg.in/yaml.v2"
)

func main() {
	parseFlags()
	b := &Benchmark{Queue: randQueue()}
	b.Configure(*cfile)

	go func() {
		runtime.SetMutexProfileFraction(1)
		port := ":6060"
		fmt.Println("pprof ListenAndServe:", port)
		fmt.Println(http.ListenAndServe(port, nil))
	}()

	b.Run()
	b.Summary()
}

type Benchmark struct {
	Queue   string `yaml:"-"`
	Mutex   que.Mutex
	DB      DB      `yaml:"db"`
	Enqueue Enqueue `yaml:"enqueue"`
	Worker  Worker  `yaml:"worker"`

	enqueueStartAt time.Time
	enqueueEndAt   time.Time
	enqueuePoints  []Latency `yaml:"-"`

	performStartAt time.Time
	performEndAt   time.Time
	performPoints  []Latency `yaml:"-"`
}

type DB struct {
	Driver string `yaml:"driver"`
	Source string `yaml:"source"`

	ConnMaxLifetime time.Duration `yaml:"connMaxLifetime"`
	MaxOpenConns    int           `yaml:"maxOpenConns"`
	MaxIdleConns    int           `yaml:"maxIdleConns"`
}

type Enqueue struct {
	Concurrent   int `yaml:"concurrent"`
	Count        int `yaml:"count"`
	ArgsByteSize int `yaml:"argsByteSize"`
}

type Worker struct {
	MaxLockPerSecond          float64 `yaml:"maxLockPerSecond"`
	MaxBufferJobsCount        int     `yaml:"maxBufferJobsCount"`
	MaxPerformPerSecond       float64 `yaml:"maxPerformPerSecond"`
	MaxConcurrentPerformCount int     `yaml:"maxConcurrentPerformCount"`
}

type Latency struct {
	Timestamp time.Time
	Duration  time.Duration
}

func metric(result chan<- Latency, f func()) {
	metricNow(time.Now(), result, f)
}

func metricNow(now time.Time, result chan<- Latency, f func()) {
	f()
	result <- Latency{Timestamp: now, Duration: time.Now().Sub(now)}
}

func (b *Benchmark) Configure(file string) {
	in, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalln(err)
	}
	err = yaml.UnmarshalStrict(in, b)
	if err != nil {
		log.Fatalf("parse config file %s with err: %v", file, err)
	}
}

func (b *Benchmark) openDB() *sql.DB {
	db, err := sql.Open(b.DB.Driver, b.DB.Source)
	if err != nil {
		log.Fatalf("sql.Open(driver=%s, source=%s) failed with err: %v", b.DB.Driver, b.DB.Source, err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatalf("db.Ping(driver=%s, source=%s) failed with err: %v", b.DB.Driver, b.DB.Source, err)
	}
	db.SetConnMaxLifetime(b.DB.ConnMaxLifetime)
	db.SetMaxOpenConns(b.DB.MaxOpenConns)
	db.SetMaxIdleConns(b.DB.MaxIdleConns)
	return db
}

func (b *Benchmark) newQueue(db *sql.DB) que.Queue {
	q, err := pg.New(db)
	if err != nil {
		log.Fatalf("pg.New() with err: %v", err)
	}
	b.Mutex = q.Mutex()
	b.Queue = randQueue()
	fmt.Printf("using queue %q\n", b.Queue)
	return q
}

func (b *Benchmark) enqueue(q que.Queue, enqueueC <-chan int, metricC chan<- Latency) {
	args := randArgs(b.Enqueue.ArgsByteSize)
	for i := 0; i < b.Enqueue.Concurrent; i++ {
		go func() {
			for {
				_, ok := <-enqueueC
				if !ok {
					return
				}
				metric(metricC, func() {
					_, err := q.Enqueue(context.Background(), nil, que.Plan{
						Queue: b.Queue,
						RunAt: time.Now(),
						Args:  que.Args(args),
					})
					if err != nil {
						log.Fatalf("Enqueue with err: %v", err)
					}
				})
			}
		}()
	}
}

var performFirst sync.Once

func (b *Benchmark) runWorker(q que.Queue, metricC chan<- Latency) *que.Worker {
	worker, err := que.NewWorker(que.WorkerOptions{
		Queue:              b.Queue,
		Mutex:              b.Mutex,
		MaxLockPerSecond:   b.Worker.MaxLockPerSecond,
		MaxBufferJobsCount: b.Worker.MaxBufferJobsCount,
		Perform: func(ctx context.Context, job que.Job) error {
			performFirst.Do(func() {
				b.performStartAt = time.Now()
			})

			var err error
			metricNow(job.Plan().RunAt, metricC, func() {
				err = job.Destroy(ctx)
			})
			return err
		},
		MaxPerformPerSecond:       b.Worker.MaxPerformPerSecond,
		MaxConcurrentPerformCount: b.Worker.MaxConcurrentPerformCount,
	})
	if err != nil {
		log.Fatalf("NewWorker with err: %v", err)
	}
	go func() {
		err = worker.Run()
		if err != nil && err != que.ErrWorkerStoped {
			log.Fatalf("worker.Run returns err: %v", err)
		}
	}()
	return worker
}

func (b *Benchmark) Run() {
	db := b.openDB()
	q := b.newQueue(db)

	var count = b.Enqueue.Count

	metricPerformC := make(chan Latency, count)
	metricPerformDone := make(chan time.Time, 1)
	worker := b.runWorker(q, metricPerformC)
	go func(done chan<- time.Time) {
		for i := 0; i < count; i++ {
			lat := <-metricPerformC
			b.performPoints = append(b.performPoints, lat)
		}
		done <- time.Now()
	}(metricPerformDone)

	enqueueC := make(chan int, b.Enqueue.Concurrent)
	metricEnqueueC := make(chan Latency, count)
	b.enqueue(q, enqueueC, metricEnqueueC)
	metricEnqueueDone := make(chan time.Time, 1)
	go func() {
		b.enqueueStartAt = time.Now()
		for i := 0; i < count; i++ {
			enqueueC <- i
		}
		close(enqueueC)
	}()
	go func(done chan<- time.Time) {
		for i := 0; i < count; i++ {
			lat := <-metricEnqueueC
			b.enqueuePoints = append(b.enqueuePoints, lat)
		}
		done <- time.Now()
	}(metricEnqueueDone)

	enqueueEndAt := <-metricEnqueueDone
	performEndAt := <-metricPerformDone
	b.enqueueEndAt = enqueueEndAt
	b.performEndAt = performEndAt

	err := worker.Stop(context.Background())
	if err != nil {
		log.Fatalf("Stop worker with err: %v", err)
	}
	err = b.Mutex.Release()
	if err != nil {
		log.Fatalf("Release mutex with err: %v", err)
	}
}

func (b *Benchmark) Summary() {
	enqueRPS := float64(b.Enqueue.Count) / b.enqueueEndAt.Sub(b.enqueueStartAt).Seconds()
	performRPS := float64(b.Enqueue.Count) / b.performEndAt.Sub(b.performStartAt).Seconds()
	enqueueStat := b.calculateStat(b.enqueuePoints)
	performStat := b.calculateStat(b.performPoints)
	b.printHeader()
	b.printStat("Enqueue", enqueRPS, enqueueStat)
	b.printStat("Perform", performRPS, performStat)
}

func (b *Benchmark) calculateStat(points []Latency) map[string]float64 {
	stat := make(map[string]float64)
	sort.Sort(sortByDuration(points))
	var totalDuration time.Duration
	for _, point := range points {
		totalDuration += point.Duration
	}
	stat["avg"] = float64(totalDuration) / float64(len(points)) / 1000000
	stat["min"] = float64(points[0].Duration.Nanoseconds()) / 1000000
	stat["max"] = float64(points[len(points)-1].Duration.Nanoseconds()) / 1000000
	stat["p25"] = float64(points[int(float64(len(points)-1)*float64(0.25))].Duration.Nanoseconds()) / 1000000
	stat["p50"] = float64(points[int(float64(len(points)-1)*float64(0.5))].Duration.Nanoseconds()) / 1000000
	stat["p75"] = float64(points[int(float64(len(points)-1)*float64(0.75))].Duration.Nanoseconds()) / 1000000
	stat["p90"] = float64(points[int(float64(len(points)-1)*float64(0.90))].Duration.Nanoseconds()) / 1000000
	stat["p99"] = float64(points[int(float64(len(points)-1)*float64(0.99))].Duration.Nanoseconds()) / 1000000
	return stat
}

func (b *Benchmark) printStat(method string, rps float64, stat map[string]float64) {
	fmt.Printf("| %7s | \033[1;31m%7.1f \033[0m  |%5.0f %5.0f %5.0f %5.0f %5.0f %5.0f %5.0f %5.0f |\n",
		method, rps,
		stat["avg"], stat["min"], stat["p25"], stat["p50"], stat["p75"], stat["p90"], stat["p99"], stat["max"])
}

type sortByDuration []Latency

func (a sortByDuration) Len() int           { return len(a) }
func (a sortByDuration) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sortByDuration) Less(i, j int) bool { return a[i].Duration < a[j].Duration }

func (b *Benchmark) printHeader() {
	fmt.Println("                      +-------------------------------------------------")
	fmt.Println("                      |                Duration (ms)                   |")
	fmt.Println("+---------+-----------+------------------------------------------------+")
	fmt.Println("| Method  |    RPS    |  avg   min   p25   p50   p75   p90   p99   max |")
	fmt.Println("+---------+-----------+------------------------------------------------+")
}

var cfile = flag.String("f", "bm.yaml", "configure file path")

const cfileExample = `db: # ref: https://golang.org/pkg/database/sql/#DB
  driver: postgres # postgres
  source: postgres://myuser:mypassword@127.0.0.1:5432/mydb?sslmode=disable
  connMaxLifetime: 100s
  maxOpenConns: 80
  maxIdleConns: 50
enqueue:
  concurrent: 40
  count: 10000
  argsByteSize: 50
worker: # see WorkerOptions
  maxLockPerSecond: 20
  maxBufferJobsCount: 200
  maxPerformPerSecond: 2000
  maxConcurrentPerformCount: 100`

func parseFlags() {
	usage := flag.Usage
	flag.Usage = func() {
		usage()
		fmt.Fprintf(flag.CommandLine.Output(), "\nconfigure example:\n\n%s\n\n", cfileExample)
	}
	flag.Parse()
}

func randQueue() string {
	const bufLen = 10
	var buf [bufLen]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		log.Fatalf("rand queue with err: %v", err)
	}
	var q [3 + 2*bufLen]byte
	q[0] = 'b'
	q[1] = 'm'
	q[2] = '-'
	hex.Encode(q[3:], buf[:])
	return string(q[:])
}

func randArgs(size int) []byte {
	if size <= 0 {
		return nil
	}
	if size%2 == 1 {
		size++
	}
	buf := make([]byte, size/2)
	_, err := rand.Read(buf)
	if err != nil {
		log.Fatalf("rand args with err: %v", err)
	}
	data, err := json.Marshal([]interface{}{hex.EncodeToString(buf)})
	if err != nil {
		log.Fatalf("marshal args with err: %v", err)
	}
	return data
}
