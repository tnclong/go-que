package scheduler

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tnclong/go-que"
)

var wantSchedule = Schedule{
	"hello": Item{
		Queue:          "que.test.hello",
		Args:           `[1]`,
		Cron:           "* * * * *",
		RecoveryPolicy: "ignore",
	},
	"orderWeekReport": Item{
		Queue:          "que.test.order.report.week",
		Args:           `["1", "2", "3"]`,
		Cron:           "0 8 * * 1",
		RecoveryPolicy: "reparation",
		RetryPolicy: que.RetryPolicy{
			InitialInterval:        20 * time.Second,
			MaxInterval:            time.Minute,
			NextIntervalMultiplier: 2,
			IntervalRandomPercent:  10,
			MaxRetryCount:          5,
		},
	},
}

func TestProvide(t *testing.T) {
	r := strings.NewReader(`hello:
  queue: "que.test.hello"
  args: >-
          [1]
  cron: "* * * * *"
  recoveryPolicy: "ignore"
orderWeekReport:
  queue: "que.test.order.report.week"
  args: >-
          ["1", "2", "3"]
  cron: "0 8 * * 1"
  recoveryPolicy: "reparation"
  retryPolicy:
    initialInterval: 20s
    maxInterval: 1m
    nextIntervalMultiplier: 2
    intervalRandomPercent: 10
    maxRetryCount: 5
`)
	schedule, err := Provide(r)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wantSchedule, schedule) {
		t.Fatalf("want %#v but get %#v", wantSchedule, schedule)
	}
}
