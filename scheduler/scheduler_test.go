package scheduler

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/tnclong/go-que"
)

func TestDecodeArgs(t *testing.T) {
	argsData := mustMarshal(map[string]string{})
	_, err := decodeArgs(argsData)
	if err == nil {
		t.Fatal("want a err when decode a json object")
	}

	now := time.Now()
	nowFunc = func() time.Time {
		return now
	}
	argsData = mustMarshal([]interface{}{})
	args, err := decodeArgs(argsData)
	if err != nil {
		t.Fatal(err)
	}
	if !args.lastRunAt.Equal(now) {
		t.Fatalf("want last run at %v but get %v", now, args.lastRunAt)
	}
	if len(args.names) != 0 {
		t.Fatalf("want a length of names is 0 but get %d", len(args.names))
	}

	argsData = mustMarshal([]interface{}{now, "q1", "q2"})
	args, err = decodeArgs(argsData)
	if err != nil {
		t.Fatal(err)
	}
	if !args.lastRunAt.Equal(now) {
		t.Fatalf("want last run at %v but get %v", now, args.lastRunAt)
	}
	if !args.contains("q1") || !args.contains("q2") {
		t.Fatalf("want names %+v contains q1 and q2", args.names)
	}
	if args.contains("q3") {
		t.Fatalf("want names %+v NOT contains q3", args.names)
	}
}

func mustMarshal(e interface{}) []byte {
	data, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	return data
}

func TestCalculate(t *testing.T) {
	now := mustParseTime("2020-02-16T19:14:45+08:00")
	lastRunAt := mustParseTime("2020-02-16T19:11:45+08:00")
	schedule := Schedule{
		"exists.ignore": Item{
			Queue:          "exists.ignore",
			Args:           `[1]`,
			Cron:           "* * * * *",
			RecoveryPolicy: "ignore",
		},
		"exists.reparation": Item{
			Queue:          "exists.reparation",
			Args:           `["2"]`,
			Cron:           "* * * * *",
			RecoveryPolicy: "reparation",
		},
		"not-exists.reparation": Item{
			Queue:          "not-exists.reparation",
			Args:           `[]`,
			Cron:           "* * * * *",
			RecoveryPolicy: "reparation",
		},
	}

	var tcs = []struct {
		Name     string
		Schedule Schedule
		Args     args

		Plans []que.Plan
	}{
		{
			Name:     "exists.ignore",
			Schedule: schedule,
			Args: args{
				lastRunAt: lastRunAt,
				names: []string{
					"exists.ignore",
				},
			},
			Plans: []que.Plan{
				{
					Queue: "exists.ignore",
					Args:  []byte(`[1]`),
					RunAt: mustParseTime("2020-02-16T19:14:00+08:00"),
				},
			},
		},
		{
			Name:     "exists.reparation",
			Schedule: schedule,
			Args: args{
				lastRunAt: lastRunAt,
				names: []string{
					"exists.reparation",
				},
			},
			Plans: []que.Plan{
				{
					Queue: "exists.reparation",
					Args:  []byte(`["2"]`),
					RunAt: mustParseTime("2020-02-16T19:12:00+08:00"),
				},
				{
					Queue: "exists.reparation",
					Args:  []byte(`["2"]`),
					RunAt: mustParseTime("2020-02-16T19:13:00+08:00"),
				},
				{
					Queue: "exists.reparation",
					Args:  []byte(`["2"]`),
					RunAt: mustParseTime("2020-02-16T19:14:00+08:00"),
				},
			},
		},
		{
			Name:     "not-exists.reparation",
			Schedule: schedule,
			Args: args{
				lastRunAt: lastRunAt,
				names:     []string{},
			},
			Plans: nil,
		},
	}

	for _, tc := range tcs {
		plans := calculate(tc.Schedule, now, tc.Args)
		if !reflect.DeepEqual(plans, tc.Plans) {
			t.Fatalf("%s: want\n%+v but get\n%+v", tc.Name, tc.Plans, plans)
		}
	}
}

func mustParseTime(str string) time.Time {
	d, err := time.Parse(time.RFC3339, str)
	if err != nil {
		panic(err)
	}
	return d
}
