package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	_ "github.com/lib/pq"
	"github.com/tnclong/go-que"
	"github.com/tnclong/go-que/mock"
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

	argsData = mustMarshal([]interface{}{now, []string{"q1", "q2"}})
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

func TestSchedulerPerform(t *testing.T) {
	tcs := []struct {
		Name string

		Provider    Provider
		NowFunc     func() time.Time
		Derivations map[string]Derivation
		MockJob     func(ctx context.Context, mj *mock.MockJob)
		MockQueue   func(ctx context.Context, mq *mock.MockQueue)
	}{
		{
			Name: "nil schedule",

			Provider: &MemProvider{
				Schedule: nil,
			},
			NowFunc: func() time.Time {
				return mustParseTime("2020-02-16T19:14:45+08:00")
			},
			Derivations: nil,
			MockJob: func(ctx context.Context, mj *mock.MockJob) {
				mj.EXPECT().Plan().Return(que.Plan{Args: que.Args(mustParseTime("2020-02-16T19:11:45+08:00"))}).Times(1)
				mj.EXPECT().In(gomock.Not(nil)).Times(1)
				mj.EXPECT().Destroy(gomock.Eq(ctx)).Return(nil).Times(1)
			},
			MockQueue: func(ctx context.Context, mq *mock.MockQueue) {
				mq.EXPECT().Enqueue(
					gomock.Eq(ctx),
					gomock.Not(nil),
					gomock.Eq(que.Plan{
						Queue:           "que.scheduler",
						Args:            que.Args(mustParseTime("2020-02-16T19:14:45+08:00"), []string{}),
						RunAt:           mustParseTime("2020-02-16T19:15:00+08:00"),
						RetryPolicy:     retryPolicy,
						UniqueID:        &uniqueID,
						UniqueLifecycle: uniqueLifecycle,
					}),
				).Return([]int64{1}, nil).Times(1)
			},
		},

		{
			Name: "exists in schedule but ignore at first time enqueue",

			Provider: &MemProvider{
				Schedule: Schedule{
					"name.enqueue.ignore": Item{
						Queue:          "que.enqueue.ignore",
						Args:           `[3]`,
						Cron:           "* * * * *",
						RecoveryPolicy: Ignore,
					},
				},
			},
			NowFunc: func() time.Time {
				return mustParseTime("2020-02-16T19:14:45+08:00")
			},
			Derivations: nil,
			MockJob: func(ctx context.Context, mj *mock.MockJob) {
				mj.EXPECT().Plan().Return(que.Plan{Args: que.Args(mustParseTime("2020-02-16T19:11:45+08:00"))}).Times(1)
				mj.EXPECT().In(gomock.Not(nil)).Times(1)
				mj.EXPECT().Destroy(gomock.Eq(ctx)).Return(nil).Times(1)
			},
			MockQueue: func(ctx context.Context, mq *mock.MockQueue) {
				mq.EXPECT().Enqueue(
					gomock.Eq(ctx),
					gomock.Not(nil),
					gomock.Eq(que.Plan{
						Queue:           "que.scheduler",
						Args:            que.Args(mustParseTime("2020-02-16T19:14:45+08:00"), []string{"name.enqueue.ignore"}),
						RunAt:           mustParseTime("2020-02-16T19:15:00+08:00"),
						RetryPolicy:     retryPolicy,
						UniqueID:        &uniqueID,
						UniqueLifecycle: uniqueLifecycle,
					}),
				).Return([]int64{1}, nil).Times(1)
			},
		},

		{
			Name: "reparative unscheduled jobs",

			Provider: &MemProvider{
				Schedule: Schedule{
					"name.recovery.reparation": Item{
						Queue:          "que.recovery.reparation",
						Args:           `["3"]`,
						Cron:           "* * * * *",
						RecoveryPolicy: Reparation,
						RetryPolicy: que.RetryPolicy{
							InitialInterval:        10 * time.Second,
							MaxInterval:            20 * time.Second,
							NextIntervalMultiplier: 1.5,
							IntervalRandomPercent:  33,
							MaxRetryCount:          3,
						},
					},
				},
			},
			NowFunc: func() time.Time {
				return mustParseTime("2020-02-16T19:14:45+08:00")
			},
			Derivations: nil,
			MockJob: func(ctx context.Context, mj *mock.MockJob) {
				mj.EXPECT().Plan().Return(que.Plan{Args: que.Args(mustParseTime("2020-02-16T19:11:45+08:00"), []string{"name.recovery.reparation"})}).Times(1)
				mj.EXPECT().In(gomock.Not(nil)).Times(1)
				mj.EXPECT().Destroy(gomock.Eq(ctx)).Return(nil).Times(1)
			},
			MockQueue: func(ctx context.Context, mq *mock.MockQueue) {
				mq.EXPECT().Enqueue(
					gomock.Eq(ctx),
					gomock.Not(nil),
					gomock.Eq(que.Plan{
						Queue: "que.recovery.reparation",
						Args:  []byte(`["3"]`),
						RunAt: mustParseTime("2020-02-16T19:12:00+08:00"),
						RetryPolicy: que.RetryPolicy{
							InitialInterval:        10 * time.Second,
							MaxInterval:            20 * time.Second,
							NextIntervalMultiplier: 1.5,
							IntervalRandomPercent:  33,
							MaxRetryCount:          3,
						},
					}),
					gomock.Eq(que.Plan{
						Queue: "que.recovery.reparation",
						Args:  []byte(`["3"]`),
						RunAt: mustParseTime("2020-02-16T19:13:00+08:00"),
						RetryPolicy: que.RetryPolicy{
							InitialInterval:        10 * time.Second,
							MaxInterval:            20 * time.Second,
							NextIntervalMultiplier: 1.5,
							IntervalRandomPercent:  33,
							MaxRetryCount:          3,
						},
					}),
					gomock.Eq(que.Plan{
						Queue: "que.recovery.reparation",
						Args:  []byte(`["3"]`),
						RunAt: mustParseTime("2020-02-16T19:14:00+08:00"),
						RetryPolicy: que.RetryPolicy{
							InitialInterval:        10 * time.Second,
							MaxInterval:            20 * time.Second,
							NextIntervalMultiplier: 1.5,
							IntervalRandomPercent:  33,
							MaxRetryCount:          3,
						},
					}),
				).Return([]int64{1, 2, 3}, nil).Times(1)
				mq.EXPECT().Enqueue(
					gomock.Eq(ctx),
					gomock.Not(nil),
					gomock.Eq(que.Plan{
						Queue:           "que.scheduler",
						Args:            que.Args(mustParseTime("2020-02-16T19:14:45+08:00"), []string{"name.recovery.reparation"}),
						RunAt:           mustParseTime("2020-02-16T19:15:00+08:00"),
						RetryPolicy:     retryPolicy,
						UniqueID:        &uniqueID,
						UniqueLifecycle: uniqueLifecycle,
					}),
				).Return([]int64{4}, nil).Times(1)
			},
		},

		{
			Name: "ignore unscheduled jobs",

			Provider: &MemProvider{
				Schedule: Schedule{
					"name.recovery.ignore": Item{
						Queue:          "que.recovery.ignore",
						Args:           `["3"]`,
						Cron:           "* * * * *",
						RecoveryPolicy: Ignore,
					},
				},
			},
			NowFunc: func() time.Time {
				return mustParseTime("2020-02-16T19:14:45+08:00")
			},
			Derivations: nil,
			MockJob: func(ctx context.Context, mj *mock.MockJob) {
				mj.EXPECT().Plan().Return(que.Plan{Args: que.Args(mustParseTime("2020-02-16T19:11:45+08:00"), []string{"name.recovery.ignore"})}).Times(1)
				mj.EXPECT().In(gomock.Not(nil)).Times(1)
				mj.EXPECT().Destroy(gomock.Eq(ctx)).Return(nil).Times(1)
			},
			MockQueue: func(ctx context.Context, mq *mock.MockQueue) {
				mq.EXPECT().Enqueue(
					gomock.Eq(ctx),
					gomock.Not(nil),
					gomock.Eq(que.Plan{
						Queue: "que.recovery.ignore",
						Args:  []byte(`["3"]`),
						RunAt: mustParseTime("2020-02-16T19:14:00+08:00"),
					}),
				).Return([]int64{1}, nil).Times(1)
				mq.EXPECT().Enqueue(
					gomock.Eq(ctx),
					gomock.Not(nil),
					gomock.Eq(que.Plan{
						Queue:           "que.scheduler",
						Args:            que.Args(mustParseTime("2020-02-16T19:14:45+08:00"), []string{"name.recovery.ignore"}),
						RunAt:           mustParseTime("2020-02-16T19:15:00+08:00"),
						RetryPolicy:     retryPolicy,
						UniqueID:        &uniqueID,
						UniqueLifecycle: uniqueLifecycle,
					}),
				).Return([]int64{2}, nil).Times(1)
			},
		},

		{
			Name: "derive zero plan",

			Provider: &MemProvider{
				Schedule: Schedule{
					"name.derive.zero": Item{
						Queue:          "que.derive.zero",
						Args:           `["3"]`,
						Cron:           "* * * * *",
						RecoveryPolicy: Ignore,
					},
				},
			},
			NowFunc: func() time.Time {
				return mustParseTime("2020-02-16T19:14:45+08:00")
			},
			Derivations: map[string]Derivation{
				"name.derive.zero": DerivationFunc(func(ctx context.Context, tx *sql.Tx, plans []que.Plan) ([]que.Plan, error) {
					return nil, nil
				}),
			},
			MockJob: func(ctx context.Context, mj *mock.MockJob) {
				mj.EXPECT().Plan().Return(que.Plan{Args: que.Args(mustParseTime("2020-02-16T19:11:45+08:00"), []string{"name.derive.zero"})}).Times(1)
				mj.EXPECT().In(gomock.Not(nil)).Times(1)
				mj.EXPECT().Destroy(gomock.Eq(ctx)).Return(nil).Times(1)
			},
			MockQueue: func(ctx context.Context, mq *mock.MockQueue) {
				mq.EXPECT().Enqueue(
					gomock.Eq(ctx),
					gomock.Not(nil),
					gomock.Eq(que.Plan{
						Queue:           "que.scheduler",
						Args:            que.Args(mustParseTime("2020-02-16T19:14:45+08:00"), []string{"name.derive.zero"}),
						RunAt:           mustParseTime("2020-02-16T19:15:00+08:00"),
						RetryPolicy:     retryPolicy,
						UniqueID:        &uniqueID,
						UniqueLifecycle: uniqueLifecycle,
					}),
				).Return([]int64{1}, nil).Times(1)
			},
		},

		{
			Name: "derive two plans",

			Provider: &MemProvider{
				Schedule: Schedule{
					"name.derive.two": Item{
						Queue:          "que.derive.two",
						Args:           `["3"]`,
						Cron:           "* * * * *",
						RecoveryPolicy: Ignore,
					},
				},
			},
			NowFunc: func() time.Time {
				return mustParseTime("2020-02-16T19:14:45+08:00")
			},
			Derivations: map[string]Derivation{
				"name.derive.two": DerivationFunc(func(ctx context.Context, tx *sql.Tx, plans []que.Plan) ([]que.Plan, error) {
					plans = append(plans, plans...)
					return plans, nil
				}),
			},
			MockJob: func(ctx context.Context, mj *mock.MockJob) {
				mj.EXPECT().Plan().Return(que.Plan{Args: que.Args(mustParseTime("2020-02-16T19:11:45+08:00"), []string{"name.derive.two"})}).Times(1)
				mj.EXPECT().In(gomock.Not(nil)).Times(1)
				mj.EXPECT().Destroy(gomock.Eq(ctx)).Return(nil).Times(1)
			},
			MockQueue: func(ctx context.Context, mq *mock.MockQueue) {
				mq.EXPECT().Enqueue(
					gomock.Eq(ctx),
					gomock.Not(nil),
					gomock.Eq(que.Plan{
						Queue: "que.derive.two",
						Args:  []byte(`["3"]`),
						RunAt: mustParseTime("2020-02-16T19:14:00+08:00"),
					}),
					gomock.Eq(que.Plan{
						Queue: "que.derive.two",
						Args:  []byte(`["3"]`),
						RunAt: mustParseTime("2020-02-16T19:14:00+08:00"),
					}),
				).Return([]int64{1, 2}, nil).Times(1)
				mq.EXPECT().Enqueue(
					gomock.Eq(ctx),
					gomock.Not(nil),
					gomock.Eq(que.Plan{
						Queue:           "que.scheduler",
						Args:            que.Args(mustParseTime("2020-02-16T19:14:45+08:00"), []string{"name.derive.two"}),
						RunAt:           mustParseTime("2020-02-16T19:15:00+08:00"),
						RetryPolicy:     retryPolicy,
						UniqueID:        &uniqueID,
						UniqueLifecycle: uniqueLifecycle,
					}),
				).Return([]int64{3}, nil).Times(1)
			},
		},
	}

	db := openDB(t)
	defer db.Close()
	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ctx := context.Background()
			mockQueue := mock.NewMockQueue(ctrl)
			tc.MockQueue(ctx, mockQueue)
			mockJob := mock.NewMockJob(ctrl)
			tc.MockJob(ctx, mockJob)

			nowFunc = tc.NowFunc
			scheduler := &Scheduler{
				DB:          db,
				Queue:       "que.scheduler",
				Enqueue:     mockQueue.Enqueue,
				Provider:    tc.Provider,
				Derivations: tc.Derivations,
			}
			if err := scheduler.Perform(ctx, mockJob); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func openDB(t *testing.T) *sql.DB {
	driver := os.Getenv("QUE_DB_DRIVER")
	source := os.Getenv("QUE_DB_SOURCE")

	db, err := sql.Open(driver, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	return db
}

func mustParseTime(str string) time.Time {
	d, err := time.Parse(time.RFC3339, str)
	if err != nil {
		panic(err)
	}
	return d
}

func TestCalculateWithInitiateTime(t *testing.T) {
	// Reference times for testing various scheduling scenarios
	earlierTime := mustParseTime("2020-02-16T19:11:45+08:00")
	midTime := mustParseTime("2020-02-16T19:12:30+08:00")
	laterTime := mustParseTime("2020-02-16T19:13:15+08:00")
	currentTime := mustParseTime("2020-02-16T19:14:45+08:00")

	cronExactTime := mustParseTime("2020-02-16T19:13:00+08:00") // Exactly at a cron execution point

	tests := []struct {
		name         string
		schedule     Schedule
		args         args
		wantPlans    int
		wantFirstRun time.Time
	}{
		{
			name: "item with InitiateTime earlier than lastRunAt",
			schedule: Schedule{
				"test1": Item{
					Queue:          "test_queue",
					Args:           `["arg1"]`,
					Cron:           "* * * * *",
					InitiateTime:   &midTime,
					RecoveryPolicy: Reparation,
				},
			},
			args: args{
				lastRunAt: laterTime,
				names:     []string{"test1"},
			},
			wantPlans:    1,
			wantFirstRun: mustParseTime("2020-02-16T19:14:00+08:00"),
		},
		{
			name: "item with InitiateTime later than lastRunAt",
			schedule: Schedule{
				"test2": Item{
					Queue:          "test_queue",
					Args:           `["arg1"]`,
					Cron:           "* * * * *",
					InitiateTime:   &laterTime,
					RecoveryPolicy: Reparation,
				},
			},
			args: args{
				lastRunAt: earlierTime,
				names:     []string{"test2"},
			},
			wantPlans:    1,
			wantFirstRun: mustParseTime("2020-02-16T19:14:00+08:00"),
		},
		{
			name: "item with nil InitiateTime",
			schedule: Schedule{
				"test3": Item{
					Queue:          "test_queue",
					Args:           `["arg1"]`,
					Cron:           "* * * * *",
					RecoveryPolicy: Reparation,
				},
			},
			args: args{
				lastRunAt: earlierTime,
				names:     []string{"test3"},
			},
			wantPlans:    3,
			wantFirstRun: mustParseTime("2020-02-16T19:12:00+08:00"),
		},
		{
			name: "item not in args but with InitiateTime",
			schedule: Schedule{
				"test4": Item{
					Queue:          "test_queue",
					Args:           `["arg1"]`,
					Cron:           "* * * * *",
					InitiateTime:   &midTime,
					RecoveryPolicy: Reparation,
				},
			},
			args: args{
				lastRunAt: earlierTime,
				names:     []string{},
			},
			wantPlans:    2,
			wantFirstRun: mustParseTime("2020-02-16T19:13:00+08:00"),
		},
		{
			name: "item not in args and without InitiateTime",
			schedule: Schedule{
				"test5": Item{
					Queue:          "test_queue",
					Args:           `["arg1"]`,
					Cron:           "* * * * *",
					RecoveryPolicy: Reparation,
				},
			},
			args: args{
				lastRunAt: earlierTime,
				names:     []string{},
			},
			wantPlans: 0,
		},
		{
			name: "item with InitiateTime exactly matching a cron time",
			schedule: Schedule{
				"test6": Item{
					Queue:          "test_queue",
					Args:           `["arg1"]`,
					Cron:           "* * * * *",
					InitiateTime:   &cronExactTime,
					RecoveryPolicy: Reparation,
				},
			},
			args: args{
				lastRunAt: earlierTime,
				names:     []string{"test6"},
			},
			wantPlans:    2,
			wantFirstRun: mustParseTime("2020-02-16T19:13:00+08:00"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plans := calculate(tt.schedule, currentTime, tt.args)

			if got := len(plans); got != len(tt.schedule) && tt.wantPlans > 0 {
				t.Errorf("calculate() got %v plans, want some plans", got)
			}

			for _, itemPlans := range plans {
				if got := len(itemPlans); got != tt.wantPlans {
					t.Errorf("calculate() got %v plans, want %v", got, tt.wantPlans)
				}

				if tt.wantPlans > 0 {
					if !itemPlans[0].RunAt.Equal(tt.wantFirstRun) {
						t.Errorf("First plan RunAt = %v, want %v", itemPlans[0].RunAt, tt.wantFirstRun)
					}
				}
			}
		})
	}
}
