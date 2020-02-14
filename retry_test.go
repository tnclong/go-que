package que

import (
	"testing"
	"time"
)

var intervalCases = []struct {
	name        string
	retryPolicy RetryPolicy
	min         []time.Duration
	max         []time.Duration
	retryCount  int32
}{
	{
		name: "default",
	},
	{
		name: "default but MaxRetryCount=3",
		retryPolicy: RetryPolicy{
			MaxRetryCount: 3,
		},
		min:        []time.Duration{10 * time.Second, 10 * time.Second, 10 * time.Second},
		max:        []time.Duration{10 * time.Second, 10 * time.Second, 10 * time.Second},
		retryCount: 3,
	},
	{
		name: "default MaxInterval",
		retryPolicy: RetryPolicy{
			InitialInterval:        8 * time.Second,
			MaxInterval:            24 * time.Second,
			NextIntervalMultiplier: 2.0,
			MaxRetryCount:          4,
		},
		min:        []time.Duration{8 * time.Second, 16 * time.Second, 24 * time.Second, 24 * time.Second},
		max:        []time.Duration{8 * time.Second, 16 * time.Second, 24 * time.Second, 24 * time.Second},
		retryCount: 4,
	},
	{
		name: "default IntervalRandomPercent",
		retryPolicy: RetryPolicy{
			InitialInterval:        10 * time.Second,
			NextIntervalMultiplier: 2.0,
			IntervalRandomPercent:  20,
			MaxRetryCount:          4,
		},
		min:        []time.Duration{8 * time.Second, 16 * time.Second, 32 * time.Second, 64 * time.Second},
		max:        []time.Duration{12 * time.Second, 24 * time.Second, 48 * time.Second, 96 * time.Second},
		retryCount: 4,
	},
}

func TestNextInterval(t *testing.T) {
	for i := 0; i < 100; i++ {
		for _, tc := range intervalCases {
			policy := tc.retryPolicy
			var retryCount int32
			for i := range tc.min {
				interval, ok := policy.NextInterval(int32(i))
				if !ok {
					t.Fatalf("%s(%v): want interval in [%v, %v] but retry finished", tc.name, i, tc.min[i], tc.max[i])
				}
				if !(interval >= tc.min[i] && interval <= tc.max[i]) {
					t.Fatalf("%s(%v): want interval in [%v, %v] but get %v", tc.name, i, tc.min[i], tc.max[i], interval)
				}
				retryCount++
			}
			for {
				if _, ok := policy.NextInterval(retryCount); ok {
					retryCount++
				} else {
					break
				}
			}
			if retryCount != tc.retryCount {
				t.Fatalf("%s: want retry count is %v but %v", tc.name, tc.retryCount, retryCount)
			}
		}
	}
}
