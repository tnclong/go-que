package que

import (
	"math"
	"math/rand"
	"time"
)

// RetryPolicy guides how to retry perform job.
type RetryPolicy struct {
	// InitialInterval is interval for the first retry.
	// If NextIntervalMultiplier is 1.0 then it is used for all retries.
	InitialInterval time.Duration

	// MaxInterval is max interval between adjacent tries.
	// NextIntervalMultiplier leads to next interval increase.
	// Default is 100x of InitialInterval.
	MaxInterval time.Duration
	// NextIntervalMultiplier is a multiplicator used in calculating next interval.
	// Must greater or equals to 1.0.
	NextIntervalMultiplier float64
	// IntervalRandomPercent gives interval a variation of ±(IntervalRandomPercent%).
	// Must in [0,100]
	IntervalRandomPercent uint8

	// MaxRetryCount is max retry count.
	// Job will try to exec again after a interval if set MaxRetryCount to 1.
	// 0 is not retry
	// < 0 is retry forever
	MaxRetryCount int
}

// NextInterval calculates next retry interval according to retryCount and RetryPolicy.
// Caller must stop retry when get a false.
func (rp RetryPolicy) NextInterval(retryCount int) (time.Duration, bool) {
	if retryCount < 0 || (rp.MaxRetryCount >= 0 && retryCount >= rp.MaxRetryCount) {
		return 0, false
	}
	initialInterval := rp.InitialInterval
	if initialInterval <= 0 {
		initialInterval = 10 * time.Second
	}
	var nextInterval time.Duration
	if retryCount == 0 || rp.NextIntervalMultiplier <= 1 {
		nextInterval = initialInterval
	} else {
		interval := float64(initialInterval) * math.Pow(rp.NextIntervalMultiplier, float64(retryCount))
		maxInterval := rp.MaxInterval
		if maxInterval <= 0 {
			maxInterval = initialInterval * 100
		}
		if interval > float64(maxInterval) {
			nextInterval = maxInterval
		} else {
			nextInterval = time.Duration(interval)
		}
	}
	return randomInterval(nextInterval, rp.IntervalRandomPercent), true
}

func randomInterval(interval time.Duration, percent uint8) time.Duration {
	if percent == 0 {
		return interval
	}
	if percent > 100 {
		percent = 100
	}

	delta := interval * time.Duration(percent) / 100
	minInterval := interval - delta
	maxInterval := interval + delta

	return minInterval + time.Duration(rand.Float64()*float64(maxInterval-minInterval+1))
}
