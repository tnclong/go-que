package scheduler

import (
	"errors"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/tnclong/go-que"
)

// Schedule is a set of named items.
type Schedule map[string]Item

// Item represents a job need to enqueue queue according to cron.
type Item struct {
	Queue string `yaml:"queue"`
	// Args is a json array.
	Args string `yaml:"args"`
	// See https://en.wikipedia.org/wiki/Cron or https://github.com/robfig/cron
	Cron string `yaml:"cron"`
	// InitiateTime defines when job scheduling should begin,
	// preventing unexpected historical jobs on first inclusion
	InitiateTime *time.Time `yaml:"initiateTime"`

	RecoveryPolicy RecoveryPolicy `yaml:"recoveryPolicy"`

	RetryPolicy que.RetryPolicy `yaml:"retryPolicy"`
}

// RecoveryPolicy guides how to process schedule after a long period of downtime.
type RecoveryPolicy string

const (
	// Ignore ignores missed events and schedule each item from now.
	Ignore RecoveryPolicy = "ignore"
	// Reparation schedule each item from last scheduled item run time to now.
	Reparation RecoveryPolicy = "reparation"
)

// ValidateSchedule validates schedule content is legal.
func ValidateSchedule(schedule Schedule) error {
	for name, item := range schedule {
		if name == "" {
			return errors.New("name is empty string")
		}
		if err := ValidateItem(item); err != nil {
			return errors.New(name + ": " + err.Error())
		}
	}
	return nil
}

// ValidateItem validates item is legal.
func ValidateItem(item Item) error {
	if item.Queue == "" {
		return errors.New("queue is empty string")
	}
	if item.Cron == "" {
		return errors.New("cron is empty string")
	}
	if _, err := parseCron(item.Cron); err != nil {
		return err
	}
	if item.RecoveryPolicy != Ignore && item.RecoveryPolicy != Reparation {
		return errors.New("invald recovery policy")
	}
	return nil
}

func parseCron(spec string) (cron.Schedule, error) {
	return cron.ParseStandard(spec)
}
