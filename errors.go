package que

import (
	"errors"
	"fmt"
	"strings"
)

// ErrQueue uses for any error related to a queue.
type ErrQueue struct {
	Queue string
	Err   error
}

func (eq *ErrQueue) Error() string {
	return fmt.Sprintf("%s: %s", eq.Queue, eq.Err.Error())
}

func (eq *ErrQueue) Unwrap() error {
	return eq.Err
}

// Caller judge these errors by errors.Is() in std library.
var (
	ErrQueueAlreadyClosed = errors.New("queue already closed")

	ErrNotLockedJobsInDB    = errors.New("jobs we hadn't locked in db")
	ErrNotLockedJobsInLocal = errors.New("jobs we hadn't lcoked in local")

	ErrWorkerStoped = errors.New("worker stoped")
)

// ErrUnlock uses for any error when Unlock.
type ErrUnlock struct {
	IDs []int64
	Err error
}

func (eu *ErrUnlock) Error() string {
	return fmt.Sprintf("%s when unlock(%v)", eu.Err.Error(), eu.IDs)
}

func (eu *ErrUnlock) Unwrap() error {
	return eu.Err
}

// MultiErr represents multi errors.
type MultiErr []error

// Append appends err to *MultiErr.
func (me *MultiErr) Append(err ...error) {
	for _, e := range err {
		if e != nil {
			if me2, ok := e.(*MultiErr); ok {
				*me = append(*me, (*me2)...)
			} else {
				*me = append(*me, e)
			}
		}
	}
}

func (me *MultiErr) Is(err error) bool {
	for _, e := range *me {
		if errors.Is(e, err) {
			return true
		}
	}
	return false
}

func (me *MultiErr) Error() string {
	if n := len(*me); n > 0 {
		var b strings.Builder
		b.WriteString((*me)[0].Error())
		for _, err := range (*me)[1:] {
			b.WriteString("; ")
			b.WriteString(err.Error())
		}
		return b.String()
	}
	return ""
}

// Err returns nil, if length of errors is zero.
// Err returns origin error, if length of errors is 1.
// Err returns self, if length of errors grater than 1.
func (me *MultiErr) Err() error {
	n := len(*me)
	if n == 0 {
		return nil
	}
	if n == 1 {
		return (*me)[0]
	}
	return me
}

// Empty returns true, if length of errors is zero.
func (me *MultiErr) Empty() bool {
	return len(*me) == 0
}
