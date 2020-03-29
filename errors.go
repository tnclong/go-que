package que

import (
	"errors"
	"fmt"
)

// ErrQueue wraps all other errors in this package.
type ErrQueue struct {
	Err error
}

func (eq *ErrQueue) Error() string {
	return fmt.Sprintf("que: %s", eq.Err.Error())
}

func (eq *ErrQueue) Unwrap() error {
	return eq.Err
}

// Caller judge these errors by errors.Is() in std library.
var (
	ErrViolateUniqueConstraint = errors.New("unique id violates unique constraint")

	ErrUnlockedJobs = errors.New("unlock of unlocked jobs")
	ErrBadMutex     = errors.New("bad mutex")

	ErrWorkerStoped = errors.New("worker stoped")
)
