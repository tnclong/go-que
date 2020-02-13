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
	ErrNotLockedJobsInDB    = errors.New("jobs hadn't locked in db")
	ErrNotLockedJobsInLocal = errors.New("jobs hadn't lcoked in local")

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
