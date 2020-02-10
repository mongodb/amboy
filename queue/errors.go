package queue

import (
	"fmt"

	"github.com/pkg/errors"
)

type duplJobError struct {
	msg string
}

func (e *duplJobError) Error() string { return e.msg }

// NewDuplicatJobError creates a new error object to represent a
// duplicate job error.
func NewDuplicatJobError(msg string) error { return &duplJobError{msg: msg} }

// NewDuplicatJobErrorf creates a new error object to represent a
// duplicate job error with a formatted message.
func NewDuplicatJobErrorf(msg string, args ...interface{}) error {
	return NewDuplicatJobError(fmt.Sprintf(msg, args...))
}

// MakeDuplicateJobError constructs a duplicate job error from an
// existing error of any type.
func MakeDuplicateJobError(err error) error {
	if err == nil {
		return nil
	}

	return NewDuplicatJobError(err.Error())
}

// IsDuplicateJobError tests an error object to see if it is a
// duplicate job error.
func IsDuplicateJobError(err error) bool {
	if err == nil {
		return false
	}

	_, ok := errors.Cause(err).(*duplJobError)
	return ok
}
