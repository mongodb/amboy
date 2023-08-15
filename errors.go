package amboy

import (
	"context"
	"fmt"

	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

// JobNotFoundError represents an error indicating that a job could not be found
// in a queue.
type JobNotFoundError struct {
	msg string
}

// Error returns the error message from the job not found error to provide more
// context as to why the job was not found.
func (e *JobNotFoundError) Error() string { return e.msg }

// NewJobNotFoundError creates a new error indicating that a job could not be
// found in the queue.
func NewJobNotFoundError(msg string) *JobNotFoundError { return &JobNotFoundError{msg: msg} }

// NewJobNotFoundErrorf creates a new error with a formatted message, indicating
// that a job could not be found in the queue.
func NewJobNotFoundErrorf(msg string, args ...interface{}) *JobNotFoundError {
	return NewJobNotFoundError(fmt.Sprintf(msg, args...))
}

// MakeJobNotFoundError constructs an error from an existing one, indicating
// that a job could not be found in the queue.
func MakeJobNotFoundError(err error) *JobNotFoundError {
	if err == nil {
		return nil
	}

	return NewJobNotFoundError(err.Error())
}

// IsJobNotFound checks if an error was due to not being able to find the job
// in the queue.
func IsJobNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	_, ok := errors.Cause(err).(*JobNotFoundError)
	return ok
}

// EnqueueUniqueJob is a generic wrapper for adding jobs to queues (using the
// Put() method), but that ignores duplicate job errors.
func EnqueueUniqueJob(ctx context.Context, queue Queue, job Job) error {
	err := queue.Put(ctx, job)

	if IsDuplicateJobError(err) {
		return nil
	}

	return errors.WithStack(err)
}

// EnqueueManyUniqueJobs is a generic wrapper for adding jobs to a queue (using the
// PutMany() method) ignoring duplicate job errors.
func EnqueueManyUniqueJobs(ctx context.Context, queue Queue, jobs []Job) error {
	err := queue.PutMany(ctx, jobs)

	if IsDuplicateJobError(err) {
		return nil
	}

	return errors.WithStack(err)
}

type writeErrors struct {
	duplicateJobErrors   []error
	duplicateScopeErrors []error
	otherErrors          []error
}

func (w *writeErrors) Error() string {
	catcher := grip.NewBasicCatcher()
	catcher.Extend(w.duplicateScopeErrors)
	catcher.Extend(w.duplicateJobErrors)
	catcher.Extend(w.otherErrors)

	return catcher.String()
}

// Cause returns the primary cause of the WriteError. The primary cause is defined as follows:
//   - If a non-duplicate-job error is encountered it is returned.
//   - If every error is a duplicate job error and at least one of them is a duplicate scope error a duplicate scope error is returned.
//   - If every error is a duplicate job error and none of them is a duplicate scope error a duplicate job error is returned.
func (w *writeErrors) Cause() error {
	if len(w.otherErrors) > 0 {
		return w
	}
	if len(w.duplicateScopeErrors) > 0 {
		return MakeDuplicateJobScopeError(w)
	}
	if len(w.duplicateJobErrors) > 0 {
		return MakeDuplicateJobError(w)
	}

	return nil
}

// CollateWriteErrors collates errors into a [writeErrors].
func CollateWriteErrors(errs []error) *writeErrors {
	if len(errs) == 0 {
		return nil
	}

	var writeErrs writeErrors
	for _, err := range errs {
		if IsDuplicateJobScopeError(err) {
			writeErrs.duplicateScopeErrors = append(writeErrs.duplicateScopeErrors, err)
		} else if IsDuplicateJobError(err) {
			writeErrs.duplicateJobErrors = append(writeErrs.duplicateJobErrors, err)
		} else {
			writeErrs.otherErrors = append(writeErrs.otherErrors, err)
		}
	}
	return &writeErrs
}

type duplJobError struct {
	msg string
}

func (e *duplJobError) Error() string { return e.msg }

// NewDuplicateJobError creates a new error to represent a duplicate job error,
// for use by queue implementations.
func NewDuplicateJobError(msg string) error { return &duplJobError{msg: msg} }

// NewDuplicateJobErrorf creates a new error to represent a duplicate job error
// with a formatted message, for use by queue implementations.
func NewDuplicateJobErrorf(msg string, args ...interface{}) error {
	return NewDuplicateJobError(fmt.Sprintf(msg, args...))
}

// MakeDuplicateJobError constructs a duplicate job error from an existing error
// of any type, for use by queue implementations.
func MakeDuplicateJobError(err error) error {
	if err == nil {
		return nil
	}

	return NewDuplicateJobError(err.Error())
}

// IsDuplicateJobError checks if an error is due to a duplicate job in the
// queue.
func IsDuplicateJobError(err error) bool {
	if err == nil {
		return false
	}

	switch errors.Cause(err).(type) {
	case *duplJobError, *duplJobScopeError:
		return true
	default:
		return false
	}
}

type duplJobScopeError struct {
	*duplJobError
}

// NewDuplicateJobScopeError creates a new error object to represent a duplicate
// job scope error, for use by queue implementations.
func NewDuplicateJobScopeError(msg string) error {
	return &duplJobScopeError{duplJobError: &duplJobError{msg: msg}}
}

// NewDuplicateJobScopeErrorf creates a new error object to represent a
// duplicate job scope error with a formatted message, for use by queue
// implementations.
func NewDuplicateJobScopeErrorf(msg string, args ...interface{}) error {
	return NewDuplicateJobScopeError(fmt.Sprintf(msg, args...))
}

// MakeDuplicateJobScopeError constructs a duplicate job scope error from an
// existing error of any type, for use by queue implementations.
func MakeDuplicateJobScopeError(err error) error {
	if err == nil {
		return nil
	}

	return NewDuplicateJobScopeError(err.Error())
}

// IsDuplicateJobScopeError checks if an error is due to a duplicate job scope
// in the queue.
func IsDuplicateJobScopeError(err error) bool {
	if err == nil {
		return false
	}

	_, ok := errors.Cause(err).(*duplJobScopeError)
	return ok
}
