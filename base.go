package amboy

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/mongodb/amboy/dependency"
	"github.com/mongodb/amboy/priority"
)

// JobBase is a type that all new checks should compose, and provides
// an implementation of most common Job methods which most jobs
// need not implement themselves.
type JobBase struct {
	TaskID     string   `bson:"name" json:"name" yaml:"name"`
	IsComplete bool     `bson:"completed" json:"completed" yaml:"completed"`
	JobType    JobType  `bson:"job_type" json:"job_type" yaml:"job_type"`
	Errors     []string `bson:"errors" json:"errors" yaml:"errors"`

	dep   dependency.Manager
	mutex sync.RWMutex
	// adds common priority tracking.
	priority.Value `bson:"priority" json:"priority" yaml:"priority"`
}

////////////////////////////////////////////////////////////////////////
//
// Safe methods for manipulating the object.
//
////////////////////////////////////////////////////////////////////////

// MarkComplete signals that the job is complete, and is not part of
// the Job interface.
func (b *JobBase) MarkComplete() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.IsComplete = true
}

// AddError takes an error object and if it is non-nil, tracks it
// internally. This operation is thread safe, but not part of the Job
// interface.
func (b *JobBase) AddError(err error) {
	if err != nil {
		b.mutex.Lock()
		defer b.mutex.Unlock()

		b.Errors = append(b.Errors, fmt.Sprintf("%+v", err))
	}
}

// HasErrors checks the stored errors in the object and reports if
// there are any stored errors. This operation is thread safe, but not
// part of the Job interface.
func (b *JobBase) HasErrors() bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return len(b.Errors) > 0
}

// SetID makes it possible to change the ID of an amboy.Job. It is not
// part of the amboy.Job interface.
func (b *JobBase) SetID(n string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.TaskID = n
}

////////////////////////////////////////////////////////////////////////
//
// Implementation of common interface members.
//
////////////////////////////////////////////////////////////////////////

// ID returns the name of the job, and is a component of the Job
// interface.
func (b *JobBase) ID() string {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return b.TaskID
}

// Completed returns true if the job has been marked completed, and is
// a component of the Job interface.
func (b *JobBase) Completed() bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return b.IsComplete
}

// Type returns the JobType specification for this object, and
// is a component of the Job interface.
func (b *JobBase) Type() JobType {
	return b.JobType
}

// Dependency returns an amboy Job dependency interface object, and is
// a component of the Job interface.
func (b *JobBase) Dependency() dependency.Manager {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return b.dep
}

// SetDependency allows you to inject a different Job dependency
// object, and is a component of the Job interface.
func (b *JobBase) SetDependency(d dependency.Manager) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.dep = d
}

// Error returns all of the error objects produced by the job.
func (b *JobBase) Error() error {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if len(b.Errors) == 0 {
		return nil
	}

	return errors.New(strings.Join(b.Errors, "\n"))
}
