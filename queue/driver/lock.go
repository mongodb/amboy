package driver

import (
	"sync"

	"golang.org/x/net/context"
)

// LocalJobLock provides an implementation of the JobLock interface
// used by the drivers to allow different queues to operate on the
// same underlying data. This implementation uses a Go mute, and is
// useful for drivers that are shared only between queues within one
// process.
type InternalLock struct {
	name      string
	isLocked  bool
	mutex     sync.Mutex
	metaMutex sync.RWMutex
}

// NewLocalJobLock Returns a new initialized lock instance.
func NewInternalLock(name string) *InternalLock {
	return &InternalLock{name: name}
}

// Name returns the name of the lock, which should also refer to the
// name of this lock, which is the same as the job it protects.
func (l *InternalLock) Name() string {
	return l.name
}

// Lock blocks until the mutex is secured. It takes a context object
// to comply with the interface but the operation can not be canceled.
func (l *InternalLock) Lock(_ context.Context) {
	l.mutex.Lock()

	l.metaMutex.Lock()
	defer l.metaMutex.Unlock()
	l.isLocked = true
}

// Unlock blocks until the mutex is released. It takes a context object
// to comply with the interface but the operation can not be canceled.
func (l *InternalLock) Unlock(_ context.Context) {
	l.metaMutex.Lock()
	defer l.metaMutex.Unlock()

	l.isLocked = false
	l.mutex.Unlock()
}

// IsLocked reports on the lock's current state. It takes a context object
// to comply with the interface but the operation can not be canceled.
func (l *InternalLock) IsLocked(_ context.Context) bool {
	l.metaMutex.RLock()
	defer l.metaMutex.RUnlock()

	return l.isLocked
}

// IsLockedElsewhere always returns false, given that this lock
// implementation does not support locks in multiple systems. It takes
// a context object to comply with the interface but the operation can
// not be canceled.
func (l *InternalLock) IsLockedElsewhere(_ context.Context) bool {
	// there is no elsewhere in this model, so just return is
	// locked.
	return false
}
