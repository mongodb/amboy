package remote

import (
	"sync"

	"golang.org/x/net/context"
)

type LocalJobLock struct {
	name      string
	isLocked  bool
	mutex     sync.Mutex
	metaMutex sync.RWMutex
}

func NewLocalJobLock(name string) *LocalJobLock {
	return &LocalJobLock{name: name}
}

func (l *LocalJobLock) Name() string {
	return l.name
}

func (l *LocalJobLock) Lock(_ context.Context) {
	l.mutex.Lock()

	l.metaMutex.Lock()
	defer l.metaMutex.Unlock()
	l.isLocked = true
}

func (l *LocalJobLock) Unlock(_ context.Context) {
	l.metaMutex.Lock()
	defer l.metaMutex.Unlock()

	l.isLocked = false
	l.mutex.Unlock()
}

func (l *LocalJobLock) IsLocked(_ context.Context) bool {
	l.metaMutex.RLock()
	defer l.metaMutex.RUnlock()

	return l.isLocked
}

func (l *LocalJobLock) IsLockedElsewhere(_ context.Context) bool {
	// there is no elsewhere in this model, so just return is
	// locked.
	return false
}
