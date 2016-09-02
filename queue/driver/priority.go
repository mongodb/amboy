package driver

import (
	"fmt"
	"sync"

	"github.com/mongodb/amboy"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////
//
// Driver implementation for use with the remote queue implementation
//
////////////////////////////////////////////////////////////////////////

type Priority struct {
	storage *PriorityStorage
	closer  context.CancelFunc
	locks   struct {
		cache map[string]JobLock
		mutex sync.RWMutex
	}
}

func NewPriority() *Priority {
	p := &Priority{
		storage: NewPriorityStorage(),
	}

	p.locks.cache = make(map[string]JobLock)

	return p
}

func (p *Priority) Open(ctx context.Context) error {
	if p.closer != nil {
		return nil
	}

	_, cancel := context.WithCancel(ctx)
	p.closer = cancel

	return nil
}

func (p *Priority) Close() {
	if p.closer != nil {
		p.closer()
	}
}

func (p *Priority) Put(j amboy.Job) error {
	if _, ok := p.storage.Get(j.ID()); ok {
		return errors.Errorf("job %s exists, use Save", j.ID())
	}

	p.storage.Push(j)

	return nil
}

func (p *Priority) Get(name string) (amboy.Job, error) {
	job, ok := p.storage.Get(name)
	if !ok {
		return nil, errors.Errorf("job named '%s' does not exist", name)
	}

	return job, nil
}

func (p *Priority) Reload(j amboy.Job) amboy.Job {
	job, ok := p.storage.Get(j.ID())

	if !ok {
		return j
	}

	return job
}

func (p *Priority) Save(j amboy.Job) error {
	name := j.ID()

	if _, ok := p.storage.Get(name); !ok {
		return errors.Errorf("job %s does not exist, use Put", name)
	}

	p.storage.Push(j)

	p.locks.mutex.Lock()
	defer p.locks.mutex.Unlock()
	if _, ok := p.locks.cache[name]; ok && j.Completed() {
		delete(p.locks.cache, name)
	}

	return nil
}

func (p *Priority) Jobs() <-chan amboy.Job {
	return p.storage.Contents()
}

func (p *Priority) Next() amboy.Job {
	j := p.storage.Pop()

	if j == nil || j.Completed() {
		return nil
	}

	return j

}

func (p *Priority) Stats() Stats {
	stats := Stats{
		Total: p.storage.Size(),
	}

	ctx := context.TODO()

	p.locks.mutex.RLock()
	for _, lock := range p.locks.cache {
		if lock.IsLocked(ctx) {
			stats.Locked++
			continue
		}
		stats.Unlocked++
	}
	p.locks.mutex.RUnlock()

	for job := range p.storage.Contents() {
		if job.Completed() {
			stats.Complete++
			continue
		}
		stats.Pending++
	}

	return stats
}

func (p *Priority) GetLock(ctx context.Context, j amboy.Job) (JobLock, error) {
	name := j.ID()

	_, ok := p.storage.Get(name)
	if !ok {
		return nil, fmt.Errorf("job %s is not tracked", name)
	}

	p.locks.mutex.Lock()
	defer p.locks.mutex.Unlock()

	lock, ok := p.locks.cache[name]
	if ok {
		return lock, nil
	}

	if ctx.Err() != nil {
		return nil, errors.New("job canceled before getting lock")
	}

	lock = NewInternalLock(name)
	p.locks.cache[name] = lock

	return lock, nil
}
