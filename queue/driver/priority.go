package driver

import (
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

// Priority implements the Driver interface, wrapping a
// PriorityStorage instance. This allows "local" (i.e. intraprocess)
// shared queues that dispatch jobs in priority order.
type Priority struct {
	storage *PriorityStorage
	closer  context.CancelFunc
	locks   struct {
		cache map[string]JobLock
		mutex sync.RWMutex
	}
}

// NewPriority returns an initialized Priority Driver instances.
func NewPriority() *Priority {
	p := &Priority{
		storage: NewPriorityStorage(),
	}

	p.locks.cache = make(map[string]JobLock)

	return p
}

// Open initilizes the resources of the Driver, and is part of the
// Driver interface. In the case of the Priority Driver, this
// operation cannot error.
func (p *Priority) Open(ctx context.Context) error {
	if p.closer != nil {
		return nil
	}

	_, cancel := context.WithCancel(ctx)
	p.closer = cancel

	return nil
}

// Close release all resources associated with the Driver instance.
func (p *Priority) Close() {
	if p.closer != nil {
		p.closer()
	}
}

// Put inserts a job into the Drivers' backing storage. For
// consistency with other Driver's the Job's ID must be unique,
// otherwise this operation will return an error. To update an
// existing job use the Save method.
func (p *Priority) Put(j amboy.Job) error {
	if _, ok := p.storage.Get(j.ID()); ok {
		return errors.Errorf("job %s exists, use Save", j.ID())
	}

	p.storage.Push(j)

	return nil
}

// Get returns a job object, specified by name/ID from the backing
// storage. If the job doesn't exist the error value is non-nil.
func (p *Priority) Get(name string) (amboy.Job, error) {
	job, ok := p.storage.Get(name)
	if !ok {
		return nil, errors.Errorf("job named '%s' does not exist", name)
	}

	return job, nil
}

// Reload updates returns a, potentially updated, version of the Job
// from the Driver's backing storage. If there is no matching job
// stored in the Driver, Reload returns the same job.
func (p *Priority) Reload(j amboy.Job) amboy.Job {
	job, ok := p.storage.Get(j.ID())

	if !ok {
		return j
	}

	return job
}

// Save updates the stored version of the job in the Driver's backing
// storage. If the job is not tracked by the Driver, this operation is
// an error.
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

// Jobs returns an iterator of all Job objects tracked by the Driver.
func (p *Priority) Jobs() <-chan amboy.Job {
	return p.storage.Contents()
}

// Next returns the next, highest priority Job from the Driver's
// backing storage. If there are no queued jobs, the job object is
// nil.
func (p *Priority) Next() amboy.Job {
	j := p.storage.Pop()

	if j == nil || j.Completed() {
		return nil
	}

	return j

}

// Stats returns a report of the Driver's current state in the form of
// a driver.Stats document.
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

// GetLock produces the JobLock object for the specified Job.
func (p *Priority) GetLock(ctx context.Context, j amboy.Job) (JobLock, error) {
	name := j.ID()

	_, ok := p.storage.Get(name)
	if !ok {
		return nil, errors.Errorf("job %s is not tracked", name)
	}

	p.locks.mutex.Lock()
	defer p.locks.mutex.Unlock()

	if ctx.Err() != nil {
		return nil, errors.New("job canceled before getting lock")
	}

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
