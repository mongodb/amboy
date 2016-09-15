package driver

import (
	"sync"

	"github.com/mongodb/amboy"
	"github.com/pkg/errors"
	"github.com/tychoish/grip"
	"golang.org/x/net/context"
)

// Internal implements the driver interface, but rather than
// connecting to a remote data source, this implementation is mostly
// for testing the queue implementation locally, and providing a proof
// of concept for the remote driver. May also be useful for converting
// a remote queue into a local-only architecture in a
// dependency-injection situation.
type Internal struct {
	jobs struct {
		dispatched map[string]struct{}
		m          map[string]amboy.Job
		sync.RWMutex
	}
	locks struct {
		m map[string]JobLock
		sync.RWMutex
	}
	closer context.CancelFunc
}

// NewInternal creates a local persistence layer object.
func NewInternal() *Internal {
	d := &Internal{}
	d.jobs.m = make(map[string]amboy.Job)
	d.jobs.dispatched = make(map[string]struct{})
	d.locks.m = make(map[string]JobLock)

	return d
}

// Open is a noop for the Internal implementation, and exists to
// satisfy the Driver interface.
func (d *Internal) Open(ctx context.Context) error {
	_, cancel := context.WithCancel(ctx)
	d.closer = cancel

	return nil
}

// Close is a noop for the Internal implementation, and exists to
// satisfy the Driver interface.
func (d *Internal) Close() {
	if d.closer != nil {
		d.closer()
	}
}

// Put saves a new job in the queue, and returns an error if the job
// already exists in the persistence system.
func (d *Internal) Put(j amboy.Job) error {
	d.jobs.Lock()
	defer d.jobs.Unlock()

	name := j.ID()

	if _, ok := d.jobs.m[name]; !ok {
		d.jobs.m[name] = j
		grip.Debugf("putting job %s", name)
		return nil

	}

	return errors.Errorf("job named %s exists, use Save", name)
}

// Get retrieves a job object from the persistence system based on the
// name (ID) of the job. If no job exists by this name, the error is
// non-nil.
func (d *Internal) Get(name string) (amboy.Job, error) {
	d.jobs.RLock()
	defer d.jobs.RUnlock()

	j, ok := d.jobs.m[name]

	if ok {
		return j, nil
	}

	return nil, errors.Errorf("no job named %s exists", name)
}

// Reload takes a job object and returns a more up to date version of
// the same job object. If there is an error retrieving the document,
// reload will return the original document. All errors are logged.
func (d *Internal) Reload(j amboy.Job) amboy.Job {
	newJob, err := d.Get(j.ID())

	if err != nil {
		grip.Error(err)
		return j
	}

	return newJob
}

// Save takes a job and persists it in the storage for this driver. If
// there is no job with a matching ID, then this operation returns an
// error.
func (d *Internal) Save(j amboy.Job) error {
	d.jobs.Lock()
	defer d.jobs.Unlock()
	name := j.ID()

	if _, ok := d.jobs.m[name]; ok {
		d.jobs.m[name] = j

		grip.Debugf("saving job %s", name)
		return nil
	}

	d.locks.Lock()
	defer d.locks.Unlock()
	if _, ok := d.locks.m[name]; ok && j.Completed() {
		delete(d.locks.m, name)
	}

	return errors.Errorf("cannot save a job named %s, which doesn't exist. use Put", name)
}

// GetLock takes a Job and returns a JobLock implementation for
// this job, with an error value if there was a problem creating the
// lock. In the Internal implementation, this operation has the side effect of adding
func (d *Internal) GetLock(ctx context.Context, j amboy.Job) (JobLock, error) {
	name := j.ID()

	if ctx.Err() != nil {
		return nil, errors.New("job canceled before getting lock")
	}

	d.jobs.Lock()
	defer d.jobs.Unlock()

	_, ok := d.jobs.m[name]
	if !ok {
		return nil, errors.Errorf("job %s is not tracked", name)
	}

	if ctx.Err() != nil {
		return nil, errors.New("job canceled before getting lock")
	}

	d.locks.Lock()
	defer d.locks.Unlock()

	lock, ok := d.locks.m[name]
	if ok {
		return lock, nil
	}

	lock = NewInternalLock(name)
	d.locks.m[name] = lock
	return lock, nil
}

// Jobs is a generator of all Job objects stored by the driver. There
// is no additional filtering of the jobs produced by this generator.
func (d *Internal) Jobs() <-chan amboy.Job {
	d.jobs.RLock()
	defer d.jobs.RUnlock()
	output := make(chan amboy.Job, len(d.jobs.m))

	go func() {
		d.jobs.RLock()
		defer d.jobs.RUnlock()
		for _, job := range d.jobs.m {
			output <- job
		}
		close(output)
	}()

	return output
}

// Next returns a job that is not complete from the queue. If there
// are no pending jobs, then this method returns nil, but does not
// block.
func (d *Internal) Next() amboy.Job {
	d.jobs.Lock()
	defer d.jobs.Unlock()

	// TODO: this is a particularly inefficient implementation of
	// this method, because there is no distinction internally
	// between storage for completed, dispatched, and pending work.
	for name, job := range d.jobs.m {
		if _, ok := d.jobs.dispatched[name]; ok {
			continue
		}

		if job.Completed() {
			d.jobs.dispatched[name] = struct{}{}
			continue
		}

		d.jobs.dispatched[name] = struct{}{}
		return job
	}

	// if we get here then there are no pending jobs and we should
	// just return nil
	return nil
}

// Stats iterates through all of the jobs stored in the driver and
// determines how many locked, completed, and pending jobs are stored
// in the queue.
func (d *Internal) Stats() Stats {
	stats := Stats{}
	ctx := context.TODO()

	d.jobs.RLock()
	stats.Total = len(d.jobs.m)
	for name := range d.jobs.m {
		j := d.jobs.m[name]
		if j.Completed() {
			stats.Complete++
		} else {
			stats.Pending++
		}
	}
	d.jobs.RUnlock()

	d.locks.RLock()
	for _, lock := range d.locks.m {
		if lock.IsLocked(ctx) {
			stats.Locked++
		}
	}
	d.locks.RUnlock()

	stats.Unlocked = stats.Total - stats.Locked

	return stats
}
