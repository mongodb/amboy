package remote

import (
	"errors"
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"github.com/mongodb/amboy"
	"github.com/tychoish/grip"
)

// LocalDriver implements the driver interface, but rather than
// connecting to a remote data source, this implementation is mostly
// for testing the queue implementation locally, and providing a proof
// of concept for the remote driver. May also be useful for converting
// a remote queue into a local-only architecture in a
// dependency-injection situation.
type LocalDriver struct {
	jobs struct {
		dispatched map[string]bool
		m          map[string]amboy.Job
		sync.RWMutex
	}
	locks struct {
		m map[string]RemoteJobLock
		sync.RWMutex
	}
	closer context.CancelFunc
}

// NewLocalDriver creates a local persistence layer object.
func NewLocalDriver() *LocalDriver {
	d := &LocalDriver{}
	d.jobs.m = make(map[string]amboy.Job)
	d.jobs.dispatched = make(map[string]bool)
	d.locks.m = make(map[string]RemoteJobLock)

	return d
}

// Open is a noop for the LocalDriver implementation, and exists to
// satisfy the Driver interface.
func (d *LocalDriver) Open(ctx context.Context) error {
	_, cancel := context.WithCancel(ctx)
	d.closer = cancel

	return nil
}

// Close is a noop for the LocalDriver implementation, and exists to
// satisfy the Driver interface.
func (d *LocalDriver) Close() {
	if d.closer != nil {
		d.closer()
	}
}

// Put saves a new job in the queue, and returns an error if the job
// already exists in the persistence system.
func (d *LocalDriver) Put(j amboy.Job) error {
	d.jobs.Lock()
	defer d.jobs.Unlock()

	name := j.ID()

	if _, ok := d.jobs.m[name]; !ok {
		d.jobs.m[name] = j
		grip.Debugf("putting job %s", name)
		return nil

	}

	return fmt.Errorf("job named %s exists, use Save", name)
}

// Get retrieves a job object from the persistence system based on the
// name (ID) of the job. If no job exists by this name, the error is
// non-nil.
func (d *LocalDriver) Get(name string) (amboy.Job, error) {
	d.jobs.RLock()
	defer d.jobs.RUnlock()

	j, ok := d.jobs.m[name]

	if ok {
		return j, nil
	}

	return nil, fmt.Errorf("no job named %s exists", name)
}

// Reload takes a job object and returns a more up to date version of
// the same job object. If there is an error retrieving the document,
// reload will return the original document. All errors are logged.
func (d *LocalDriver) Reload(j amboy.Job) amboy.Job {
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
func (d *LocalDriver) Save(j amboy.Job) error {
	d.jobs.Lock()
	defer d.jobs.Unlock()
	name := j.ID()

	if _, ok := d.jobs.m[name]; ok {
		d.jobs.m[name] = j
		grip.Debugf("saving job %s", name)
		return nil
	}
	return fmt.Errorf("cannot save a job named %s, which doesn't exist. use Put", name)

}

// GetLock takes a Job and returns a RemoteJobLock implementation for
// this job, with an error value if there was a problem creating the
// lock. In the LocalDriver implementation, this operation has the side effect of adding
func (d *LocalDriver) GetLock(ctx context.Context, j amboy.Job) (RemoteJobLock, error) {
	name := j.ID()

	if ctx.Err() != nil {
		return nil, errors.New("job canceled before getting lock")
	}

	d.jobs.Lock()
	defer d.jobs.Unlock()

	_, ok := d.jobs.m[name]
	if !ok {
		return nil, fmt.Errorf("job %s is not tracked", name)
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

	lock = NewLocalJobLock(name)
	d.locks.m[name] = lock
	return lock, nil
}

func (d *LocalDriver) Jobs() <-chan amboy.Job {
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

func (d *LocalDriver) Next() amboy.Job {
	d.jobs.Lock()
	defer d.jobs.Unlock()

	for name, job := range d.jobs.m {
		if exists := d.jobs.dispatched[name]; exists {
			continue
		}

		if !job.Completed() {
			d.jobs.dispatched[name] = true
			return job
		}
	}

	// if we get here then there are no pending jobs and we should
	// just return nil
	return nil
}

func (d *LocalDriver) Stats() Stats {
	stats := Stats{}

	ctx := context.TODO()

	for job := range d.Jobs() {
		stats.Total++

		lock, err := d.GetLock(ctx, job)
		grip.CatchError(err)

		if lock.IsLocked(ctx) {
			stats.Locked++
		} else {
			stats.Unlocked++
		}

		if job.Completed() {
			stats.Complete++
		} else {
			stats.Pending++
		}
	}
	return stats
}
