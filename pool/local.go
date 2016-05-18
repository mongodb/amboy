/*
Local Workers Pool

The LocalWorkers is a simple worker pool implementation that spawns a
collection of (n) workers and dispatches jobs to worker threads, that
consume work items from the Queue's Next() method.
*/
package pool

import (
	"errors"
	"fmt"
	"sync"

	"github.com/mongodb/amboy"
	"github.com/tychoish/grip"
)

// LocalWorkers is a very minimal implementation of a worker pool, and
// supports a configurable number of workers to process Job tasks.
type LocalWorkers struct {
	size    int
	started bool
	wg      *sync.WaitGroup
	queue   amboy.Queue
	grip    grip.Journaler
	catcher *grip.MultiCatcher
}

// NewLocalWorkers is a constructor for LocalWorkers objects, and
// takes arguments for the number of worker processes and a amboy.Queue
// object.
func NewLocalWorkers(numWorkers int, q amboy.Queue) *LocalWorkers {
	r := &LocalWorkers{
		queue:   q,
		size:    numWorkers,
		wg:      &sync.WaitGroup{},
		catcher: grip.NewCatcher(),
	}

	r.grip = grip.NewJournaler("amboy.pool.local")
	r.grip.SetSender(grip.Sender())

	if r.size <= 0 {
		grip.Infof("setting minimal pool size is 1, overriding setting of '%d'", r.size)
		r.size = 1
	}

	return r
}

// SetQueue allows callers to inject alternate amboy.Queue objects into
// constructed Runner objects. Returns an error if the Runner has
// started.
func (r *LocalWorkers) SetQueue(q amboy.Queue) error {
	if r.started {
		return errors.New("cannot add new queue after starting a runner")
	}

	r.queue = q
	return nil
}

// Started returns true when the Runner has begun executing tasks. For
// LocalWorkers this means that workers are running.
func (r *LocalWorkers) Started() bool {
	return r.started
}

// Size returns the size of the worker pool.
func (r *LocalWorkers) Size() int {
	return r.size
}

// SetSize allows users to change the size of the worker pool after
// creating the Runner.  Returns an error if the Runner has started.
func (r *LocalWorkers) SetSize(s int) error {
	if r.started {
		return errors.New("cannot change size of worker pool after starting")
	}

	if s < 1 {
		return fmt.Errorf("cannot set poolsize to < 1 (%d), pool size is %d",
			s, r.size)
	}

	r.size = s
	return nil
}

// Start initializes all worker process, and returns an error if the
// Runner has already started.
func (r *LocalWorkers) Start() error {
	if r.started {
		return errors.New("runner has already started, cannot start again")
	}

	r.started = true
	r.grip.Debugf("running %d workers", r.size)
	for w := 1; w <= r.size; w++ {
		r.wg.Add(1)

		go func(name string) {
			r.grip.Debugf("worker (%s) waiting for jobs", name)

			for {
				job, err := r.queue.Next()
				if err != nil {
					if r.queue.Closed() {
						break
					}
					r.grip.Debug(err)
				} else {
					r.catcher.Add(job.Run())
					r.queue.Complete(job)
				}
			}
			r.grip.Debugf("worker (%s) exiting", name)
			r.wg.Done()
		}(fmt.Sprintf("worker-%d", w))
	}

	return nil
}

// Wait blocks until all workers have exited.
func (r *LocalWorkers) Wait() {
	r.wg.Wait()
	r.grip.Infof("all (%d) runners have exited", r.size)
}

// Error Returns an error object with the concatinated contents of all
// errors returned by Job Run methods, if there are any errors.
func (r *LocalWorkers) Error() error {
	return r.catcher.Resolve()
}
