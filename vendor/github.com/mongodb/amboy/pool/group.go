/*
Grouped Pool

The MultiPool implementation is substantially similar to the Simple
Pool; however, one MultiPool instance can service jobs from multiple
Queues. In this case, the Runner *must* be attached to all of the
queues before workers are spawned and the queues begin dispatching
tasks.
*/
package pool

import (
	"errors"
	"fmt"
	"sync"

	"github.com/mongodb/amboy"
	"github.com/tychoish/grip"
)

// Group is a Runner implementation that can, potentially, run
// tasks from multiple queues at the same time.
type Group struct {
	size    int
	started bool
	wg      *sync.WaitGroup
	grip    grip.Journaler
	catcher grip.MultiCatcher
	queues  []amboy.Queue

	*sync.RWMutex
}

type workUnit struct {
	j amboy.Job
	q amboy.Queue
}

// NewGroup start creates a new Runner instance with the
// specified number of worker processes capable of running jobs from
// multiple queues.
func NewGroup(numWorkers int) *Group {
	r := &Group{
		size:    numWorkers,
		wg:      &sync.WaitGroup{},
		RWMutex: &sync.RWMutex{},
	}
	r.grip = grip.NewJournaler("amboy.runner.group")
	r.grip.CloneSender(grip.Sender())

	return r
}

// SetQueue adds a new queue object to the Runner instance. There is
// no way to remove a amboy.Queue from a runner object, and no way to add a
// a amboy.Queue after starting to dispatch jobs.
func (r *Group) SetQueue(q amboy.Queue) error {
	if q.Started() {
		return errors.New("cannot add a started queue to a runner")
	}

	if r.Started() {
		return errors.New("cannot add a queue to a started runner")

	}

	r.queues = append(r.queues, q)
	return nil
}

// Started returns true if the runner's worker threads have started, and false otherwise.
func (r *Group) Started() bool {
	r.RLock()
	defer r.RUnlock()

	return r.started
}

// Size returns the configured number of worker threads controlled by
// the Runner. In the case of the Group, this *does not* include
// the number of threads used as part of the amboy.Queue aggregation process.
func (r *Group) Size() int {
	return r.size
}

// SetSize allows users to change the size of the worker pool after
// creating the Runner.  Returns an error if the Runner has started.
func (r *Group) SetSize(s int) error {
	if r.Started() {
		return errors.New("cannot change size of worker pool after starting")
	}

	if s < 1 {
		return fmt.Errorf("cannot set poolsize to < 1 (%d), pool size is %d",
			s, r.size)
	}

	r.size = s
	return nil
}

func (r *Group) startMerger() <-chan *workUnit {
	wg := &sync.WaitGroup{}
	work := make(chan *workUnit, len(r.queues))

	// Start a new thread for each queue, send workUnit objects
	// through the channel.
	for idx, queue := range r.queues {
		wg.Add(1)

		go func(q amboy.Queue, num int) {
			if !q.Started() {
				_ = q.Start()
			}

			r.grip.Debugf("starting to dispatch jobs from queue-%d", num)
			for {
				job, err := q.Next()
				if err != nil {
					if q.Closed() {
						break
					}
					r.grip.Debug(err)
				} else {
					w := &workUnit{
						j: job,
						q: q,
					}
					work <- w
				}
			}

			r.grip.Debugf("completed all jobs in queue-%d", num)
			wg.Done()
		}(queue, idx)
	}

	go func() {
		wg.Wait()
		close(work)
	}()

	return work
}

// Start initializes all worker process, and returns an error if the
// Runner has already started.
func (r *Group) Start() error {
	if r.Started() {
		// PoolGrooup noops on successive Start operations so
		// that so that multiple queues can call start.
		return nil
	}

	// Group is mostly similar to LocalWorker, but maintains a
	// background thread for each queue that puts Jobs onto a
	// channel, that the actual workers pull tasks from.
	work := r.startMerger()

	r.Lock()
	r.started = true
	r.Unlock()

	r.grip.Debugf("running %d workers", r.size)
	for w := 1; w <= r.size; w++ {
		r.wg.Add(1)
		name := fmt.Sprintf("worker-%d", w)

		go func(name string) {
			r.grip.Debugf("worker (%s) waiting for jobs", name)

			for unit := range work {
				r.catcher.Add(unit.j.Run())
				unit.q.Complete(unit.j)
			}

			r.grip.Debugf("worker (%s) exiting", name)
			r.wg.Done()
		}(name)
	}

	return nil
}

// Wait blocks until all workers have exited.
func (r *Group) Wait() {
	r.wg.Wait()
	r.grip.Infof("all (%d) runners have exited", r.size)
}

// Error Returns an error object with the concatenated contents of all
// errors returned by Job Run methods, if there are any errors.
func (r *Group) Error() error {
	return r.catcher.Resolve()
}
