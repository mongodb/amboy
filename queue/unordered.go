/*
Local Unordered Queue

The unordered queue provides a basic, single-instance, amboy.Queue
that runs jobs locally in the context of the application with no
persistence layer. The unordered queue does not guarantee any
particular execution order, nor does it compute dependences between
jobs, but, as an implementation detail, dispatches jobs to workers in
a first-in-first-out (e.g. FIFO) model.

By default, LocalUnordered uses the amboy/pool.Workers implementation
of amboy.Runner interface.
*/
package queue

import (
	"errors"
	"fmt"
	"sync"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/pool"
	"github.com/tychoish/grip"
)

// LocalUnordered implements a local-only, channel based, queue
// interface, and it is a good prototype for testing, in addition to
// non-distributed workloads.
type LocalUnordered struct {
	closed       bool
	started      bool
	numCompleted int
	numStarted   int
	channel      chan amboy.Job
	closeQueue   chan bool

	tasks struct {
		m map[string]amboy.Job
		*sync.RWMutex
	}

	// Composed functionality:
	runner amboy.Runner
	grip   grip.Journaler
}

// NewLocalUnordered is a constructor for the LocalUnordered
// implementation of the Queue interface. The constructor takes a
// single argument, for the number of workers the Runner instance
// should have. The channels have a buffer of at least 8 or 2 times
// the number of workers up a total of 64.
func NewLocalUnordered(workers int) *LocalUnordered {
	bufferSize := workers * 2

	if bufferSize > 64 {
		bufferSize = 64
	}

	if bufferSize < 8 {
		bufferSize = 8
	}

	q := &LocalUnordered{
		channel:    make(chan amboy.Job, bufferSize),
		closeQueue: make(chan bool),
	}

	q.tasks.RWMutex = &sync.RWMutex{}
	q.tasks.m = make(map[string]amboy.Job)

	q.grip = grip.NewJournaler(fmt.Sprintf("amboy.queue.simple"))
	q.grip.CloneSender(grip.Sender())
	q.grip.Debugln("queue buffer size:", bufferSize)

	r := pool.NewLocalWorkers(workers, q)
	q.runner = r

	return q
}

// Put adds a job to the amboy.Job Queue. Returns an error if the
// Queue has not yet started, is closed, or if a amboy.Job with the
// same name (i.e. amboy.Job.ID()) exists.
func (q *LocalUnordered) Put(j amboy.Job) error {
	name := j.ID()

	if q.closed {
		return fmt.Errorf("cannot add %s because queue is closed", name)
	}

	if !q.started {
		return fmt.Errorf("cannot add %s because queue has not started", name)

	}

	q.tasks.Lock()
	defer q.tasks.Unlock()

	if _, ok := q.tasks.m[name]; ok {
		return fmt.Errorf("cannot add %s, because a job exists with that name", name)
	}

	q.tasks.m[name] = j
	q.numStarted++
	q.channel <- j
	q.grip.Debugf("added job (%s) to queue", j.ID())

	return nil
}

// Runner returns the embedded task runner.
func (q *LocalUnordered) Runner() amboy.Runner {
	return q.runner
}

// SetRunner allows users to substitute alternate Runner
// implementations at run time. This method fails if the runner has
// started.
func (q *LocalUnordered) SetRunner(r amboy.Runner) error {
	if q.runner.Started() {
		return errors.New("cannot change runners after starting")
	}

	q.runner = r
	return nil
}

// Started returns true when the Queue has begun dispatching tasks to
// runners.
func (q *LocalUnordered) Started() bool {
	return q.started
}

// Start kicks off the background process that dispatches Jobs. Also
// starts the embedded runner, and errors if it cannot start. Should
// handle all errors from this method as fatal errors. If you call
// start on a queue that has been started, subsequent calls to Start()
// are a noop, and do not return an error.
func (q *LocalUnordered) Start() error {
	if q.closed {
		return errors.New("cannot start a completed queue")
	}

	if q.started {
		return nil
	}

	err := q.runner.Start()
	if err != nil {
		return err
	}

	q.started = true

	// we have a background thread blocking on, and waiting for
	// the close signal.
	go func() {
		<-q.closeQueue
		// close all the channels
		close(q.channel)
		q.grip.Warning("job server exiting")
	}()

	q.grip.Info("job server running")
	return nil
}

// Next returns a job from the Queue. This call is non-blocking. If
// there are no pending jobs at the moment, then Next returns an
// error. If the queue is closed and all jobs are complete, then Next
// also returns an error.
func (q *LocalUnordered) Next() (amboy.Job, error) {
	select {
	case job, ok := <-q.channel:
		if !ok {
			return nil, errors.New("all jobs complete")
		}

		return job, nil
	default:
		return nil, errors.New("no pending jobs")
	}

}

// Results provides an iterator of all "result objects," or completed
// amboy.Job objects. Does not wait for all results to be complete, and is
// closed when all results have been exhausted, even if there are more
// results pending. Other implementations may have different semantics
// for this method.
func (q *LocalUnordered) Results() <-chan amboy.Job {
	output := make(chan amboy.Job, q.numCompleted)

	go func() {
		q.tasks.RLock()
		defer q.tasks.RUnlock()
		for _, job := range q.tasks.m {
			if job.Completed() {
				output <- job
			}
		}
		close(output)
	}()

	return output
}

// Get takes a name and returns a completed job.
func (q *LocalUnordered) Get(name string) (amboy.Job, bool) {
	q.tasks.RLock()
	defer q.tasks.RUnlock()

	j, ok := q.tasks.m[name]

	return j, ok
}

// Stats returns a statistics object with data about the total number
// of jobs tracked by the queue.
func (q *LocalUnordered) Stats() *amboy.QueueStats {
	s := &amboy.QueueStats{}

	q.tasks.RLock()
	defer q.tasks.RUnlock()

	s.Completed = q.numCompleted
	s.Total = len(q.tasks.m)
	s.Pending = s.Total - s.Completed
	s.Running = q.numStarted - s.Completed

	return s
}

// Complete marks a job as complete, moving it from the in progress
// state to the completed state.
func (q *LocalUnordered) Complete(j amboy.Job) {
	q.grip.Debugf("marking job (%s) as complete", j.ID())
	q.tasks.Lock()
	defer q.tasks.Unlock()
	q.numCompleted++
}

func (q *LocalUnordered) close() {
	q.closeQueue <- true

	q.tasks.Lock()
	defer q.tasks.Unlock()
	q.closed = true
}

// Wait blocks until all jobs have completed and then closes all
// channels and resources associated with the queue. Returns
// immediately if the Queue is already complete.
func (q *LocalUnordered) Wait() {
	for {
		stats := q.Stats()
		q.grip.Debugf("waiting for %d pending jobs (total=%d)", stats.Pending, stats.Total)
		if stats.Pending == 0 {
			break
		}
	}
}

// Close calls Wait() and the cleans up after all resources, including
// open channels and running worker processes.
func (q *LocalUnordered) Close() {
	q.Wait()

	q.close()
	q.runner.Wait()
}

// Closed is true when the queue has successfully exited and false otherwise.
func (q *LocalUnordered) Closed() bool {
	q.tasks.RLock()
	defer q.tasks.RUnlock()

	if q.closed && q.Stats().Pending == 0 {
		return true
	}

	return false
}
