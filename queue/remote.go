package queue

import (
	"sync"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/amboy/queue/driver"
	"github.com/pkg/errors"
	"github.com/tychoish/grip"
	"golang.org/x/net/context"
)

// RemoteUnordered are queues that use a Driver as backend for job
// storage and processing and do not impose any additional ordering
// beyond what's provided by the driver.
type RemoteUnordered struct {
	started bool
	driver  driver.Driver
	channel chan amboy.Job
	runner  amboy.Runner
	mutex   sync.RWMutex
}

// NewRemoteUnordered returns a queue that has been initialized with a
// local worker pool Runner instance of the specified size.
func NewRemoteUnordered(size int) *RemoteUnordered {
	q := &RemoteUnordered{
		channel: make(chan amboy.Job),
	}
	q.runner = pool.NewLocalWorkers(size, q)

	grip.Infof("creating new remote job queue with %d workers", size)

	return q
}

// Put adds a Job to the queue. It is generally an error to add the
// same job to a queue more than once, but this depends on the
// implementation of the underlying driver.
func (q *RemoteUnordered) Put(j amboy.Job) error {
	return q.driver.Put(j)
}

// Get retrieves a job from the queue's storage. The second value
// reflects the existence of a job of that name in the queue's
// storage.
func (q *RemoteUnordered) Get(name string) (amboy.Job, bool) {
	if q.driver == nil {
		return nil, false
	}

	job, err := q.driver.Get(name)
	if err != nil {
		grip.Debug(err)
		return nil, false
	}

	return job, true
}

func (q *RemoteUnordered) jobServer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			job := q.driver.Next()

			if job == nil {
				continue
			}

			q.channel <- job
		}
	}
}

// Next returns a Job from the queue. Returns a nil Job object if the
// context is canceled. The operation is blocking until an
// undispatched, unlocked job is available. This operation takes a job
// lock.
func (q *RemoteUnordered) Next(ctx context.Context) amboy.Job {
	start := time.Now()
	count := 0
	for {
		count++
		select {
		case <-ctx.Done():
			return nil
		case job := <-q.channel:
			lock, err := q.driver.GetLock(ctx, job)
			if err != nil {
				grip.Warning(err)
				continue
			}

			if lock.IsLocked(ctx) {
				continue
			}

			lock.Lock(ctx)
			grip.Debugf("returning job from remote source, count = %d; duration = %s",
				count, time.Since(start))
			return job
		}
	}

}

// Started reports if the queue has begun processing jobs.
func (q *RemoteUnordered) Started() bool {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return q.started
}

// Complete takes a context and, asynchronously, marks the job
// complete, in the queue.
func (q *RemoteUnordered) Complete(ctx context.Context, j amboy.Job) {
	go func() {
		const retryInterval = 1 * time.Second
		timer := time.NewTimer(retryInterval)
		defer timer.Stop()

		for {
			lock, err := q.driver.GetLock(ctx, j)
			if err != nil {
				grip.Errorf("problem getting lock for job '%s': %+v", j.ID(), err)

				timer.Reset(retryInterval)
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
					continue
				}
			}

			if lock.IsLockedElsewhere(ctx) {
				continue
			}

			if !lock.IsLocked(ctx) {
				lock.Lock(ctx)
			}

			if ctx.Err() != nil {
				return
			}
			err = q.driver.Save(j)
			if err == nil {
				lock.Unlock(ctx)
				return
			}

			timer.Reset(retryInterval)
			grip.Errorf("problem marking %s complete: %+v", j.ID(), err)
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				continue
			}
		}
	}()
}

// Results provides a generator that iterates all completed jobs.
func (q *RemoteUnordered) Results() <-chan amboy.Job {
	output := make(chan amboy.Job)
	go func() {
		defer close(output)
		for j := range q.driver.Jobs() {
			if j.Completed() {
				output <- j
			}

		}
	}()
	return output
}

// Stats returns a amboy.QueueStats object that reflects the progress
// jobs in the queue.
func (q *RemoteUnordered) Stats() amboy.QueueStats {
	dStats := q.driver.Stats()
	output := amboy.QueueStats{
		Running:   dStats.Locked,
		Completed: dStats.Complete,
		Pending:   dStats.Pending,
		Total:     dStats.Total,
	}
	grip.Debugf("--> STATS REPORT:\n\tDRIVER=%+v\n\tQUEUE=%+v", dStats, output)
	return output
}

// Runner returns (a pointer generally) to the instances' embedded
// amboy.Runner instance. Typically used to call the runner's close
// method.
func (q *RemoteUnordered) Runner() amboy.Runner {
	return q.runner
}

// SetRunner allows callers to inject alternate runner implementations
// before starting the queue. After the queue is started it is an
// error to use SetRunner.
func (q *RemoteUnordered) SetRunner(r amboy.Runner) error {
	if q.runner.Started() {
		return errors.New("cannot change runners after starting")
	}

	q.runner = r
	return nil
}

// Driver provides access to the embedded driver instance which
// provides access to the Queue's persistence layer. This method is
// not part of the amboy.Queue interface.
func (q *RemoteUnordered) Driver() driver.Driver {
	return q.driver
}

// SetDriver allows callers to inject at runtime alternate driver
// instances. It is an error to change Driver instances after starting
// a queue. This method is not part of the amboy.Queue interface.
func (q *RemoteUnordered) SetDriver(d driver.Driver) error {
	if q.Started() {
		return errors.New("cannot change drivers after starting queue")
	}

	q.driver = d
	return nil
}

// Start initiates the job dispatching and prcessing functions of the
// queue. If the queue is started this is a noop, however, if the
// driver or runner are not initialized, this operation returns an
// error. To release the resources created when starting the queue,
// cancel the context used when starting the queue.
func (q *RemoteUnordered) Start(ctx context.Context) error {
	if q.Started() {
		return nil
	}

	if q.driver == nil {
		return errors.New("cannot start queue with an uninitialized driver")
	}

	if q.runner == nil {
		return errors.New("cannot start queue with an uninitialized runner")
	}

	err := q.runner.Start(ctx)
	if err != nil {
		return errors.Wrap(err, "problem starting runner in remote queue")
	}

	err = q.driver.Open(ctx)
	if err != nil {
		return errors.Wrap(err, "problem starting driver in remote queue")
	}

	go q.jobServer(ctx)
	q.mutex.Lock()
	q.started = true
	q.mutex.Unlock()

	return nil
}

// Wait blocks until there are no pending jobs in the queue.
func (q *RemoteUnordered) Wait() {
	for {
		stats := q.Stats()
		if stats.Total == stats.Completed {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}
