package queue

import (
	"sync"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/queue/driver"
	"github.com/pkg/errors"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/message"
	"golang.org/x/net/context"
)

type remoteBase struct {
	started bool
	driver  driver.Driver
	channel chan amboy.Job
	blocked map[string]struct{}
	runner  amboy.Runner
	mutex   sync.RWMutex
}

func newRemoteBase() *remoteBase {
	return &remoteBase{
		channel: make(chan amboy.Job),
		blocked: make(map[string]struct{}),
	}
}

// Put adds a Job to the queue. It is generally an error to add the
// same job to a queue more than once, but this depends on the
// implementation of the underlying driver.
func (q *remoteBase) Put(j amboy.Job) error {
	return q.driver.Put(j)
}

// Get retrieves a job from the queue's storage. The second value
// reflects the existence of a job of that name in the queue's
// storage.
func (q *remoteBase) Get(name string) (amboy.Job, bool) {
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

func (q *remoteBase) jobServer(ctx context.Context) {
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

// Started reports if the queue has begun processing jobs.
func (q *remoteBase) Started() bool {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return q.started
}

// Complete takes a context and, asynchronously, marks the job
// complete, in the queue.
func (q *remoteBase) Complete(ctx context.Context, j amboy.Job) {
	const retryInterval = time.Second
	timer := time.NewTimer(0)
	defer timer.Stop()

	q.mutex.Lock()
	delete(q.blocked, j.ID())
	q.mutex.Unlock()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			lock, err := q.driver.GetLock(ctx, j)
			if err != nil {
				grip.Debugf("problem getting lock for job '%s': %+v", j.ID(), err)
				timer.Reset(retryInterval)
				continue
			}

			if lock.IsLockedElsewhere(ctx) {
				timer.Reset(retryInterval)
				continue
			}

			if !lock.IsLocked(ctx) {
				lock.Lock(ctx)
			}

			if ctx.Err() != nil {
				return
			}

			if err = q.driver.Save(j); err != nil {
				grip.Debugf("problem persisting job '%s', %+v", j.ID(), err)
				timer.Reset(retryInterval)
				continue
			}

			lock.Unlock(ctx)
			return
		}

	}
}

// Results provides a generator that iterates all completed jobs.
func (q *remoteBase) Results() <-chan amboy.Job {
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
func (q *remoteBase) Stats() amboy.QueueStats {
	dStats := q.driver.Stats()
	output := amboy.QueueStats{
		Running:   dStats.Locked,
		Completed: dStats.Complete,
		Pending:   dStats.Pending,
		Total:     dStats.Total,
	}

	q.mutex.RLock()
	defer q.mutex.RUnlock()

	grip.Debug(message.Fields{
		"msg":       "remote queue stats",
		"running":   dStats.Locked,
		"completed": dStats.Complete,
		"pending":   dStats.Pending,
		"total":     dStats.Total,
		"blocked":   len(q.blocked),
	})

	output.Blocked = len(q.blocked)

	return output
}

// Runner returns (a pointer generally) to the instances' embedded
// amboy.Runner instance. Typically used to call the runner's close
// method.
func (q *remoteBase) Runner() amboy.Runner {
	return q.runner
}

// SetRunner allows callers to inject alternate runner implementations
// before starting the queue. After the queue is started it is an
// error to use SetRunner.
func (q *remoteBase) SetRunner(r amboy.Runner) error {
	if q.runner != nil && q.runner.Started() {
		return errors.New("cannot change runners after starting")
	}

	q.runner = r
	return nil
}

// Driver provides access to the embedded driver instance which
// provides access to the Queue's persistence layer. This method is
// not part of the amboy.Queue interface.
func (q *remoteBase) Driver() driver.Driver {
	return q.driver
}

// SetDriver allows callers to inject at runtime alternate driver
// instances. It is an error to change Driver instances after starting
// a queue. This method is not part of the amboy.Queue interface.
func (q *remoteBase) SetDriver(d driver.Driver) error {
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
func (q *remoteBase) Start(ctx context.Context) error {
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

func (q *remoteBase) addBlocked(n string) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.blocked[n] = struct{}{}
}
