package queue

import (
	"sync"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/amboy/queue/remote"
	"github.com/pkg/errors"
	"github.com/tychoish/grip"
	"golang.org/x/net/context"
)

type RemoteUnordered struct {
	started bool
	driver  remote.Driver
	channel chan amboy.Job
	runner  amboy.Runner
	mutex   sync.RWMutex
}

func NewRemoteUnordered(size int) *RemoteUnordered {
	q := &RemoteUnordered{
		channel: make(chan amboy.Job),
	}
	q.runner = pool.NewLocalWorkers(size, q)

	grip.Infof("creating new remote job queue with %d workers", size)

	return q
}

func (q *RemoteUnordered) Put(j amboy.Job) error {
	return q.driver.Put(j)
}

func (q *RemoteUnordered) Get(name string) (amboy.Job, bool) {
	job, err := q.driver.Get(name)
	if err != nil {
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
			grip.Infof("returning job from remote source, count = %d; duration = %s",
				count, time.Since(start))
			return job
		}
	}

}

func (q *RemoteUnordered) Started() bool {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return q.started
}

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

func (q *RemoteUnordered) Runner() amboy.Runner {
	return q.runner
}

func (q *RemoteUnordered) SetRunner(r amboy.Runner) error {
	if q.runner.Started() {
		return errors.New("cannot change runners after starting")
	}

	q.runner = r
	return nil
}

func (q *RemoteUnordered) Driver() remote.Driver {
	return q.driver
}

func (q *RemoteUnordered) SetDriver(d remote.Driver) error {
	if q.Started() {
		return errors.New("cannot change drivers after starting queue")
	}

	q.driver = d
	return nil
}

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

func (q *RemoteUnordered) Wait() {
	for {
		stats := q.Stats()

		if stats.Pending == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}
