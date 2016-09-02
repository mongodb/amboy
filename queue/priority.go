package queue

import (
	"errors"
	"sync"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/amboy/queue/driver"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/sometimes"
	"golang.org/x/net/context"
)

type LocalPriorityQueue struct {
	storage  *driver.PriorityStorage
	channel  chan amboy.Job
	runner   amboy.Runner
	counters struct {
		started   int
		completed int
		sync.RWMutex
	}
}

func NewLocalPriorityQueue(workers int) *LocalPriorityQueue {
	q := &LocalPriorityQueue{
		storage: driver.NewPriorityStorage(),
	}

	q.runner = pool.NewLocalWorkers(workers, q)
	return q
}

func (q *LocalPriorityQueue) Put(j amboy.Job) error {
	q.storage.Push(j)

	return nil
}

func (q *LocalPriorityQueue) Get(name string) (amboy.Job, bool) {
	return q.storage.Get(name)
}

func (q *LocalPriorityQueue) Next(ctx context.Context) amboy.Job {
	select {
	case <-ctx.Done():
		return nil
	case job := <-q.channel:
		q.counters.Lock()
		defer q.counters.Unlock()
		q.counters.started++
		return job
	}
}

func (q *LocalPriorityQueue) Started() bool {
	return q.channel != nil
}

func (q *LocalPriorityQueue) Results() <-chan amboy.Job {
	output := make(chan amboy.Job)

	go func() {
		for job := range q.storage.Contents() {
			if job.Completed() {
				output <- job
			}
		}
		close(output)
	}()

	return output
}

func (q *LocalPriorityQueue) Runner() amboy.Runner {
	return q.runner
}

func (q *LocalPriorityQueue) SetRunner(r amboy.Runner) error {
	if q.Started() {
		return errors.New("cannot set")
	}

	q.runner = r

	return nil
}

func (q *LocalPriorityQueue) Stats() amboy.QueueStats {
	stats := amboy.QueueStats{
		Total:   q.storage.Size(),
		Pending: q.storage.Pending(),
	}

	q.counters.RLock()
	defer q.counters.RUnlock()

	stats.Completed = q.counters.completed
	stats.Running = q.counters.started - q.counters.completed

	return stats
}

func (q *LocalPriorityQueue) Complete(ctx context.Context, j amboy.Job) {
	go func() {
		grip.Debugf("marking job (%s) as complete", j.ID())
		q.counters.Lock()
		defer q.counters.Unlock()

		q.counters.completed++
	}()
}

func (q *LocalPriorityQueue) Start(ctx context.Context) error {
	if q.channel != nil {
		return nil

	}
	q.channel = make(chan amboy.Job)

	go q.storage.JobServer(ctx, q.channel)

	q.runner.Start(ctx)

	grip.Info("job server running")

	return nil
}

func (q *LocalPriorityQueue) Wait() {
	for {
		stats := q.Stats()
		grip.DebugWhenf(sometimes.Fifth(),
			"%d jobs complete of %d total", stats.Completed, stats.Total)
		if stats.Total == stats.Completed {
			break
		}
	}
}
