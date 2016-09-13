package queue

import (
	"sync"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/amboy/queue/driver"
	"github.com/pkg/errors"
	"github.com/tychoish/grip"
	"golang.org/x/net/context"
)

type LocalLimitedSize struct {
	channel  chan amboy.Job
	results  *driver.CappedResultStorage
	runner   amboy.Runner
	capacity int
	counters struct {
		queued    map[string]struct{}
		total     int
		started   int
		completed int
		sync.RWMutex
	}
}

func NewLocalLimitedSize(workers, capacity int) *LocalLimitedSize {
	q := &LocalLimitedSize{
		results:  driver.NewCappedResultStorage(capacity),
		capacity: capacity,
	}
	q.runner = pool.NewLocalWorkers(workers, q)
	q.counters.queued = make(map[string]struct{})

	return q
}

func (q *LocalLimitedSize) Put(j amboy.Job) error {
	name := j.ID()

	if _, ok := q.results.Get(name); ok {
		return errors.Errorf("cannot dispatch '%s', already complete", name)
	}

	q.counters.Lock()
	defer q.counters.Unlock()

	if _, ok := q.counters.queued[name]; ok {
		return errors.Errorf("cannot dispatch '%s', already in progress.", name)
	}

	select {
	case q.channel <- j:
		q.counters.total++
		q.counters.queued[j.ID()] = struct{}{}

		return nil
	default:
		return errors.Errorf("queue not open. could not add %s", j.ID())
	}
}

func (q *LocalLimitedSize) Get(name string) (amboy.Job, bool) {
	return q.results.Get(name)
}

func (q *LocalLimitedSize) Next(ctx context.Context) amboy.Job {
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

func (q *LocalLimitedSize) Started() bool {
	return q.channel != nil
}

func (q *LocalLimitedSize) Results() <-chan amboy.Job {
	return q.results.Contents()
}

func (q *LocalLimitedSize) Runner() amboy.Runner {
	return q.runner
}

func (q *LocalLimitedSize) SetRunner(r amboy.Runner) error {
	if q.Started() {
		return errors.New("cannot set runner on started queue")
	}

	q.runner = r

	return nil
}

func (q *LocalLimitedSize) Stats() amboy.QueueStats {
	q.counters.RLock()
	defer q.counters.RUnlock()

	return amboy.QueueStats{
		Total:     q.counters.total,
		Completed: q.counters.completed,
		Running:   q.counters.started - q.counters.completed,
		Pending:   len(q.channel),
	}
}

func (q *LocalLimitedSize) Complete(ctx context.Context, j amboy.Job) {
	go func() {
		name := j.ID()
		grip.Debugf("marking job (%s) as complete", name)
		q.counters.Lock()
		defer q.counters.Unlock()

		q.counters.completed++
		delete(q.counters.queued, name)
		q.results.Add(j)
	}()
}

func (q *LocalLimitedSize) Start(ctx context.Context) error {
	if q.channel != nil {
		return nil
	}

	q.channel = make(chan amboy.Job, q.capacity)

	err := q.runner.Start(ctx)

	if err != nil {
		return err
	}

	grip.Info("job server running")
	return nil
}

func (q *LocalLimitedSize) Wait() {
	for {
		stats := q.Stats()
		grip.Debugf("%d jobs complete of %d total", stats.Completed, stats.Total)
		if stats.Total == stats.Completed {
			break
		}
	}
}
