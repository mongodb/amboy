package pool

import (
	"errors"
	"sync"

	"github.com/mongodb/amboy"
	"github.com/tychoish/grip"
)

type QueueTester struct {
	started     bool
	isComplete  bool
	pool        amboy.Runner
	numComplete int
	toProcess   chan amboy.Job
	storage     map[string]amboy.Job

	*sync.RWMutex
}

func NewQueueTester(p amboy.Runner) *QueueTester {
	q := NewQueueTesterInstance()

	_ = p.SetQueue(q)
	q.pool = p

	return q
}

// Separate constructor for the object so we can avoid the side
// effects of the extra SetQueue for tests where that doesn't make
// sense.
func NewQueueTesterInstance() *QueueTester {
	return &QueueTester{
		toProcess: make(chan amboy.Job, 10),
		storage:   make(map[string]amboy.Job),
		RWMutex:   &sync.RWMutex{},
	}
}

func (q *QueueTester) Put(j amboy.Job) error {
	q.toProcess <- j
	q.storage[j.ID()] = j
	return nil
}

func (q *QueueTester) Get(name string) (amboy.Job, bool) {
	job, ok := q.storage[name]
	return job, ok
}

func (q *QueueTester) Started() bool {
	q.RLock()
	defer q.RUnlock()

	return q.started
}

func (q *QueueTester) Complete(j amboy.Job) {
	q.Lock()
	defer q.Unlock()

	q.numComplete++

	return
}

func (q *QueueTester) Stats() *amboy.QueueStats {
	q.RLock()
	defer q.RUnlock()

	return &amboy.QueueStats{
		Running:   len(q.storage) - len(q.toProcess),
		Completed: q.numComplete,
		Pending:   len(q.toProcess),
		Total:     len(q.storage),
	}
}

func (q *QueueTester) Runner() amboy.Runner {
	return q.pool
}

func (q *QueueTester) SetRunner(r amboy.Runner) error {
	if q.Started() {
		return errors.New("cannot set runner in a started pool")
	}
	q.pool = r
	return nil
}

func (q *QueueTester) Next() (amboy.Job, error) {
	job, ok := <-q.toProcess

	if !ok {
		return nil, errors.New("all work is complete")
	}

	return job, nil
}

func (q *QueueTester) Start() error {
	if q.Started() {
		return nil
	}

	err := q.pool.Start()
	if err != nil {
		return err
	}

	q.Lock()
	defer q.Unlock()

	q.started = true
	return nil
}

func (q *QueueTester) Results() <-chan amboy.Job {
	output := make(chan amboy.Job, q.Stats().Completed+3)
	defer close(output)

	go func(m map[string]amboy.Job) {
		for _, job := range m {
			if job.Completed() {
				output <- job
			}
		}
	}(q.storage)

	return output
}

func (q *QueueTester) Wait() {
	if len(q.toProcess) == 0 {
		return
	}

	for {
		grip.Debugf("%+v\n", q.Stats())
		if len(q.storage) == q.numComplete {
			break
		}
	}
}

func (q *QueueTester) Close() {
	q.Lock()
	defer q.Unlock()

	close(q.toProcess)
	q.isComplete = true
}

// Closed is true when the queue has successfully exited.
func (q *QueueTester) Closed() bool {
	q.RLock()
	defer q.RUnlock()
	return q.isComplete
}
