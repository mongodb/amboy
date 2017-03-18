package pool

import (
	"github.com/mongodb/amboy"
	"github.com/pkg/errors"
	"github.com/mongodb/grip"
	"golang.org/x/net/context"
)

// SingleRunner is an implementation of of the amboy.Runner interface
// that hosts runs all tasks on one, and only one worker. Useful for
// testing the system with a different task executor.
type SingleRunner struct {
	closer   chan struct{}
	canceler context.CancelFunc
	queue    amboy.Queue
}

// NewSingleRunner returns a new single-worker pool.
func NewSingleRunner() *SingleRunner {
	return &SingleRunner{
		closer: make(chan struct{}),
	}
}

// Started returns true when the Runner has begun executing tasks. For
// LocalWorkers this means that workers are running.
func (r *SingleRunner) Started() bool {
	return r.canceler != nil
}

// SetQueue allows callers to inject alternate amboy.Queue objects into
// constructed Runner objects. Returns an error if the Runner has
// started.
func (r *SingleRunner) SetQueue(q amboy.Queue) error {
	if r.canceler != nil {
		return errors.New("cannot add new queue after starting a runner")
	}

	r.queue = q
	return nil
}

// Start takes a context and starts the internal worker and job
// processing thread. You can terminate the work of the Runner by
// canceling the context, or with the close method. Returns an error
// if the queue is not set. If the Runner is already running, Start is
// a no-op.
func (r *SingleRunner) Start(ctx context.Context) error {
	if r.canceler != nil {
		return nil
	}

	if r.queue == nil {
		return errors.New("cannot start runner without a queue set")
	}

	workerCtx, cancel := context.WithCancel(ctx)
	r.canceler = cancel

	jobs := startWorkerServer(workerCtx, r.queue)

	go func() {
		worker(workerCtx, jobs, r.queue)
		r.closer <- struct{}{}
		close(r.closer)

		grip.Info("worker process complete")
	}()

	grip.Info("started single queue worker")

	return nil
}

// Close terminates the work on the Runner. If a job is executing, the
// job will complete and the process will terminate before beginning a
// new job. If the queue has not started, Close is a no-op.
func (r *SingleRunner) Close() {
	if r.canceler != nil {
		r.canceler()
		<-r.closer
	}
}
