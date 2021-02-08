package queue

import (
	"context"
	"sync"
	"time"

	"github.com/mongodb/amboy"
	"github.com/pkg/errors"
)

// kim: NOTE: maybe it's good enough to just use a local amboy.Runner from
// pool/local.go, then attach specific work as a job to execute.

type retryHandler struct {
	queue         amboy.Queue
	opts          amboy.RetryHandlerOptions
	pending       map[string]amboy.Job
	started       bool
	wg            sync.WaitGroup
	mu            sync.RWMutex
	cancelWorkers context.CancelFunc
}

func newRetryHandler(q amboy.Queue, opts amboy.RetryHandlerOptions) amboy.RetryHandler {
	return &retryHandler{
		queue:   q,
		opts:    opts,
		pending: map[string]amboy.Job{},
	}
}

func (rh *retryHandler) Start(ctx context.Context) error {
	rh.mu.Lock()
	defer rh.mu.Unlock()

	rh.started = true

	workerCtx, workerCancel := context.WithCancel(ctx)
	rh.cancelWorkers = workerCancel
	for i := 0; i < rh.opts.NumWorkers; i++ {
		rh.wg.Add(1)
		go func() {
			// kim: TODO: need panic handler
			defer rh.wg.Done()
			rh.waitForJob(workerCtx)
		}()
	}
	return nil
}

func (rh *retryHandler) Started() bool {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	return rh.started
}

func (rh *retryHandler) SetQueue(q amboy.Queue) error {
	rh.mu.Lock()
	defer rh.mu.Unlock()
	if rh.started {
		return errors.New("cannot set retry handler queue after it's already been started")
	}
	rh.queue = q
	return nil
}

func (rh *retryHandler) Put(ctx context.Context, j amboy.Job) error {
	if j == nil {
		return errors.New("cannot retry a nil job")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	rh.mu.Lock()
	defer rh.mu.Unlock()

	if _, ok := rh.pending[j.ID()]; ok {
		return errors.Errorf("cannot retry job %s multiple times", j.ID())
	}

	rh.pending[j.ID()] = j

	return nil
}

func (rh *retryHandler) Close(ctx context.Context) {
	rh.mu.Lock()
	defer rh.mu.Unlock()
	if rh.cancelWorkers != nil {
		rh.cancelWorkers()
	}
	rh.started = false

	rh.wg.Wait()
}

func (rh *retryHandler) waitForJob(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			j := rh.nextJob()
			if j == nil {
				continue
			}
			rh.handleJob(ctx, j)
		}
	}
}

func (rh *retryHandler) nextJob() amboy.Job {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	for id, found := range rh.pending {
		j := found
		delete(rh.pending, id)
		return j
	}
	return nil
}

// kim: TODO: spin off retry workers from retry handler since it could be quite
// complicated.
func (rh *retryHandler) handleJob(ctx context.Context, j amboy.Job) error {
	startAt := time.Now()
	for i := 0; i < rh.opts.MaxRetryAttempts; i++ {
		if time.Since(startAt) > rh.opts.MaxRetryTime {
			return errors.Errorf("giving up after %d attempts, %f seconds due to maximum retry time", i, rh.opts.MaxRetryTime.Seconds())
		}
		if err := rh.queue.Put(ctx, j); err != nil {
			continue
		}
		return nil
	}
	return errors.Errorf("exhausted all %d retry attempts", rh.opts.MaxRetryAttempts)
}

type retryWorker struct {
}
