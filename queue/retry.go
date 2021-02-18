package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
)

type basicRetryHandler struct {
	queue         amboy.RetryableQueue
	opts          amboy.RetryHandlerOptions
	pending       map[string]amboy.RetryableJob
	started       bool
	wg            sync.WaitGroup
	mu            sync.RWMutex
	cancelWorkers context.CancelFunc
}

func newBasicRetryHandler(q amboy.RetryableQueue, opts amboy.RetryHandlerOptions) (*basicRetryHandler, error) {
	if q == nil {
		return nil, errors.New("queue cannot be nil")
	}
	if err := opts.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid options")
	}
	return &basicRetryHandler{
		queue:   q,
		opts:    opts,
		pending: map[string]amboy.RetryableJob{},
	}, nil
}

func (rh *basicRetryHandler) Start(ctx context.Context) error {
	rh.mu.Lock()
	defer rh.mu.Unlock()

	if rh.started {
		return nil
	}

	if rh.queue == nil {
		return errors.New("cannot start retry handler without a queue")
	}

	rh.started = true

	workerCtx, workerCancel := context.WithCancel(ctx)
	rh.cancelWorkers = workerCancel
	for i := 0; i < rh.opts.NumWorkers; i++ {
		rh.wg.Add(1)
		go func() {
			defer func() {
				if err := recovery.HandlePanicWithError(recover(), nil, "retry handler worker"); err != nil {
					go func() {
						grip.Error(message.WrapError(rh.waitForJob(workerCtx), message.Fields{
							"message":  "retry job worker failed",
							"service":  "amboy.queue.retry",
							"queue_id": rh.queue.ID(),
						}))
					}()
					return
				}
				rh.wg.Done()
			}()

			grip.Error(rh.waitForJob(workerCtx))
		}()
	}
	return nil
}

func (rh *basicRetryHandler) Started() bool {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	return rh.started
}

func (rh *basicRetryHandler) SetQueue(q amboy.RetryableQueue) error {
	rh.mu.Lock()
	defer rh.mu.Unlock()
	if rh.started {
		return errors.New("cannot set retry handler queue after it's already been started")
	}
	rh.queue = q
	return nil
}

func (rh *basicRetryHandler) Put(ctx context.Context, j amboy.RetryableJob) error {
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

func (rh *basicRetryHandler) Close(ctx context.Context) {
	rh.mu.Lock()
	if !rh.started {
		rh.mu.Unlock()
		return
	}
	if rh.cancelWorkers != nil {
		rh.cancelWorkers()
	}
	rh.started = false
	rh.mu.Unlock()

	rh.wg.Wait()
}

func (rh *basicRetryHandler) waitForJob(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			var j amboy.RetryableJob
			defer func() {
				if err := recovery.HandlePanicWithError(recover(), nil, "handling job retry"); err != nil {
					if j != nil {
						rh.pending[j.ID()] = j
					}
				}
			}()
			j = rh.nextJob()
			if j == nil {
				timer.Reset(rh.opts.WorkerCheckInterval)
				continue
			}
			if err := rh.handleJob(ctx, j); err != nil && ctx.Err() == nil {
				grip.Error(message.WrapError(err, message.Fields{
					"message":  "could not retry job",
					"queue_id": rh.queue.ID(),
					"job_id":   j.ID(),
				}))
				j.AddError(err)
			}

			if err := rh.tryMarkProcessed(ctx, j); err != nil {
				grip.Error(message.WrapError(err, message.Fields{
					"message":  "could not mark retryable job as processed",
					"queue_id": rh.queue.ID(),
					"job_id":   j.ID(),
				}))
			}

			timer.Reset(rh.opts.WorkerCheckInterval)
		}
	}
}

func (rh *basicRetryHandler) nextJob() amboy.RetryableJob {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	for id, found := range rh.pending {
		j := found
		delete(rh.pending, id)
		return j
	}
	return nil
}

func (rh *basicRetryHandler) handleJob(ctx context.Context, j amboy.RetryableJob) error {
	startAt := time.Now()
	catcher := grip.NewBasicCatcher()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for i := 0; i < rh.opts.MaxRetryAttempts; i++ {
		if time.Since(startAt) > rh.opts.MaxRetryTime {
			return errors.Errorf("giving up after %d attempts, %f seconds due to maximum retry time", i, rh.opts.MaxRetryTime.Seconds())
		}

		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			if err := rh.tryEnqueueJob(ctx, j); err != nil {
				catcher.Wrapf(err, "enqueue retry job attempt %d", i)
				timer.Reset(rh.opts.RetryBackoff)
				continue
			}
			return nil
		}
	}

	if catcher.HasErrors() {
		return errors.Wrapf(catcher.Resolve(), "exhausted all %d attempts to enqueue retry job without success", rh.opts.MaxRetryAttempts)
	}

	return errors.Errorf("exhausted all %d attempts to enqueue retry job without success", rh.opts.MaxRetryAttempts)
}

func (rh *basicRetryHandler) tryEnqueueJob(ctx context.Context, j amboy.RetryableJob) error {
	originalInfo := j.RetryInfo()
	err := func() error {
		oldInfo := j.RetryInfo()

		// Load the most up-to-date copy in case the cached in-memory job is
		// outdated.
		newJob, ok := rh.queue.GetAttempt(ctx, j.ID(), oldInfo.CurrentAttempt)
		if !ok {
			return errors.New("could not find job")
		}

		newInfo := newJob.RetryInfo()
		if !newInfo.Retryable || !newInfo.NeedsRetry {
			return nil
		}

		// TODO (EVG-13584): add job retry locking mechanism (similar to
		// (amboy.Job).Lock()) to ensure that this thread on this host has sole
		// ownership of the job.

		if oldInfo != newInfo {
			return errors.New("precondition failed: in-memory retry information does not match queue's stored information")
		}

		newInfo.NeedsRetry = false
		newInfo.CurrentAttempt++
		newJob.UpdateRetryInfo(newInfo.Options())

		oldInfo.NeedsRetry = false
		j.UpdateRetryInfo(oldInfo.Options())

		err := rh.queue.SaveAndPut(ctx, j, newJob)
		if amboy.IsDuplicateJobError(err) {
			// The new job is already in the queue, do nothing.
			return nil
		} else if err != nil {
			return errors.Wrap(err, "enqueueing retry job")
		}

		return nil
	}()

	if err != nil {
		// Restore the original retry information if it failed to re-enqueue.
		j.UpdateRetryInfo(originalInfo.Options())
	}

	return err
}

// tryMarkProcessed attempts to mark the job as processed, so that it does not
// attempt to retry again.
func (rh *basicRetryHandler) tryMarkProcessed(ctx context.Context, j amboy.RetryableJob) error {
	const (
		attemptInterval = time.Second
		maxAttempts     = 10
	)

	timer := time.NewTimer(0)
	defer timer.Stop()

	var attempt int
	catcher := grip.NewBasicCatcher()
	for {
		select {
		case <-ctx.Done():
			catcher.Wrapf(ctx.Err(), "giving up on attempt %d", attempt+1)
			return catcher.Resolve()
		case <-timer.C:
			j.UpdateRetryInfo(amboy.JobRetryOptions{
				NeedsRetry: utility.FalsePtr(),
			})

			if err := rh.queue.Save(ctx, j); err != nil {
				if attempt+1 >= maxAttempts {
					return errors.Wrapf(catcher.Resolve(), "giving up after attempt %d", maxAttempts)
				}

				catcher.Wrapf(err, "attempt %d", attempt)
				attempt++
				timer.Reset(attemptInterval)

				continue
			}

			return nil
		}
	}
}

func retryAttemptPrefix(attempt int) string {
	return fmt.Sprintf("attempt-%d", attempt)
}
