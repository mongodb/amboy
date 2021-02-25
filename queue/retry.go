package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
)

// statsRetryHandler is the same as an amboy.RetryHandler but allows it to
// provide additional runtime statistics on its jobs. This is primarily for
// queue-internal tracking and testing purposes.
type statsRetryHandler interface {
	amboy.RetryHandler
	// stats returns information about the job statistics for an
	// amboy.RetryHandler. Results are reset with each runtime execution.
	stats() retryHandlerStats
}

// retryHandlerStats captures runtime statistics on an amboy.RetryHandler.
type retryHandlerStats struct {
	pending   int
	completed int
	retried   int
}

type retryingJobOutcome struct {
	job     amboy.RetryableJob
	retried bool
}

type basicRetryHandler struct {
	queue         amboy.RetryableQueue
	opts          amboy.RetryHandlerOptions
	pending       map[string]amboy.RetryableJob
	completed     map[string]retryingJobOutcome
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
		queue:     q,
		opts:      opts,
		pending:   map[string]amboy.RetryableJob{},
		completed: map[string]retryingJobOutcome{},
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
		go rh.waitForJob(workerCtx)
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

func (rh *basicRetryHandler) waitForJob(ctx context.Context) {
	defer func() {
		if err := recovery.HandlePanicWithError(recover(), nil, "retry handler worker"); err != nil {
			grip.Error(message.WrapError(err, message.Fields{
				"message":  "retry job worker failed",
				"service":  "amboy.queue.retry",
				"queue_id": rh.queue.ID(),
			}))
			go rh.waitForJob(ctx)
			return
		}

		rh.wg.Done()
	}()

	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			var j amboy.RetryableJob

			defer func() {
				if err := recovery.HandlePanicWithError(recover(), nil, "handling job retry"); err != nil {
					fields := message.Fields{
						"message":  "job retry failed",
						"service":  "amboy.queue.retry",
						"queue_id": rh.queue.ID(),
					}
					if j != nil {
						fields["job_id"] = j.ID()
					}
					grip.Error(message.WrapError(err, fields))
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
			var err error
			if err = rh.handleJob(ctx, j); err != nil && ctx.Err() == nil {
				grip.Error(message.WrapError(err, message.Fields{
					"message":  "could not retry job",
					"queue_id": rh.queue.ID(),
					"job_id":   j.ID(),
				}))
				j.AddError(err)

				// Since the job could not retry successfully, do not let the
				// job retry again.
				if err := rh.queue.CompleteRetrying(ctx, j); err != nil {
					grip.Warning(message.WrapError(err, message.Fields{
						"message":  "failed to mark job retry as processed",
						"job_id":   j.ID(),
						"job_type": j.Type().Name,
					}))
				}
			}

			rh.mu.Lock()
			rh.completed[j.ID()] = retryingJobOutcome{
				job:     j,
				retried: err == nil,
			}
			rh.mu.Unlock()

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
	for i := 1; i <= rh.opts.MaxRetryAttempts; i++ {
		if time.Since(startAt) > rh.opts.MaxRetryTime {
			return errors.Errorf("giving up after %d attempts, %.3f due to maximum retry time", i, rh.opts.MaxRetryTime.Seconds())
		}

		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			canRetry, err := rh.tryEnqueueJob(ctx, j)
			if err != nil {
				catcher.Wrapf(err, "enqueue retry job attempt %d", i)

				if canRetry {
					timer.Reset(rh.opts.RetryBackoff)
					continue
				}

				return catcher.Resolve()
			}

			return nil
		}
	}

	if catcher.HasErrors() {
		return errors.Wrapf(catcher.Resolve(), "exhausted all %d attempts to enqueue retry job without success", rh.opts.MaxRetryAttempts)
	}

	return errors.Errorf("exhausted all %d attempts to enqueue retry job without success", rh.opts.MaxRetryAttempts)
}

func (rh *basicRetryHandler) tryEnqueueJob(ctx context.Context, j amboy.RetryableJob) (canRetryOnErr bool, err error) {
	originalInfo := j.RetryInfo()

	canRetry, err := func() (bool, error) {
		oldInfo := j.RetryInfo()

		// Load the most up-to-date copy in case the cached in-memory job is
		// outdated.
		newJob, ok := rh.queue.GetAttempt(ctx, j.ID(), oldInfo.CurrentAttempt)
		if !ok {
			return true, errors.New("could not find job")
		}

		newInfo := newJob.RetryInfo()
		if !newInfo.Retryable || !newInfo.NeedsRetry {
			return false, errors.New("job in the queue indicates the job does not need to retry anymore")
		}
		if oldInfo.CurrentAttempt+1 > newInfo.GetMaxAttempts() {
			return false, errors.New("job has exceeded its maximum attempt limit")
		}

		if oldInfo != newInfo {
			return false, errors.New("in-memory retry information does not match queue's stored information")
		}

		// Lock the job so that this retry handler has sole ownership of it.
		if err = j.Lock(rh.queue.ID(), rh.queue.Info().LockTimeout); err != nil {
			return false, errors.Wrap(err, "locking job")
		}
		if err = rh.queue.Save(ctx, j); err != nil {
			return false, errors.Wrap(err, "saving job lock")
		}

		rh.prepareNewRetryJob(newJob)

		err = rh.queue.CompleteRetryingAndPut(ctx, j, newJob)
		if amboy.IsDuplicateJobError(err) || errors.Cause(err) == mongo.ErrNoDocuments {
			return false, err
		} else if err != nil {
			return true, errors.Wrap(err, "enqueueing retry job")
		}

		return false, nil
	}()

	if err != nil {
		// Restore the original retry information if it failed to re-enqueue.
		j.UpdateRetryInfo(originalInfo.Options())
	}

	return canRetry, err
}

func (rh *basicRetryHandler) prepareNewRetryJob(j amboy.RetryableJob) {
	ri := j.RetryInfo()
	ri.NeedsRetry = false
	ri.CurrentAttempt++
	j.UpdateRetryInfo(ri.Options())

	ti := j.TimeInfo()
	waitUntil := ti.WaitUntil
	if ri.WaitUntil != 0 {
		waitUntil = time.Now().Add(ri.WaitUntil)
	}
	dispatchBy := ti.DispatchBy
	if ri.DispatchBy != 0 {
		dispatchBy = time.Now().Add(ri.DispatchBy)
	}
	j.SetTimeInfo(amboy.JobTimeInfo{
		DispatchBy: dispatchBy,
		WaitUntil:  waitUntil,
		MaxTime:    ti.MaxTime,
	})

	j.SetStatus(amboy.JobStatusInfo{})
}

func (rh *basicRetryHandler) stats() retryHandlerStats {
	rh.mu.RLock()
	defer rh.mu.RUnlock()

	var numRetried int
	for _, outcome := range rh.completed {
		if outcome.retried {
			numRetried++
		}
	}

	return retryHandlerStats{
		pending:   len(rh.pending),
		retried:   numRetried,
		completed: len(rh.completed),
	}
}

func retryAttemptPrefix(attempt int) string {
	return fmt.Sprintf("attempt-%d", attempt)
}
