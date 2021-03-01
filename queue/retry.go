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
		return nil
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
						rh.mu.Lock()
						rh.pending[j.ID()] = j
						rh.mu.Unlock()
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
					"service":  "amboy.queue.retry",
				}))
				j.AddError(err)

				// Since the job could not retry successfully, do not let the
				// job retry again.
				if err := rh.queue.CompleteRetrying(ctx, j); err != nil {
					grip.Warning(message.WrapError(err, message.Fields{
						"message":  "failed to mark job retry as processed",
						"job_id":   j.ID(),
						"job_type": j.Type().Name,
						"service":  "amboy.queue.retry",
					}))
				}
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

	j.UpdateRetryInfo(amboy.JobRetryOptions{
		Start: utility.ToTimePtr(time.Now()),
	})
	for i := 1; i <= rh.opts.MaxRetryAttempts; i++ {
		if time.Since(startAt) > rh.opts.MaxRetryTime {
			catcher.Errorf("giving up after %s (%d attempts) due to exceeding maximum allowed retry time", rh.opts.MaxRetryTime.String(), i)
			return catcher.Resolve()
		}

		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			canRetry, err := rh.tryEnqueueJob(ctx, j)
			if err != nil {
				catcher.Wrapf(err, "enqueue retry job attempt %d", i)
				grip.Debug(message.WrapError(err, message.Fields{
					"message":        "failed to enqueue job retry",
					"job_id":         j.ID(),
					"queue_id":       rh.queue.ID(),
					"attempt":        i,
					"max_attempts":   rh.opts.MaxRetryAttempts,
					"retry_time":     time.Since(startAt),
					"max_retry_time": rh.opts.MaxRetryTime.String(),
					"can_retry":      canRetry,
					"service":        "amboy.queue.retry",
				}))

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
		// Load the most up-to-date copy in case the cached in-memory job is
		// outdated.
		newJob, ok := rh.queue.GetAttempt(ctx, j.ID(), originalInfo.CurrentAttempt)
		if !ok {
			return true, errors.New("could not find job")
		}

		newInfo := newJob.RetryInfo()
		if !newInfo.Retryable || !newInfo.NeedsRetry {
			return false, errors.New("job in the queue indicates the job does not need to retry anymore")
		}
		if originalInfo.CurrentAttempt+1 > newInfo.GetMaxAttempts() {
			return false, errors.New("job has exceeded its maximum attempt limit")
		}

		lockTimeout := rh.queue.Info().LockTimeout
		grip.DebugWhen(time.Since(j.Status().ModificationTime) > lockTimeout, message.Fields{
			"message":        "received stale retrying job",
			"stale_owner":    j.Status().Owner,
			"stale_mod_time": j.Status().ModificationTime,
			"job_id":         j.ID(),
			"queue_id":       rh.queue.ID(),
			"service":        "amboy.queue.retry",
		})
		// Lock the job so that this retry handler has sole ownership of it.
		if err = j.Lock(rh.queue.ID(), lockTimeout); err != nil {
			return false, errors.Wrap(err, "locking job")
		}
		if err = rh.queue.Save(ctx, j); err != nil {
			return false, errors.Wrap(err, "saving job lock")
		}

		rh.prepareNewRetryJob(newJob)

		err = rh.queue.CompleteRetryingAndPut(ctx, j, newJob)
		if amboy.IsDuplicateJobError(err) || isMongoNoDocumentsMatched(err) {
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
	ri.Start = time.Time{}
	ri.End = time.Time{}
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

func retryAttemptPrefix(attempt int) string {
	return fmt.Sprintf("attempt-%d", attempt)
}
