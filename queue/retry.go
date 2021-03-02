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

// BasicRetryHandler implements the amboy.RetryHandler interface. It provides a
// simple component that can be attached to an amboy.RetryableQueue to support
// automatically retrying jobs.
type BasicRetryHandler struct {
	queue         amboy.RetryableQueue
	opts          amboy.RetryHandlerOptions
	pending       map[string]amboy.Job
	started       bool
	wg            sync.WaitGroup
	mu            sync.RWMutex
	cancelWorkers context.CancelFunc
}

// NewBasicRetryHandler initializes and returns an BasicRetryHandler that can be
// used as an amboy.RetryHandler implementation.
func NewBasicRetryHandler(q amboy.RetryableQueue, opts amboy.RetryHandlerOptions) (*BasicRetryHandler, error) {
	if q == nil {
		return nil, errors.New("queue cannot be nil")
	}
	if err := opts.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid options")
	}
	return &BasicRetryHandler{
		queue:   q,
		opts:    opts,
		pending: map[string]amboy.Job{},
	}, nil
}

// Start initiates processing of jobs that need to retry.
func (rh *BasicRetryHandler) Start(ctx context.Context) error {
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

// Started returns whether or not the BasicRetryHandler has started processing
// retryable jobs or not.
func (rh *BasicRetryHandler) Started() bool {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	return rh.started
}

// SetQueue provides a mechanism to swap out amboy.RetryableQueue
// implementations before it has started processing retryable jobs.
func (rh *BasicRetryHandler) SetQueue(q amboy.RetryableQueue) error {
	rh.mu.Lock()
	defer rh.mu.Unlock()
	if rh.started {
		return errors.New("cannot set retry handler queue after it's already been started")
	}
	rh.queue = q
	return nil
}

// Put adds a new job to be retried.
func (rh *BasicRetryHandler) Put(ctx context.Context, j amboy.Job) error {
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

	grip.Debug(message.Fields{
		"message":     "put job in retry handler",
		"job_id":      j.ID(),
		"job_attempt": j.RetryInfo().CurrentAttempt,
		"num_pending": len(rh.pending),
		"queue_id":    rh.queue.ID(),
		"service":     "amboy.queue.retry",
	})

	rh.pending[j.ID()] = j

	return nil
}

// Close finishes processing the remaining retrying jobs and cleans up all
// resources.
func (rh *BasicRetryHandler) Close(ctx context.Context) {
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

func (rh *BasicRetryHandler) waitForJob(ctx context.Context) {
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
			var j amboy.Job

			defer func() {
				if err := recovery.HandlePanicWithError(recover(), nil, "handling job retry"); err != nil {
					fields := message.Fields{
						"message":  "job retry failed",
						"service":  "amboy.queue.retry",
						"queue_id": rh.queue.ID(),
					}
					if j != nil {
						fields["job_id"] = j.ID()
						fields["job_attempt"] = j.RetryInfo().CurrentAttempt
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

			grip.Debug(message.Fields{
				"message":     "retrying job",
				"job_id":      j.ID(),
				"job_attempt": j.RetryInfo().CurrentAttempt,
				"num_pending": len(rh.pending),
				"queue_id":    rh.queue.ID(),
				"service":     "amboy.queue.retry",
			})
			startAt := time.Now()

			if err := rh.handleJob(ctx, j); err != nil && ctx.Err() == nil {
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

			grip.Debug(message.Fields{
				"message":     "finished retrying job",
				"job_id":      j.ID(),
				"job_attempt": j.RetryInfo().CurrentAttempt,
				"num_pending": len(rh.pending),
				"queue_id":    rh.queue.ID(),
				"duration":    time.Since(startAt),
				"service":     "amboy.queue.retry",
			})

			timer.Reset(rh.opts.WorkerCheckInterval)
		}
	}
}

func (rh *BasicRetryHandler) nextJob() amboy.Job {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	for id, found := range rh.pending {
		j := found
		delete(rh.pending, id)
		return j
	}
	return nil
}

func (rh *BasicRetryHandler) handleJob(ctx context.Context, j amboy.Job) error {
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

func (rh *BasicRetryHandler) tryEnqueueJob(ctx context.Context, j amboy.Job) (canRetryOnErr bool, err error) {
	originalInfo := j.RetryInfo()

	canRetry, err := func() (bool, error) {
		// Load the most up-to-date copy in case the cached in-memory job is
		// outdated.
		newJob, ok := rh.queue.GetAttempt(ctx, j.ID(), originalInfo.CurrentAttempt)
		if !ok {
			return true, errors.New("could not find job")
		}

		newInfo := newJob.RetryInfo()
		if !newInfo.ShouldRetry() {
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

func (rh *BasicRetryHandler) prepareNewRetryJob(j amboy.Job) {
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
