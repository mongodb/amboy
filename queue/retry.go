package queue

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
)

type retryHandler struct {
	// kim: TODO: should add a RetryableQueue interface with an extra PutRetry()
	// method. That way, it's optional to implement the retryability features.
	queue         amboy.Queue
	opts          amboy.RetryHandlerOptions
	pending       map[string]amboy.Job
	started       bool
	wg            sync.WaitGroup
	mu            sync.RWMutex
	cancelWorkers context.CancelFunc
}

func newRetryHandler(q amboy.Queue, opts amboy.RetryHandlerOptions) (amboy.RetryHandler, error) {
	if q == nil {
		return nil, errors.New("queue cannot be nil")
	}
	if err := opts.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid options")
	}
	return &retryHandler{
		queue:   q,
		opts:    opts,
		pending: map[string]amboy.Job{},
	}, nil
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
			defer func() {
				if err := recovery.HandlePanicWithError(recover(), nil, "retry handler worker"); err != nil {
					go rh.waitForJob(workerCtx)
					return
				}
				rh.wg.Done()
			}()

			grip.Error(rh.waitForJob(workerCtx))
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
			return nil
		default:
			var j amboy.Job
			defer func() {
				if err := recovery.HandlePanicWithError(recover(), nil, "handling job retry"); err != nil {
					if j != nil {
						rh.pending[j.ID()] = j
					}
				}
			}()
			// TODO (EVG-13540): make this use a channel instead checking in a
			// no-op loop.
			j = rh.nextJob()
			if j == nil {
				continue
			}
			if err := rh.handleJob(ctx, j); err != nil && ctx.Err() == nil {
				grip.Error(message.WrapError(err, message.Fields{
					"message":  "could not retry job",
					"queue_id": rh.queue.ID(),
					"job_id":   j.ID(),
				}))
			}

			// Once the job has been processed (either success or failure),
			// mark it as processed so it does not attempt to retry again.
			j.UpdateRetryInfo(amboy.JobRetryOptions{
				Retryable:  utility.FalsePtr(),
				NeedsRetry: utility.FalsePtr(),
			})
			// TODO (EVG-13540): this has to retry this op until success,
			// like the theoretical infinite loop in queue.Complete(). It should
			// also be done in a transaction-like way when the new job is
			// inserted, so that the swap occurs safely.
			if err := rh.queue.Save(ctx, j); err != nil {
				grip.Critical(message.WrapError(err, message.Fields{}))
			}
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

func (rh *retryHandler) handleJob(ctx context.Context, j amboy.Job) error {
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
			catcher.Wrapf(rh.tryEnqueueRetryJob(ctx, j), "enqueue retry job attempt %d", i)
			// TODO (EVG-13540): consider adding jitter.
			timer.Reset(rh.opts.RetryBackoff)
		}
	}

	if catcher.HasErrors() {
		return errors.Wrapf(catcher.Resolve(), "exhausted all %d attempts to enqueue retry job without success", rh.opts.MaxRetryAttempts)
	}

	return errors.Errorf("exhausted all %d attempts to enqueue retry job without success", rh.opts.MaxRetryAttempts)
}

func (rh *retryHandler) tryEnqueueRetryJob(ctx context.Context, j amboy.Job) error {
	// Load the most up-to-date copy in case the cached in-memory job is
	// outdated.
	// TODO (EVG-13540): determine if this will be an expensive query or not.
	newJob, ok := rh.queue.Get(ctx, j.ID())
	if !ok {
		return errors.New("could not find job")
	}

	if !newJob.RetryInfo().Retryable || !newJob.RetryInfo().NeedsRetry {
		return nil
	}

	// TODO (EVG-13584): add job retry locking mechanism (similar to
	// (amboy.Job).Lock()) to ensure that this thread on this host has sole
	// ownership of the job.

	info := newJob.RetryInfo()
	id := makeRetryJobID(newJob.ID(), info.CurrentTrial)
	if id == "" {
		return nil
	}
	newJob.SetID(id)
	info.CurrentTrial++
	info.NeedsRetry = false
	info.Retryable = false
	newJob.UpdateRetryInfo(info.Options())

	// TODO (EVG-13540): handle safe transfer of scopes to new job if they're
	// applied on enqueue. _id clashes should not retry, but should scope
	// clashes still retry enqueueing?
	err := rh.queue.Put(ctx, newJob)
	if amboy.IsDuplicateJobError(err) {
		// The job is already in the queue, do nothing.
		return nil
	} else if err != nil {
		return errors.Wrap(err, "enqueueing retry job")
	}

	return nil
}

// makeRetryJobID creates the job ID for the retry job.
func makeRetryJobID(id string, currAttempt int) string {
	currAttemptSuffix := fmt.Sprintf(".attempt-%d", currAttempt)
	if currAttempt != 0 && !strings.HasSuffix(id, currAttemptSuffix) {
		// If the job has already been retried once but doesn't have the
		// expected suffix applied by the retry handler, the job ID is in an
		// invalid format.
		return ""
	}

	return fmt.Sprintf("%s.attempt-%d", strings.TrimSuffix(id, currAttemptSuffix), currAttempt+1)
}
