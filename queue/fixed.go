package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/google/uuid"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/amboy/registry"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
)

// LocalLimitedSize implements the amboy.Queue interface, and unlike other
// implementations, the size of the queue is limited for both incoming jobs and
// completed jobs. This makes it possible to use these queues in situations as
// parts of services and in longer-running contexts.
//
// Specify a job capacity when constructing the queue; in total, the queue will
// store no more than 2x the specified capacity, and for completed jobs, will
// store no more than the specified capacity.
type limitedSizeLocal struct {
	pending                      chan amboy.Job
	started                      bool
	toDelete                     []string
	capacity                     int
	storage                      map[string]amboy.Job
	scopes                       ScopeManager
	dispatcher                   Dispatcher
	retryHandler                 amboy.RetryHandler
	staleRetryingMonitorInterval time.Duration
	lifetimeCtx                  context.Context

	retryingCount int
	deletedCount  int
	staleCount    int
	id            string
	runner        amboy.Runner
	mu            sync.RWMutex
}

// NewLocalLimitedSize constructs a LocalLimitedSize queue instance
// with the specified number of workers and capacity.
func NewLocalLimitedSize(workers, capacity int) amboy.RetryableQueue {
	q := &limitedSizeLocal{
		capacity: capacity,
		storage:  make(map[string]amboy.Job),
		scopes:   NewLocalScopeManager(),
		id:       fmt.Sprintf("queue.local.unordered.fixed.%s", uuid.New().String()),
		pending:  make(chan amboy.Job, capacity),
		toDelete: make([]string, 0, capacity),
	}
	q.dispatcher = NewDispatcher(q)
	q.runner = pool.NewLocalWorkers(workers, q)
	return q
}

func (q *limitedSizeLocal) ID() string {
	return q.id
}

// getNameWithMetadata returns the internally-stored name for a job.
func (q *limitedSizeLocal) getNameWithMetadata(j amboy.Job) string {
	if !j.RetryInfo().Retryable {
		return j.ID()
	}

	return q.getNameForAttempt(j.ID(), j.RetryInfo().CurrentAttempt)
}

// getNameForAttempt returns the internally-stored name for a job given its
// retry attempt.
func (q *limitedSizeLocal) getNameForAttempt(name string, attempt int) string {
	return buildCompoundID(retryAttemptPrefix(attempt), name)
}

// Put adds a job to the queue, returning an error if the queue isn't opened, or
// if a job of that name exists in the queue. If the queue is at capacity. Put()
// will block until either there is capacity or the context is done.
func (q *limitedSizeLocal) Put(ctx context.Context, j amboy.Job) error {
	if err := q.validateAndPreparePut(j); err != nil {
		return errors.WithStack(err)
	}

	name := q.getNameWithMetadata(j)

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.jobStored(name) {
		return amboy.NewDuplicateJobErrorf("cannot enqueue duplicate job '%s'", j.ID())
	}

	if j.ShouldApplyScopesOnEnqueue() {
		if err := q.scopes.Acquire(name, j.Scopes()); err != nil {
			return errors.Wrapf(err, "applying scopes to job")
		}
	}

	if err := q.putPending(ctx, j); err != nil {
		catcher := grip.NewBasicCatcher()
		catcher.Wrapf(err, "could not add job '%s'", j.ID())
		catcher.Wrap(q.scopes.Release(name, j.Scopes()), "releasing job's acquired scopes")
		return catcher.Resolve()
	}

	return nil
}

func (q *limitedSizeLocal) validateAndPreparePut(j amboy.Job) error {
	j.UpdateTimeInfo(amboy.JobTimeInfo{
		Created: time.Now(),
	})
	if err := j.TimeInfo().Validate(); err != nil {
		return errors.Wrap(err, "invalid job timeinfo")
	}
	return nil
}

func (q *limitedSizeLocal) putPending(ctx context.Context, j amboy.Job) error {
	jobCopy, err := q.copyJob(j)
	if err != nil {
		return errors.Wrap(err, "copying job")
	}
	name := q.getNameWithMetadata(j)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case q.pending <- jobCopy:
		q.storage[name] = jobCopy
		return nil
	}
}

func (q *limitedSizeLocal) Save(ctx context.Context, j amboy.Job) error {
	name := q.getNameWithMetadata(j)
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.jobStored(name) {
		return errors.Errorf("cannot save '%s', which is not tracked", name)
	}

	if err := q.scopes.Acquire(name, j.Scopes()); err != nil {
		return errors.Wrapf(err, "applying scopes to job")
	}

	q.saveCopy(j)

	return nil
}

func (q *limitedSizeLocal) copyJob(j amboy.Job) (amboy.Job, error) {
	ji, err := registry.MakeJobInterchange(j, amboy.JSON)
	if err != nil {
		return nil, errors.Wrap(err, "creating job interchange")
	}
	jobCopy, err := ji.Resolve(amboy.JSON)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return jobCopy, nil
}

// Get returns a job by name. This will include all jobs currently stored in
// the queue.
func (q *limitedSizeLocal) Get(ctx context.Context, name string) (amboy.Job, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	j, ok := q.getCopy(name)
	if ok {
		return j, true
	}

	// If this is a retryable job, search incrementally until we cannot find a
	// higher attempt.
	retryableName := q.getNameForAttempt(name, 0)
	j, ok = q.getCopy(retryableName)
	if !ok {
		return nil, false
	}
	for attempt := 0; ; attempt++ {
		nextRetryableName := q.getNameForAttempt(name, attempt)
		nextAttempt, ok := q.getCopy(nextRetryableName)
		if !ok {
			break
		}
		j = nextAttempt
	}

	return j, true
}

func (q *limitedSizeLocal) GetAttempt(ctx context.Context, id string, attempt int) (amboy.Job, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	name := q.getNameForAttempt(id, attempt)
	j, ok := q.getCopy(name)
	if !ok {
		return nil, false
	}

	return j, ok
}

// Next returns the next pending job, and is used by amboy.Runner
// implementations to fetch work. This operation blocks until a job is
// available or the context is canceled.
func (q *limitedSizeLocal) Next(ctx context.Context) amboy.Job {
	misses := 0
	for {
		if misses > q.capacity {
			return nil
		}

		select {
		case j := <-q.pending:
			name := q.getNameWithMetadata(j)
			ti := j.TimeInfo()
			if ti.IsStale() {
				q.mu.Lock()
				delete(q.storage, name)
				q.staleCount++
				q.mu.Unlock()

				grip.Notice(message.Fields{
					"state":    "stale",
					"job_id":   j.ID(),
					"job_type": j.Type().Name,
				})
				misses++
				continue
			}

			if !ti.IsDispatchable() {
				go q.requeue(j)
				misses++
				continue
			}

			if err := q.dispatcher.Dispatch(ctx, j); err != nil {
				go q.requeue(j)
				misses++
				continue
			}

			if err := q.scopes.Acquire(name, j.Scopes()); err != nil {
				q.dispatcher.Release(ctx, j)
				go q.requeue(j)
				misses++
				continue
			}

			return j
		case <-ctx.Done():
			return nil
		}
	}
}

func (q *limitedSizeLocal) requeue(j amboy.Job) {
	defer recovery.LogStackTraceAndContinue("re-queue waiting job", j.ID())
	select {
	case <-q.lifetimeCtx.Done():
	case q.pending <- j:
	}
}

func (q *limitedSizeLocal) Info() amboy.QueueInfo {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.info()
}

func (q *limitedSizeLocal) info() amboy.QueueInfo {
	return amboy.QueueInfo{
		Started:     q.started,
		LockTimeout: amboy.LockTimeout,
	}
}

// Results is a generator of all completed jobs in the queue. Retrying jobs are
// not returned until they finish retrying. Results are only returned for jobs
// that are still held in the queue's storage.
func (q *limitedSizeLocal) Results(ctx context.Context) <-chan amboy.Job {
	q.mu.RLock()
	defer q.mu.RUnlock()

	jobs := make(chan amboy.Job, len(q.toDelete))
	defer close(jobs)
	for _, name := range q.toDelete {
		j, ok := q.getCopy(name)
		if !ok {
			continue
		}
		select {
		case <-ctx.Done():
			return jobs
		case jobs <- j:
		}
	}

	return jobs
}

// JobStats returns an iterator for job status documents for all jobs
// in the queue. For this queue implementation *queued* jobs are returned
// first.
func (q *limitedSizeLocal) JobStats(ctx context.Context) <-chan amboy.JobStatusInfo {
	q.mu.RLock()
	defer q.mu.RUnlock()

	out := make(chan amboy.JobStatusInfo, len(q.storage))
	defer close(out)
	for name, job := range q.storage {
		stat := job.Status()
		stat.ID = name
		select {
		case <-ctx.Done():
			return out
		case out <- stat:
		}
	}

	return out
}

// Runner returns the Queue's embedded amboy.Runner instance.
func (q *limitedSizeLocal) Runner() amboy.Runner {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.runner
}

// SetRunner allows callers to, if the queue has not started, inject a
// different runner implementation.
func (q *limitedSizeLocal) SetRunner(r amboy.Runner) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.started {
		return errors.New("cannot set runner on started queue")
	}

	q.runner = r

	return nil
}

func (q *limitedSizeLocal) RetryHandler() amboy.RetryHandler {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.retryHandler
}

func (q *limitedSizeLocal) SetRetryHandler(rh amboy.RetryHandler) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.retryHandler != nil && q.retryHandler.Started() {
		return errors.New("cannot change retry handler after it is already started")
	}
	if err := rh.SetQueue(q); err != nil {
		return err
	}
	q.retryHandler = rh
	return nil
}

func (q *limitedSizeLocal) SetStaleRetryingMonitorInterval(interval time.Duration) {
	q.staleRetryingMonitorInterval = interval
}

// Stats returns information about the current state of jobs in the
// queue, and the amount of work completed.
func (q *limitedSizeLocal) Stats(ctx context.Context) amboy.QueueStats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	s := amboy.QueueStats{
		Total:     len(q.storage) + q.staleCount,
		Completed: len(q.toDelete) + q.deletedCount,
		Retrying:  q.retryingCount,
		Pending:   len(q.pending),
	}
	s.Running = s.Total - s.Completed - s.Pending
	return s
}

// Complete marks a job complete in the queue.
func (q *limitedSizeLocal) Complete(ctx context.Context, j amboy.Job) {
	if ctx.Err() != nil {
		return
	}

	q.dispatcher.Complete(ctx, j)

	q.mu.Lock()
	defer q.mu.Unlock()

	name := q.getNameWithMetadata(j)
	if !q.jobStored(name) {
		return
	}

	if err := q.complete(ctx, j); err != nil {
		grip.Error(message.Fields{
			"message":  "could not complete job",
			"job_id":   j.ID(),
			"queue_id": q.ID(),
		})
	}

	q.saveCopy(j)
}

func (q *limitedSizeLocal) complete(ctx context.Context, j amboy.Job) error {
	name := q.getNameWithMetadata(j)

	if !j.RetryInfo().ShouldRetry() || !j.ShouldApplyScopesOnEnqueue() {
		if err := q.scopes.Release(name, j.Scopes()); err != nil {
			return errors.Wrapf(err, "releasing scopes '%s' during completion", j.Scopes())
		}
	}

	q.prepareComplete(j)

	if j.RetryInfo().ShouldRetry() {
		q.retryingCount++
		return nil
	}

	q.prepareToDelete(j)

	return nil
}

func (q *limitedSizeLocal) prepareComplete(j amboy.Job) {
	status := j.Status()
	status.Completed = true
	status.InProgress = false
	status.ModificationTime = time.Now()
	status.ModificationCount++
	j.SetStatus(status)
}

func (q *limitedSizeLocal) prepareToDelete(j amboy.Job) {
	if len(q.toDelete) != 0 && len(q.toDelete) == q.capacity-1 {
		delete(q.storage, q.toDelete[0])
		q.toDelete = q.toDelete[1:]
		q.deletedCount++
	}

	name := q.getNameWithMetadata(j)
	q.toDelete = append(q.toDelete, name)
}

// CompleteRetrying marks a job in the queue as completed retrying.
func (q *limitedSizeLocal) CompleteRetrying(ctx context.Context, j amboy.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.prepareCompleteRetrying(j)

	name := q.getNameWithMetadata(j)
	if !q.jobStored(name) {
		return nil
	}

	if err := q.complete(ctx, j); err != nil {
		return errors.WithStack(err)
	}

	q.saveCopy(j)

	q.retryingCount--

	return nil
}

func (q *limitedSizeLocal) prepareCompleteRetrying(j amboy.Job) {
	j.UpdateRetryInfo(amboy.JobRetryOptions{
		NeedsRetry: utility.FalsePtr(),
		End:        utility.ToTimePtr(time.Now()),
	})
}

// CompleteRetryingAndPut marks a job in the queue toComplete as completed
// retrying and adds a new job toPut. The two operations are atomic.
func (q *limitedSizeLocal) CompleteRetryingAndPut(ctx context.Context, toComplete, toPut amboy.Job) error {
	toPutName := q.getNameWithMetadata(toPut)
	toCompleteName := q.getNameWithMetadata(toComplete)

	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.jobStored(toCompleteName) {
		return errors.Errorf("cannot find job '%s'", toComplete.ID())
	}
	if q.jobStored(toPutName) {
		return amboy.NewDuplicateJobErrorf("cannot enqueue duplicate job '%s'", toPut.ID())
	}

	if err := q.validateAndPreparePut(toPut); err != nil {
		return errors.WithStack(err)
	}

	q.prepareCompleteRetrying(toComplete)

	if err := q.scopes.ReleaseAndAcquire(toCompleteName, toComplete.Scopes(), toPutName, toPut.Scopes()); err != nil {
		return errors.Wrap(err, "releasing scopes from completed job and acquiring scopes for new job")
	}

	if err := q.putPending(ctx, toPut); err != nil {
		catcher := grip.NewBasicCatcher()
		catcher.Wrap(err, "adding new job")
		catcher.Wrapf(q.scopes.ReleaseAndAcquire(toPutName, toPut.Scopes(), toCompleteName, toComplete.Scopes()), "releasing scopes from new job and re-acquiring scopes of completed job")
		return errors.Wrap(err, "adding new job")
	}

	q.prepareComplete(toComplete)
	q.prepareToDelete(toComplete)
	q.saveCopy(toComplete)

	q.retryingCount--

	return nil
}

// saveCopy copies and saves the job in the storage. If it cannot create a
// copy, it will save the original job j.
func (q *limitedSizeLocal) saveCopy(j amboy.Job) {
	name := q.getNameWithMetadata(j)
	jobCopy, err := q.copyJob(j)
	if err != nil {
		grip.Error(message.Fields{
			"message":  "could not copy job",
			"job_id":   j.ID(),
			"queue_id": q.ID(),
		})
		q.storage[name] = j
		return
	}
	q.storage[name] = jobCopy
}

// getCopy finds and returns a copy of the job in storage. If it cannot create a
// copy, it will return the job directly from storage.
func (q *limitedSizeLocal) getCopy(name string) (amboy.Job, bool) {
	j, ok := q.storage[name]
	if !ok {
		return nil, false
	}
	jobCopy, err := q.copyJob(j)
	if err != nil {
		grip.Debug(message.WrapError(err, message.Fields{
			"message":  "could not copy job",
			"job_id":   j.ID(),
			"queue_id": q.ID(),
		}))
		return j, true
	}
	return jobCopy, true
}

// jobStored returns whether or not a job is in storage.
func (q *limitedSizeLocal) jobStored(name string) bool {
	_, ok := q.storage[name]
	return ok
}

// Start starts the runner and initializes the pending job storage. Only
// produces an error if the underlying runner fails to start.
func (q *limitedSizeLocal) Start(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.started {
		return errors.New("cannot start a running queue")
	}

	q.lifetimeCtx = ctx

	err := q.runner.Start(ctx)
	if err != nil {
		return err
	}

	if q.retryHandler != nil {
		if err := q.retryHandler.Start(ctx); err != nil {
			return errors.Wrap(err, "starting retry handler")
		}
		go q.monitorStaleRetryingJobs(ctx)
	}

	q.started = true

	grip.Info("job server running")

	return nil
}

func (q *limitedSizeLocal) monitorStaleRetryingJobs(ctx context.Context) {
	defer func() {
		if err := recovery.HandlePanicWithError(recover(), nil, "stale retry job monitor"); err != nil {
			grip.Error(message.WrapError(err, message.Fields{
				"message":  "stale retry job monitor failed",
				"queue_id": q.ID(),
			}))
			go q.monitorStaleRetryingJobs(ctx)
		}
	}()

	monitorInterval := defaultStaleRetryingMonitorInterval
	if q.staleRetryingMonitorInterval != 0 {
		monitorInterval = q.staleRetryingMonitorInterval
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			q.handleStaleRetryingJobs(ctx)
			timer.Reset(monitorInterval)
		}
	}
}

func (q *limitedSizeLocal) handleStaleRetryingJobs(ctx context.Context) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	for _, j := range q.storage {
		if !j.RetryInfo().ShouldRetry() {
			continue
		}
		if j.RetryInfo().ShouldRetry() && time.Since(j.Status().ModificationTime) < q.info().LockTimeout {
			continue
		}
		grip.Error(message.WrapError(q.retryHandler.Put(ctx, j), message.Fields{
			"message":  "could not enqueue stale retrying job",
			"job_id":   j.ID(),
			"queue_id": q.ID(),
		}))
	}
}

func (q *limitedSizeLocal) Close(ctx context.Context) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.runner != nil {
		q.runner.Close(ctx)
	}
	if q.retryHandler != nil {
		q.retryHandler.Close(ctx)
	}
}
