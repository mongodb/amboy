package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/google/uuid"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/dependency"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/amboy/registry"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func init() {
	mockJobCounters = &mockJobRunEnv{}
	registry.AddJobType("mock", func() amboy.Job { return newMockJob() })
	registry.AddJobType("sleep", func() amboy.Job { return newSleepJob() })
	registry.AddJobType("retryable", func() amboy.Job { return &mockRetryableJob{} })
}

// mockRetryableQueue provides a mock implementation of a remoteQueue whose
// runtime behavior is configurable. Methods can be mocked out; otherwise, the
// default behavior is identical to that of its underlying remoteQueue.
type mockRemoteQueue struct {
	remoteQueue

	// Mockable methods
	putJob        func(ctx context.Context, q remoteQueue, j amboy.Job) error
	getJob        func(ctx context.Context, q remoteQueue, id string) (amboy.Job, bool)
	saveJob       func(ctx context.Context, q remoteQueue, j amboy.Job) error
	saveAndPutJob func(ctx context.Context, q remoteQueue, toSave, toPut amboy.RetryableJob) error
	nextJob       func(ctx context.Context, q remoteQueue) amboy.Job
	completeJob   func(ctx context.Context, q remoteQueue, j amboy.Job)
	jobResults    func(ctx context.Context, q remoteQueue) <-chan amboy.Job
	jobStats      func(ctx context.Context, q remoteQueue) <-chan amboy.JobStatusInfo
	queueStats    func(ctx context.Context, q remoteQueue) amboy.QueueStats
	queueInfo     func(q remoteQueue) amboy.QueueInfo
	startQueue    func(ctx context.Context, q remoteQueue) error
	closeQueue    func(ctx context.Context, q remoteQueue)
}

type mockRemoteQueueOptions struct {
	queue          remoteQueue
	driver         remoteQueueDriver
	makeDispatcher func(q amboy.Queue) Dispatcher
	rh             amboy.RetryHandler
}

func (opts *mockRemoteQueueOptions) validate() error {
	if opts.queue == nil {
		return errors.New("cannot initialize mock remote queue without a backing remote queue implementation")
	}

	return nil
}

func newMockRemoteQueue(opts mockRemoteQueueOptions) (*mockRemoteQueue, error) {
	if err := opts.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid options")
	}
	mq := &mockRemoteQueue{remoteQueue: opts.queue}
	if opts.driver != nil {
		if err := opts.queue.SetDriver(opts.driver); err != nil {
			return nil, errors.Wrap(err, "configuring queue with driver")
		}
	}
	if opts.makeDispatcher != nil {
		dispatcher := opts.makeDispatcher(mq)
		if opts.driver != nil {
			opts.driver.SetDispatcher(dispatcher)
		}
	}
	if opts.rh != nil {
		if err := mq.SetRetryHandler(opts.rh); err != nil {
			return nil, errors.Wrap(err, "constructing queue with retry handler")
		}
	}

	return mq, nil
}

func (q *mockRemoteQueue) Put(ctx context.Context, j amboy.Job) error {
	if q.putJob != nil {
		return q.putJob(ctx, q.remoteQueue, j)
	}
	return q.remoteQueue.Put(ctx, j)
}

func (q *mockRemoteQueue) Get(ctx context.Context, id string) (amboy.Job, bool) {
	if q.getJob != nil {
		return q.getJob(ctx, q.remoteQueue, id)
	}
	return q.remoteQueue.Get(ctx, id)
}

func (q *mockRemoteQueue) Save(ctx context.Context, j amboy.Job) error {
	if q.saveJob != nil {
		return q.saveJob(ctx, q.remoteQueue, j)
	}
	return q.remoteQueue.Save(ctx, j)
}

func (q *mockRemoteQueue) SaveAndPut(ctx context.Context, toSave, toPut amboy.RetryableJob) error {
	if q.saveAndPutJob != nil {
		return q.saveAndPutJob(ctx, q.remoteQueue, toSave, toPut)
	}
	return q.remoteQueue.SaveAndPut(ctx, toSave, toPut)
}

func (q *mockRemoteQueue) Next(ctx context.Context) amboy.Job {
	if q.nextJob != nil {
		return q.nextJob(ctx, q.remoteQueue)
	}
	return q.remoteQueue.Next(ctx)
}

func (q *mockRemoteQueue) Complete(ctx context.Context, j amboy.Job) {
	if q.completeJob != nil {
		q.completeJob(ctx, q.remoteQueue, j)
		return
	}
	q.remoteQueue.Complete(ctx, j)
}

func (q *mockRemoteQueue) Results(ctx context.Context) <-chan amboy.Job {
	if q.jobResults != nil {
		return q.jobResults(ctx, q.remoteQueue)
	}
	return q.remoteQueue.Results(ctx)
}

func (q *mockRemoteQueue) JobStats(ctx context.Context) <-chan amboy.JobStatusInfo {
	if q.jobStats != nil {
		return q.jobStats(ctx, q.remoteQueue)
	}
	return q.remoteQueue.JobStats(ctx)
}

func (q *mockRemoteQueue) Stats(ctx context.Context) amboy.QueueStats {
	if q.queueStats != nil {
		return q.queueStats(ctx, q.remoteQueue)
	}
	return q.remoteQueue.Stats(ctx)
}

func (q *mockRemoteQueue) Info() amboy.QueueInfo {
	if q.queueInfo != nil {
		return q.queueInfo(q.remoteQueue)
	}
	return q.remoteQueue.Info()
}

func (q *mockRemoteQueue) Start(ctx context.Context) error {
	if q.startQueue != nil {
		return q.startQueue(ctx, q.remoteQueue)
	}
	return q.remoteQueue.Start(ctx)
}

func (q *mockRemoteQueue) Close(ctx context.Context) {
	if q.closeQueue != nil {
		q.closeQueue(ctx, q.remoteQueue)
		return
	}
	q.remoteQueue.Close(ctx)
}

// mockRetryableJob provides a mock implementation of an amboy.RetryableJob
// whose runtime behavior is configurable.
type mockRetryableJob struct {
	job.Base
	addError          error
	addRetryableError error
	updateRetryInfo   *amboy.JobRetryOptions
	op                func()
}

func newMockRetryableJob(id string) *mockRetryableJob {
	j := &mockRetryableJob{}
	j.UpdateRetryInfo(amboy.JobRetryOptions{
		Retryable: utility.TruePtr(),
	})
	j.SetDependency(dependency.NewAlways())
	j.SetID(fmt.Sprintf("mock-retryable-%s", id))
	return j
}

func (j *mockRetryableJob) Run(ctx context.Context) {
	defer j.MarkComplete()
	if j.addError != nil {
		j.AddError(j.addError)
	}

	if j.addRetryableError != nil {
		j.AddRetryableError(j.addRetryableError)
	}

	if j.updateRetryInfo != nil {
		j.UpdateRetryInfo(*j.updateRetryInfo)
	}

	if j.op != nil {
		j.op()
	}
}

func TestMockRetryableJob(t *testing.T) {
	assert.Implements(t, (*amboy.RetryableJob)(nil), &mockRetryableJob{})
}

func TestMockRetryableQueue(t *testing.T) {
	assert.Implements(t, (*amboy.RetryableQueue)(nil), &mockRemoteQueue{})
}

var mockJobCounters *mockJobRunEnv

type mockJobRunEnv struct {
	runCount int
	mu       sync.Mutex
}

func (e *mockJobRunEnv) Inc() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.runCount++
}

func (e *mockJobRunEnv) Count() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.runCount
}

func (e *mockJobRunEnv) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.runCount = 0
}

type mockJob struct {
	job.Base `bson:"job_base" json:"job_base" yaml:"job_base"`
}

func newMockJob() *mockJob {
	j := &mockJob{
		Base: job.Base{
			JobType: amboy.JobType{
				Name:    "mock",
				Version: 0,
			},
		},
	}
	j.SetDependency(dependency.NewAlways())
	return j
}

func (j *mockJob) Run(_ context.Context) {
	defer j.MarkComplete()

	mockJobCounters.Inc()
}

type sleepJob struct {
	Sleep time.Duration
	job.Base
}

func newSleepJob() *sleepJob {
	j := &sleepJob{
		Base: job.Base{
			JobType: amboy.JobType{
				Name:    "sleep",
				Version: 0,
			},
		},
	}
	j.SetDependency(dependency.NewAlways())
	j.SetID(uuid.New().String())
	return j
}

func (j *sleepJob) Run(ctx context.Context) {
	defer j.MarkComplete()

	if j.Sleep == 0 {
		return
	}

	timer := time.NewTimer(j.Sleep)
	defer timer.Stop()

	select {
	case <-timer.C:
		return
	case <-ctx.Done():
		return
	}
}
