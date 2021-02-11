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
	putJob        func(remoteQueue, context.Context, amboy.Job) error
	getJob        func(q remoteQueue, ctx context.Context, id string) (amboy.Job, bool)
	saveJob       func(remoteQueue, context.Context, amboy.Job) error
	saveAndPutJob func(q remoteQueue, ctx context.Context, toSave, toPut amboy.RetryableJob) error
	nextJob       func(remoteQueue, context.Context) amboy.Job
	completeJob   func(remoteQueue, context.Context, amboy.Job)
	jobResults    func(remoteQueue, context.Context) <-chan amboy.Job
	jobStats      func(remoteQueue, context.Context) <-chan amboy.JobStatusInfo
	queueStats    func(remoteQueue, context.Context) amboy.QueueStats
	queueInfo     func(remoteQueue) amboy.QueueInfo
	startQueue    func(remoteQueue, context.Context) error
	closeQueue    func(remoteQueue, context.Context)
}

func newMockRemoteQueue(q remoteQueue, driver remoteQueueDriver, makeDispatcher func(q amboy.Queue) Dispatcher, rh amboy.RetryHandler) (*mockRemoteQueue, error) {
	mq := &mockRemoteQueue{remoteQueue: q}
	if driver != nil {
		if err := q.SetDriver(driver); err != nil {
			return nil, errors.Wrap(err, "configuring queue with driver")
		}
	}
	if makeDispatcher != nil {
		dispatcher := makeDispatcher(mq)
		if driver != nil {
			driver.SetDispatcher(dispatcher)
		}
	}
	if rh != nil {
		if err := mq.SetRetryHandler(rh); err != nil {
			return nil, errors.Wrap(err, "constructing queue with retry handler")
		}
	}

	return mq, nil
}

func (q *mockRemoteQueue) Put(ctx context.Context, j amboy.Job) error {
	if q.putJob != nil {
		return q.putJob(q.remoteQueue, ctx, j)
	}
	return q.remoteQueue.Put(ctx, j)
}

func (q *mockRemoteQueue) Get(ctx context.Context, id string) (amboy.Job, bool) {
	if q.getJob != nil {
		return q.getJob(q.remoteQueue, ctx, id)
	}
	return q.remoteQueue.Get(ctx, id)
}

func (q *mockRemoteQueue) Save(ctx context.Context, j amboy.Job) error {
	if q.saveJob != nil {
		return q.saveJob(q.remoteQueue, ctx, j)
	}
	return q.remoteQueue.Save(ctx, j)
}

func (q *mockRemoteQueue) SaveAndPut(ctx context.Context, toSave, toPut amboy.RetryableJob) error {
	if q.saveAndPutJob != nil {
		return q.saveAndPutJob(q.remoteQueue, ctx, toSave, toPut)
	}
	return q.remoteQueue.SaveAndPut(ctx, toSave, toPut)
}

func (q *mockRemoteQueue) Next(ctx context.Context) amboy.Job {
	if q.nextJob != nil {
		return q.nextJob(q.remoteQueue, ctx)
	}
	return q.remoteQueue.Next(ctx)
}

func (q *mockRemoteQueue) Complete(ctx context.Context, j amboy.Job) {
	if q.completeJob != nil {
		q.completeJob(q.remoteQueue, ctx, j)
		return
	}
	q.remoteQueue.Complete(ctx, j)
}

func (q *mockRemoteQueue) Results(ctx context.Context) <-chan amboy.Job {
	if q.jobResults != nil {
		return q.jobResults(q.remoteQueue, ctx)
	}
	return q.remoteQueue.Results(ctx)
}

func (q *mockRemoteQueue) JobStats(ctx context.Context) <-chan amboy.JobStatusInfo {
	if q.jobStats != nil {
		return q.jobStats(q.remoteQueue, ctx)
	}
	return q.remoteQueue.JobStats(ctx)
}

func (q *mockRemoteQueue) Stats(ctx context.Context) amboy.QueueStats {
	if q.queueStats != nil {
		return q.queueStats(q.remoteQueue, ctx)
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
		return q.startQueue(q.remoteQueue, ctx)
	}
	return q.remoteQueue.Start(ctx)
}

func (q *mockRemoteQueue) Close(ctx context.Context) {
	if q.closeQueue != nil {
		q.closeQueue(q.remoteQueue, ctx)
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
