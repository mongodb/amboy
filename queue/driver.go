package queue

import (
	"context"
	"time"

	"github.com/mongodb/amboy"
)

// remoteQueueDriver describes the interface between a queue and an out of
// process persistence layer, like a database.
type remoteQueueDriver interface {
	ID() string
	Open(context.Context) error
	Close(context.Context) error

	// Get finds a job by job ID. For retryable jobs, this returns the latest
	// job attempt.
	Get(context.Context, string) (amboy.Job, error)
	// GetAttempt returns a retryable job by job ID and attempt number. If used
	// to find a non-retryable job, this should return nil job and an error.
	GetAttempt(ctx context.Context, id string, attempt int) (amboy.Job, error)
	// GetAllAttempts returns all attempts of a retryable job by job ID. If used
	// to find a non-retryable job, this should return no jobs and an error.
	GetAllAttempts(ctx context.Context, id string) ([]amboy.Job, error)
	// Put inserts a new job in the backing storage.
	Put(context.Context, amboy.Job) error
	// PutMany inserts new jobs in the backing storage.
	PutMany(context.Context, []amboy.Job) error
	// Save updates an existing job in the backing storage. Implementations may
	// not allow calls to Save to run concurrently.
	Save(context.Context, amboy.Job) error
	// CompleteAndPut updates an existing job toComplete and inserts a new job
	// toPut atomically. Implementations may not allow calls to CompleteAndPut
	// to run concurrently.
	CompleteAndPut(ctx context.Context, toComplete amboy.Job, toPut amboy.Job) error

	Jobs(context.Context) <-chan amboy.Job
	// RetryableJobs returns retryable jobs, subject to a filter.
	RetryableJobs(context.Context, retryableJobFilter) <-chan amboy.Job
	Next(context.Context) amboy.Job

	Stats(context.Context) amboy.QueueStats
	JobInfo(context.Context) <-chan amboy.JobInfo
	Complete(context.Context, amboy.Job) error

	LockTimeout() time.Duration

	SetDispatcher(Dispatcher)
	Dispatcher() Dispatcher
}

// retryableJobFilter represents a query filter on retryable jobs.
type retryableJobFilter string

const (
	// RetryableJobAll refers to all retryable jobs.
	retryableJobAll retryableJobFilter = "all-retryable"
	// RetryableJobAllRetrying refers to all retryable jobs that are currently
	// waiting to retry.
	retryableJobAllRetrying retryableJobFilter = "all-retrying"
	// RetryableJobActiveRetrying refers to retryable jobs that have recently
	// retried.
	retryableJobActiveRetrying retryableJobFilter = "active-retrying"
	// RetryableJobStaleRetrying refers to retryable jobs that should be
	// retrying but have not done so recently.
	retryableJobStaleRetrying retryableJobFilter = "stale-retrying"
)
