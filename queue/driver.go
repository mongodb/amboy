package queue

import (
	"context"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
)

// remoteQueueDriver describes the interface between a queue and an out of
// process persistence layer, like a database.
type remoteQueueDriver interface {
	ID() string
	Open(context.Context) error
	Close()

	// Get finds a job by job ID. For retryable jobs, this returns the latest
	// job attempt.
	Get(context.Context, string) (amboy.Job, error)
	// GetAttempt returns a job by job ID and attempt number. Only applicable to
	// retryable jobs. If used for a non-retryable job, this should return nil
	// and an error.
	GetAttempt(ctx context.Context, id string, attempt int) (amboy.RetryableJob, error)
	// Put inserts a new job in the backing storage.
	Put(context.Context, amboy.Job) error
	// Save updates an existing job in the backing storage. Implementations may
	// not allow calls to Save to run concurrently.
	Save(context.Context, amboy.Job) error
	// CompleteAndPut updates an existing job toComplete and inserts a new job
	// toPut atomically. Implementations may not allow calls to CompleteAndPut
	// to run concurrently.
	CompleteAndPut(ctx context.Context, toComplete amboy.Job, toPut amboy.Job) error

	Jobs(context.Context) <-chan amboy.Job
	// RetryableJobs returns retryable jobs, subject to a filter.
	RetryableJobs(context.Context, RetryableJobFilter) <-chan amboy.RetryableJob
	Next(context.Context) amboy.Job

	Stats(context.Context) amboy.QueueStats
	JobStats(context.Context) <-chan amboy.JobStatusInfo
	Complete(context.Context, amboy.Job) error

	LockTimeout() time.Duration

	SetDispatcher(Dispatcher)
	Dispatcher() Dispatcher
}

// RetryableJobFilter represents a filter on jobs that are currently retrying.
type RetryableJobFilter string

const (
	// RetryableJobAll refers to all retryable jobs.
	RetryableJobAll RetryableJobFilter = "all"
	// RetryableJobAllRetrying refers to all jobs that are currently waiting to
	// retry.
	RetryableJobAllRetrying RetryableJobFilter = "all-retrying"
	// RetryableJobActiveRetrying refers to jobs that have recently retried.
	RetryableJobActiveRetrying RetryableJobFilter = "active-retrying"
	// RetryableJobStaleRetrying refers to jobs that should be retrying but have
	// not done so recently.
	RetryableJobStaleRetrying RetryableJobFilter = "stale-retrying"
)

// MongoDBOptions is a struct passed to the NewMongo constructor to
// communicate mgoDriver specific settings about the driver's behavior
// and operation.
type MongoDBOptions struct {
	URI                      string
	DB                       string
	GroupName                string
	UseGroups                bool
	Priority                 bool
	CheckWaitUntil           bool
	CheckDispatchBy          bool
	SkipQueueIndexBuilds     bool
	SkipReportingIndexBuilds bool
	Format                   amboy.Format
	WaitInterval             time.Duration
	// TTL sets the number of seconds for a TTL index on the "info.created"
	// field. If set to zero, the TTL index will not be created and
	// and documents may live forever in the database.
	TTL time.Duration
	// LockTimeout overrides the default job lock timeout if set.
	LockTimeout time.Duration
}

// DefaultMongoDBOptions constructs a new options object with default
// values: connecting to a MongoDB instance on localhost, using the
// "amboy" database, and *not* using priority ordering of jobs.
func DefaultMongoDBOptions() MongoDBOptions {
	return MongoDBOptions{
		URI:                      "mongodb://localhost:27017",
		DB:                       "amboy",
		Priority:                 false,
		UseGroups:                false,
		CheckWaitUntil:           true,
		CheckDispatchBy:          false,
		SkipQueueIndexBuilds:     false,
		SkipReportingIndexBuilds: false,
		WaitInterval:             time.Second,
		Format:                   amboy.BSON,
		LockTimeout:              amboy.LockTimeout,
	}
}

// Validate validates that the required options are given and sets fields that
// are unspecified and have a default value.
func (opts *MongoDBOptions) Validate() error {
	catcher := grip.NewBasicCatcher()
	catcher.NewWhen(opts.URI == "", "must specify connection URI")
	catcher.NewWhen(opts.DB == "", "must specify database")
	catcher.NewWhen(opts.LockTimeout < 0, "cannot have negative lock timeout")
	if opts.LockTimeout == 0 {
		opts.LockTimeout = amboy.LockTimeout
	}
	if !opts.Format.IsValid() {
		opts.Format = amboy.BSON
	}
	return catcher.Resolve()
}
