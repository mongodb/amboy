package queue

import (
	"context"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

// MongoDBQueueOptions represent options to create a queue that stores jobs in a
// persistence layer to support distributed systems of workers.
type MongoDBQueueOptions struct {
	// DB represents options for the MongoDB driver.
	DB *MongoDBOptions
	// NumWorkers is the default number of workers the queue should use. This
	// has lower precedence than WorkerPoolSize.
	NumWorkers *int
	// WorkerPoolSize returns the number of workers the queue should use. If
	// set, this takes precedence over NumWorkers.
	WorkerPoolSize func(string) int
	// Abortable indicates whether executing jobs can be aborted.
	Abortable *bool
	// Retryable represents options to retry jobs after they complete.
	Retryable *RetryableQueueOptions
	// DefaultMaxTime is the default value for the maximum time that jobs in the
	// queue can run before the queue aborts them.
	DefaultMaxTime time.Duration
}

// Validate checks that the given queue options are valid.
func (o *MongoDBQueueOptions) Validate() error {
	catcher := grip.NewBasicCatcher()
	catcher.NewWhen(utility.FromIntPtr(o.NumWorkers) == 0 && o.WorkerPoolSize == nil, "must specify either a static, positive number of workers or a worker pool size")
	catcher.NewWhen(utility.FromIntPtr(o.NumWorkers) < 0, "cannot specify a negative number of workers")
	catcher.NewWhen(o.DefaultMaxTime < 0, "cannot specify a negative default max time")
	if o.Retryable != nil {
		catcher.Wrap(o.Retryable.Validate(), "invalid retryable queue options")
	}
	if o.DB != nil {
		catcher.Wrap(o.DB.Validate(), "invalid DB options")
	} else {
		catcher.New("must specify DB options")
	}
	return catcher.Resolve()
}

// BuildQueue constructs a MongoDB-backed remote queue from the queue options.
func (o *MongoDBQueueOptions) BuildQueue(ctx context.Context) (amboy.Queue, error) {
	return o.buildQueue(ctx)
}

func (o *MongoDBQueueOptions) buildQueue(ctx context.Context) (remoteQueue, error) {
	if err := o.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid queue options")
	}

	workers := utility.FromIntPtr(o.NumWorkers)
	if o.WorkerPoolSize != nil {
		workers = o.WorkerPoolSize(o.DB.Collection)
		if workers == 0 {
			workers = utility.FromIntPtr(o.NumWorkers)
		}
	}

	var q remoteQueue
	var err error
	var retryable RetryableQueueOptions
	if o.Retryable != nil {
		retryable = *o.Retryable
	}
	qOpts := remoteOptions{
		numWorkers:     workers,
		retryable:      retryable,
		defaultMaxTime: o.DefaultMaxTime,
	}
	if q, err = newRemoteWithOptions(qOpts); err != nil {
		return nil, errors.Wrap(err, "initializing remote queue")
	}

	if utility.FromBoolPtr(o.Abortable) {
		p := pool.NewAbortablePool(workers, q)
		if err = q.SetRunner(p); err != nil {
			return nil, errors.Wrap(err, "configuring queue with runner")
		}
	}

	var d remoteQueueDriver
	if o.DB.Client != nil {
		d, err = openNewMongoDriver(ctx, *o.DB)
		if err != nil {
			return nil, errors.Wrap(err, "creating and opening driver")
		}
	} else {
		d, err = newMongoDriver(*o.DB)
		if err != nil {
			return nil, errors.Wrap(err, "creating and opening driver")
		}
		if err = d.Open(ctx); err != nil {
			return nil, errors.Wrap(err, "opening driver")
		}
	}
	if err := q.SetDriver(d); err != nil {
		return nil, errors.Wrap(err, "setting driver")
	}

	return q, nil
}

// getMongoDBQueueOptions resolves the given queue options into MongoDB-specific
// queue options. If the given options are not MongoDB options, this will return
// an error.
func getMongoDBQueueOptions(opts ...amboy.QueueOptions) ([]MongoDBQueueOptions, error) {
	var mdbOpts []MongoDBQueueOptions

	for _, o := range opts {
		switch opt := o.(type) {
		case *MongoDBQueueOptions:
			if opt != nil {
				mdbOpts = append(mdbOpts, *opt)
			}
		default:
			return nil, errors.Errorf("found queue options of type '%T', but they must be MongoDB options", opt)
		}
	}

	return mdbOpts, nil
}

// mergeMongoDBQueueOptions merges all the given MongoDBQueueOptions into a
// single set of options. Options are applied in the order they're specified and
// conflicting options are overwritten.
func mergeMongoDBQueueOptions(opts ...MongoDBQueueOptions) MongoDBQueueOptions {
	var merged MongoDBQueueOptions

	for _, o := range opts {
		if o.DB != nil {
			merged.DB = o.DB
		}
		if o.NumWorkers != nil {
			merged.NumWorkers = o.NumWorkers
		}
		if o.WorkerPoolSize != nil {
			merged.WorkerPoolSize = o.WorkerPoolSize
		}
		if o.Abortable != nil {
			merged.Abortable = o.Abortable
		}
		if o.Retryable != nil {
			merged.Retryable = o.Retryable
		}
	}

	return merged
}

// NewMongoDBQueue builds a new queue that persists jobs to a MongoDB
// instance. These queues allow workers running in multiple processes
// to service shared workloads in multiple processes.
func NewMongoDBQueue(ctx context.Context, opts MongoDBQueueOptions) (amboy.RetryableQueue, error) {
	if err := opts.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	return opts.buildQueue(ctx)
}

// remote implements the amboy.RetryableQueue interface. It uses a Driver to
// access a backend for job storage and processing. The queue does not impose
// any additional job ordering beyond what's provided by the driver.
type remote struct {
	*remoteBase
}

// newRemote returns a queue that has been initialized with a configured local
// worker pool with the specified number of workers.
func newRemote(size int, maxTime time.Duration) (remoteQueue, error) {
	return newRemoteWithOptions(remoteOptions{numWorkers: size, defaultMaxTime: maxTime})
}

// newRemoteWithOptions returns a queue that has been initialized with a
// configured runner and the given options.
func newRemoteWithOptions(opts remoteOptions) (remoteQueue, error) {
	b, err := newRemoteBaseWithOptions(opts)
	if err != nil {
		return nil, errors.Wrap(err, "initializing remote base")
	}
	q := &remote{remoteBase: b}
	q.dispatcher = NewDispatcher(q)
	if err := q.SetRunner(pool.NewLocalWorkers(opts.numWorkers, q)); err != nil {
		return nil, errors.Wrap(err, "configuring runner")
	}
	grip.Infof("creating new remote job queue with %d workers", opts.numWorkers)

	return q, nil
}

// Next returns a Job from the queue. Returns a nil Job object if the context is
// canceled. The operation is blocking until an undispatched, unlocked job is
// available. This operation takes a job lock.
func (q *remote) Next(ctx context.Context) amboy.Job {
	count := 0
	for {
		count++
		select {
		case <-ctx.Done():
			return nil
		case job := <-q.channel:
			return job
		}
	}
}
