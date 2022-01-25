package queue

import (
	"context"

	"github.com/evergreen-ci/utility"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/pool"
	"github.com/pkg/errors"
)

// kim: TODO: replace with MongoDBQueueOptions
// MongoDBQueueCreationOptions describes the options passed to the remote
// queue, that store jobs in a remote persistence layer to support
// distributed systems of workers.
// type MongoDBQueueCreationOptions struct {
//     Size      int
//     Name      string
//     Ordered   bool
//     MDB       MongoDBOptions
//     Client    *mongo.Client
//     Retryable RetryableQueueOptions
// }

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
	// Ordered indicates whether the queue should obey job dependency ordering.
	Ordered *bool
	// Abortable indicates whether executing jobs can be aborted.
	Abortable *bool
	// Retryable represents options to retry jobs after they complete.
	Retryable *RetryableQueueOptions
}

// BuildQueue constructs a MongoDB-backed remote queue from the queue options.
func (o *MongoDBQueueOptions) BuildQueue(ctx context.Context) (amboy.Queue, error) {
	return o.buildQueue(ctx)
}

func (o *MongoDBQueueOptions) buildQueue(ctx context.Context) (remoteQueue, error) {
	workers := utility.FromIntPtr(o.NumWorkers)
	if o.WorkerPoolSize != nil {
		workers = o.WorkerPoolSize(o.DB.Name)
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
		numWorkers: workers,
		retryable:  retryable,
	}
	if utility.FromBoolPtr(o.Ordered) {
		if q, err = newRemoteSimpleOrderedWithOptions(qOpts); err != nil {
			return nil, errors.Wrap(err, "initializing ordered queue")
		}
	} else {
		if q, err = newRemoteUnorderedWithOptions(qOpts); err != nil {
			return nil, errors.Wrap(err, "initializing unordered queue")
		}
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

// NewMongoDBQueue builds a new queue that persists jobs to a MongoDB
// instance. These queues allow workers running in multiple processes
// to service shared workloads in multiple processes.
func NewMongoDBQueue(ctx context.Context, opts MongoDBQueueOptions) (amboy.RetryableQueue, error) {
	if err := opts.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	return opts.buildQueue(ctx)
}

// kim: TODO: remove
// Validate ensure that the arguments defined are valid.
// func (opts *MongoDBQueueCreationOptions) Validate() error {
//     catcher := grip.NewBasicCatcher()
//
//     catcher.NewWhen(opts.Name == "", "must specify a name")
//
//     catcher.NewWhen(opts.Client == nil && (opts.MDB.URI == "" && opts.MDB.DB == ""),
//         "must specify database options")
//
//     catcher.Wrap(opts.Retryable.Validate(), "invalid retryable queue options")
//
//     return catcher.Resolve()
// }

// func (opts *MongoDBQueueCreationOptions) build(ctx context.Context) (amboy.RetryableQueue, error) {
//
//     var q remoteQueue
//     var err error
//     qOpts := remoteOptions{
//         numWorkers: opts.Size,
//         retryable:  opts.Retryable,
//     }
//     if opts.Ordered {
//         if q, err = newRemoteSimpleOrderedWithOptions(qOpts); err != nil {
//             return nil, errors.Wrap(err, "initializing ordered queue")
//         }
//     } else {
//         if q, err = newRemoteUnorderedWithOptions(qOpts); err != nil {
//             return nil, errors.Wrap(err, "initializing unordered queue")
//         }
//     }
//
//     var driver remoteQueueDriver
//     if opts.Client == nil {
//         if opts.MDB.UseGroups {
//             driver, err = newMongoGroupDriver(opts.Name, opts.MDB, opts.MDB.GroupName)
//             if err != nil {
//                 return nil, errors.Wrap(err, "problem creating group driver")
//             }
//         } else {
//             driver, err = newMongoDriver(opts.Name, opts.MDB)
//             if err != nil {
//                 return nil, errors.Wrap(err, "problem creating driver")
//             }
//         }
//
//         err = driver.Open(ctx)
//     } else {
//         if opts.MDB.UseGroups {
//             driver, err = openNewMongoGroupDriver(ctx, opts.Name, opts.MDB, opts.MDB.GroupName)
//         } else {
//             driver, err = openNewMongoDriver(ctx, opts.Name, opts.MDB)
//         }
//     }
//
//     if err != nil {
//         return nil, errors.Wrap(err, "problem building driver")
//     }
//
//     if err = q.SetDriver(driver); err != nil {
//         return nil, errors.Wrap(err, "problem configuring queue")
//     }
//
//     return q, nil
// }
