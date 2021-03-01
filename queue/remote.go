package queue

import (
	"context"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
)

// MongoDBQueueCreationOptions describes the options passed to the remote
// queue, that store jobs in a remote persistence layer to support
// distributed systems of workers.
type MongoDBQueueCreationOptions struct {
	Size         int
	Name         string
	Ordered      bool
	MDB          MongoDBOptions
	Client       *mongo.Client
	RetryHandler amboy.RetryHandlerOptions
	// StaleRetryingCheckFrequency is how often the queue periodically checks
	// for stale retrying jobs.
	StaleRetryingCheckFrequency time.Duration
}

// NewMongoDBQueue builds a new queue that persists jobs to a MongoDB
// instance. These queues allow workers running in multiple processes
// to service shared workloads in multiple processes.
func NewMongoDBQueue(ctx context.Context, opts MongoDBQueueCreationOptions) (amboy.RetryableQueue, error) {
	if err := opts.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	return opts.build(ctx)
}

// Validate ensure that the arguments defined are valid.
func (opts *MongoDBQueueCreationOptions) Validate() error {
	catcher := grip.NewBasicCatcher()

	catcher.NewWhen(opts.Name == "", "must specify a name")

	catcher.NewWhen(opts.Client == nil && (opts.MDB.URI == "" && opts.MDB.DB == ""),
		"must specify database options")

	return catcher.Resolve()
}

func (opts *MongoDBQueueCreationOptions) build(ctx context.Context) (amboy.RetryableQueue, error) {
	var driver remoteQueueDriver
	var err error

	var q remoteQueue
	if opts.Ordered {
		if q, err = newSimpleRemoteOrdered(opts.Size); err != nil {
			return nil, errors.Wrap(err, "initializing ordered queue")
		}
	} else {
		if q, err = newRemoteUnordered(opts.Size); err != nil {
			return nil, errors.Wrap(err, "initializing unordered queue")
		}
	}

	rh, err := newBasicRetryHandler(q, opts.RetryHandler)
	if err != nil {
		return nil, errors.Wrap(err, "initializing retry handler")
	}
	if err = q.SetRetryHandler(rh); err != nil {
		return nil, errors.Wrap(err, "configuring queue retry handler")
	}

	if opts.Client == nil {
		if opts.MDB.UseGroups {
			driver, err = newMongoGroupDriver(opts.Name, opts.MDB, opts.MDB.GroupName)
			if err != nil {
				return nil, errors.Wrap(err, "problem creating group driver")
			}
		} else {
			driver, err = newMongoDriver(opts.Name, opts.MDB)
			if err != nil {
				return nil, errors.Wrap(err, "problem creating driver")
			}
		}

		err = driver.Open(ctx)
	} else {
		if opts.MDB.UseGroups {
			driver, err = openNewMongoGroupDriver(ctx, opts.Name, opts.MDB, opts.MDB.GroupName, opts.Client)
		} else {
			driver, err = openNewMongoDriver(ctx, opts.Name, opts.MDB, opts.Client)
		}
	}

	if err != nil {
		return nil, errors.Wrap(err, "problem building driver")
	}

	if err = q.SetDriver(driver); err != nil {
		return nil, errors.Wrap(err, "problem configuring queue")
	}

	return q, nil
}
