package queue

import (
	"context"

	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
)

// RemoteCreationArguments descripts the options passed to the remote
// queue. These
type RemoteCreationArguments struct {
	Size    int
	Name    string
	Ordered bool
	MDB     MongoDBOptions
	Client  *mongo.Client
}

// NewRemoteQueue builds a new queue backed by a remote storage. These
// queues allow workers running in multiple processes to service
// shared workloads in multiple processes.
func NewRemoteQueue(ctx context.Context, args RemoteCreationArguments) (amboy.Queue, error) {
	if err := args.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	return args.build(ctx)
}

func (opts *RemoteCreationArguments) Validate() error {
	catcher := grip.NewBasicCatcher()

	catcher.NewWhen(opts.Name == "", "must specify a name")

	catcher.NewWhen(opts.Client == nil && (opts.MDB.URI == "" && opts.MDB.DB == ""),
		"must specify database options")

	return catcher.Resolve()
}

func (opts *RemoteCreationArguments) build(ctx context.Context) (amboy.Queue, error) {
	var driver remoteQueueDriver
	var err error

	if opts.Client == nil {
		if opts.MDB.UseGroups {
			driver = newMongoGroupDriver(opts.Name, opts.MDB, opts.MDB.GroupName)
		} else {
			driver = newMongoDriver(opts.Name, opts.MDB)
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

	var q remoteQueue
	if opts.Ordered {
		q = newSimpleRemoteOrdered(opts.Size)
	} else {
		q = newRemoteUnordered(opts.Size)
	}

	if err = q.SetDriver(driver); err != nil {
		return nil, errors.Wrap(err, "problem configuring queue")
	}

	return q, nil
}
