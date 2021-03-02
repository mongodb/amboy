package queue

import (
	"context"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

// RemoteUnordered are queues that use a Driver as backend for job
// storage and processing and do not impose any additional ordering
// beyond what's provided by the driver.
type remoteUnordered struct {
	*remoteBase
}

// newRemoteUnordered returns a queue that has been initialized with a
// local worker pool Runner instance of the specified size.
func newRemoteUnordered(size int) (remoteQueue, error) {
	q := &remoteUnordered{
		remoteBase: newRemoteBase(),
	}

	q.dispatcher = NewDispatcher(q)
	if err := q.SetRunner(pool.NewLocalWorkers(size, q)); err != nil {
		return nil, errors.Wrap(err, "configuring runner")
	}
	grip.Infof("creating new remote job queue with %d workers", size)

	return q, nil
}

// Next returns a Job from the queue. Returns a nil Job object if the
// context is canceled. The operation is blocking until an
// undispatched, unlocked job is available. This operation takes a job
// lock.
func (q *remoteUnordered) Next(ctx context.Context) amboy.Job {
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
