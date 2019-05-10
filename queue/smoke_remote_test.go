package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
	mgo "gopkg.in/mgo.v2"
)

func cleanupMgo(dbname, name string, session *mgo.Session) error {
	start := time.Now()
	defer session.Close()

	if err := session.DB(dbname).C(addJobsSuffix(name)).DropCollection(); err != nil {
		return errors.WithStack(err)
	}

	grip.Infof("clean up operation for %s took %s", name, time.Since(start))
	return nil
}

func cleanupMongo(ctx context.Context, dbname, name string, client *mongo.Client) error {
	start := time.Now()

	if err := client.Database(dbname).Collection(addJobsSuffix(name)).Drop(ctx); err != nil {
		return errors.WithStack(err)
	}

	grip.Infof("clean up operation for %s took %s", name, time.Since(start))
	return client.Disconnect(ctx)
}

func runSmokeMultipleQueuesRunJobsOnce(ctx context.Context, drivers []Driver, cleanup func(), assert *assert.Assertions) {
	queues := []*remoteUnordered{}

	for _, driver := range drivers {
		q := NewRemoteUnordered(4).(*remoteUnordered)
		assert.NoError(driver.Open(ctx))
		assert.NoError(q.SetDriver(driver))
		assert.NoError(q.Start(ctx))
		queues = append(queues, q)
	}
	const (
		inside  = 15
		outside = 10
	)

	wg := &sync.WaitGroup{}
	for i := 0; i < len(drivers); i++ {
		for ii := 0; ii < outside; ii++ {
			wg.Add(1)
			go func(i int) {
				for iii := 0; iii < inside; iii++ {
					j := newMockJob()
					jobID := fmt.Sprintf("%d-%d-%d-%d", i, i, iii, job.GetNumber())
					j.SetID(jobID)
					assert.NoError(queues[0].Put(j))
				}
				wg.Done()
			}(i)
		}
	}

	grip.Notice("waiting to add all jobs")
	wg.Wait()

	grip.Notice("waiting to run jobs")

	for _, q := range queues {
		amboy.WaitCtxInterval(ctx, q, 100*time.Millisecond)
	}

	assert.Equal(len(drivers)*inside*outside, mockJobCounters.Count())
	cleanup()
}

func TestSmokeMgoDriverRemoteTwoQueueRunsJobsOnlyOnce(t *testing.T) {
	assert := assert.New(t) // nolint
	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"
	name := uuid.NewV4().String()
	drivers := []Driver{
		NewMgoDriver(name, opts),
		NewMgoDriver(name, opts),
	}
	cleanup := func() { grip.Alert(cleanupMgo(opts.DB, name, drivers[0].(*mgoDriver).session.Clone())) }

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	mockJobCounters.Reset()
	runSmokeMultipleQueuesRunJobsOnce(ctx, drivers, cleanup, assert)
}

func TestSmokeMgoDriverRemoteManyQueueRunsJobsOnlyOnce(t *testing.T) {
	assert := assert.New(t) // nolint
	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"
	name := uuid.NewV4().String()
	drivers := []Driver{
		NewMgoDriver(name, opts),
		NewMgoDriver(name, opts),
		NewMgoDriver(name, opts),
		NewMgoDriver(name, opts),
		NewMgoDriver(name, opts),
		NewMgoDriver(name, opts),
	}
	cleanup := func() { grip.Alert(cleanupMgo(opts.DB, name, drivers[0].(*mgoDriver).session.Clone())) }

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	mockJobCounters.Reset()
	runSmokeMultipleQueuesRunJobsOnce(ctx, drivers, cleanup, assert)
}

func TestSmokeMongoDriverRemoteTwoQueueRunsJobsOnlyOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	assert := assert.New(t) // nolint
	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"
	name := uuid.NewV4().String()
	drivers := []Driver{
		NewMongoDriver(name, opts),
		NewMongoDriver(name, opts),
	}
	cleanup := func() { grip.Alert(cleanupMongo(ctx, opts.DB, name, drivers[0].(*mongoDriver).client)) }

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	mockJobCounters.Reset()
	runSmokeMultipleQueuesRunJobsOnce(ctx, drivers, cleanup, assert)
}

func TestSmokeMongoGroupDriverRemoteTwoQueueRunsJobsOnlyOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	assert := assert.New(t) // nolint
	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"
	name := uuid.NewV4().String()
	drivers := []Driver{
		NewMongoGroupDriver(name, opts, "one"),
		NewMongoGroupDriver(name, opts, "one"),
	}
	cleanup := func() { grip.Alert(cleanupMongo(ctx, opts.DB, name, drivers[0].(*mongoGroupDriver).client)) }

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	mockJobCounters.Reset()
	runSmokeMultipleQueuesRunJobsOnce(ctx, drivers, cleanup, assert)
}

func TestSmokeMgoGroupDriverRemoteTwoQueueRunsJobsOnlyOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	assert := assert.New(t) // nolint
	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"
	name := uuid.NewV4().String()
	drivers := []Driver{
		NewMgoGroupDriver(name, opts, "one"),
		NewMgoGroupDriver(name, opts, "one"),
	}
	cleanup := func() { grip.Alert(cleanupMgo(opts.DB, name, drivers[0].(*mgoGroupDriver).session)) }

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	mockJobCounters.Reset()
	runSmokeMultipleQueuesRunJobsOnce(ctx, drivers, cleanup, assert)
}

func TestSmokeMongoDriverRemoteManyQueueRunsJobsOnlyOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	assert := assert.New(t) // nolint
	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"
	name := uuid.NewV4().String()
	drivers := []Driver{
		NewMongoDriver(name, opts),
		NewMongoDriver(name, opts),
		NewMongoDriver(name, opts),
		NewMongoDriver(name, opts),
		NewMongoDriver(name, opts),
		NewMongoDriver(name, opts),
	}
	cleanup := func() { grip.Alert(cleanupMongo(ctx, opts.DB, name, drivers[0].(*mongoDriver).client)) }

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	mockJobCounters.Reset()
	runSmokeMultipleQueuesRunJobsOnce(ctx, drivers, cleanup, assert)
}

func TestSmokeMongoGroupDriverRemoteManyQueueRunsJobsOnlyOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	assert := assert.New(t) // nolint
	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"
	name := uuid.NewV4().String()
	drivers := []Driver{
		NewMongoGroupDriver(name, opts, "first"),
		NewMongoGroupDriver(name, opts, "second"),
		NewMongoGroupDriver(name, opts, "first"),
		NewMongoGroupDriver(name, opts, "second"),
		NewMongoGroupDriver(name, opts, "first"),
		NewMongoGroupDriver(name, opts, "second"),
	}
	cleanup := func() { grip.Alert(cleanupMongo(ctx, opts.DB, name, drivers[0].(*mongoGroupDriver).client)) }

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	mockJobCounters.Reset()
	runSmokeMultipleQueuesRunJobsOnce(ctx, drivers, cleanup, assert)
}

func TestSmokeMgoGroupDriverRemoteManyQueueRunsJobsOnlyOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	assert := assert.New(t) // nolint
	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"
	name := uuid.NewV4().String()
	drivers := []Driver{
		NewMgoGroupDriver(name, opts, "first"),
		NewMgoGroupDriver(name, opts, "second"),
		NewMgoGroupDriver(name, opts, "first"),
		NewMgoGroupDriver(name, opts, "second"),
		NewMgoGroupDriver(name, opts, "first"),
		NewMgoGroupDriver(name, opts, "second"),
	}
	cleanup := func() { grip.Alert(cleanupMgo(opts.DB, name, drivers[0].(*mgoGroupDriver).session)) }

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	mockJobCounters.Reset()
	runSmokeMultipleQueuesRunJobsOnce(ctx, drivers, cleanup, assert)
}
