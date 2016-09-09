package queue

import (
	"testing"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/amboy/queue/remote"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/level"
	"golang.org/x/net/context"
	"gopkg.in/mgo.v2"
)

func init() {
	grip.SetThreshold(level.Info)
	grip.CatchError(grip.UseNativeLogger())
	grip.SetName("amboy.queue.tests")
}

////////////////////////////////////////////////////////////////////////////////
//
// Basic Smoke tests for the un-ordered queue.
//
////////////////////////////////////////////////////////////////////////////////

func runUnorderedSmokeTest(ctx context.Context, q amboy.Queue, size int, assert *assert.Assertions) {
	assert.NoError(q.Start(ctx))

	for _, name := range []string{"test", "second", "workers", "forty-two", "true", "false", ""} {
		assert.NoError(q.Put(job.NewShellJob("echo "+name, "")))
	}

	assert.Equal(q.Stats().Total, 7)
	q.Wait()

	grip.Infof("workers complete for %d worker smoke test", size)
	assert.Equal(q.Stats().Completed, 7)
	for result := range q.Results() {
		assert.True(result.Completed())
	}
	grip.Infof("completed results check for %d worker smoke test", size)
}

func TestUnorderedSingleThreadedLocalPool(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := NewLocalUnordered(1)
	runUnorderedSmokeTest(ctx, q, 1, assert)
}

func TestUnorderedSingleThreadedSingleRunner(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := NewLocalUnordered(1)

	runner := pool.NewSingleRunner()
	assert.NoError(runner.SetQueue(q))
	assert.NoError(q.SetRunner(runner))

	runUnorderedSmokeTest(ctx, q, 1, assert)
}

func TestSmokeUnorderedWorkerPools(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, poolSize := range []int{2, 4, 8, 16, 32, 64} {
		q := NewLocalUnordered(poolSize)
		runUnorderedSmokeTest(ctx, q, poolSize, assert)
	}
}

func TestSmokeRemoteUnorderedWorkerSingleThreadedWithLocalDriver(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := NewRemoteUnordered(1)
	assert.NoError(q.SetDriver(remote.NewLocalDriver()))

	runUnorderedSmokeTest(ctx, q, 1, assert)
}

func TestSmokeRemoteUnorderedWorkerPoolsWithLocalDriver(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, poolSize := range []int{2, 4, 8, 16, 32, 64} {
		q := NewRemoteUnordered(poolSize)
		assert.NoError(q.SetDriver(remote.NewLocalDriver()))
		runUnorderedSmokeTest(ctx, q, poolSize, assert)
	}
}

func TestSmokeRemoteUnorderedSingleThreadedWithMongoDBDriver(t *testing.T) {
	assert := assert.New(t)
	name := uuid.NewV4().String()
	uri := "mongodb://localhost:27017"

	ctx, cancel := context.WithCancel(context.Background())
	q := NewRemoteUnordered(1)
	d := remote.NewMongoDBQueueDriver(name, uri)
	assert.NoError(d.Open(ctx))

	assert.NoError(q.SetDriver(d))

	runUnorderedSmokeTest(ctx, q, 1, assert)
	cancel()
	d.Close()
	grip.CatchError(cleanupMongoDB(name, uri))
}

func TestSmokeRemoteUnorderedSingleRunnerWithMongoDBDriver(t *testing.T) {
	assert := assert.New(t)
	name := uuid.NewV4().String()
	uri := "mongodb://localhost:27017"

	ctx, cancel := context.WithCancel(context.Background())
	q := NewRemoteUnordered(1)

	runner := pool.NewSingleRunner()
	assert.NoError(runner.SetQueue(q))
	assert.NoError(q.SetRunner(runner))

	d := remote.NewMongoDBQueueDriver(name, uri)
	assert.NoError(d.Open(ctx))

	assert.NoError(q.SetDriver(d))

	runUnorderedSmokeTest(ctx, q, 1, assert)
	cancel()
	d.Close()
	grip.CatchError(cleanupMongoDB(name, uri))
}

func TestSmokeRemoteUnorderedWorkerPoolsWithMongoDBDriver(t *testing.T) {
	assert := assert.New(t)
	uri := "mongodb://localhost:27017"

	baseCtx := context.Background()

	for _, poolSize := range []int{2, 4, 8, 16, 32, 64} {
		start := time.Now()
		grip.Infof("running mongodb queue smoke test with %d jobs", poolSize)
		q := NewRemoteUnordered(poolSize)
		name := uuid.NewV4().String()

		ctx, cancel := context.WithCancel(baseCtx)
		d := remote.NewMongoDBQueueDriver(name, uri)
		assert.NoError(q.SetDriver(d))

		runUnorderedSmokeTest(ctx, q, poolSize, assert)
		cancel()
		d.Close()

		grip.Infof("test with %d jobs, duration = %s", poolSize, time.Since(start))
		err := cleanupMongoDB(name, uri)
		grip.AlertWhenf(err != nil,
			"encountered error cleaning up %s: %+v", name, err)
	}
}

func cleanupMongoDB(name, uri string) error {
	start := time.Now()

	session, err := mgo.Dial(uri)
	if err != nil {
		return err
	}
	defer session.Close()

	err = session.DB("amboy").C(name + ".jobs").DropCollection()
	if err != nil {
		return err
	}
	err = session.DB("amboy").C(name + ".locks").DropCollection()
	if err != nil {
		return err
	}

	grip.Infof("clean up operation for %s took %s", name, time.Since(start))
	return nil
}
