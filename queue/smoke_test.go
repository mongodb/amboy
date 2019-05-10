package queue

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/level"
	"github.com/mongodb/grip/send"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/mgo.v2"
)

func init() {
	grip.SetName("amboy.queue.tests")
	grip.Error(grip.SetSender(send.MakeNative()))

	lvl := grip.GetSender().Level()
	lvl.Threshold = level.Error
	_ = grip.GetSender().SetLevel(lvl)

	job.RegisterDefaultJobs()
}

const defaultLocalQueueCapcity = 10000

////////////////////////////////////////////////////////////////////////////////
//
// Generic smoke/integration tests for queues.
//
////////////////////////////////////////////////////////////////////////////////

func runMultiQueueSingleBackEndSmokeTest(ctx context.Context, qOne, qTwo amboy.Queue, assert *assert.Assertions) {
	assert.NoError(qOne.Start(ctx))
	assert.NoError(qTwo.Start(ctx))

	num := 200
	adderProcs := 4

	wg := &sync.WaitGroup{}
	for o := 0; o < adderProcs; o++ {
		wg.Add(1)
		go func(o int) {
			defer wg.Done()
			// add a bunch of jobs: half to one queue and half to the other.
			for i := 0; i < num; i++ {
				cmd := fmt.Sprintf("echo %d.%d", o, i)
				j := job.NewShellJob(cmd, "")
				if i%2 == 0 {
					assert.NoError(qOne.Put(j))
					assert.Error(qOne.Put(j))
					continue
				}
				assert.NoError(qTwo.Put(j))
			}
		}(o)
	}
	wg.Wait()

	num = num * adderProcs

	grip.Info("added jobs to queues")

	// check that both queues see all jobs
	statsOne := qOne.Stats()
	statsTwo := qTwo.Stats()

	assert.Equal(statsOne.Total, num)
	assert.Equal(statsTwo.Total, num)

	grip.Infof("before wait statsOne: %+v", statsOne)
	grip.Infof("before wait statsTwo: %+v", statsTwo)

	// wait for all jobs to complete.
	amboy.WaitCtxInterval(ctx, qOne, 100*time.Millisecond)
	amboy.WaitCtxInterval(ctx, qTwo, 100*time.Millisecond)

	grip.Infof("after wait statsOne: %+v", qOne.Stats())
	grip.Infof("after wait statsTwo: %+v", qTwo.Stats())

	// check that all the results in the queues are are completed,
	// and unique
	firstCount := 0
	results := make(map[string]struct{})
	for result := range qOne.Results(ctx) {
		firstCount++
		assert.True(result.Status().Completed)
		results[result.ID()] = struct{}{}
	}

	secondCount := 0
	// make sure that all of the results in the second queue match
	// the results in the first queue.
	for result := range qTwo.Results(ctx) {
		secondCount++
		assert.True(result.Status().Completed)
		results[result.ID()] = struct{}{}
	}

	assert.Equal(firstCount, secondCount)
	assert.Equal(len(results), firstCount)
}

//////////////////////////////////////////////////////////////////////
//
// Integration tests with different queue and driver implementations
//
//////////////////////////////////////////////////////////////////////

func TestSmokeMultipleMgoDriverRemoteUnorderedQueuesWithTheSameName(t *testing.T) {
	assert := assert.New(t) // nolint
	ctx, cancel := context.WithCancel(context.Background())

	name := strings.Replace(uuid.NewV4().String(), "-", ".", -1)

	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"

	// create queues with two runners, mongodb backed drivers, and
	// configure injectors
	qOne := NewRemoteUnordered(runtime.NumCPU() / 2)
	dOne := NewMgoDriver(name, opts).(*mgoDriver)
	qTwo := NewRemoteUnordered(runtime.NumCPU() / 2)
	dTwo := NewMgoDriver(name, opts).(*mgoDriver)
	assert.NoError(dOne.Open(ctx))
	assert.NoError(dTwo.Open(ctx))
	assert.NoError(qOne.SetDriver(dOne))
	assert.NoError(qTwo.SetDriver(dTwo))

	runMultiQueueSingleBackEndSmokeTest(ctx, qOne, qTwo, assert)

	// release runner/driver resources.
	cancel()

	// do cleanup.
	grip.Error(cleanupMgo(opts.DB, name, dOne.session))
}

func TestSmokeMultipleMongoDriverRemoteUnorderedQueuesWithTheSameName(t *testing.T) {
	assert := assert.New(t) // nolint
	ctx, cancel := context.WithCancel(context.Background())

	name := strings.Replace(uuid.NewV4().String(), "-", ".", -1)

	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"

	// create queues with two runners, mongodb backed drivers, and
	// configure injectors
	qOne := NewRemoteUnordered(runtime.NumCPU() / 2)
	dOne := NewMongoDriver(name, opts).(*mongoDriver)
	qTwo := NewRemoteUnordered(runtime.NumCPU() / 2)
	dTwo := NewMongoDriver(name, opts).(*mongoDriver)
	assert.NoError(dOne.Open(ctx))
	assert.NoError(dTwo.Open(ctx))
	assert.NoError(qOne.SetDriver(dOne))
	assert.NoError(qTwo.SetDriver(dTwo))

	runMultiQueueSingleBackEndSmokeTest(ctx, qOne, qTwo, assert)

	// do cleanup.
	grip.Error(cleanupMongo(ctx, opts.DB, name, dOne.client))

	// release runner/driver resources.
	cancel()
}

func TestSmokeMultipleMgoBackedRemoteUnorderedQueuesWithTheSameName(t *testing.T) {
	assert := assert.New(t) // nolint
	ctx, cancel := context.WithCancel(context.Background())

	name := strings.Replace(uuid.NewV4().String(), "-", ".", -1)

	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"

	// create queues with two runners, mongodb backed drivers, and
	// configure injectors
	qOne := NewRemoteUnordered(runtime.NumCPU() / 2)
	dOne := NewMgoDriver(name, opts).(*mgoDriver)
	qTwo := NewRemoteUnordered(runtime.NumCPU() / 2)
	dTwo := NewMgoDriver(name, opts).(*mgoDriver)
	assert.NoError(dOne.Open(ctx))
	assert.NoError(dTwo.Open(ctx))
	assert.NoError(qOne.SetDriver(dOne))
	assert.NoError(qTwo.SetDriver(dTwo))

	runMultiQueueSingleBackEndSmokeTest(ctx, qOne, qTwo, assert)

	// release runner/driver resources.
	cancel()

	// do cleanup.
	grip.Error(cleanupMgo(opts.DB, name, dOne.session))
}

func TestSmokeMultipleMongoBackedRemoteUnorderedQueuesWithTheSameName(t *testing.T) {
	assert := assert.New(t) // nolint
	ctx, cancel := context.WithCancel(context.Background())

	name := strings.Replace(uuid.NewV4().String(), "-", ".", -1)

	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"

	// create queues with two runners, mongodb backed drivers, and
	// configure injectors
	qOne := NewRemoteUnordered(runtime.NumCPU() / 2)
	dOne := NewMongoDriver(name, opts).(*mongoDriver)
	qTwo := NewRemoteUnordered(runtime.NumCPU() / 2)
	dTwo := NewMongoDriver(name, opts).(*mongoDriver)
	assert.NoError(dOne.Open(ctx))
	assert.NoError(dTwo.Open(ctx))
	assert.NoError(qOne.SetDriver(dOne))
	assert.NoError(qTwo.SetDriver(dTwo))

	runMultiQueueSingleBackEndSmokeTest(ctx, qOne, qTwo, assert)

	// do cleanup.
	grip.Error(cleanupMongo(ctx, opts.DB, name, dOne.client))

	// release runner/driver resources.
	cancel()
}

func TestSmokeMultipleLocalBackedRemoteOrderedQueuesWithOneDriver(t *testing.T) {
	if os.Getenv("EVR_TASK_ID") != "" {
		t.Skip("skipping weird test evergreen, only where it deadlocks.",
			"this failure is difficult to debug",
			"the is useful for validating the driver/remote queue interface, but isn't useful")
	}

	assert := assert.New(t) // nolint
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)

	qOne := NewSimpleRemoteOrdered(runtime.NumCPU() / 2)
	qTwo := NewSimpleRemoteOrdered(runtime.NumCPU() / 2)
	d := NewInternalDriver()
	assert.NoError(qOne.SetDriver(d))
	assert.NoError(qTwo.SetDriver(d))

	runMultiQueueSingleBackEndSmokeTest(ctx, qOne, qTwo, assert)
	defer cancel()
	d.Close()
}

func TestSmokeMultipleMgoDriverRemoteOrderedQueuesWithTheSameName(t *testing.T) {
	assert := assert.New(t) // nolint
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

	name := strings.Replace(uuid.NewV4().String(), "-", ".", -1)

	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"

	// create queues with two runners, mongodb backed drivers, and
	// configure injectors
	qOne := NewSimpleRemoteOrdered(runtime.NumCPU() / 2)
	dOne := NewMgoDriver(name, opts).(*mgoDriver)
	qTwo := NewSimpleRemoteOrdered(runtime.NumCPU() / 2)
	dTwo := NewMgoDriver(name, opts).(*mgoDriver)
	assert.NoError(dOne.Open(ctx))
	assert.NoError(dTwo.Open(ctx))
	assert.NoError(qOne.SetDriver(dOne))
	assert.NoError(qTwo.SetDriver(dTwo))

	runMultiQueueSingleBackEndSmokeTest(ctx, qOne, qTwo, assert)

	// release runner/driver resources.
	cancel()

	// do cleanup.
	grip.Error(cleanupMgo(opts.DB, name, dOne.session))
}

func TestSmokeMultipleMongoDriverRemoteOrderedQueuesWithTheSameName(t *testing.T) {
	assert := assert.New(t) // nolint
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	name := strings.Replace(uuid.NewV4().String(), "-", ".", -1)

	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"

	// create queues with two runners, mongodb backed drivers, and
	// configure injectors
	qOne := NewSimpleRemoteOrdered(runtime.NumCPU() / 2)
	dOne := NewMongoDriver(name, opts).(*mongoDriver)
	qTwo := NewSimpleRemoteOrdered(runtime.NumCPU() / 2)
	dTwo := NewMongoDriver(name, opts).(*mongoDriver)
	assert.NoError(dOne.Open(ctx))
	assert.NoError(dTwo.Open(ctx))
	assert.NoError(qOne.SetDriver(dOne))
	assert.NoError(qTwo.SetDriver(dTwo))

	runMultiQueueSingleBackEndSmokeTest(ctx, qOne, qTwo, assert)

	// do cleanup.
	grip.Error(cleanupMongo(ctx, opts.DB, name, dOne.client))
}

func TestSmokeMultipleLocalBackedRemoteUnorderedQueuesWithOneDriver(t *testing.T) {
	if os.Getenv("EVR_TASK_ID") != "" {
		t.Skip("skipping weird test evergreen, only where it deadlocks.",
			"this failure is difficult to debug",
			"the is useful for validating the driver/remote queue interface, but isn't useful")
	}

	assert := assert.New(t) // nolint
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	qOne := NewRemoteUnordered(runtime.NumCPU() / 2)
	qTwo := NewRemoteUnordered(runtime.NumCPU() / 2)
	d := NewInternalDriver()
	assert.NoError(qOne.SetDriver(d))
	assert.NoError(qTwo.SetDriver(d))

	runMultiQueueSingleBackEndSmokeTest(ctx, qOne, qTwo, assert)
}

func TestSmokeMultipleQueuesWithPriorityDriver(t *testing.T) {
	assert := assert.New(t) // nolint
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	qOne := NewRemoteUnordered(runtime.NumCPU() / 2)
	qTwo := NewRemoteUnordered(runtime.NumCPU() / 2)
	d := NewPriorityDriver()
	assert.NoError(qOne.SetDriver(d))
	assert.NoError(qTwo.SetDriver(d))

	runMultiQueueSingleBackEndSmokeTest(ctx, qOne, qTwo, assert)
}

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
