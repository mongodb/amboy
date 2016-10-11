package queue

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/amboy/queue/driver"
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
	job.RegisterDefaultJobs()
}

////////////////////////////////////////////////////////////////////////////////
//
// Generic smoke/integration tests for queues.
//
////////////////////////////////////////////////////////////////////////////////

func runUnorderedSmokeTest(ctx context.Context, q amboy.Queue, size int, assert *assert.Assertions) {
	err := q.Start(ctx)
	if !assert.NoError(err) {
		return
	}

	testNames := []string{"test", "second", "workers", "forty-two", "true", "false", ""}
	numJobs := size * len(testNames)

	wg := &sync.WaitGroup{}

	for i := 0; i < size; i++ {
		wg.Add(1)
		go func(num int) {
			for _, name := range testNames {
				cmd := fmt.Sprintf("echo %s.%d", name, num)
				assert.NoError(q.Put(job.NewShellJob(cmd, "")),
					fmt.Sprintf("with %d workers", num))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	assert.Equal(numJobs, q.Stats().Total, fmt.Sprintf("with %d workers", size))
	q.Wait()

	grip.Infof("workers complete for %d worker smoke test", size)
	assert.Equal(numJobs, q.Stats().Completed, fmt.Sprintf("%+v", q.Stats()))
	for result := range q.Results() {
		assert.True(result.Completed(), fmt.Sprintf("with %d workers", size))
	}
	grip.Infof("completed results check for %d worker smoke test", size)
}

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
					continue
				}
				assert.NoError(qTwo.Put(j))
			}
		}(o)
	}
	wg.Wait()

	grip.Info("added jobs to queues")

	num = num * adderProcs

	// check that both queues see all jobs
	statsOne := qOne.Stats()
	statsTwo := qTwo.Stats()
	assert.Equal(statsOne.Total, num)
	assert.Equal(statsTwo.Total, num)
	grip.Infof("before wait statsOne: %+v", statsOne)
	grip.Infof("before wait statsTwo: %+v", statsTwo)

	// wait for all jobs to complete.
	qOne.Wait()
	qTwo.Wait()

	grip.Infof("after wait statsOne: %+v", qOne.Stats())
	grip.Infof("after wait statsTwo: %+v", qTwo.Stats())

	// check that all the results in queue one are completed, and that
	results := make(map[string]struct{})
	for result := range qOne.Results() {
		assert.True(result.Completed())
		results[result.ID()] = struct{}{}
	}

	// make sure that all of the results in the second queue match
	// the results in the first queue.
	for result := range qTwo.Results() {
		assert.True(result.Completed())
		_, ok := results[result.ID()]
		assert.True(ok)
	}
}

//////////////////////////////////////////////////////////////////////
//
// Integration tests with different queue and driver implementations
//
//////////////////////////////////////////////////////////////////////

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

	for _, poolSize := range []int{2, 4, 8, 16, 32, 64} {
		ctx, cancel := context.WithCancel(context.Background())

		q := NewLocalUnordered(poolSize)
		runUnorderedSmokeTest(ctx, q, poolSize, assert)

		cancel()
	}
}

func TestSmokeRemoteUnorderedWorkerSingleThreadedWithInternalDriver(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	q := NewRemoteUnordered(1)
	d := driver.NewInternal()
	defer d.Close()
	assert.NoError(q.SetDriver(d))

	runUnorderedSmokeTest(ctx, q, 1, assert)
}

func TestSmokeRemoteUnorderedWorkerPoolsWithInternalDriver(t *testing.T) {
	assert := assert.New(t)
	baseCtx := context.Background()

	for _, poolSize := range []int{2, 4, 8, 16, 32, 64} {
		ctx, cancel := context.WithTimeout(baseCtx, time.Minute)

		q := NewRemoteUnordered(poolSize)
		d := driver.NewInternal()
		assert.NoError(q.SetDriver(d))
		runUnorderedSmokeTest(ctx, q, poolSize, assert)

		cancel()
		d.Close()
	}
}

func TestSmokeRemoteUnorderedSingleThreadedWithMongoDBDriver(t *testing.T) {
	assert := assert.New(t)
	name := uuid.NewV4().String()
	opts := driver.DefaultMongoDBOptions()

	ctx, cancel := context.WithCancel(context.Background())
	q := NewRemoteUnordered(1)
	d := driver.NewMongoDB(name, opts)
	assert.NoError(d.Open(ctx))

	assert.NoError(q.SetDriver(d))

	runUnorderedSmokeTest(ctx, q, 1, assert)
	cancel()
	d.Close()
	grip.CatchError(cleanupMongoDB(name, opts))
}

func TestSmokeRemoteUnorderedSingleRunnerWithMongoDBDriver(t *testing.T) {
	assert := assert.New(t)
	name := uuid.NewV4().String()
	opts := driver.DefaultMongoDBOptions()

	ctx, cancel := context.WithCancel(context.Background())
	q := NewRemoteUnordered(1)

	runner := pool.NewSingleRunner()
	assert.NoError(runner.SetQueue(q))
	assert.NoError(q.SetRunner(runner))

	d := driver.NewMongoDB(name, opts)
	assert.NoError(d.Open(ctx))

	assert.NoError(q.SetDriver(d))

	runUnorderedSmokeTest(ctx, q, 1, assert)
	cancel()
	d.Close()
	grip.CatchError(cleanupMongoDB(name, opts))
}

func TestSmokeRemoteUnorderedWorkerPoolsWithMongoDBDriver(t *testing.T) {
	assert := assert.New(t)
	opts := driver.DefaultMongoDBOptions()
	baseCtx := context.Background()

	for _, poolSize := range []int{2, 4, 8, 16, 32, 64} {
		start := time.Now()
		grip.Infof("running mongodb queue smoke test with %d jobs", poolSize)
		q := NewRemoteUnordered(poolSize)
		name := uuid.NewV4().String()

		ctx, cancel := context.WithCancel(baseCtx)
		d := driver.NewMongoDB(name, opts)
		assert.NoError(q.SetDriver(d))

		runUnorderedSmokeTest(ctx, q, poolSize, assert)
		cancel()
		d.Close()

		grip.Infof("test with %d jobs, duration = %s", poolSize, time.Since(start))
		err := cleanupMongoDB(name, opts)
		grip.AlertWhenf(err != nil,
			"encountered error cleaning up %s: %+v", name, err)
	}
}

func TestSmokePriorityQueueWithSingleWorker(t *testing.T) {
	assert := assert.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	q := NewLocalPriorityQueue(1)
	runner := pool.NewSingleRunner()
	assert.NoError(runner.SetQueue(q))

	assert.NoError(q.SetRunner(runner))

	runUnorderedSmokeTest(ctx, q, 1, assert)
}

func TestSmokePriorityQueueWithWorkerPools(t *testing.T) {
	assert := assert.New(t)
	baseCtx := context.Background()

	for _, poolSize := range []int{2, 4, 6, 7, 16, 32, 64} {
		grip.Infoln("testing priority queue for:", poolSize)
		ctx, cancel := context.WithTimeout(baseCtx, time.Minute)

		q := NewLocalPriorityQueue(poolSize)
		runUnorderedSmokeTest(ctx, q, poolSize, assert)

		cancel()
	}
}

func TestSmokePriorityDriverWithRemoteQueueSingleWorker(t *testing.T) {
	assert := assert.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	q := NewRemoteUnordered(1)

	runner := pool.NewSingleRunner()
	assert.NoError(runner.SetQueue(q))
	assert.NoError(q.SetRunner(runner))

	d := driver.NewPriority()
	assert.NoError(d.Open(ctx))
	assert.NoError(q.SetDriver(d))

	runUnorderedSmokeTest(ctx, q, 1, assert)
	d.Close()
}

func TestSmokePriorityDriverWIthRemoteQueueWithWorkerPools(t *testing.T) {
	assert := assert.New(t)
	baseCtx := context.Background()

	for _, poolSize := range []int{2, 4, 6, 7, 16, 32, 64} {
		grip.Infoln("testing priority queue for:", poolSize)
		ctx, cancel := context.WithTimeout(baseCtx, time.Minute)

		q := NewRemoteUnordered(poolSize)
		d := driver.NewPriority()
		assert.NoError(q.SetDriver(d))
		runUnorderedSmokeTest(ctx, q, poolSize, assert)

		cancel()
		d.Close()
	}
}

func TestSmokeMultipleMongoDBQueuesWithTheSameName(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())

	name := uuid.NewV4().String()
	opts := driver.DefaultMongoDBOptions()

	// create queues with two runners, mongodb backed drivers, and
	// configure injectors
	qOne := NewRemoteUnordered(runtime.NumCPU() / 2)
	dOne := driver.NewMongoDB(name, opts)
	qTwo := NewRemoteUnordered(runtime.NumCPU() / 2)
	dTwo := driver.NewMongoDB(name, opts)
	assert.NoError(dOne.Open(ctx))
	assert.NoError(dTwo.Open(ctx))
	assert.NoError(qOne.SetDriver(dOne))
	assert.NoError(qTwo.SetDriver(dTwo))

	runMultiQueueSingleBackEndSmokeTest(ctx, qOne, qTwo, assert)

	// release runner/driver resources.
	cancel()

	// do cleanup.
	grip.CatchError(cleanupMongoDB(name, opts))
}

func TestSmokeMultipleLocalQueuesWithOneDriver(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	qOne := NewRemoteUnordered(runtime.NumCPU() / 2)
	qTwo := NewRemoteUnordered(runtime.NumCPU() / 2)
	d := driver.NewInternal()
	assert.NoError(qOne.SetDriver(d))
	assert.NoError(qTwo.SetDriver(d))

	runMultiQueueSingleBackEndSmokeTest(ctx, qOne, qTwo, assert)
}

func TestSmokeMultipleQueuesWithPriorityDriver(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	qOne := NewRemoteUnordered(runtime.NumCPU() / 2)
	qTwo := NewRemoteUnordered(runtime.NumCPU() / 2)
	d := driver.NewPriority()
	assert.NoError(qOne.SetDriver(d))
	assert.NoError(qTwo.SetDriver(d))

	runMultiQueueSingleBackEndSmokeTest(ctx, qOne, qTwo, assert)
}

func TestSmokeLimitedSizeQueueWithSingleWorker(t *testing.T) {
	assert := assert.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := NewLocalLimitedSize(1, 150)
	runner := pool.NewSingleRunner()
	assert.NoError(runner.SetQueue(q))

	assert.NoError(q.SetRunner(runner))

	runUnorderedSmokeTest(ctx, q, 1, assert)
}

func TestSmokeLimitedSizeQueueWithWorkerPools(t *testing.T) {
	assert := assert.New(t)
	baseCtx := context.Background()

	for _, poolSize := range []int{2, 4, 6, 7, 16, 32, 64} {
		grip.Infoln("testing priority queue for:", poolSize)
		ctx, cancel := context.WithCancel(baseCtx)

		q := NewLocalLimitedSize(poolSize, 7*poolSize)
		runUnorderedSmokeTest(ctx, q, poolSize, assert)

		cancel()
	}
}

func cleanupMongoDB(name string, opt driver.MongoDBOptions) error {
	start := time.Now()

	session, err := mgo.Dial(opt.URI)
	if err != nil {
		return err
	}
	defer session.Close()

	err = session.DB(opt.DB).C(name + ".jobs").DropCollection()
	if err != nil {
		return err
	}
	err = session.DB(opt.DB).C(name + ".locks").DropCollection()
	if err != nil {
		return err
	}

	grip.Infof("clean up operation for %s took %s", name, time.Since(start))
	return nil
}
