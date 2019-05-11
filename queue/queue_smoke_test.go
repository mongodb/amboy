package queue

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/grip"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func UnorderedTest(bctx context.Context, t *testing.T, test QueueTestCase, driver DriverTestCase, runner PoolTestCase, size SizeTestCase) {
	ctx, cancel := context.WithCancel(bctx)
	defer cancel()

	q, err := test.Constructor(ctx, size.Size)
	require.NoError(t, err)
	require.NoError(t, runner.SetPool(q, size.Size))
	dcloser, err := driver.SetDriver(ctx, q, newDriverID())
	defer func() { require.NoError(t, dcloser(ctx)) }()
	require.NoError(t, err)

	if test.OrderedSupported && !test.OrderedStartsBefore {
		// pass
	} else {
		require.NoError(t, q.Start(ctx))
	}

	testNames := []string{"test", "second", "workers", "forty-two", "true", "false", ""}
	numJobs := size.Size * len(testNames)

	wg := &sync.WaitGroup{}

	for i := 0; i < size.Size; i++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			for _, name := range testNames {
				cmd := fmt.Sprintf("echo %s.%d", name, num)
				j := job.NewShellJob(cmd, "")
				assert.NoError(t, q.Put(j),
					fmt.Sprintf("with %d workers", num))
				_, ok := q.Get(j.ID())
				assert.True(t, ok)
			}
		}(i)
	}
	wg.Wait()
	if test.OrderedSupported && !test.OrderedStartsBefore {
		require.NoError(t, q.Start(ctx))
	}
	time.Sleep(100 * time.Millisecond)

	amboy.WaitCtxInterval(ctx, q, 100*time.Millisecond)

	assert.Equal(t, numJobs, q.Stats().Total, fmt.Sprintf("with %d workers", size.Size))

	amboy.WaitCtxInterval(ctx, q, 100*time.Millisecond)

	grip.Infof("workers complete for %d worker smoke test", size.Size)
	assert.Equal(t, numJobs, q.Stats().Completed, fmt.Sprintf("%+v", q.Stats()))
	for result := range q.Results(ctx) {
		assert.True(t, result.Status().Completed, fmt.Sprintf("with %d workers", size.Size))

		// assert that we had valid time info persisted
		ti := result.TimeInfo()
		assert.NotZero(t, ti.Start)
		assert.NotZero(t, ti.End)
	}

	statCounter := 0
	for stat := range q.JobStats(ctx) {
		statCounter++
		assert.True(t, stat.ID != "")
	}
	assert.Equal(t, numJobs, statCounter, fmt.Sprintf("want jobStats for every job"))

	grip.Infof("completed results check for %d worker smoke test", size.Size)
}

func OrderedTest(bctx context.Context, t *testing.T, test QueueTestCase, driver DriverTestCase, runner PoolTestCase, size SizeTestCase) {
	ctx, cancel := context.WithCancel(bctx)
	defer cancel()

	q, err := test.Constructor(ctx, size.Size)
	require.NoError(t, err)
	require.NoError(t, runner.SetPool(q, size.Size))

	dcloser, err := driver.SetDriver(ctx, q, newDriverID())
	defer func() { require.NoError(t, dcloser(ctx)) }()
	require.NoError(t, err)

	var lastJobName string

	testNames := []string{"amboy", "cusseta", "jasper", "sardis", "dublin"}

	numJobs := size.Size / 2 * len(testNames)

	tempDir, err := ioutil.TempDir("", strings.Join([]string{"amboy-ordered-queue-smoke-test",
		uuid.NewV4().String()}, "-"))
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	if test.OrderedStartsBefore {
		require.NoError(t, q.Start(ctx))
	}
	for i := 0; i < size.Size/2; i++ {
		for _, name := range testNames {
			fn := filepath.Join(tempDir, fmt.Sprintf("%s.%d", name, i))
			cmd := fmt.Sprintf("echo %s", fn)
			j := job.NewShellJob(cmd, fn)
			if lastJobName != "" {
				require.NoError(t, j.Dependency().AddEdge(lastJobName))
			}
			lastJobName = j.ID()

			require.NoError(t, q.Put(j))
		}
	}

	if !test.OrderedStartsBefore {
		require.NoError(t, q.Start(ctx))
	}

	require.Equal(t, numJobs, q.Stats().Total, fmt.Sprintf("with %d workers", size.Size))
	amboy.WaitCtxInterval(ctx, q, 50*time.Millisecond)
	require.Equal(t, numJobs, q.Stats().Completed, fmt.Sprintf("%+v", q.Stats()))
	for result := range q.Results(ctx) {
		require.True(t, result.Status().Completed, fmt.Sprintf("with %d workers", size.Size))
	}

	statCounter := 0
	for stat := range q.JobStats(ctx) {
		statCounter++
		require.True(t, stat.ID != "")
	}
	require.Equal(t, statCounter, numJobs)
}

func WaitUntilTest(bctx context.Context, t *testing.T, test QueueTestCase, driver DriverTestCase, runner PoolTestCase, size SizeTestCase) {
	ctx, cancel := context.WithCancel(bctx)
	defer cancel()

	q, err := test.Constructor(ctx, size.Size)
	require.NoError(t, err)
	require.NoError(t, runner.SetPool(q, size.Size))

	dcloser, err := driver.SetDriver(ctx, q, newDriverID())
	defer func() { require.NoError(t, dcloser(ctx)) }()
	require.NoError(t, err)

	require.NoError(t, q.Start(ctx))

	testNames := []string{"test", "second", "workers", "forty-two", "true", "false", ""}
	numJobs := size.Size * len(testNames)

	wg := &sync.WaitGroup{}

	for i := 0; i < size.Size; i++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			for _, name := range testNames {
				cmd := fmt.Sprintf("echo %s.%d.a", name, num)
				j := job.NewShellJob(cmd, "")
				ti := j.TimeInfo()
				require.Zero(t, ti.WaitUntil)
				require.NoError(t, q.Put(j), fmt.Sprintf("(a) with %d workers", num))
				_, ok := q.Get(j.ID())
				require.True(t, ok)

				cmd = fmt.Sprintf("echo %s.%d.b", name, num)
				j2 := job.NewShellJob(cmd, "")
				j2.UpdateTimeInfo(amboy.JobTimeInfo{
					WaitUntil: time.Now().Add(time.Hour),
				})
				ti2 := j2.TimeInfo()
				require.NotZero(t, ti2.WaitUntil)
				require.NoError(t, q.Put(j2), fmt.Sprintf("(b) with %d workers", num))
				_, ok = q.Get(j2.ID())
				require.True(t, ok)
			}
		}(i)
	}
	wg.Wait()

	require.Equal(t, numJobs*2, q.Stats().Total, fmt.Sprintf("with %d workers", size.Size))

	// wait for things to finish
	time.Sleep(2 * time.Second)

	completed := 0
	for result := range q.Results(ctx) {
		status := result.Status()
		ti := result.TimeInfo()

		if status.Completed || status.InProgress {
			completed++
			require.Zero(t, ti.WaitUntil)
			continue
		}
	}
	assert.Equal(t, numJobs, completed)
}

func OneExecutionTest(bctx context.Context, t *testing.T, test QueueTestCase, driver DriverTestCase, runner PoolTestCase, size SizeTestCase) {
	if test.Name == "LocalOrdered" {
		t.Skip("topological sort deadlocks")
	}
	ctx, cancel := context.WithCancel(bctx)
	defer cancel()

	q, err := test.Constructor(ctx, size.Size)
	require.NoError(t, err)
	require.NoError(t, runner.SetPool(q, size.Size))

	dcloser, err := driver.SetDriver(ctx, q, newDriverID())
	defer func() { require.NoError(t, dcloser(ctx)) }()
	require.NoError(t, err)

	mockJobCounters.Reset()
	count := 40

	if !test.OrderedSupported || test.OrderedStartsBefore {
		require.NoError(t, q.Start(ctx))
	}

	for i := 0; i < count; i++ {
		j := newMockJob()
		jobID := fmt.Sprintf("%d.%d.mock.single-exec", i, job.GetNumber())
		j.SetID(jobID)
		assert.NoError(t, q.Put(j))
	}

	if test.OrderedSupported && !test.OrderedStartsBefore {
		require.NoError(t, q.Start(ctx))
	}

	amboy.WaitCtxInterval(ctx, q, 10*time.Millisecond)
	assert.Equal(t, count, mockJobCounters.Count())
}

func MultiExecutionTest(bctx context.Context, t *testing.T, test QueueTestCase, driver DriverTestCase, runner PoolTestCase, size SizeTestCase, multi MultipleExecutionTestCase) {
	ctx, cancel := context.WithCancel(bctx)
	defer cancel()

	qOne, err := test.Constructor(ctx, size.Size)
	require.NoError(t, err)
	qTwo, err := test.Constructor(ctx, size.Size)
	require.NoError(t, err)
	require.NoError(t, runner.SetPool(qOne, size.Size))
	require.NoError(t, runner.SetPool(qTwo, size.Size))

	closer, err := multi.Setup(ctx, qOne, qTwo)
	require.NoError(t, err)
	defer func() { require.NoError(t, closer(ctx)) }()

	assert.NoError(t, qOne.Start(ctx))
	assert.NoError(t, qTwo.Start(ctx))

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
					assert.NoError(t, qOne.Put(j))
					assert.Error(t, qOne.Put(j))
					continue
				}
				assert.NoError(t, qTwo.Put(j))
			}
		}(o)
	}
	wg.Wait()

	num = num * adderProcs

	grip.Info("added jobs to queues")

	// check that both queues see all jobs
	statsOne := qOne.Stats()
	statsTwo := qTwo.Stats()

	require.Equal(t, statsOne.Total, num)
	require.Equal(t, statsTwo.Total, num)

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
		assert.True(t, result.Status().Completed)
		results[result.ID()] = struct{}{}
	}

	secondCount := 0
	// make sure that all of the results in the second queue match
	// the results in the first queue.
	for result := range qTwo.Results(ctx) {
		secondCount++
		assert.True(t, result.Status().Completed)
		results[result.ID()] = struct{}{}
	}

	assert.Equal(t, firstCount, secondCount)
	assert.Equal(t, len(results), firstCount)
}

func ManyQueueTest(bctx context.Context, t *testing.T, test QueueTestCase, driver DriverTestCase, runner PoolTestCase, size SizeTestCase) {
	ctx, cancel := context.WithCancel(bctx)
	defer cancel()
	driverID := newDriverID()

	sz := size.Size
	if sz > 8 {
		sz = 8
	} else if sz < 2 {
		sz = 2
	}
	drivers, closer, err := driver.Constructor(ctx, driverID, sz)
	require.NoError(t, err)
	defer func() { require.NoError(t, closer(ctx)) }()

	queues := []Remote{}
	for _, driver := range drivers {
		q, err := test.Constructor(ctx, size.Size)
		require.NoError(t, err)
		queue := q.(Remote)

		require.NoError(t, queue.SetDriver(driver))
		require.NoError(t, q.Start(ctx))
		queues = append(queues, queue)
	}

	const (
		inside  = 15
		outside = 10
	)

	mockJobCounters.Reset()
	wg := &sync.WaitGroup{}
	for i := 0; i < len(drivers); i++ {
		for ii := 0; ii < outside; ii++ {
			wg.Add(1)
			go func(f, s int) {
				defer wg.Done()
				for iii := 0; iii < inside; iii++ {
					j := newMockJob()
					j.SetID(fmt.Sprintf("%d-%d-%d-%d", f, s, iii, job.GetNumber()))
					if iii%2 == 0 {
						assert.NoError(t, queues[0].Put(j))
					} else {
						assert.NoError(t, queues[1].Put(j))
					}

				}
			}(i, ii)
		}
	}

	grip.Notice("waiting to add all jobs")
	wg.Wait()

	grip.Notice("waiting to run jobs")

	for _, q := range queues {
		amboy.WaitCtxInterval(ctx, q, 20*time.Millisecond)
	}

	assert.Equal(t, len(drivers)*inside*outside, mockJobCounters.Count())

}
