package queue

import (
	"testing"

	"github.com/mongodb/amboy/job"
	"github.com/mongodb/amboy/pool"
	"github.com/stretchr/testify/assert"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/level"
	"golang.org/x/net/context"
)

func init() {
	grip.SetThreshold(level.Info)
}

////////////////////////////////////////////////////////////////////////////////
//
// Basic Smoke tests for the un-ordered queue.
//
////////////////////////////////////////////////////////////////////////////////

func runUnorderedSmokeTest(size int, assert *assert.Assertions) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := NewLocalUnordered(1)

	assert.NoError(q.Start(ctx))

	for _, name := range []string{"test", "second", "workers"} {
		assert.NoError(q.Put(job.NewShellJob("echo "+name, "")))
	}
	q.Wait()

	for result := range q.Results() {
		assert.True(result.Completed())
	}
}

func TestSingleRunnerPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	assert := assert.New(t)

	q := NewLocalUnordered(1)
	runner := pool.NewSingleRunner()
	runner.SetQueue(q)
	assert.NoError(q.SetRunner(runner))

	assert.NoError(q.Start(ctx))

	for _, name := range []string{"test", "second", "workers"} {
		assert.NoError(q.Put(job.NewShellJob("echo "+name, "")))
	}
	q.Wait()

	for result := range q.Results() {
		assert.True(result.Completed())
	}
}

func TestUnorderedSingleThreaded(t *testing.T) {
	assert := assert.New(t)

	runUnorderedSmokeTest(1, assert)
}

func TestUnorderedMediumWorkerPools(t *testing.T) {
	assert := assert.New(t)

	for _, poolSize := range []int{2, 4, 8} {
		runUnorderedSmokeTest(poolSize, assert)
	}
}

func TestUnorderedLargeWorkerPools(t *testing.T) {
	assert := assert.New(t)

	for _, poolSize := range []int{16, 32, 64} {
		runUnorderedSmokeTest(poolSize, assert)
	}
}
