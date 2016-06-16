package queue

import (
	"testing"

	"github.com/mongodb/amboy/job"
	"github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////////////////////////////////
//
// Basic Smoke tests for the un-ordered queue.
//
////////////////////////////////////////////////////////////////////////////////

func runUnorderedSmokeTest(size int, assert *assert.Assertions) {
	q := NewLocalUnordered(size)
	assert.NoError(q.Start())
	assert.Equal(q.Runner().Size(), size)

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
