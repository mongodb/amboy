// +build cgo,!gccgo

package queue

import (
	"testing"

	"github.com/mongodb/amboy/dependency"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/amboy/pool"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type OrderedQueueSuite struct {
	size    int
	queue   *LocalOrdered
	require *require.Assertions
	suite.Suite
}

func TestOrderedQueueSuiteOneWorker(t *testing.T) {
	s := &OrderedQueueSuite{}
	s.size = 1
	suite.Run(t, s)
}

func TestOrderedQueueSuiteThreeWorker(t *testing.T) {
	s := &OrderedQueueSuite{}
	s.size = 3
	suite.Run(t, s)
}

func (s *OrderedQueueSuite) SetupSuite() {
	s.require = s.Require()
}

func (s *OrderedQueueSuite) SetupTest() {
	s.queue = NewLocalOrdered(s.size)
}

func (s *OrderedQueueSuite) TestPutReturnsErrorAfterClosingQueue() {
	s.False(s.queue.closed)

	// there's no non-blocking method to close an open queue, so
	// this is a stand in:
	s.queue.closed = true

	s.True(s.queue.closed)
	j := job.NewShellJob("true", "")
	s.Error(s.queue.Put(j))
}

func (s *OrderedQueueSuite) TestPutReturnsErrorForDuplicateNameTasks() {
	s.False(s.queue.closed)
	j := job.NewShellJob("true", "")

	s.NoError(s.queue.Put(j))
	s.Error(s.queue.Put(j))
}

func (s *OrderedQueueSuite) TestPuttingAJobIntoAQueueImpactsStats() {
	stats := s.queue.Stats()
	s.Equal(0, stats.Total)
	s.Equal(0, stats.Pending)
	s.Equal(0, stats.Running)
	s.Equal(0, stats.Completed)

	j := job.NewShellJob("true", "")
	s.NoError(s.queue.Put(j))

	jReturn, ok := s.queue.Get(j.ID())
	s.True(ok)
	s.Exactly(jReturn, j)

	stats = s.queue.Stats()
	s.Equal(1, stats.Total)
	s.Equal(1, stats.Pending)
	s.Equal(0, stats.Running)
	s.Equal(0, stats.Completed)
}

func (s *OrderedQueueSuite) TestPuttingJobIntoQueueAfterStartingReturnsError() {
	j := job.NewShellJob("true", "")
	s.NoError(s.queue.Put(j))

	s.NoError(s.queue.Start())
	s.Error(s.queue.Put(j))
}

func (s *OrderedQueueSuite) TestInternalRunnerCanBeChangedBeforeStartingTheQueue() {
	originalRunner := s.queue.Runner()
	newRunner := pool.NewLocalWorkers(2, s.queue)
	s.NotEqual(originalRunner, newRunner)

	s.NoError(s.queue.SetRunner(newRunner))
	s.Exactly(newRunner, s.queue.Runner())
}

func (s *OrderedQueueSuite) TestInternalRunnerCannotBeChangedAfterStartingAQueue() {
	runner := s.queue.Runner()
	s.False(s.queue.Started())
	s.NoError(s.queue.Start())
	s.True(s.queue.Started())

	newRunner := pool.NewLocalWorkers(2, s.queue)
	s.Error(s.queue.SetRunner(newRunner))
	s.NotEqual(runner, newRunner)
}

func (s *OrderedQueueSuite) TestResultsChannelProducesPointersToConsistentJobObjects() {
	job := job.NewShellJob("true", "")
	s.False(job.Completed())

	s.NoError(s.queue.Put(job))
	s.NoError(s.queue.Start())

	s.queue.Wait()

	result, ok := <-s.queue.Results()
	s.True(ok)
	s.Equal(job.ID(), result.ID())
	s.True(result.Completed())
}

func (s *OrderedQueueSuite) TestQueuePropogatesRunnerStartError() {
	// fake the runner out so that it will fail
	r := pool.NewLocalWorkers(1, s.queue)
	s.NoError(r.Start())
	s.queue.runner = r

	s.False(s.queue.Started())
	s.Error(s.queue.Start())
}

func (s *OrderedQueueSuite) TestQueueFailsToStartWhenClosed() {
	s.queue.Close()
	s.True(s.queue.closed)
	s.Error(s.queue.Start())
}

func (s *OrderedQueueSuite) TestQueueCanOnlyBeStartedOnce() {
	s.False(s.queue.Started())
	s.NoError(s.queue.Start())
	s.True(s.queue.Started())

	s.queue.Wait()
	s.True(s.queue.Started())

	// you can call start more than once until the queue has
	// completed/closed
	s.NoError(s.queue.Start())
	s.True(s.queue.Started())
}

func (s *OrderedQueueSuite) TestQueueFailsToStartIfGraphIsOutOfSync() {
	// this shouldn't be possible, but if there's a bug in Put,
	// there's some internal structures that might get out of
	// sync, so a test seems in order.

	// first simulate a put bug
	j := job.NewShellJob("true", "")

	s.NoError(s.queue.Put(j))

	jtwo := job.NewShellJob("echo foo", "")
	s.queue.tasks.m["foo"] = jtwo

	s.Error(s.queue.Start())
}

func (s *OrderedQueueSuite) TestQueueFailsToStartIfTaskGraphIsCyclic() {
	j1 := job.NewShellJob("true", "")
	j2 := job.NewShellJob("true", "")

	s.NoError(j1.Dependency().AddEdge(j2.ID()))
	s.NoError(j2.Dependency().AddEdge(j1.ID()))

	s.Len(j1.Dependency().Edges(), 1)
	s.Len(j2.Dependency().Edges(), 1)

	s.NoError(s.queue.Put(j1))
	s.NoError(s.queue.Put(j2))

	s.Error(s.queue.Start())
}

func (s *OrderedQueueSuite) TestQueueFailsToStartIfDependencyDoesNotExist() {
	// this shouldn't be possible, but if the tasks and graph
	// mappings get out of sync, then there's an error on start.

	j1 := job.NewShellJob("true", "")
	j2 := job.NewShellJob("true", "")
	s.NoError(j2.Dependency().AddEdge(j1.ID()))
	s.NoError(j2.Dependency().AddEdge("fooo"))

	s.Len(j1.Dependency().Edges(), 0)
	s.Len(j2.Dependency().Edges(), 2)

	s.NoError(s.queue.Put(j1))
	s.NoError(s.queue.Put(j2))

	s.Error(s.queue.Start())
}

func (s *OrderedQueueSuite) TestPassedIsCompletedButDoesNotRun() {
	j1 := job.NewShellJob("echo foo", "")
	j2 := job.NewShellJob("true", "")
	j1.SetDependency(dependency.NewCreatesFile("ordered_test.go"))
	s.NoError(j1.Dependency().AddEdge(j2.ID()))
	s.Equal(dependency.Passed, j1.Dependency().State())

	s.NoError(s.queue.Put(j1))
	s.NoError(s.queue.Put(j2))

	s.NoError(s.queue.Start())
	s.queue.Wait()
	s.False(j1.Completed())
	s.True(j2.Completed())
}

func (s *OrderedQueueSuite) TestNextMethodReturnsErrorIfQueueIsClosed() {
	s.NoError(s.queue.Start())
	s.queue.Close()
	s.True(s.queue.closed)

	job, err := s.queue.Next()
	s.Nil(job)
	s.Error(err)
}
