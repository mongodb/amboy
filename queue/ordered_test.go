// +build cgo,!gccgo

package queue

import (
	"testing"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/dependency"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/amboy/pool"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/net/context"
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

func (s *OrderedQueueSuite) TestPutReturnsErrorForDuplicateNameTasks() {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.Start(ctx))
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := s.queue.Runner()
	s.False(s.queue.Started())
	s.NoError(s.queue.Start(ctx))
	s.True(s.queue.Started())

	newRunner := pool.NewLocalWorkers(2, s.queue)
	s.Error(s.queue.SetRunner(newRunner))
	s.NotEqual(runner, newRunner)
}

func (s *OrderedQueueSuite) TestResultsChannelProducesPointersToConsistentJobObjects() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job := job.NewShellJob("true", "")
	s.False(job.Completed())

	s.NoError(s.queue.Put(job))
	s.NoError(s.queue.Start(ctx))

	amboy.Wait(s.queue)

	result, ok := <-s.queue.Results()
	s.True(ok)
	s.Equal(job.ID(), result.ID())
	s.True(result.Completed())
}

func (s *OrderedQueueSuite) TestQueueCanOnlyBeStartedOnce() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.False(s.queue.Started())
	s.NoError(s.queue.Start(ctx))
	s.True(s.queue.Started())

	amboy.Wait(s.queue)
	s.True(s.queue.Started())

	// you can call start more than once until the queue has
	// completed
	s.NoError(s.queue.Start(ctx))
	s.True(s.queue.Started())
}

func (s *OrderedQueueSuite) TestQueueFailsToStartIfGraphIsOutOfSync() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// this shouldn't be possible, but if there's a bug in Put,
	// there's some internal structures that might get out of
	// sync, so a test seems in order.

	// first simulate a put bug
	j := job.NewShellJob("true", "")

	s.NoError(s.queue.Put(j))

	jtwo := job.NewShellJob("echo foo", "")
	s.queue.tasks.m["foo"] = jtwo

	s.Error(s.queue.Start(ctx))
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Error(s.queue.Start(ctx))
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Error(s.queue.Start(ctx))
}

func (s *OrderedQueueSuite) TestPassedIsCompletedButDoesNotRun() {
	j1 := job.NewShellJob("echo foo", "")
	j2 := job.NewShellJob("true", "")
	j1.SetDependency(dependency.NewCreatesFile("ordered_test.go"))
	s.NoError(j1.Dependency().AddEdge(j2.ID()))
	s.Equal(dependency.Passed, j1.Dependency().State())

	s.NoError(s.queue.Put(j1))
	s.NoError(s.queue.Put(j2))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.Start(ctx))
	amboy.Wait(s.queue)
	s.False(j1.Completed())
	s.True(j2.Completed())
}
