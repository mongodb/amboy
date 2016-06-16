package queue

import (
	"testing"

	"github.com/mongodb/amboy/job"
	"github.com/mongodb/amboy/pool"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type LocalQueueSuite struct {
	size    int
	queue   *LocalUnordered
	require *require.Assertions
	suite.Suite
}

func TestLocalQueueSuiteOneWorker(t *testing.T) {
	s := &LocalQueueSuite{}
	s.size = 1
	suite.Run(t, s)
}

func TestLocalQueueSuiteThreeWorkers(t *testing.T) {
	s := &LocalQueueSuite{}
	s.size = 3
	suite.Run(t, s)
}

func (s *LocalQueueSuite) SetupSuite() {
	s.require = s.Require()
}

func (s *LocalQueueSuite) SetupTest() {
	s.queue = NewLocalUnordered(s.size)
}

func (s *LocalQueueSuite) TestDefaultStateOfQueueObjectIsExpected() {
	s.False(s.queue.closed)
	s.False(s.queue.started)

	s.Len(s.queue.tasks.m, 0)

	s.IsType(s.queue.runner, &pool.LocalWorkers{})
}

func (s *LocalQueueSuite) TestPutReturnsErrorAfterClosingQueue() {
	s.False(s.queue.closed)

	// there's no non-blocking method to close an open queue, so
	// this is a stand in:
	s.queue.closed = true

	s.True(s.queue.closed)
	j := job.NewShellJob("true", "")
	s.Error(s.queue.Put(j))
}

func (s *LocalQueueSuite) TestPutReturnsErrorForDuplicateNameTasks() {
	s.False(s.queue.closed)
	j := job.NewShellJob("true", "")

	s.NoError(s.queue.Start())

	s.NoError(s.queue.Put(j))
	s.Error(s.queue.Put(j))
}

func (s *LocalQueueSuite) TestPuttingAJobIntoAQueueImpactsStats() {
	stats := s.queue.Stats()
	s.Equal(0, stats.Total)
	s.Equal(0, stats.Pending)
	s.Equal(0, stats.Running)
	s.Equal(0, stats.Completed)

	s.NoError(s.queue.Start())

	j := job.NewShellJob("true", "")
	s.NoError(s.queue.Put(j))

	jReturn, ok := s.queue.Get(j.ID())
	s.True(ok)
	s.Exactly(jReturn, j)

	stats = s.queue.Stats()
	s.Equal(1, stats.Total)
	s.Equal(1, stats.Pending)
	s.Equal(1, stats.Running)
	s.Equal(0, stats.Completed)
}

func (s *LocalQueueSuite) TestResultsChannelProducesPointersToConsistentJobObjects() {
	job := job.NewShellJob("true", "")
	s.False(job.Completed())

	s.NoError(s.queue.Start())
	s.NoError(s.queue.Put(job))

	s.queue.Close()

	result, ok := <-s.queue.Results()
	s.True(ok)
	s.Equal(job.ID(), result.ID())
	s.True(result.Completed())
}

func (s *LocalQueueSuite) TestJobsChannelProducesJobObjects() {
	names := map[string]bool{"ru": true, "es": true, "zh": true, "fr": true, "it": true}
	s.NoError(s.queue.Runner().SetSize(1))

	s.Require().Equal(1, s.queue.Runner().Size())

	// this test hinges on the queue system, and this
	// implementation in particular, being FIFO.
	s.NoError(s.queue.Start())

	for name := range names {
		job := job.NewShellJob("echo "+name, "")
		s.NoError(s.queue.Put(job))
	}

	s.queue.Wait()

	for j := range s.queue.Results() {
		shellJob, ok := j.(*job.ShellJob)
		s.True(ok)
		s.True(names[shellJob.Output])
	}
	s.queue.Close()
}

func (s *LocalQueueSuite) TestInternalRunnerCanBeChangedBeforeStartingTheQueue() {
	originalRunner := s.queue.Runner()
	newRunner := pool.NewLocalWorkers(2, s.queue)
	s.NotEqual(originalRunner, newRunner)

	s.NoError(s.queue.SetRunner(newRunner))
	s.Exactly(newRunner, s.queue.Runner())
}

func (s *LocalQueueSuite) TestInternalRunnerCannotBeChangedAfterStartingAQueue() {
	runner := s.queue.Runner()
	s.False(s.queue.Started())
	s.NoError(s.queue.Start())
	s.True(s.queue.Started())

	newRunner := pool.NewLocalWorkers(2, s.queue)
	s.Error(s.queue.SetRunner(newRunner))
	s.NotEqual(runner, newRunner)
}

func (s *LocalQueueSuite) TestQueueCanOnlyBeStartedOnce() {
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

func (s *LocalQueueSuite) TestCompleteQueueStartsWillReturnError() {
	s.False(s.queue.Started())
	s.NoError(s.queue.Start())
	s.True(s.queue.Started())

	s.queue.Wait()
	s.queue.close()

	s.Error(s.queue.Start())
}

func (s *LocalQueueSuite) TestQueuePropogatesRunnerStartError() {
	// fake the runner out so that it will fail
	r := pool.NewLocalWorkers(1, s.queue)
	s.NoError(r.Start())
	s.queue.runner = r

	s.False(s.queue.Started())
	s.Error(s.queue.Start())
}

func (s *LocalQueueSuite) TestPutReturnsErrorIfQueueIsNotStarted() {
	s.False(s.queue.started)
	j := job.NewShellJob("true", "")
	s.Error(s.queue.Put(j))
}
