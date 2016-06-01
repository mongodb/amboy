package pool

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/job"
)

// UnorderedGroupSuite is a collection of tests for an alternate pool
// implementation that allows a single pool of workers to service jobs
// from multiple queues.
type UnorderedGroupSuite struct {
	size    int
	pool    *Group
	require *require.Assertions
	suite.Suite
}

func TestUnorderedGroupSuiteSizeOne(t *testing.T) {
	s := &UnorderedGroupSuite{}
	s.size = 1
	suite.Run(t, s)
}

func TestUnorderedGroupSuiteSizeThree(t *testing.T) {
	s := &UnorderedGroupSuite{}
	s.size = 3
	suite.Run(t, s)
}

func (s *UnorderedGroupSuite) SetupSuite() {
	s.require = s.Require()
}

func (s *UnorderedGroupSuite) SetupTest() {
	s.pool = NewGroup(s.size)
}

func (s *UnorderedGroupSuite) TestConstructedInstanaceImplementsInterface() {
	s.Implements((*amboy.Runner)(nil), s.pool)
}

func (s *UnorderedGroupSuite) TestSetQueueExtendsInternalTracking() {
	s.Len(s.pool.queues, 0)

	for i := 1; i <= 20; i++ {
		q := NewQueueTesterInstance()
		err := s.pool.SetQueue(q)
		s.NoError(err)
		s.Len(s.pool.queues, i)
	}
}

func (s *UnorderedGroupSuite) TestSetQueueRejectsChangesAfterPoolHasStarted() {
	s.Len(s.pool.queues, 0)

	s.False(s.pool.Started())
	s.pool.started = true
	s.True(s.pool.Started())

	q := NewQueueTesterInstance()
	s.Error(s.pool.SetQueue(q))
	s.True(s.pool.Started())

	s.Len(s.pool.queues, 0)
}

func (s *UnorderedGroupSuite) TestSetQueueRejectsChangesAfterQueueHasStarted() {
	s.Len(s.pool.queues, 0)

	q := NewQueueTesterInstance()
	s.NoError(s.pool.SetQueue(q))
	s.Len(s.pool.queues, 1)

	newQ := NewQueueTesterInstance()
	s.False(newQ.Started())
	newQ.started = true
	s.True(newQ.Started())

	s.False(s.pool.Started())
	s.Error(s.pool.SetQueue(newQ))

	s.Len(s.pool.queues, 1)
}

func (s *UnorderedGroupSuite) TestSizeMethodReturnsValueOfPoolSize() {
	s.Equal(s.size, s.pool.Size())
	s.Equal(s.pool.size, s.pool.Size())
}

func (s *UnorderedGroupSuite) TestPoolSizeCannotBeSetLessThanOne() {
	s.Equal(s.pool.Size(), s.size)

	for i := -20; i <= 0; i++ {
		err := s.pool.SetSize(i)

		s.Error(err)
		s.Equal(s.pool.Size(), s.size)
	}
}

func (s *UnorderedGroupSuite) TestSettingPoolSizeAreSettableForUnstartedPoolsWithValidSizes() {
	s.Equal(s.pool.Size(), s.size)

	s.False(s.pool.Started())

	for i := 1; i <= 20; i++ {
		err := s.pool.SetSize(i)

		s.NoError(err)
		s.Equal(s.pool.Size(), i)
	}
}

func (s *UnorderedGroupSuite) TestPoolSizeAreNotSettableForStartedPools() {
	s.Equal(s.pool.Size(), s.size)

	s.NoError(s.pool.Start())
	s.True(s.pool.Started())
	for i := 1; i <= 20; i++ {
		err := s.pool.SetSize(i)

		s.Error(err)
		s.Equal(s.pool.Size(), s.size)
	}
}

func (s *UnorderedGroupSuite) TestGroupDoesNotErrorsOnSuccessiveStarts() {
	s.False(s.pool.Started())

	s.NoError(s.pool.Start())
	s.True(s.pool.Started())

	for i := 0; i < 20; i++ {
		s.NoError(s.pool.Start())
		s.True(s.pool.Started())
	}
}

func (s *UnorderedGroupSuite) TestQueueIsMutableBeforeStartingPool() {
	s.Len(s.pool.queues, 0)
	s.False(s.pool.Started())

	queue := NewQueueTesterInstance()
	newQueue := NewQueueTesterInstance()
	s.NoError(s.pool.SetQueue(newQueue))
	s.NoError(s.pool.SetQueue(queue))

	s.Len(s.pool.queues, 2)
}

func (s *UnorderedGroupSuite) TestPoolStartsAndProcessesJobs() {
	queueOne := NewQueueTesterInstance()
	jobsOne := []amboy.Job{
		job.NewShellJob("echo one", ""),
		job.NewShellJob("echo two", ""),
		job.NewShellJob("echo three", ""),
		job.NewShellJob("echo four", ""),
		job.NewShellJob("echo five", ""),
	}
	for _, job := range jobsOne {
		s.NoError(queueOne.Put(job))
	}

	queueTwo := NewQueueTesterInstance()
	jobsTwo := []amboy.Job{
		job.NewShellJob("echo one", ""),
		job.NewShellJob("echo two", ""),
		job.NewShellJob("echo three", ""),
		job.NewShellJob("echo four", ""),
		job.NewShellJob("echo five", ""),
	}
	for _, job := range jobsTwo {
		s.NoError(queueTwo.Put(job))
	}

	s.False(queueOne.Started())
	s.False(queueTwo.Started())

	s.NoError(queueOne.SetRunner(s.pool))
	s.NoError(queueTwo.SetRunner(s.pool))

	s.NoError(s.pool.SetQueue(queueOne))
	s.NoError(s.pool.SetQueue(queueTwo))
	s.NoError(s.pool.Start())

	s.True(s.pool.Started())
	// only starting one queue: the pool will start the queue if
	// it needs to.

	queueOne.Close()
	queueTwo.Close()
	s.pool.Wait()

	for _, job := range jobsOne {
		s.True(job.Completed())
	}
	for _, job := range jobsTwo {
		s.True(job.Completed())
	}

	s.NoError(s.pool.Error())
}
