package pool

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/job"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/level"
)

type LocalWorkersSuite struct {
	size  int
	pool  *LocalWorkers
	queue *QueueTester
	suite.Suite
}

func TestLocalWorkersSuiteSizeOne(t *testing.T) {
	s := new(LocalWorkersSuite)
	s.size = 1

	suite.Run(t, s)
}

func TestLocalWorkersSuiteSizeThree(t *testing.T) {
	s := new(LocalWorkersSuite)
	s.size = 3

	suite.Run(t, s)
}

func TestLocalWorkersSuiteSizeOneHundred(t *testing.T) {
	s := new(LocalWorkersSuite)
	s.size = 100

	suite.Run(t, s)
}

func (s *LocalWorkersSuite) SetupSuite() {
	grip.SetThreshold(level.Critical)
}

func (s *LocalWorkersSuite) SetupTest() {
	s.pool = NewLocalWorkers(s.size, nil)
	s.queue = NewQueueTester(s.pool)
}

func (s *LocalWorkersSuite) TestConstructedInstanceImplementsInterface() {
	s.Implements((*amboy.Runner)(nil), s.pool)
}

func (s *LocalWorkersSuite) TestPoolSizeCannotBeSetLessThanOne() {
	s.Equal(s.pool.Size(), s.size)

	for i := -20; i <= 0; i++ {
		err := s.pool.SetSize(i)

		s.Error(err)
		s.Equal(s.pool.Size(), s.size)
	}
}

func (s *LocalWorkersSuite) TestSettingPoolSizeAreSettableForUnstartedPoolsWithValidSizes() {
	s.Equal(s.pool.Size(), s.size)

	s.False(s.pool.Started())

	for i := 1; i <= 20; i++ {
		err := s.pool.SetSize(i)

		s.NoError(err)
		s.Equal(s.pool.Size(), i)
	}
}

func (s *LocalWorkersSuite) TestPoolSizeAreNotSettableForStartedPools() {
	s.Equal(s.pool.Size(), s.size)

	s.NoError(s.pool.Start())
	s.True(s.pool.Started())
	for i := 1; i <= 20; i++ {
		err := s.pool.SetSize(i)

		s.Error(err)
		s.Equal(s.pool.Size(), s.size)
	}
}

func (s *LocalWorkersSuite) TestPoolErrorsOnSuccessiveStarts() {
	s.False(s.pool.Started())

	s.NoError(s.pool.Start())
	s.True(s.pool.Started())

	for i := 0; i < 20; i++ {
		s.Error(s.pool.Start())
		s.True(s.pool.Started())
	}
}

func (s *LocalWorkersSuite) TestPoolStartsAndProcessesJobs() {
	const num int = 100
	var jobs []amboy.Job

	for i := 0; i < num; i++ {
		cmd := fmt.Sprintf("echo 'task=%d'", i)
		jobs = append(jobs, job.NewShellJob(cmd, ""))
	}

	s.False(s.pool.Started())
	s.False(s.queue.Started())
	s.NoError(s.pool.Error())

	s.NoError(s.queue.Start())

	for _, job := range jobs {
		s.NoError(s.queue.Put(job))
	}

	s.True(s.pool.Started())
	s.True(s.queue.Started())

	s.queue.Close() // this should call pool.Wait()
	s.pool.Wait()   // this is effectively a noop, but just to be sure
	for _, job := range jobs {
		s.True(job.Completed())
	}

	s.NoError(s.pool.Error())
}

func (s *LocalWorkersSuite) TestQueueIsMutableBeforeStartingPool() {
	s.NotNil(s.pool.queue)
	s.False(s.pool.Started())

	newQueue := NewQueueTester(s.pool)
	s.NoError(s.pool.SetQueue(newQueue))

	s.Equal(newQueue, s.pool.queue)
	s.NotEqual(s.queue, s.pool.queue)
}

func (s *LocalWorkersSuite) TestQueueIsNotMutableAfterStartingPool() {
	s.NotNil(s.pool.queue)
	s.False(s.pool.Started())

	s.NoError(s.pool.Start())
	s.True(s.pool.Started())

	newQueue := NewQueueTester(s.pool)
	s.Error(s.pool.SetQueue(newQueue))

	s.Equal(s.queue, s.pool.queue)
	s.NotEqual(newQueue, s.pool.queue)
}

// This test makes sense to do without the fixtures in the suite

func TestLocalWorkerPoolConstructorDoesNotAllowSizeValuesLessThanOne(t *testing.T) {
	assert := assert.New(t)
	var pool *LocalWorkers

	for _, size := range []int{-10, -1, 0} {
		pool = NewLocalWorkers(size, nil)
		assert.Equal(1, pool.size)
	}
}
