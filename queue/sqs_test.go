package queue

import (
	"context"
	"testing"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/grip"
	"github.com/stretchr/testify/suite"
)

type SQSFifoQueueSuite struct {
	queue amboy.Queue
	suite.Suite
}

func TestSQSFifoQueueSuite(t *testing.T) {
	suite.Run(t, new(SQSFifoQueueSuite))
}

func (s *SQSFifoQueueSuite) SetupTest() {
	var err error
	s.queue, err = NewSQSFifoQueue(randomString(4), 4)
	s.NoError(err)
	s.queue.Start(context.Background())
	stats := s.queue.Stats()
	s.Equal(0, stats.Total)
	s.Equal(0, stats.Running)
	s.Equal(0, stats.Pending)
	s.Equal(0, stats.Completed)
}

func (s *SQSFifoQueueSuite) TestPutMethodErrorsForDuplicateJobs() {
	j := job.NewShellJob("echo true", "")
	s.NoError(s.queue.Put(j))
	s.Error(s.queue.Put(j))
}

func (s *SQSFifoQueueSuite) TestJobStatsReturnsAllJobs() {
	j := job.NewShellJob("echo true", "")
	j2 := job.NewShellJob("echo true", "")
	grip.Alert("Putting jobs")
	s.NoError(s.queue.Put(j))
	grip.Alert("Job1 put")
	s.NoError(s.queue.Put(j2))
	grip.Alert("job2 put, getting jobStats")

	counter := 0
	for range s.queue.JobStats(context.Background()) {
		counter++
	}
	s.Equal(2, counter)
}

func (s *SQSFifoQueueSuite) TestGetMethodReturnsRequestedJob() {
	j := job.NewShellJob("echo true", "")
	id := j.ID()
	s.NoError(s.queue.Put(j))
	job, ok := s.queue.Get(id)
	s.True(ok)
	s.NotNil(job)
	s.Equal(id, job.ID())
}

func (s *SQSFifoQueueSuite) TestCannotSetRunnerWhenQueueStarted() {
	s.True(s.queue.Started())
	s.Error(s.queue.SetRunner(pool.NewSingle()))
}

func (s *SQSFifoQueueSuite) TestSetRunnerWhenQueueNotStarted() {
	var err error
	s.queue, err = NewSQSFifoQueue(randomString(4), 4)
	s.NoError(err)
	r := pool.NewSingle()
	s.NoError(s.queue.SetRunner(r))
	s.Equal(r, s.queue.Runner())
}

func (s *SQSFifoQueueSuite) TestCompleteMethodChangesStats() {
	j := job.NewShellJob("echo true", "")
	grip.Alert("Putting job")
	s.NoError(s.queue.Put(j))
	grip.Alert("Job put")
	s.queue.Complete(context.Background(), j)
	grip.Alert("Job complete")

	stats := s.queue.Stats()
	grip.Alert("Stats retrieved")
	s.Equal(1, stats.Total)
	s.Equal(1, stats.Completed)
}

func (s *SQSFifoQueueSuite) TestResultsProducesCompletedJobs() {
	j := job.NewShellJob("echo true", "")
	j2 := job.NewShellJob("echo true", "")
	s.queue.Put(j)
	s.queue.Put(j2)
	s.queue.Complete(context.Background(), j)

	counter := 0
	results := s.queue.Results(context.Background())
	for job := range results {
		s.NotNil(job)
		if job != nil {
			s.Equal(j.ID(), job.ID())
		}
		counter++
	}

	stats := s.queue.Stats()
	s.Equal(1, stats.Completed)
	s.True(stats.Total == 1 || stats.Total == 2)
	s.Equal(1, counter)

}
