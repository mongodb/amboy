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
	jobID string
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

	j := job.NewShellJob("echo true", "")
	s.jobID = j.ID()
	s.NoError(s.queue.Put(j))
}

func (s *SQSFifoQueueSuite) TestPutMethodErrorsForDuplicateJobs() {
	grip.Alert("getting job")
	job, ok := s.queue.Get(s.jobID)
	s.True(ok)
	grip.Alert("putting duplicate")
	s.Error(s.queue.Put(job))
}

func (s *SQSFifoQueueSuite) TestGetMethodReturnsRequestedJob() {
	grip.Alert("Started TestGetMethod")
	job, ok := s.queue.Get(s.jobID)
	s.True(ok)
	s.NotNil(job)
	s.Equal(s.jobID, job.ID())
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

func (s *SQSFifoQueueSuite) TestCompleteMethodChangesStatsAndResults() {
	j := job.NewShellJob("echo true", "")
	grip.Alert("Putting job")
	s.NoError(s.queue.Put(j))
	grip.Alert("Job put")
	s.queue.Complete(context.Background(), j)
	grip.Alert("Job complete")

	counter := 0
	results := s.queue.Results(context.Background())
	for job := range results {
		s.Require().NotNil(job)
		s.Equal(j.ID(), job.ID())
		counter++
	}
	grip.Alert("Went through results")
	stats := s.queue.Stats()
	s.Equal(1, stats.Completed)
	s.Equal(1, counter)
}
