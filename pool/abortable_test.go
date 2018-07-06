package pool

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/mongodb/amboy"
	"github.com/stretchr/testify/suite"
)

type AbortablePoolSuite struct {
	pool  *abortablePool
	queue *QueueTester
	suite.Suite
}

type cancelFuncCounter struct {
	mu      sync.Mutex
	counter int
}

func (f *cancelFuncCounter) Canceler() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counter++
}

func TestAbortablePoolSuite(t *testing.T) {
	suite.Run(t, new(AbortablePoolSuite))
}

func (s *AbortablePoolSuite) SetupTest() {
	s.pool = &abortablePool{
		jobs: make(map[string]context.CancelFunc),
	}
	s.queue = &QueueTester{
		pool:      s.pool,
		toProcess: make(chan amboy.Job),
		storage:   make(map[string]amboy.Job),
	}

}

func (s *AbortablePoolSuite) TestImplementationCompliance() {
	s.Implements((*amboy.Runner)(nil), s.pool)
	s.Implements((*amboy.AbortableRunner)(nil), s.pool)

	constructed := NewAbortablePool(2, s.queue)
	s.NotNil(constructed)
	s.Implements((*amboy.Runner)(nil), constructed)
	s.Implements((*amboy.AbortableRunner)(nil), constructed)
}

func (s *AbortablePoolSuite) TestConstructorUnflappability() {
	constructed := NewAbortablePool(-1, nil)
	s.NotNil(constructed)
}

func (s *AbortablePoolSuite) TestCloserCancelsFuncs() {
	closer := &cancelFuncCounter{}
	s.Equal(0, closer.counter)
	s.Len(s.pool.jobs, 0)

	s.pool.jobs["id"] = closer.Canceler
	s.Equal(0, closer.counter)
	s.Len(s.pool.jobs, 1)

	s.pool.Close()
	s.Equal(1, closer.counter)
	s.Len(s.pool.jobs, 0)
}

func (s *AbortablePoolSuite) TestSingleAborterErrorsForUnknownJob() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Error(s.pool.Abort(ctx, "foo"))
	s.Error(s.pool.Abort(ctx, "DOES NOT EXIST"))
}

func (s *AbortablePoolSuite) TestSingleAborterCancelsJob() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	closer := &cancelFuncCounter{}
	s.Equal(0, closer.counter)
	s.pool.jobs["id"] = closer.Canceler
	s.Len(s.pool.jobs, 1)
	s.Equal(0, closer.counter)

	s.Error(s.pool.Abort(ctx, "foo"))
	s.Len(s.pool.jobs, 1)
	s.Equal(0, closer.counter)

	s.pool.queue = s.queue
	_ = s.pool.Abort(ctx, "id")
	s.Len(s.pool.jobs, 0)
	s.Equal(1, closer.counter)
}

func (s *AbortablePoolSuite) TestAbortAllWorks() {
	closers := make([]*cancelFuncCounter, 10)

	s.Len(s.pool.jobs, 0)
	count := 0
	seen := 0
	for idx := range closers {
		closers[idx] = &cancelFuncCounter{}

		seen++
		count += closers[idx].counter
		s.pool.jobs[fmt.Sprint(idx)] = closers[idx].Canceler
	}
	s.Equal(10, seen)
	s.Equal(0, count)

	s.Len(s.pool.jobs, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.pool.queue = s.queue
	s.pool.AbortAll(ctx)
	s.Len(s.pool.jobs, 0)
	seen = 0
	for _, c := range closers {
		count += c.counter
		seen++
	}
	s.Equal(10, seen)
	s.Equal(10, count)
}

func (s *AbortablePoolSuite) TestIntrospectionMethods() {
	closers := make([]*cancelFuncCounter, 10)

	s.Len(s.pool.jobs, 0)
	count := 0
	seen := 0
	for idx := range closers {
		closers[idx] = &cancelFuncCounter{}

		seen++
		count += closers[idx].counter
		s.pool.jobs[fmt.Sprint(idx)] = closers[idx].Canceler
	}

	for idx := range closers {
		s.True(s.pool.IsRunning(fmt.Sprint(idx)))
		s.False(s.pool.IsRunning(fmt.Sprintf("%d-NOT_RUNNING", idx)))
	}

	jobNames := s.pool.RunningJobs()
	s.Len(jobNames, 10)
	sort.Strings(jobNames)

	for idx := range closers {
		s.Equal(jobNames[idx], fmt.Sprint(idx))
	}
}

func (s *AbortablePoolSuite) TestAbortableRunJob() {
	s.Len(s.pool.jobs, 0)

	j := &jobThatPanics{}
	ctx := context.Background()
	s.Panics(func() { s.pool.runJob(ctx, j) })
	s.Len(s.pool.jobs, 0)
}
