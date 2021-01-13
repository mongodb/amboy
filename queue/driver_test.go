package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/job"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

// All drivers should be able to pass this suite of tests which
// exercise the complete functionality of the interface, without
// reaching into the implementation details of any specific interface.

type DriverSuite struct {
	driver            remoteQueueDriver
	driverConstructor func() (remoteQueueDriver, error)
	tearDown          func() error
	ctx               context.Context
	cancel            context.CancelFunc
	suite.Suite
}

// Each driver should invoke this suite:

func TestDriverSuiteWithMongoDBInstance(t *testing.T) {
	tests := new(DriverSuite)
	name := "test-" + uuid.New().String()
	opts := DefaultMongoDBOptions()
	opts.DB = "amboy_test"

	tests.driverConstructor = func() (remoteQueueDriver, error) {
		return newMongoDriver(name, opts)
	}

	tests.tearDown = func() error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mDriver, ok := tests.driver.(*mongoDriver)
		if !ok {
			return errors.New("cannot tear down mongo driver tests because test suite is not running a mongo driver")
		}
		if err := mDriver.getCollection().Database().Drop(ctx); err != nil {
			return errors.Wrapf(err, "removing collection '%s'", mDriver.getCollection().Name())
		}
		return nil
	}

	suite.Run(t, tests)
}

// Implementation of the suite:

func (s *DriverSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	job.RegisterDefaultJobs()
}

func (s *DriverSuite) SetupTest() {
	var err error
	s.driver, err = s.driverConstructor()
	s.Require().NoError(err)
	s.NoError(s.driver.Open(s.ctx))
}

func (s *DriverSuite) TearDownTest() {
	if s.tearDown != nil {
		s.Require().NoError(s.tearDown())
	}
}

func (s *DriverSuite) TearDownSuite() {
	s.cancel()
}

func (s *DriverSuite) TestInitialValues() {
	stats := s.driver.Stats(s.ctx)
	s.Equal(0, stats.Completed)
	s.Equal(0, stats.Running)
	s.Equal(0, stats.Pending)
	s.Equal(0, stats.Blocked)
	s.Equal(0, stats.Total)
}

func (s *DriverSuite) TestPutJobDoesNotAllowDuplicateIds() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	j := job.NewShellJob("echo foo", "")

	err := s.driver.Put(ctx, j)
	s.NoError(err)

	for i := 0; i < 10; i++ {
		err := s.driver.Put(ctx, j)
		s.Error(err)
		s.True(amboy.IsDuplicateJobError(err))
	}
}

func (s *DriverSuite) TestPutJobDoesNotApplyScopesInQueueByDefault() {
	j := job.NewShellJob("echo foo", "")
	j.SetScopes([]string{"scope"})

	s.Require().NoError(s.driver.Put(s.ctx, j))

	s.Equal(1, s.driver.Stats(s.ctx).Total)

	j = job.NewShellJob("echo bar", "")
	j.SetScopes([]string{"scope"})

	s.Require().NoError(s.driver.Put(s.ctx, j))

	s.Equal(2, s.driver.Stats(s.ctx).Total)
}

func (s *DriverSuite) TestPutJobAppliesScopesInQueueIfSet() {
	j := job.NewShellJob("echo foo", "")
	j.SetScopes([]string{"scope"})
	j.SetApplyScopesOnEnqueue(true)

	s.Require().NoError(s.driver.Put(s.ctx, j))

	s.Equal(1, s.driver.Stats(s.ctx).Total)

	j = job.NewShellJob("echo bar", "")
	j.SetScopes([]string{"scope"})
	j.SetApplyScopesOnEnqueue(true)

	s.Error(s.driver.Put(s.ctx, j))

	s.Equal(1, s.driver.Stats(s.ctx).Total)
}

func (s *DriverSuite) TestPutJobAllowsSameScopesInQueueIfDuplicateScopedJobDoesNotApplyScopeOnEnqueue() {
	j := job.NewShellJob("echo foo", "")
	j.SetScopes([]string{"scope"})
	j.SetApplyScopesOnEnqueue(true)

	s.Require().NoError(s.driver.Put(s.ctx, j))

	s.Equal(1, s.driver.Stats(s.ctx).Total)

	j = job.NewShellJob("echo bar", "")
	j.SetScopes([]string{"scope"})

	s.Require().NoError(s.driver.Put(s.ctx, j))

	s.Equal(2, s.driver.Stats(s.ctx).Total)
}

func (s *DriverSuite) TestPutJobAllowsSameScopesInQueueIfInitialScopedJobDoesNotApplyScopeOnEnqueue() {
	j := job.NewShellJob("echo foo", "")
	j.SetScopes([]string{"scope"})

	s.Require().NoError(s.driver.Put(s.ctx, j))

	s.Equal(1, s.driver.Stats(s.ctx).Total)

	j = job.NewShellJob("echo bar", "")
	j.SetScopes([]string{"scope"})
	j.SetApplyScopesOnEnqueue(true)

	s.Require().NoError(s.driver.Put(s.ctx, j))

	s.Equal(2, s.driver.Stats(s.ctx).Total)
}

func (s *DriverSuite) TestPutAndSaveJobSucceedsIfScopeIsAppliedOnEnqueue() {
	j := job.NewShellJob("echo foo", "")
	j.SetScopes([]string{"scope"})

	s.Require().NoError(s.driver.Put(s.ctx, j))
	s.Require().NoError(s.driver.Save(s.ctx, j))
}

func (s *DriverSuite) TestSaveJobPersistsJobInDriver() {
	j := job.NewShellJob("echo foo", "")

	s.Equal(0, s.driver.Stats(s.ctx).Total)

	err := s.driver.Put(s.ctx, j)
	s.NoError(err)

	s.Equal(1, s.driver.Stats(s.ctx).Total)

	// saving a job a second time shouldn't be an error on save
	// and shouldn't result in a new job
	err = s.driver.Save(s.ctx, j)
	s.NoError(err)

	s.Equal(1, s.driver.Stats(s.ctx).Total)
}

func (s *DriverSuite) TestSaveAndGetRoundTripObjects() {
	j := job.NewShellJob("echo foo", "")
	name := j.ID()

	s.Equal(0, s.driver.Stats(s.ctx).Total)

	err := s.driver.Put(s.ctx, j)
	s.NoError(err)

	s.Equal(1, s.driver.Stats(s.ctx).Total)
	n, err := s.driver.Get(s.ctx, name)

	if s.NoError(err) {
		nsh := n.(*job.ShellJob)
		s.Equal(nsh.ID(), j.ID())
		s.Equal(nsh.Command, j.Command)

		s.Equal(n.ID(), name)
		s.Equal(1, s.driver.Stats(s.ctx).Total)
	}
}

func (s *DriverSuite) TestReloadRefreshesJobFromMemory() {
	j := job.NewShellJob("echo foo", "")

	originalCommand := j.Command
	err := s.driver.Put(s.ctx, j)
	s.NoError(err)

	s.Equal(1, s.driver.Stats(s.ctx).Total)

	newCommand := "echo bar"
	j.Command = newCommand

	err = s.driver.Save(s.ctx, j)
	s.Equal(1, s.driver.Stats(s.ctx).Total)
	s.NoError(err)

	reloadedJob, err := s.driver.Get(s.ctx, j.ID())
	s.Require().NoError(err)
	j = reloadedJob.(*job.ShellJob)
	s.NotEqual(originalCommand, j.Command)
	s.Equal(newCommand, j.Command)
}

func (s *DriverSuite) TestGetReturnsErrorIfJobDoesNotExist() {
	j, err := s.driver.Get(s.ctx, "does-not-exist")
	s.Error(err)
	s.Nil(j)
}

func (s *DriverSuite) TestStatsCallReportsCompletedJobs() {
	j := job.NewShellJob("echo foo", "")

	s.Equal(0, s.driver.Stats(s.ctx).Total)
	s.NoError(s.driver.Put(s.ctx, j))
	s.Equal(1, s.driver.Stats(s.ctx).Total)
	s.Equal(0, s.driver.Stats(s.ctx).Completed)
	s.Equal(1, s.driver.Stats(s.ctx).Pending)
	s.Equal(0, s.driver.Stats(s.ctx).Blocked)
	s.Equal(0, s.driver.Stats(s.ctx).Running)

	j.MarkComplete()
	s.NoError(s.driver.Save(s.ctx, j))
	s.Equal(1, s.driver.Stats(s.ctx).Total)
	s.Equal(1, s.driver.Stats(s.ctx).Completed)
	s.Equal(0, s.driver.Stats(s.ctx).Pending)
	s.Equal(0, s.driver.Stats(s.ctx).Blocked)
	s.Equal(0, s.driver.Stats(s.ctx).Running)
}

func (s *DriverSuite) TestNextMethodSkipsCompletedJobs() {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	j := job.NewShellJob("echo foo", "")
	j.MarkComplete()

	s.NoError(s.driver.Put(s.ctx, j))
	s.Equal(1, s.driver.Stats(s.ctx).Total)

	s.Equal(1, s.driver.Stats(s.ctx).Total)
	s.Equal(0, s.driver.Stats(s.ctx).Blocked)
	s.Equal(0, s.driver.Stats(s.ctx).Pending)
	s.Equal(1, s.driver.Stats(s.ctx).Completed)

	s.Nil(s.driver.Next(ctx), fmt.Sprintf("%T", s.driver))
}

func (s *DriverSuite) TestNextMethodDoesNotReturnLastJob() {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	j := job.NewShellJob("echo foo", "")
	s.Require().NoError(j.Lock("taken", amboy.LockTimeout))

	s.NoError(s.driver.Put(s.ctx, j))
	s.Equal(1, s.driver.Stats(s.ctx).Total)
	s.Equal(0, s.driver.Stats(s.ctx).Blocked)
	s.Equal(1, s.driver.Stats(s.ctx).Pending)
	s.Equal(0, s.driver.Stats(s.ctx).Completed)

	s.Nil(s.driver.Next(ctx), fmt.Sprintf("%T", s.driver))
}

func (s *DriverSuite) TestJobsMethodReturnsAllJobs() {
	mocks := make(map[string]*job.ShellJob)

	for idx := range [24]int{} {
		name := fmt.Sprintf("echo test num %d", idx)
		j := job.NewShellJob(name, "")
		s.NoError(s.driver.Put(s.ctx, j))
		mocks[j.ID()] = j
	}

	counter := 0
	for j := range s.driver.Jobs(s.ctx) {
		task := j.(*job.ShellJob)
		counter++
		mock, ok := mocks[j.ID()]
		if s.True(ok) {
			s.Equal(mock.ID(), task.ID())
			s.Equal(mock.Command, task.Command)
		}
	}

	s.Equal(counter, len(mocks))
}

func (s *DriverSuite) TestStatsMethodReturnsAllJobs() {
	names := make(map[string]struct{})

	for i := 0; i < 30; i++ {
		cmd := fmt.Sprintf("echo 'foo: %d'", i)
		j := job.NewShellJob(cmd, "")

		s.NoError(s.driver.Put(s.ctx, j))
		names[j.ID()] = struct{}{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	counter := 0
	for stat := range s.driver.JobStats(ctx) {
		_, ok := names[stat.ID]
		s.True(ok)
		counter++
	}
	s.Equal(len(names), counter)
	s.Equal(counter, 30)
}

func (s *DriverSuite) TestReturnsDefaultLockTimeout() {
	s.Equal(amboy.LockTimeout, s.driver.LockTimeout())
}

func (s *DriverSuite) TestInfoReturnsConfigurableLockTimeout() {
	opts := DefaultMongoDBOptions()
	opts.LockTimeout = 25 * time.Minute
	d, err := newMongoDriver(s.T().Name(), opts)
	s.Require().NoError(err)
	s.Equal(opts.LockTimeout, d.LockTimeout())
}
