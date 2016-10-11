package driver

import (
	"fmt"
	"testing"

	"github.com/mongodb/amboy/job"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tychoish/grip"
	"golang.org/x/net/context"
	"gopkg.in/mgo.v2"
)

// All drivers should be able to pass this suite of tests which
// exercise the complete functionality of the interface, without
// reaching into the implementation details of any specific interface.

type DriverSuite struct {
	uuid              string
	driver            Driver
	driverConstructor func() Driver
	tearDown          func()
	require           *require.Assertions
	suite.Suite
}

// Each driver should invoke this suite:

func TestDriverSuiteWithLocalInstance(t *testing.T) {
	tests := new(DriverSuite)
	tests.uuid = uuid.NewV4().String()
	tests.driverConstructor = func() Driver {
		return NewInternal()
	}

	suite.Run(t, tests)
}

func TestDriverSuiteWithPriorityInstance(t *testing.T) {
	tests := new(DriverSuite)
	tests.uuid = uuid.NewV4().String()
	tests.driverConstructor = func() Driver {
		return NewPriority()
	}

	suite.Run(t, tests)
}

func TestDriverSuiteWithMongoDBInstance(t *testing.T) {
	tests := new(DriverSuite)
	tests.uuid = uuid.NewV4().String()
	mDriver := NewMongoDB(
		"test-"+tests.uuid,
		DefaultMongoDBOptions())
	// "mongodb://localhost:27021,localhost:27022?replicaSet=rs0-1447273659")
	tests.driverConstructor = func() Driver {
		return mDriver
	}
	tests.tearDown = func() {
		for _, coll := range []*mgo.Collection{mDriver.jobsCollection, mDriver.locksCollection} {
			err := coll.DropCollection()
			grip.Infof("removed %s collection (%+v)", coll.Name, err)
		}
	}

	suite.Run(t, tests)
}

// Implementation of the suite:

func (s *DriverSuite) SetupSuite() {
	job.RegisterDefaultJobs()
	s.require = s.Require()
}

func (s *DriverSuite) SetupTest() {
	s.driver = s.driverConstructor()
	s.NoError(s.driver.Open(context.Background()))
}

func (s *DriverSuite) TearDownTest() {
	if s.tearDown != nil {
		s.tearDown()
	}
}

func (s *DriverSuite) TestInitialValues() {
	stats := s.driver.Stats()
	s.Equal(0, stats.Complete)
	s.Equal(0, stats.Locked)
	s.Equal(0, stats.Total)
	s.Equal(0, stats.Unlocked)
}

func (s *DriverSuite) TestPutJobPersistsJobInDriver() {
	j := job.NewShellJob("echo foo", "")

	s.Equal(0, s.driver.Stats().Total)

	err := s.driver.Put(j)
	s.NoError(err)

	s.Equal(1, s.driver.Stats().Total)

	err = s.driver.Put(j)
	s.Error(err)

	s.Equal(1, s.driver.Stats().Total)
}

func (s *DriverSuite) TestPutAndGetRoundTripObjects() {
	j := job.NewShellJob("echo foo", "")
	name := j.ID()

	s.Equal(0, s.driver.Stats().Total)

	err := s.driver.Put(j)
	s.NoError(err)

	s.Equal(1, s.driver.Stats().Total)
	n, err := s.driver.Get(name)

	if s.NoError(err) {
		nsh := n.(*job.ShellJob)
		s.Equal(nsh.ID(), j.ID())
		s.Equal(nsh.Command, j.Command)

		s.Equal(n.ID(), name)
		s.Equal(1, s.driver.Stats().Total)
	}
}

func (s *DriverSuite) TestReloadRefreshesJobFromMemory() {
	j := job.NewShellJob("echo foo", "")

	originalCommand := j.Command
	err := s.driver.Put(j)
	s.NoError(err)

	s.Equal(1, s.driver.Stats().Total)

	newCommand := "echo bar"
	j.Command = newCommand

	err = s.driver.Save(j)
	s.Equal(1, s.driver.Stats().Total)

	reloadedJob := s.driver.Reload(j)
	s.NoError(err)
	j = reloadedJob.(*job.ShellJob)
	s.NotEqual(originalCommand, j.Command)
	s.Equal(newCommand, j.Command)
}

func (s *DriverSuite) TestGetReturnsErrorIfJobDoesNotExist() {
	j, err := s.driver.Get("does-not-exist")
	s.Error(err)
	s.Nil(j)
}

func (s *DriverSuite) TestReloadReturnsSelfIfJobDoesNotExist() {
	j := job.NewShellJob("echo foo", "")

	s.Equal(0, s.driver.Stats().Total)

	newJob := s.driver.Reload(j)

	s.Equal(0, s.driver.Stats().Total)

	s.Exactly(j, newJob)
}

func (s *DriverSuite) TestSaveReturnsErrorIfJobDoesNotExist() {
	j := job.NewShellJob("echo foo", "")

	s.Equal(0, s.driver.Stats().Total)
	s.Error(s.driver.Save(j))
	s.Equal(0, s.driver.Stats().Total)
}

func (s *DriverSuite) TestStatsCallReportsCompletedJobs() {
	j := job.NewShellJob("echo foo", "")

	s.Equal(0, s.driver.Stats().Total)
	s.NoError(s.driver.Put(j))
	s.Equal(1, s.driver.Stats().Total)
	s.Equal(0, s.driver.Stats().Locked)
	s.Equal(0, s.driver.Stats().Complete)

	j.IsComplete = true
	s.NoError(s.driver.Save(j))
	s.Equal(1, s.driver.Stats().Total)
	s.Equal(0, s.driver.Stats().Locked)
	s.Equal(1, s.driver.Stats().Complete)
}

func (s *DriverSuite) TestNextMethodReturnsJob() {
	s.Equal(0, s.driver.Stats().Total)
	s.Nil(s.driver.Next())

	j := job.NewShellJob("echo foo", "")

	s.NoError(s.driver.Put(j))
	stats := s.driver.Stats()
	s.Equal(1, stats.Total)
	s.Equal(1, stats.Pending)

	nj := s.driver.Next()
	stats = s.driver.Stats()
	s.Equal(0, stats.Locked)
	s.Equal(1, stats.Pending)
	s.Equal(0, stats.Complete)

	if s.NotNil(nj) {
		s.Equal(j.ID(), nj.ID())
		// won't dispatch the same job more than once.
		s.Nil(s.driver.Next())
	}
}

func (s *DriverSuite) TestNextMethodSkipsCompletedJos() {
	j := job.NewShellJob("echo foo", "")
	j.IsComplete = true

	s.NoError(s.driver.Put(j))
	s.Equal(1, s.driver.Stats().Total)

	s.Equal(1, s.driver.Stats().Total)
	s.Equal(0, s.driver.Stats().Locked)
	s.Equal(1, s.driver.Stats().Complete)

	s.Nil(s.driver.Next(), fmt.Sprintf("%T", s.driver))
}

func (s *DriverSuite) TestGetLockMethodReturnsRelevantLock() {
	j := job.NewShellJob("echo lock", "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.driver.Put(j))
	lock, err := s.driver.GetLock(ctx, j)
	s.NoError(err)

	s.Equal(j.ID(), lock.Name())

	// should assert that second attempts to get the lock result in the same lock:
	secondLock, err := s.driver.GetLock(ctx, j)
	s.NoError(err)

	s.Exactly(secondLock, lock)
}

func (s *DriverSuite) TestGetLockReturnsErrorIfcontextIsCanceled() {
	j := job.NewShellJob("echo lock", "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s.NoError(s.driver.Put(j))
	lock, err := s.driver.GetLock(ctx, j)
	s.Error(err)
	s.Nil(lock)
}

func (s *DriverSuite) TestJobsMethodReturnsAllJobs() {
	mocks := make(map[string]*job.ShellJob)

	for idx := range [24]int{} {
		name := fmt.Sprintf("echo test num %d", idx)
		j := job.NewShellJob(name, "")
		s.NoError(s.driver.Put(j))
		mocks[j.ID()] = j
	}

	counter := 0
	for j := range s.driver.Jobs() {
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
