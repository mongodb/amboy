package queue

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"gopkg.in/mgo.v2"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/amboy/queue/remote"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/level"
)

type RemoteUnorderedSuite struct {
	queue             *RemoteUnordered
	driver            remote.Driver
	driverConstructor func() remote.Driver
	tearDown          func()
	require           *require.Assertions
	canceler          context.CancelFunc
	suite.Suite
}

func TestRemoteUnorderedLocalDriverSuite(t *testing.T) {
	tests := new(RemoteUnorderedSuite)
	tests.driverConstructor = func() remote.Driver {
		return remote.NewLocalDriver()
	}
	tests.tearDown = func() { return }

	suite.Run(t, tests)
}

func TestDriverSuiteWithMongoDBInstance(t *testing.T) {
	tests := new(RemoteUnorderedSuite)
	name := "test-" + uuid.NewV4().String()
	uri := "mongodb://localhost"
	tests.driverConstructor = func() remote.Driver {
		return remote.NewMongoDBQueueDriver(name, uri)
	}

	tests.tearDown = func() {
		session, err := mgo.Dial(uri)
		if err != nil {
			grip.Error(err)
			return
		}
		defer session.Close()

		err = session.DB("amboy").C(name + ".jobs").DropCollection()
		if err != nil {
			grip.Error(err)
			return
		}
		err = session.DB("amboy").C(name + ".locks").DropCollection()
		if err != nil {
			grip.Error(err)
			return
		}

	}

	suite.Run(t, tests)
}

// TODO run these same tests with different drivers by cloning the
// above Test function and replacing the driverConstructor function.

func (s *RemoteUnorderedSuite) SetupSuite() {
	s.NoError(grip.UseNativeLogger())
	grip.SetThreshold(level.Info)
	s.require = s.Require()
}

func (s *RemoteUnorderedSuite) SetupTest() {
	ctx, canceler := context.WithCancel(context.Background())
	s.driver = s.driverConstructor()
	s.canceler = canceler
	s.NoError(s.driver.Open(ctx))
	s.queue = NewRemoteUnordered(2)
}

func (s *RemoteUnorderedSuite) TearDownTest() {
	s.canceler()
	s.tearDown()
}

func (s *RemoteUnorderedSuite) TestDriverIsUnitializedByDefault() {
	s.Nil(s.queue.Driver())
}

func (s *RemoteUnorderedSuite) TestRemoteUnorderdImplementsQueueInterface() {
	s.Implements((*amboy.Queue)(nil), s.queue)
}

func (s *RemoteUnorderedSuite) TestJobPutIntoQueueFetchableViaGetMethod() {
	s.NoError(s.queue.SetDriver(s.driver))
	s.NotNil(s.queue.Driver())

	j := job.NewShellJob("echo foo", "")
	name := j.ID()
	s.NoError(s.queue.Put(j))
	fetchedJob, ok := s.queue.Get(name)
	s.True(ok)

	s.IsType(j.Dependency(), fetchedJob.Dependency())
	s.Equal(j.ID(), fetchedJob.ID())
	s.Equal(j.Type(), fetchedJob.Type())

	nj := fetchedJob.(*job.ShellJob)
	s.Equal(j.Name, nj.Name)
	s.Equal(j.IsComplete, nj.IsComplete)
	s.Equal(j.Command, nj.Command)
	s.Equal(j.Output, nj.Output)
	s.Equal(j.WorkingDir, nj.WorkingDir)
	s.Equal(j.T, nj.T)
}

func (s *RemoteUnorderedSuite) TestGetMethodHandlesMissingJobs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.SetDriver(s.driver))
	s.NotNil(s.queue.Driver())

	s.NoError(s.queue.Start(ctx))

	job := job.NewShellJob("echo foo", "")
	name := job.ID()

	// before putting a job in the queue, it shouldn't exist.
	fetchedJob, ok := s.queue.Get(name)
	s.False(ok)
	s.Nil(fetchedJob)

	s.NoError(s.queue.Put(job))

	// wrong name also returns error case
	fetchedJob, ok = s.queue.Get(name + name)
	s.False(ok)
	s.Nil(fetchedJob)
}

func (s *RemoteUnorderedSuite) TestInternalRunnerCanBeChangedBeforeStartingTheQueue() {
	s.NoError(s.queue.SetDriver(s.driver))

	originalRunner := s.queue.Runner()
	newRunner := pool.NewLocalWorkers(3, s.queue)
	s.NotEqual(originalRunner, newRunner)

	s.NoError(s.queue.SetRunner(newRunner))
	s.Exactly(newRunner, s.queue.Runner())
}

func (s *RemoteUnorderedSuite) TestInternalRunnerCannotBeChangedAfterStartingAQueue() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.SetDriver(s.driver))

	runner := s.queue.Runner()
	s.False(s.queue.Started())
	s.NoError(s.queue.Start(ctx))
	s.True(s.queue.Started())

	newRunner := pool.NewLocalWorkers(2, s.queue)
	s.Error(s.queue.SetRunner(newRunner))
	s.NotEqual(runner, newRunner)
}

func (s *RemoteUnorderedSuite) TestPuttingAJobIntoAQueueImpactsStats() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.SetDriver(s.driver))

	existing := s.queue.Stats()
	s.NoError(s.queue.Start(ctx))

	j := job.NewShellJob("true", "")
	s.NoError(s.queue.Put(j))

	_, ok := s.queue.Get(j.ID())
	s.True(ok)

	stats := s.queue.Stats()

	report := fmt.Sprintf("%+v", stats)
	s.Equal(existing.Total+1, stats.Total, report)
}

func (s *RemoteUnorderedSuite) TestQueueFailsToStartIfDriverIsNotSet() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Nil(s.queue.driver)
	s.Nil(s.queue.Driver())
	s.Error(s.queue.Start(ctx))

	s.NoError(s.queue.SetDriver(s.driver))

	s.NotNil(s.queue.driver)
	s.NotNil(s.queue.Driver())
	s.NoError(s.queue.Start(ctx))
}

func (s *RemoteUnorderedSuite) TestQueueFailsToStartIfRunnerIsNotSet() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NotNil(s.queue.Runner())

	s.NoError(s.queue.SetRunner(nil))

	s.Nil(s.queue.runner)
	s.Nil(s.queue.Runner())

	s.Error(s.queue.Start(ctx))
}

func (s *RemoteUnorderedSuite) TestSetDriverErrorsIfQueueHasStarted() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.SetDriver(s.driver))
	s.NoError(s.queue.Start(ctx))

	s.Error(s.queue.SetDriver(s.driver))
}

func (s *RemoteUnorderedSuite) TestStartMethodCanBeCalledMultipleTimes() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.SetDriver(s.driver))
	for i := 0; i < 200; i++ {
		s.NoError(s.queue.Start(ctx))
		s.True(s.queue.Started())
	}
}

// func (s *RemoteUnorderedSuite) TestNextMethodSkipsLockedJobs() {
//	j := job.NewShellJob("echo foo", "")
//	jobFree := job.NewShellJob("echo bar", "")

//	s.queue.SetDriver(s.driver)
//	s.NoError(s.queue.Put(j))

//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
//	defer cancel()

//	lock, err := s.driver.GetLock(ctx, j)
//	s.NoError(err)
//	s.NoError(s.queue.Put(jobFree))
//	lock.Lock(ctx)

//	for {
//		work := s.queue.Next(ctx)
//		if work != nil {
//			s.Equal(work.ID(), jobFree.ID())
//			return
//		}

//		select {
//		case <-ctx.Done():
//			s.NoError(err)
//			s.Fail("reached timeout")
//			break
//		default:
//			continue
//		}
//	}

// }
