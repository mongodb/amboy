package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/google/uuid"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/amboy/pool"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestMongoDBQueueOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("Validate", func(t *testing.T) {
		t.Run("SucceedsWithDefaultOptions", func(t *testing.T) {
			opts := defaultMongoDBQueueTestOptions()
			assert.NoError(t, opts.Validate())
		})
		t.Run("SucceedsWithDefaultNumWorkersAndWorkerPoolSize", func(t *testing.T) {
			opts := defaultMongoDBQueueTestOptions()
			opts.NumWorkers = utility.ToIntPtr(1)
			opts.WorkerPoolSize = func(_ string) int { return 5 }
			assert.NoError(t, opts.Validate())
		})
		t.Run("FailsWithoutKnownWorkerQuantity", func(t *testing.T) {
			opts := defaultMongoDBQueueTestOptions()
			opts.NumWorkers = nil
			opts.WorkerPoolSize = nil
			assert.Error(t, opts.Validate())
		})
		t.Run("FailsWithZeroWorkers", func(t *testing.T) {
			opts := defaultMongoDBQueueTestOptions()
			opts.NumWorkers = utility.ToIntPtr(0)
			opts.WorkerPoolSize = nil
			assert.Error(t, opts.Validate())
		})
		t.Run("FailsWithNegativeWorkers", func(t *testing.T) {
			opts := defaultMongoDBQueueTestOptions()
			opts.NumWorkers = utility.ToIntPtr(-50)
			opts.WorkerPoolSize = nil
			assert.Error(t, opts.Validate())
		})
		t.Run("SucceedsWithValidRetryableQueueOptions", func(t *testing.T) {
			opts := defaultMongoDBQueueTestOptions()
			opts.Retryable = &RetryableQueueOptions{
				StaleRetryingMonitorInterval: time.Second,
				RetryHandler: amboy.RetryHandlerOptions{
					MaxRetryAttempts: 5,
					MaxRetryTime:     time.Minute,
				},
			}
			assert.NoError(t, opts.Validate())
		})
		t.Run("FailsWithInvalidRetryableQueueOptions", func(t *testing.T) {
			opts := defaultMongoDBQueueTestOptions()
			opts.Retryable = &RetryableQueueOptions{
				StaleRetryingMonitorInterval: -time.Second,
			}
			assert.Error(t, opts.Validate())
		})
		t.Run("FailsWithInvalidDefaultMaxTime", func(t *testing.T) {
			opts := defaultMongoDBQueueTestOptions()
			opts.DefaultMaxTime = time.Duration(-1)
			assert.Error(t, opts.Validate())
		})
		t.Run("FailsWithoutDBOptions", func(t *testing.T) {
			opts := defaultMongoDBQueueTestOptions()
			opts.DB = nil
			assert.Error(t, opts.Validate())
		})
		t.Run("FailsWithInvalidDBOptions", func(t *testing.T) {
			opts := defaultMongoDBQueueTestOptions()
			opts.DB = &MongoDBOptions{
				LockTimeout: -time.Minute,
			}
			assert.Error(t, opts.Validate())
		})
	})

	t.Run("BuildQueue", func(t *testing.T) {
		t.Run("FailsWithInvalidOptions", func(t *testing.T) {
			opts := MongoDBQueueOptions{}
			q, err := opts.BuildQueue(ctx)
			assert.Error(t, err)
			assert.Zero(t, q)
		})
		t.Run("CreatesRemoteQueue", func(t *testing.T) {
			opts := defaultMongoDBQueueTestOptions()
			opts.Abortable = utility.FalsePtr()

			q, err := opts.BuildQueue(ctx)
			require.NoError(t, err)

			assert.False(t, q.Info().Started, "queue should not be started after being built")

			runner := q.Runner()
			require.NotZero(t, runner)
			_, ok := runner.(amboy.AbortableRunner)
			assert.False(t, ok, "runner pool should not be abortable")

			rq, ok := q.(*remote)
			require.True(t, ok, "queue should be remote queue")

			d := rq.Driver()
			require.NotZero(t, d)
			md, ok := d.(*mongoDriver)
			require.True(t, ok)
			assert.Equal(t, *opts.DB, md.opts, "DB options should be forwarded to the DB")
		})
		t.Run("CreatesRemoteQueueWithAbortableWorkers", func(t *testing.T) {
			opts := defaultMongoDBQueueTestOptions()
			opts.Abortable = utility.TruePtr()

			q, err := opts.BuildQueue(ctx)
			require.NoError(t, err)

			assert.False(t, q.Info().Started, "queue should not be started after being built")

			runner := q.Runner()
			require.NotZero(t, runner)
			_, ok := runner.(amboy.AbortableRunner)
			assert.True(t, ok, "runner pool should be abortable")

			rq, ok := q.(*remote)
			require.True(t, ok, "queue should be remote queue")

			d := rq.Driver()
			require.NotZero(t, d)
			md, ok := d.(*mongoDriver)
			require.True(t, ok)
			assert.Equal(t, *opts.DB, md.opts, "DB options should be forwarded to the DB")
		})
	})
}

type remoteSuite struct {
	queue             *remote
	driver            remoteQueueDriver
	driverConstructor func() (remoteQueueDriver, error)
	tearDown          func() error
	require           *require.Assertions
	ctx               context.Context
	cancel            context.CancelFunc
	suite.Suite
}

func TestRemoteSuite(t *testing.T) {
	tests := new(remoteSuite)
	opts := defaultMongoDBTestOptions()
	opts.Collection = "test-" + uuid.New().String()

	tests.driverConstructor = func() (remoteQueueDriver, error) {
		return newMongoDriver(opts)
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

func (s *remoteSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	var err error
	s.driver, err = s.driverConstructor()
	s.Require().NoError(err)
	s.Require().NoError(s.driver.Open(s.ctx))
	rq, err := newRemote(2, time.Minute)
	s.Require().NoError(err)
	q, ok := rq.(*remote)
	s.Require().True(ok)
	s.queue = q
}

func (s *remoteSuite) TearDownTest() {
	// this order is important, running teardown before canceling
	// the context to prevent closing the connection before
	// running the teardown procedure, given that some connection
	// resources may be shared in the driver.
	s.NoError(s.tearDown())
	s.cancel()
}

func (s *remoteSuite) TestDriverIsUninitializedByDefault() {
	s.Nil(s.queue.Driver())
}

func (s *remoteSuite) TestRemoteImplementsQueueInterface() {
	s.Implements((*amboy.Queue)(nil), s.queue)
	s.Implements((*amboy.RetryableQueue)(nil), s.queue)
}

func (s *remoteSuite) TestJobPutIntoQueueFetchableViaGetMethod() {
	s.Require().NoError(s.queue.SetDriver(s.driver))
	s.Require().NotNil(s.queue.Driver())

	j := job.NewShellJob("echo foo", "")
	name := j.ID()
	s.NoError(s.queue.Put(s.ctx, j))
	fetchedJob, ok := s.queue.Get(s.ctx, name)
	s.Require().True(ok)

	s.IsType(j.Dependency(), fetchedJob.Dependency())
	s.Equal(j.ID(), fetchedJob.ID())
	s.Equal(j.Type(), fetchedJob.Type())
	s.Equal(j.TimeInfo().MaxTime, time.Minute)

	nj := fetchedJob.(*job.ShellJob)
	s.Equal(j.ID(), nj.ID())
	s.Equal(j.Status().Completed, nj.Status().Completed)
	s.Equal(j.Command, nj.Command, fmt.Sprintf("%+v\n%+v", j, nj))
	s.Equal(j.Output, nj.Output)
	s.Equal(j.WorkingDir, nj.WorkingDir)
	s.Equal(j.Type(), nj.Type())
	s.Equal(j.TimeInfo().MaxTime, time.Minute)
}

func (s *remoteSuite) TestGetMethodHandlesMissingJobs() {
	s.Require().Nil(s.queue.Driver())
	s.Require().NoError(s.queue.SetDriver(s.driver))
	s.Require().NotNil(s.queue.Driver())

	s.NoError(s.queue.Start(s.ctx))

	j := job.NewShellJob("echo foo", "")
	name := j.ID()

	// before putting a job in the queue, it shouldn't exist.
	fetchedJob, ok := s.queue.Get(s.ctx, name)
	s.False(ok)
	s.Nil(fetchedJob)

	s.NoError(s.queue.Put(s.ctx, j))

	// wrong name also returns error case
	fetchedJob, ok = s.queue.Get(s.ctx, name+name)
	s.False(ok)
	s.Nil(fetchedJob)
}

func (s *remoteSuite) TestInternalRunnerCanBeChangedBeforeStartingTheQueue() {
	s.NoError(s.queue.SetDriver(s.driver))

	originalRunner := s.queue.Runner()
	newRunner := pool.NewLocalWorkers(3, s.queue)
	s.NotEqual(originalRunner, newRunner)

	s.NoError(s.queue.SetRunner(newRunner))
	s.Exactly(newRunner, s.queue.Runner())
}

func (s *remoteSuite) TestInternalRunnerCannotBeChangedAfterStartingAQueue() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.SetDriver(s.driver))

	runner := s.queue.Runner()
	s.False(s.queue.Info().Started)
	s.NoError(s.queue.Start(ctx))
	s.True(s.queue.Info().Started)

	newRunner := pool.NewLocalWorkers(2, s.queue)
	s.Error(s.queue.SetRunner(newRunner))
	s.NotEqual(runner, newRunner)
}

func (s *remoteSuite) TestPuttingAJobIntoAQueueImpactsStats() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.SetDriver(s.driver))

	existing := s.queue.Stats(ctx)
	s.NoError(s.queue.Start(ctx))

	j := job.NewShellJob("true", "")
	s.NoError(s.queue.Put(ctx, j))

	_, ok := s.queue.Get(ctx, j.ID())
	s.True(ok)

	stats := s.queue.Stats(ctx)

	report := fmt.Sprintf("%+v", stats)
	s.Equal(existing.Total+1, stats.Total, report)
}

func (s *remoteSuite) TestQueueFailsToStartIfDriverIsNotSet() {
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

func (s *remoteSuite) TestQueueFailsToStartIfRunnerIsNotSet() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NotNil(s.queue.Runner())

	s.NoError(s.queue.SetRunner(nil))

	s.Nil(s.queue.runner)
	s.Nil(s.queue.Runner())

	s.Error(s.queue.Start(ctx))
}

func (s *remoteSuite) TestSetDriverErrorsIfQueueHasStarted() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.SetDriver(s.driver))
	s.NoError(s.queue.Start(ctx))

	s.Error(s.queue.SetDriver(s.driver))
}

func (s *remoteSuite) TestStartMethodCanBeCalledMultipleTimes() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.queue.SetDriver(s.driver))
	for i := 0; i < 200; i++ {
		s.NoError(s.queue.Start(ctx))
		s.True(s.queue.Info().Started)
	}
}

func (s *remoteSuite) TestNextMethodSkipsLockedJobs() {
	s.Require().NoError(s.queue.SetDriver(s.driver))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	numLocked := 0
	lockedJobs := map[string]struct{}{}

	created := 0
	for i := 0; i < 30; i++ {
		cmd := fmt.Sprintf("echo 'foo: %d'", i)
		j := job.NewShellJob(cmd, "")

		if i%3 == 0 {
			numLocked++
			j.SetStatus(amboy.JobStatusInfo{
				InProgress: true,
			})
			err := j.Lock(s.driver.ID(), amboy.LockTimeout)
			s.NoError(err)

			s.Error(j.Lock("elsewhere", amboy.LockTimeout))
			lockedJobs[j.ID()] = struct{}{}
		}

		if s.NoError(s.queue.Put(ctx, j)) {
			created++
		}
	}

	s.queue.started = true
	s.Require().NoError(s.queue.Start(ctx))
	go s.queue.jobServer(ctx)

	observed := 0
	startAt := time.Now()
checkResults:
	for {
		if time.Since(startAt) >= time.Second {
			break checkResults
		}

		nctx, ncancel := context.WithTimeout(ctx, 100*time.Millisecond)
		work := s.queue.Next(nctx)
		ncancel()
		if work == nil {
			continue checkResults
		}
		observed++

		_, ok := lockedJobs[work.ID()]
		s.False(ok, fmt.Sprintf("%s\n\tjob: %+v\n\tqueue: %+v",
			work.ID(), work.Status(), s.queue.Stats(ctx)))

		if observed == created || observed+numLocked == created {
			break checkResults
		}

	}
	s.Require().NoError(ctx.Err())
	qStat := s.queue.Stats(ctx)
	s.True(qStat.Running >= numLocked)
	s.True(qStat.Total == created)
	s.True(qStat.Completed <= observed, fmt.Sprintf("%d <= %d", qStat.Completed, observed))
	s.Equal(created, observed+numLocked, "%+v", qStat)
}

func (s *remoteSuite) TestJobInfo() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Require().NoError(s.queue.SetDriver(s.driver))

	names := make(map[string]struct{})

	for i := 0; i < 30; i++ {
		cmd := fmt.Sprintf("echo 'foo: %d'", i)
		j := job.NewShellJob(cmd, "")

		s.NoError(s.queue.Put(ctx, j))
		names[j.ID()] = struct{}{}
	}

	counter := 0
	for info := range s.queue.JobInfo(ctx) {
		_, ok := names[info.ID]
		s.True(ok)
		counter++
	}
	s.Equal(len(names), counter)
	s.Equal(counter, 30)
}

func (s *remoteSuite) TestTimeInfoPersists() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.Require().NoError(s.queue.SetDriver(s.driver))
	j := newMockJob()
	s.Zero(j.TimeInfo())
	s.NoError(s.queue.Put(ctx, j))
	go s.queue.jobServer(ctx)
	j2 := s.queue.Next(ctx)
	s.NotZero(j2.TimeInfo())
}

func (s *remoteSuite) TestInfoReturnsDefaultLockTimeout() {
	s.Equal(amboy.LockTimeout, s.queue.Info().LockTimeout)
}

func (s *remoteSuite) TestInfoReturnsConfigurableLockTimeout() {
	opts := defaultMongoDBTestOptions()
	opts.LockTimeout = 30 * time.Minute
	opts.Collection = s.T().Name()
	d, err := newMongoDriver(opts)
	s.Require().NoError(err)
	s.Require().NoError(s.queue.SetDriver(d))
	s.Equal(opts.LockTimeout, s.queue.Info().LockTimeout)
}
