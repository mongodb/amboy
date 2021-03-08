package management

import (
	"context"
	"testing"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/google/uuid"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/dependency"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/amboy/queue"
	"github.com/mongodb/grip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ManagerSuite struct {
	queue   amboy.Queue
	manager Manager
	ctx     context.Context
	cancel  context.CancelFunc

	factory func() Manager
	setup   func()
	cleanup func()
	suite.Suite
}

func TestManagerImplementations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(defaultMongoDBTestOptions().URI))
	require.NoError(t, err)

	teardownDB := func(ctx context.Context) error {
		catcher := grip.NewBasicCatcher()
		catcher.Add(client.Database(defaultMongoDBTestOptions().DB).Drop(ctx))
		catcher.Add(client.Disconnect(ctx))
		return catcher.Resolve()
	}

	const queueSize = 128

	getInvalidFilters := func() []string {
		return []string{"", "foo", "inprog"}
	}
	getInvalidWindows := func() []time.Duration {
		return []time.Duration{-1, 0, time.Millisecond, -time.Hour}
	}

	for managerName, managerCase := range map[string]struct {
		makeQueue   func(context.Context) (amboy.Queue, error)
		makeManager func(context.Context, amboy.Queue) (Manager, error)
		teardown    func(context.Context) error
	}{
		"MongoDB": {
			makeQueue: func(ctx context.Context) (amboy.Queue, error) {
				queueOpts := queue.MongoDBQueueCreationOptions{
					Size:   queueSize,
					Name:   uuid.New().String(),
					MDB:    defaultMongoDBTestOptions(),
					Client: client,
				}
				return queue.NewMongoDBQueue(ctx, queueOpts)
			},
			makeManager: func(ctx context.Context, _ amboy.Queue) (Manager, error) {
				mgrOpts := DBQueueManagerOptions{
					Options: defaultMongoDBTestOptions(),
					Name:    uuid.New().String(),
				}
				return MakeDBQueueManager(ctx, mgrOpts, client)
			},
			teardown: teardownDB,
		},
		"MongoDBSingleGroup": {
			makeQueue: func(ctx context.Context) (amboy.Queue, error) {
				opts := defaultMongoDBTestOptions()
				opts.UseGroups = true
				opts.GroupName = "group"
				queueOpts := queue.MongoDBQueueCreationOptions{
					Size:   queueSize,
					Name:   uuid.New().String(),
					MDB:    opts,
					Client: client,
				}
				return queue.NewMongoDBQueue(ctx, queueOpts)
			},
			makeManager: func(ctx context.Context, _ amboy.Queue) (Manager, error) {
				opts := defaultMongoDBTestOptions()
				opts.UseGroups = true
				opts.GroupName = "group"
				mgrOpts := DBQueueManagerOptions{
					Options:     opts,
					Name:        uuid.New().String(),
					Group:       opts.GroupName,
					SingleGroup: true,
				}
				return MakeDBQueueManager(ctx, mgrOpts, client)
			},
			teardown: teardownDB,
		},
		"MongoDBMultiGroup": {
			makeQueue: func(ctx context.Context) (amboy.Queue, error) {
				opts := defaultMongoDBTestOptions()
				opts.UseGroups = true
				opts.GroupName = "group"
				queueOpts := queue.MongoDBQueueCreationOptions{
					Size:   queueSize,
					Name:   uuid.New().String(),
					MDB:    opts,
					Client: client,
				}
				return queue.NewMongoDBQueue(ctx, queueOpts)
			},
			makeManager: func(ctx context.Context, _ amboy.Queue) (Manager, error) {
				opts := defaultMongoDBTestOptions()
				opts.UseGroups = true
				opts.GroupName = "group"
				mgrOpts := DBQueueManagerOptions{
					Options:  opts,
					Name:     uuid.New().String(),
					Group:    opts.GroupName,
					ByGroups: true,
				}
				return MakeDBQueueManager(ctx, mgrOpts, client)
			},
			teardown: teardownDB,
		},
		"Queue-Backed": {
			makeQueue: func(ctx context.Context) (amboy.Queue, error) {
				return queue.NewLocalLimitedSizeSerializable(2, queueSize), nil
			},
			makeManager: func(ctx context.Context, q amboy.Queue) (Manager, error) {
				return NewQueueManager(q), nil
			},
			teardown: func(context.Context) error { return nil },
		},
	} {
		t.Run(managerName, func(t *testing.T) {
			for testName, testCase := range map[string]func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue){
				"JobIDsByStateSucceeds": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {

				},
				"JobIDsByStateSucceedsWithEmptyResult": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {
					for _, f := range ValidStatusFilters() {
						r, err := mgr.JobStatus(ctx, f)
						assert.NoError(t, err)
						assert.NotZero(t, r)
					}
				},
				"JobIDsByStateFailsWithInvalidFilter": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {
					for _, f := range getInvalidFilters() {
						r, err := mgr.JobIDsByState(ctx, "foo", StatusFilter(f))
						assert.Error(t, err)
						assert.Zero(t, r)
					}
				},
				"JobStatusSucceedsWithEmptyResult": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {
					for _, f := range ValidStatusFilters() {
						r, err := mgr.JobStatus(ctx, StatusFilter(f))
						assert.NoError(t, err)
						assert.NotZero(t, r)
					}
				},
				"JobStatusFailsWithInvalidFilter": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {
					for _, f := range getInvalidFilters() {
						r, err := mgr.JobStatus(ctx, StatusFilter(f))
						assert.Error(t, err)
						assert.Zero(t, r)
					}
				},
				"RecentTimingSucceedsWithEmptyResult": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {
					for _, f := range ValidRuntimeFilters() {
						r, err := mgr.RecentTiming(ctx, time.Hour, f)
						assert.NoError(t, err)
						assert.NotZero(t, r)
					}
				},
				"RecentTimingFailsWithInvalidFilter": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {
					for _, f := range getInvalidFilters() {
						r, err := mgr.RecentTiming(ctx, time.Hour, RuntimeFilter(f))
						assert.Error(t, err)
						assert.Zero(t, r)
					}
				},
				"RecentTimingFailsWithInvalidWindows": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {
					for _, w := range getInvalidWindows() {
						r, err := mgr.RecentTiming(ctx, w, Duration)
						assert.Error(t, err)
						assert.Zero(t, r)
					}
				},
				"RecentJobErrorsFailsWithInvalidFilter": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {
					for _, f := range getInvalidFilters() {
						r, err := mgr.RecentJobErrors(ctx, "foo", time.Hour, ErrorFilter(f))
						assert.Error(t, err)
						assert.Zero(t, r)
					}
				},
				"RecentJobErrorsSucceedsWithEmptyResult": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {
					for _, f := range ValidErrorFilters() {
						r, err := mgr.RecentJobErrors(ctx, "foo", time.Hour, f)
						assert.Error(t, err)
						assert.Zero(t, r)
					}
				},
				"RecentJobErrorsFailsWithInvalidWindows": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {
					for _, w := range getInvalidWindows() {
						r, err := mgr.RecentJobErrors(ctx, "foo", w, StatsOnly)
						assert.Error(t, err)
						assert.Zero(t, r)
					}
				},
				"RecentErrorsSucceedsWithEmptyResult": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {
					for _, f := range ValidErrorFilters() {
						r, err := mgr.RecentErrors(ctx, time.Hour, f)
						assert.Error(t, err)
						assert.Zero(t, r)
					}
				},
				"RecentErrorsFailsWithInvalidFilter": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {
					for _, f := range getInvalidFilters() {
						r, err := mgr.RecentErrors(ctx, time.Hour, ErrorFilter(f))
						assert.Error(t, err)
						assert.Zero(t, r)
					}
				},
				"RecentErrorsFailsWithInvalidWindows": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {
					for _, w := range getInvalidWindows() {
						r, err := mgr.RecentErrors(ctx, w, StatsOnly)
						assert.Error(t, err)
						assert.Zero(t, r)
					}
				},
				// "": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {},
				// "": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {},
				// "": func(ctx context.Context, t *testing.T, mgr Manager, q amboy.Queue) {},
			} {
				t.Run(testName, func(t *testing.T) {
					tctx, tcancel := context.WithTimeout(ctx, 10*time.Second)
					defer tcancel()

					q, err := managerCase.makeQueue(tctx)
					require.NoError(t, err)
					defer q.Close(tctx)

					mgr, err := managerCase.makeManager(tctx, q)
					require.NoError(t, err)

					defer func() {
						assert.NoError(t, managerCase.teardown(tctx))
					}()

					testCase(tctx, t, mgr, q)
				})
			}
		})
	}
}

func getFilteredJobs() []filteredJob {
	pending := newTestJob(uuid.New().String())

	inProg := newTestJob(uuid.New().String())
	inProg.SetStatus(amboy.JobStatusInfo{
		InProgress:       true,
		ModificationTime: time.Now(),
	})

	stale := newTestJob(uuid.New().String())
	stale.SetStatus(amboy.JobStatusInfo{
		InProgress:       true,
		ModificationTime: time.Now().Add(-time.Hour),
	})

	completed := newTestJob(uuid.New().String())
	completed.SetStatus(amboy.JobStatusInfo{
		Completed: true,
	})
	retrying := newTestJob(uuid.New().String())
	retrying.SetStatus(amboy.JobStatusInfo{
		Completed: true,
	})
	retrying.UpdateRetryInfo(amboy.JobRetryOptions{
		Retryable:  utility.TruePtr(),
		NeedsRetry: utility.TruePtr(),
	})

	completedRetryable := newTestJob(uuid.New().String())
	completedRetryable.SetStatus(amboy.JobStatusInfo{
		Completed: true,
	})
	completedRetryable.UpdateRetryInfo(amboy.JobRetryOptions{
		Retryable: utility.TruePtr(),
	})
	return []filteredJob{
		{
			job:            pending,
			matchedFilters: []StatusFilter{All, Pending},
		},
		{
			job:            inProg,
			matchedFilters: []StatusFilter{All, InProgress},
		},
		{
			job:            stale,
			matchedFilters: []StatusFilter{All, Stale},
		},
		{
			job:            completed,
			matchedFilters: []StatusFilter{All, Completed},
		},
		{
			job:            retrying,
			matchedFilters: []StatusFilter{All, Completed, Retrying},
		},
		{
			job:            completedRetryable,
			matchedFilters: []StatusFilter{All, Completed},
		},
	}
}

func TestManagerSuiteBackedByMongoDB(t *testing.T) {
	s := new(ManagerSuite)
	name := uuid.New().String()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := defaultMongoDBTestOptions()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(opts.URI))
	require.NoError(t, err)
	s.factory = func() Manager {
		manager, err := MakeDBQueueManager(ctx, DBQueueManagerOptions{
			Options: opts,
			Name:    name,
		}, client)
		require.NoError(t, err)
		return manager
	}

	s.setup = func() {
		require.NoError(t, client.Database(opts.DB).Drop(ctx))
		args := queue.MongoDBQueueCreationOptions{
			Size:   2,
			Name:   name,
			MDB:    opts,
			Client: client,
		}

		remote, err := queue.NewMongoDBQueue(ctx, args)
		require.NoError(t, err)
		s.queue = remote
	}

	s.cleanup = func() {
		assert.NoError(t, client.Database(opts.DB).Drop(ctx))
		assert.NoError(t, client.Disconnect(ctx))
		s.queue.Close(ctx)
	}

	suite.Run(t, s)
}

func TestManagerSuiteBackedByMongoDBSingleGroup(t *testing.T) {
	s := new(ManagerSuite)
	name := uuid.New().String()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := defaultMongoDBTestOptions()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(opts.URI))
	require.NoError(t, err)
	s.factory = func() Manager {
		manager, err := MakeDBQueueManager(ctx, DBQueueManagerOptions{
			Options:     opts,
			Name:        name,
			Group:       "foo",
			SingleGroup: true,
		}, client)
		require.NoError(t, err)
		return manager
	}

	opts.UseGroups = true
	opts.GroupName = "foo"

	s.setup = func() {
		require.NoError(t, client.Database(opts.DB).Drop(ctx))
		args := queue.MongoDBQueueCreationOptions{
			Size:   2,
			Name:   name,
			MDB:    opts,
			Client: client,
		}

		remote, err := queue.NewMongoDBQueue(ctx, args)
		require.NoError(t, err)
		s.queue = remote
	}

	s.cleanup = func() {
		assert.NoError(t, client.Database(opts.DB).Drop(ctx))
		assert.NoError(t, client.Disconnect(ctx))
		s.queue.Close(ctx)
	}

	suite.Run(t, s)
}

func TestManagerSuiteBackedByMongoDBMultiGroup(t *testing.T) {
	s := new(ManagerSuite)
	name := uuid.New().String()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := defaultMongoDBTestOptions()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(opts.URI))
	require.NoError(t, err)
	s.factory = func() Manager {
		manager, err := MakeDBQueueManager(ctx, DBQueueManagerOptions{
			Options:  opts,
			Name:     name,
			Group:    "foo",
			ByGroups: true,
		}, client)
		require.NoError(t, err)
		return manager
	}

	opts.UseGroups = true
	opts.GroupName = "foo"

	s.setup = func() {
		require.NoError(t, client.Database(opts.DB).Drop(ctx))
		args := queue.MongoDBQueueCreationOptions{
			Size:   2,
			Name:   name,
			MDB:    opts,
			Client: client,
		}

		remote, err := queue.NewMongoDBQueue(ctx, args)
		require.NoError(t, err)
		s.queue = remote
	}

	s.cleanup = func() {
		assert.NoError(t, client.Database(opts.DB).Drop(ctx))
		assert.NoError(t, client.Disconnect(ctx))
		s.queue.Close(ctx)
	}

	suite.Run(t, s)
}

func TestManagerSuiteBackedByQueueMethods(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := new(ManagerSuite)
	s.setup = func() {
		s.queue = queue.NewLocalLimitedSize(2, 128)
		s.Require().NoError(s.queue.Start(ctx))
	}

	s.factory = func() Manager {
		return NewQueueManager(s.queue)
	}

	s.cleanup = func() {}
	suite.Run(t, s)
}

type filteredJob struct {
	job            amboy.Job
	matchedFilters []StatusFilter
}

//
// func (s *ManagerSuite) testDataJobs() []filteredJob {
//     pending := newTestJob(uuid.New().String())
//
//     inProg := newTestJob(uuid.New().String())
//     inProg.SetStatus(amboy.JobStatusInfo{
//         InProgress:       true,
//         ModificationTime: time.Now(),
//     })
//
//     stale := newTestJob(uuid.New().String())
//     stale.SetStatus(amboy.JobStatusInfo{
//         InProgress:       true,
//         ModificationTime: time.Now().Add(-time.Hour),
//     })
//
//     completed := newTestJob(uuid.New().String())
//     completed.SetStatus(amboy.JobStatusInfo{
//         Completed: true,
//     })
//     retrying := newTestJob(uuid.New().String())
//     retrying.SetStatus(amboy.JobStatusInfo{
//         Completed: true,
//     })
//     retrying.UpdateRetryInfo(amboy.JobRetryOptions{
//         Retryable:  utility.TruePtr(),
//         NeedsRetry: utility.TruePtr(),
//     })
//
//     completedRetryable := newTestJob(uuid.New().String())
//     completedRetryable.SetStatus(amboy.JobStatusInfo{
//         Completed: true,
//     })
//     completedRetryable.UpdateRetryInfo(amboy.JobRetryOptions{
//         Retryable: utility.TruePtr(),
//     })
//     return []filteredJob{
//         {
//             job:            pending,
//             matchedFilters: []StatusFilter{All, Pending},
//         },
//         {
//             job:            inProg,
//             matchedFilters: []StatusFilter{All, InProgress},
//         },
//         {
//             job:            stale,
//             matchedFilters: []StatusFilter{All, Stale},
//         },
//         {
//             job:            completed,
//             matchedFilters: []StatusFilter{All, Completed},
//         },
//         {
//             job:            retrying,
//             matchedFilters: []StatusFilter{All, Completed, Retrying},
//         },
//         {
//             job:            completedRetryable,
//             matchedFilters: []StatusFilter{All, Completed},
//         },
//     }
// }

func (s *ManagerSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.setup()
	s.manager = s.factory()
}

func (s *ManagerSuite) TearDownTest() {
	s.cancel()
}

func (s *ManagerSuite) TearDownSuite() {
	s.cleanup()
}

func (s *ManagerSuite) TestJobStatusInvalidFilter() {
	for _, f := range []string{"", "foo", "inprog"} {
		r, err := s.manager.JobStatus(s.ctx, StatusFilter(f))
		s.Error(err)
		s.Nil(r)

		rr, err := s.manager.JobIDsByState(s.ctx, "foo", StatusFilter(f))
		s.Error(err)
		s.Nil(rr)
	}
}

func (s *ManagerSuite) TestTimingWithInvalidFilter() {
	for _, f := range []string{"", "foo", "inprog"} {
		r, err := s.manager.RecentTiming(s.ctx, time.Hour, RuntimeFilter(f))
		s.Error(err)
		s.Nil(r)
	}
}

func (s *ManagerSuite) TestErrorsWithInvalidFilter() {
	for _, f := range []string{"", "foo", "inprog"} {
		r, err := s.manager.RecentJobErrors(s.ctx, "foo", time.Hour, ErrorFilter(f))
		s.Error(err)
		s.Nil(r)

		r, err = s.manager.RecentErrors(s.ctx, time.Hour, ErrorFilter(f))
		s.Error(err)
		s.Nil(r)
	}
}

func (s *ManagerSuite) TestJobCounterHighLevel() {
	for _, f := range []StatusFilter{InProgress, Pending, Stale} {
		r, err := s.manager.JobStatus(s.ctx, f)
		s.NoError(err)
		s.NotNil(r)
	}

}

func (s *ManagerSuite) TestJobCountingIDHighLevel() {
	for _, f := range []StatusFilter{InProgress, Pending, Stale, Completed} {
		r, err := s.manager.JobIDsByState(s.ctx, "foo", f)
		s.NoError(err, "%s", f)
		s.NotNil(r)
	}
}

func (s *ManagerSuite) TestJobTimingMustBeLongerThanASecond() {
	for _, dur := range []time.Duration{-1, 0, time.Millisecond, -time.Hour} {
		r, err := s.manager.RecentTiming(s.ctx, dur, Duration)
		s.Error(err)
		s.Nil(r)
		je, err := s.manager.RecentJobErrors(s.ctx, "foo", dur, StatsOnly)
		s.Error(err)
		s.Nil(je)

		je, err = s.manager.RecentErrors(s.ctx, dur, StatsOnly)
		s.Error(err)
		s.Nil(je)

	}
}

func (s *ManagerSuite) TestJobTiming() {
	for _, f := range []RuntimeFilter{Duration, Latency, Running} {
		r, err := s.manager.RecentTiming(s.ctx, time.Minute, f)
		s.NoError(err)
		s.NotNil(r)
	}
}

func (s *ManagerSuite) TestRecentErrors() {
	for _, f := range []ErrorFilter{UniqueErrors, AllErrors, StatsOnly} {
		r, err := s.manager.RecentErrors(s.ctx, time.Minute, f)
		s.NoError(err)
		s.NotNil(r)
	}
}

func (s *ManagerSuite) TestRecentJobErrors() {
	for _, f := range []ErrorFilter{UniqueErrors, AllErrors, StatsOnly} {
		r, err := s.manager.RecentJobErrors(s.ctx, "shell", time.Minute, f)
		s.NoError(err)
		s.NotNil(r)
	}
}

func (s *ManagerSuite) TestCompleteJob() {
	j1 := job.NewShellJob("ls", "")
	s.Require().NoError(s.queue.Put(s.ctx, j1))
	j2 := newTestJob("complete")
	s.Require().NoError(s.queue.Put(s.ctx, j2))
	j3 := newTestJob("incomplete")
	s.Require().NoError(s.queue.Put(s.ctx, j3))

	s.Require().NoError(s.manager.CompleteJob(s.ctx, j2.ID()))
	jobCount := 0
	for info := range s.queue.JobInfo(s.ctx) {
		if info.ID == j2.ID() {
			s.True(info.Status.Completed)
			_, ok := s.manager.(*dbQueueManager)
			if ok {
				s.Equal(3, info.Status.ModificationCount)
			}
		} else {
			s.False(info.Status.Completed)
			s.Equal(0, info.Status.ModificationCount)
		}
		jobCount++
	}
	s.Equal(3, jobCount)
}

func (s *ManagerSuite) TestCompleteJobsInvalidFilter() {
	s.Error(s.manager.CompleteJobs(s.ctx, "invalid"))
}

func (s *ManagerSuite) TestCompleteJobsValidFilter() {
	testData := s.testDataJobs()
	for _, data := range testData {
		s.Require().NoError(s.queue.Put(s.ctx, data.job))
	}

	s.T().Run("Filter", func(t *testing.T) {
		for _, f := range []StatusFilter{All, Pending, InProgress, Stale, Completed, Retrying} {
			s.T().Run(string(f), func(t *testing.T) {
				s.manager.CompleteJobs(s.ctx, f)
			})
		}
	})
	// j1 := job.NewShellJob("ls", "")
	// s.Require().NoError(s.queue.Put(s.ctx, j1))
	// j2 := newTestJob("0")
	// s.Require().NoError(s.queue.Put(s.ctx, j2))
	// j3 := newTestJob("1")
	// s.Require().NoError(s.queue.Put(s.ctx, j3))
	//
	// s.Require().NoError(s.manager.CompleteJobs(s.ctx, Pending))
	// jobCount := 0
	// for info := range s.queue.JobInfo(s.ctx) {
	//     s.True(info.Status.Completed)
	//     _, ok := s.manager.(*dbQueueManager)
	//     if ok {
	//         s.Equal(3, info.Status.ModificationCount)
	//     }
	//     jobCount++
	// }
	// s.Equal(3, jobCount)
}

func (s *ManagerSuite) TestCompleteJobsByTypeInvalidFilter() {
	s.Error(s.manager.CompleteJobsByType(s.ctx, "invalid", "type"))
}

func (s *ManagerSuite) TestCompleteJobsByTypeValidFilter() {
	j1 := job.NewShellJob("ls", "")
	s.Require().NoError(s.queue.Put(s.ctx, j1))
	j2 := newTestJob("0")
	s.Require().NoError(s.queue.Put(s.ctx, j2))
	j3 := newTestJob("1")
	s.Require().NoError(s.queue.Put(s.ctx, j3))

	s.Require().NoError(s.manager.CompleteJobsByType(s.ctx, Pending, "test"))
	jobCount := 0
	for info := range s.queue.JobInfo(s.ctx) {
		if info.ID == "0" || info.ID == "1" {
			s.True(info.Status.Completed)
			_, ok := s.manager.(*dbQueueManager)
			if ok {
				s.Equal(3, info.Status.ModificationCount)
			}
		} else {
			s.False(info.Status.Completed)
			s.Equal(0, info.Status.ModificationCount)
		}
		jobCount++
	}
	s.Equal(3, jobCount)
}

func (s *ManagerSuite) TestCompleteJobsByPatternInvalidFilter() {
	s.Error(s.manager.CompleteJobsByType(s.ctx, "invalid", "prefix"))
}

func (s *ManagerSuite) TestCompleteJobsByPatternValidFilter() {
	j1 := job.NewShellJob("ls", "")
	s.Require().NoError(s.queue.Put(s.ctx, j1))
	j2 := newTestJob("pre-one")
	s.Require().NoError(s.queue.Put(s.ctx, j2))
	j3 := newTestJob("pre-two")
	s.Require().NoError(s.queue.Put(s.ctx, j3))

	s.Require().NoError(s.manager.CompleteJobsByPattern(s.ctx, Pending, "pre"))
	jobCount := 0
	for info := range s.queue.JobInfo(s.ctx) {
		if info.ID == j2.ID() || info.ID == j3.ID() {
			s.True(info.Status.Completed)
			_, ok := s.manager.(*dbQueueManager)
			if ok {
				s.Equal(3, info.Status.ModificationCount)
			}
		} else {
			s.False(info.Status.Completed)
			s.Equal(0, info.Status.ModificationCount)
		}
		jobCount++
	}
	s.Equal(3, jobCount)
}

type testJob struct {
	job.Base
}

func newTestJob(id string) *testJob {
	j := &testJob{
		Base: job.Base{
			JobType: amboy.JobType{Name: "test"},
		},
	}
	j.SetDependency(dependency.NewAlways())
	j.SetID(id)

	return j
}

func (j *testJob) Run(ctx context.Context) {
	time.Sleep(time.Minute)
	return
}
