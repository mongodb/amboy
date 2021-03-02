package queue

import (
	"context"
	"testing"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/registry"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestNewBasicRetryHandler(t *testing.T) {
	q, err := newRemoteUnordered(1)
	require.NoError(t, err)
	t.Run("SucceedsWithQueue", func(t *testing.T) {
		rh, err := NewBasicRetryHandler(q, amboy.RetryHandlerOptions{})
		assert.NoError(t, err)
		assert.NotZero(t, rh)
	})
	t.Run("FailsWithNilQueue", func(t *testing.T) {
		rh, err := NewBasicRetryHandler(nil, amboy.RetryHandlerOptions{})
		assert.Error(t, err)
		assert.Zero(t, rh)
	})
	t.Run("FailsWithInvalidOptions", func(t *testing.T) {
		rh, err := NewBasicRetryHandler(q, amboy.RetryHandlerOptions{NumWorkers: -1})
		assert.Error(t, err)
		assert.Zero(t, rh)
	})
}

func TestRetryHandlerImplementations(t *testing.T) {
	assert.Implements(t, (*amboy.RetryHandler)(nil), &BasicRetryHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for rhName, makeRetryHandler := range map[string]func(q amboy.RetryableQueue, opts amboy.RetryHandlerOptions) (amboy.RetryHandler, error){
		"Basic": func(q amboy.RetryableQueue, opts amboy.RetryHandlerOptions) (amboy.RetryHandler, error) {
			return NewBasicRetryHandler(q, opts)
		},
	} {
		t.Run(rhName, func(t *testing.T) {
			for testName, testCase := range map[string]func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)){
				"StartFailsWithoutQueue": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					_, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{})
					require.NoError(t, err)

					require.NoError(t, rh.SetQueue(nil))
					assert.Error(t, rh.Start(ctx))
				},
				"IsStartedAfterStartSucceeds": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					_, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{})
					require.NoError(t, err)

					require.NoError(t, rh.Start(ctx))
					assert.True(t, rh.Started())
				},
				"StartIsIdempotent": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					_, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{})
					require.NoError(t, err)

					require.NoError(t, rh.Start(ctx))
					require.True(t, rh.Started())

					require.NoError(t, rh.Start(ctx))
					assert.True(t, rh.Started())
				},
				"CloseSucceedsWithoutFirstStarting": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					_, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{})
					require.NoError(t, err)
					require.False(t, rh.Started())

					rh.Close(ctx)
					assert.False(t, rh.Started())
				},
				"CloseStopsRetryHandlerAfterStart": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					_, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{})
					require.NoError(t, err)

					require.NoError(t, rh.Start(ctx))
					require.True(t, rh.Started())

					rh.Close(ctx)
					assert.False(t, rh.Started())
				},
				"CloseIsIdempotent": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					_, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{})
					require.NoError(t, err)

					require.NoError(t, rh.Start(ctx))
					require.True(t, rh.Started())

					assert.NotPanics(t, func() {
						rh.Close(ctx)
						assert.False(t, rh.Started())
						rh.Close(ctx)
						assert.False(t, rh.Started())
					})
				},
				"CanRestartAfterClose": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					_, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{})
					require.NoError(t, err)

					require.NoError(t, rh.Start(ctx))
					require.True(t, rh.Started())

					rh.Close(ctx)
					require.False(t, rh.Started())

					require.NoError(t, rh.Start(ctx))
					assert.True(t, rh.Started())
				},
				"MockPutReenqueuesJobWithExpectedState": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					mq, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{})
					require.NoError(t, err)

					j := newMockRetryableJob("id")
					j.UpdateRetryInfo(amboy.JobRetryOptions{
						NeedsRetry: utility.TruePtr(),
						WaitUntil:  utility.ToTimeDurationPtr(time.Minute),
						DispatchBy: utility.ToTimeDurationPtr(time.Hour),
					})

					var calledGetAttempt, calledSave, calledCompleteRetrying, calledCompleteRetryingAndPut bool
					mq.getJobAttempt = func(context.Context, remoteQueue, string, int) (amboy.Job, bool) {
						calledGetAttempt = true
						ji, err := registry.MakeJobInterchange(j, amboy.JSON)
						if err != nil {
							return nil, false
						}
						j, err := ji.Resolve(amboy.JSON)
						if err != nil {
							return nil, false
						}
						return j, true
					}
					mq.completeRetryingJob = func(context.Context, remoteQueue, amboy.Job) error {
						calledCompleteRetrying = true
						return nil
					}
					mq.saveJob = func(context.Context, remoteQueue, amboy.Job) error {
						calledSave = true
						return nil
					}
					mq.completeRetryingAndPutJob = func(_ context.Context, _ remoteQueue, toComplete, toPut amboy.Job) error {
						calledCompleteRetryingAndPut = true

						assert.Zero(t, toComplete.RetryInfo().CurrentAttempt)

						assert.Equal(t, 1, toPut.RetryInfo().CurrentAttempt)
						assert.Zero(t, toPut.TimeInfo().Start)
						assert.Zero(t, toPut.TimeInfo().End)
						assert.WithinDuration(t, time.Now().Add(j.RetryInfo().WaitUntil), toPut.TimeInfo().WaitUntil, time.Second)
						assert.WithinDuration(t, time.Now().Add(j.RetryInfo().DispatchBy), toPut.TimeInfo().DispatchBy, time.Second)
						assert.Zero(t, toPut.Status())

						return nil
					}

					require.NoError(t, rh.Start(ctx))
					require.NoError(t, rh.Put(ctx, j))
					time.Sleep(100 * time.Millisecond)

					assert.True(t, calledGetAttempt)
					assert.True(t, calledSave)
					assert.True(t, calledCompleteRetryingAndPut)
					assert.False(t, calledCompleteRetrying)
				},
				"PutSucceedsButDoesNothingIfUnstarted": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					mq, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{})
					require.NoError(t, err)
					var calledMockQueue bool
					mq.getJobAttempt = func(context.Context, remoteQueue, string, int) (amboy.Job, bool) {
						calledMockQueue = true
						return nil, false
					}
					mq.saveJob = func(context.Context, remoteQueue, amboy.Job) error {
						calledMockQueue = true
						return nil
					}
					mq.completeRetryingAndPutJob = func(context.Context, remoteQueue, amboy.Job, amboy.Job) error {
						calledMockQueue = true
						return nil
					}

					require.False(t, rh.Started())
					require.NoError(t, rh.Put(ctx, newMockRetryableJob("id")))
					time.Sleep(10 * time.Millisecond)

					assert.False(t, calledMockQueue)
				},
				"PutNoopsIfJobDoesNotNeedToRetry": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					mq, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{})
					require.NoError(t, err)

					j := newMockRetryableJob("id")
					var getAttemptCalls, saveCalls, completeRetryingCalls, completeRetryingAndPutCalls int
					mq.getJobAttempt = func(context.Context, remoteQueue, string, int) (amboy.Job, bool) {
						getAttemptCalls++
						return j, true
					}
					mq.completeRetryingJob = func(context.Context, remoteQueue, amboy.Job) error {
						completeRetryingCalls++
						return nil
					}
					mq.saveJob = func(context.Context, remoteQueue, amboy.Job) error {
						saveCalls++
						return nil
					}
					mq.completeRetryingAndPutJob = func(context.Context, remoteQueue, amboy.Job, amboy.Job) error {
						completeRetryingAndPutCalls++
						return errors.New("fail")
					}

					require.NoError(t, rh.Start(ctx))
					require.NoError(t, rh.Put(ctx, j))

					time.Sleep(100 * time.Millisecond)

					assert.NotZero(t, getAttemptCalls)
					assert.Zero(t, saveCalls)
					assert.Zero(t, completeRetryingAndPutCalls)
					assert.NotZero(t, completeRetryingCalls)
				},
				"PutNoopsIfJobUsesAllAttempts": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					mq, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{})
					require.NoError(t, err)

					j := newMockRetryableJob("id")
					j.UpdateRetryInfo(amboy.JobRetryOptions{
						CurrentAttempt: utility.ToIntPtr(9),
						MaxAttempts:    utility.ToIntPtr(10),
					})
					var getAttemptCalls, saveCalls, completeRetryingCalls, completeRetryingAndPutCalls int
					mq.getJobAttempt = func(context.Context, remoteQueue, string, int) (amboy.Job, bool) {
						getAttemptCalls++
						return j, true
					}
					mq.saveJob = func(context.Context, remoteQueue, amboy.Job) error {
						saveCalls++
						return nil
					}
					mq.completeRetryingJob = func(context.Context, remoteQueue, amboy.Job) error {
						completeRetryingCalls++
						return nil
					}
					mq.completeRetryingAndPutJob = func(context.Context, remoteQueue, amboy.Job, amboy.Job) error {
						completeRetryingAndPutCalls++
						return errors.New("fail")
					}

					require.NoError(t, rh.Start(ctx))
					require.NoError(t, rh.Put(ctx, j))

					time.Sleep(100 * time.Millisecond)

					assert.NotZero(t, getAttemptCalls)
					assert.Zero(t, completeRetryingAndPutCalls)
					assert.NotZero(t, completeRetryingCalls)
				},
				"PutFailsWithNilJob": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					_, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{})
					require.NoError(t, err)

					assert.Error(t, rh.Put(ctx, nil))
				},
				"PutIsIdempotent": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					_, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{})
					require.NoError(t, err)

					j := newMockRetryableJob("id")
					require.NoError(t, rh.Put(ctx, j))
					assert.NoError(t, rh.Put(ctx, j))
				},
				"MaxRetryAttemptsLimitsEnqueueAttempts": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					opts := amboy.RetryHandlerOptions{
						RetryBackoff:     10 * time.Millisecond,
						MaxRetryAttempts: 3,
					}
					mq, rh, err := makeQueueAndRetryHandler(opts)
					require.NoError(t, err)

					j := newMockRetryableJob("id")
					j.UpdateRetryInfo(amboy.JobRetryOptions{
						NeedsRetry: utility.TruePtr(),
					})

					var getAttemptCalls, saveCalls, completeRetryingCalls, completeRetryingAndPutCalls int
					mq.getJobAttempt = func(context.Context, remoteQueue, string, int) (amboy.Job, bool) {
						getAttemptCalls++
						return j, true
					}
					mq.completeRetryingJob = func(context.Context, remoteQueue, amboy.Job) error {
						completeRetryingCalls++
						return nil
					}
					mq.saveJob = func(context.Context, remoteQueue, amboy.Job) error {
						saveCalls++
						return nil
					}
					mq.completeRetryingAndPutJob = func(context.Context, remoteQueue, amboy.Job, amboy.Job) error {
						completeRetryingAndPutCalls++
						return errors.New("fail")
					}

					require.NoError(t, rh.Start(ctx))
					require.NoError(t, rh.Put(ctx, j))

					time.Sleep(3 * opts.RetryBackoff * time.Duration(opts.MaxRetryAttempts))

					assert.Equal(t, opts.MaxRetryAttempts, getAttemptCalls)
					assert.Equal(t, opts.MaxRetryAttempts, saveCalls)
					assert.Equal(t, opts.MaxRetryAttempts, completeRetryingAndPutCalls)
					assert.NotZero(t, completeRetryingCalls)
				},
				"RetryBackoffWaitsBeforeAttemptingReenqueue": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					opts := amboy.RetryHandlerOptions{
						RetryBackoff:     10 * time.Millisecond,
						MaxRetryAttempts: 20,
					}
					mq, rh, err := makeQueueAndRetryHandler(opts)
					require.NoError(t, err)

					j := newMockRetryableJob("id")
					j.UpdateRetryInfo(amboy.JobRetryOptions{
						NeedsRetry: utility.TruePtr(),
					})

					var getAttemptCalls, saveCalls, completeRetryingCalls, completeRetryingAndPutCalls int
					mq.getJobAttempt = func(context.Context, remoteQueue, string, int) (amboy.Job, bool) {
						getAttemptCalls++
						return j, true
					}
					mq.completeRetryingJob = func(context.Context, remoteQueue, amboy.Job) error {
						completeRetryingCalls++
						return nil
					}
					mq.saveJob = func(context.Context, remoteQueue, amboy.Job) error {
						saveCalls++
						return nil
					}
					mq.completeRetryingAndPutJob = func(context.Context, remoteQueue, amboy.Job, amboy.Job) error {
						completeRetryingAndPutCalls++
						return errors.New("fail")
					}

					require.NoError(t, rh.Start(ctx))
					require.NoError(t, rh.Put(ctx, j))

					time.Sleep(time.Duration(opts.MaxRetryAttempts) * opts.RetryBackoff / 2)

					assert.True(t, getAttemptCalls > 1, "worker should have had time to attempt more than once")
					assert.True(t, getAttemptCalls < opts.MaxRetryAttempts, "worker should not have used up all attempts")
					assert.True(t, saveCalls > 1, "worker should have had time to attempt more than once")
					assert.True(t, saveCalls < opts.MaxRetryAttempts, "worker should not have used up all attempts")
					assert.True(t, completeRetryingAndPutCalls > 1, "workers should have had time to attempt more than once")
					assert.True(t, completeRetryingAndPutCalls < opts.MaxRetryAttempts, "worker should not have used up all attempts")
					assert.Zero(t, completeRetryingCalls, "workers should not have used up all attempts")
				},
				"MaxRetryTimeStopsEnqueueAttemptsEarly": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					opts := amboy.RetryHandlerOptions{
						RetryBackoff:     10 * time.Millisecond,
						MaxRetryTime:     20 * time.Millisecond,
						MaxRetryAttempts: 5,
					}
					mq, rh, err := makeQueueAndRetryHandler(opts)
					require.NoError(t, err)

					j := newMockRetryableJob("id")
					j.UpdateRetryInfo(amboy.JobRetryOptions{
						NeedsRetry: utility.TruePtr(),
					})

					var getAttemptCalls, saveCalls, completeRetryingCalls, completeRetryingAndPutCalls int
					mq.getJobAttempt = func(context.Context, remoteQueue, string, int) (amboy.Job, bool) {
						getAttemptCalls++
						return j, true
					}
					mq.completeRetryingJob = func(context.Context, remoteQueue, amboy.Job) error {
						completeRetryingCalls++
						return nil
					}
					mq.saveJob = func(context.Context, remoteQueue, amboy.Job) error {
						saveCalls++
						return nil
					}
					mq.completeRetryingAndPutJob = func(context.Context, remoteQueue, amboy.Job, amboy.Job) error {
						completeRetryingAndPutCalls++
						return errors.New("fail")
					}

					require.NoError(t, rh.Start(ctx))
					require.NoError(t, rh.Put(ctx, j))

					time.Sleep(time.Duration(opts.MaxRetryAttempts) * opts.RetryBackoff)

					assert.True(t, getAttemptCalls > 1, "worker should have had time to attempt more than once")
					assert.True(t, getAttemptCalls < opts.MaxRetryAttempts, "worker should have aborted early before using up all attempts")
					assert.True(t, saveCalls > 1, "worker should have had time to attempt more than once")
					assert.True(t, saveCalls < opts.MaxRetryAttempts, "worker should not have used up all attempts")
					assert.True(t, completeRetryingAndPutCalls > 1, "workers should have had time to attempt more than once")
					assert.True(t, completeRetryingAndPutCalls < opts.MaxRetryAttempts, "worker should have aborted early before using up all attempts")
					assert.NotZero(t, completeRetryingCalls, "worker should have aborted early")
				},
				"CheckIntervalThrottlesJobPickupRate": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					opts := amboy.RetryHandlerOptions{
						WorkerCheckInterval: 400 * time.Millisecond,
					}
					mq, rh, err := makeQueueAndRetryHandler(opts)
					require.NoError(t, err)

					j := newMockRetryableJob("id")
					j.UpdateRetryInfo(amboy.JobRetryOptions{
						NeedsRetry: utility.TruePtr(),
					})

					var getAttemptCalls, saveCalls, completeRetryingCalls, completeRetryingAndPutCalls int
					mq.getJobAttempt = func(context.Context, remoteQueue, string, int) (amboy.Job, bool) {
						getAttemptCalls++
						return j, true
					}
					mq.completeRetryingJob = func(context.Context, remoteQueue, amboy.Job) error {
						completeRetryingCalls++
						return nil
					}
					mq.saveJob = func(context.Context, remoteQueue, amboy.Job) error {
						saveCalls++
						return nil
					}
					mq.completeRetryingAndPutJob = func(context.Context, remoteQueue, amboy.Job, amboy.Job) error {
						completeRetryingAndPutCalls++
						return errors.New("fail")
					}

					require.NoError(t, rh.Start(ctx))

					time.Sleep(10 * time.Millisecond)
					require.NoError(t, rh.Put(ctx, j))

					time.Sleep(opts.WorkerCheckInterval / 2)

					assert.Zero(t, getAttemptCalls, "worker should not have checked for job yet")
					assert.Zero(t, saveCalls, "worker should not have checked for job yet")
					assert.Zero(t, completeRetryingAndPutCalls, "worker should not have checked for job yet")
					assert.Zero(t, completeRetryingCalls, "worker should not have checked for job yet")
				},
			} {
				t.Run(testName, func(t *testing.T) {
					tctx, tcancel := context.WithTimeout(ctx, 10*time.Second)
					defer tcancel()

					q, err := newRemoteUnordered(10)
					require.NoError(t, err)

					makeQueueAndRetryHandler := func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error) {
						mqOpts := mockRemoteQueueOptions{
							queue:          q,
							makeDispatcher: NewDispatcher,
							makeRetryHandler: func(q amboy.RetryableQueue) (amboy.RetryHandler, error) {
								return makeRetryHandler(q, opts)
							},
						}
						mq, err := newMockRemoteQueue(mqOpts)
						if err != nil {
							return nil, nil, errors.WithStack(err)
						}

						return mq, mq.RetryHandler(), nil
					}

					testCase(tctx, t, makeQueueAndRetryHandler)
				})
			}
		})
	}
}

func TestRetryHandlerQueueIntegration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := defaultMongoDBTestOptions()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(opts.URI))
	require.NoError(t, err)

	driver, err := openNewMongoDriver(ctx, newDriverID(), opts, client)
	require.NoError(t, err)

	require.NoError(t, driver.Open(ctx))
	defer driver.Close()

	mDriver, ok := driver.(*mongoDriver)
	require.True(t, ok)

	for rhName, makeRetryHandler := range map[string]func(q amboy.RetryableQueue, opts amboy.RetryHandlerOptions) (amboy.RetryHandler, error){
		"Basic": func(q amboy.RetryableQueue, opts amboy.RetryHandlerOptions) (amboy.RetryHandler, error) {
			return NewBasicRetryHandler(q, opts)
		},
	} {
		t.Run(rhName, func(t *testing.T) {
			for queueName, makeQueue := range map[string]func(size int) (remoteQueue, error){
				"RemoteUnordered": func(size int) (remoteQueue, error) {
					q, err := newRemoteUnordered(size)
					if err != nil {
						return nil, errors.WithStack(err)
					}
					return q, nil
				},
				"RemoteOrdered": func(size int) (remoteQueue, error) {
					q, err := newSimpleRemoteOrdered(size)
					if err != nil {
						return nil, errors.WithStack(err)
					}
					return q, nil
				},
			} {
				t.Run(queueName, func(t *testing.T) {
					for testName, testCase := range map[string]func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (remoteQueue, amboy.RetryHandler, error)){
						"RetryingSucceedsAndQueueHasExpectedState": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (remoteQueue, amboy.RetryHandler, error)) {
							q, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{
								MaxRetryAttempts: 1,
							})
							require.NoError(t, err)

							require.NoError(t, rh.Start(ctx))

							j := newMockRetryableJob("id")
							j.UpdateRetryInfo(amboy.JobRetryOptions{
								NeedsRetry: utility.TruePtr(),
							})
							now := utility.BSONTime(time.Now())
							j.UpdateTimeInfo(amboy.JobTimeInfo{
								Start:      now,
								End:        now,
								WaitUntil:  now.Add(time.Hour),
								DispatchBy: now.Add(2 * time.Hour),
								MaxTime:    time.Minute,
							})
							j.SetStatus(amboy.JobStatusInfo{
								Completed:         true,
								ModificationCount: 10,
								ModificationTime:  now,
								Owner:             q.ID(),
							})

							require.NoError(t, q.Put(ctx, j))

							require.NoError(t, rh.Put(ctx, j))

							jobRetried := make(chan struct{})
							go func() {
								defer close(jobRetried)
								for {
									select {
									case <-ctx.Done():
										return
									default:
										if _, ok := q.GetAttempt(ctx, j.ID(), 1); ok {
											return
										}
									}
								}
							}()

							select {
							case <-ctx.Done():
								require.FailNow(t, "context is done before job could retry")
							case <-jobRetried:
								oldJob, ok := q.GetAttempt(ctx, j.ID(), 0)
								require.True(t, ok, "old job should still exist")

								oldRetryInfo := oldJob.RetryInfo()
								assert.True(t, oldRetryInfo.Retryable)
								assert.False(t, oldRetryInfo.NeedsRetry)
								assert.Zero(t, oldRetryInfo.CurrentAttempt)
								assert.NotZero(t, oldRetryInfo.Start)
								assert.NotZero(t, oldRetryInfo.End)
								assert.Equal(t, bsonJobStatusInfo(j.Status()), oldJob.Status())
								assert.Equal(t, bsonJobTimeInfo(j.TimeInfo()), oldJob.TimeInfo())

								newJob, ok := q.GetAttempt(ctx, j.ID(), 1)
								require.True(t, ok, "new job should have been enqueued")

								newRetryInfo := newJob.RetryInfo()
								assert.True(t, newRetryInfo.Retryable)
								assert.False(t, newRetryInfo.NeedsRetry)
								assert.Equal(t, 1, newRetryInfo.CurrentAttempt)
								assert.Zero(t, newRetryInfo.Start)
								assert.Zero(t, newRetryInfo.End)

								newStatus := newJob.Status()
								assert.False(t, newStatus.InProgress)
								assert.False(t, newStatus.Completed)
								assert.Zero(t, newStatus.Owner)
								assert.Zero(t, newStatus.ModificationCount)
								assert.Zero(t, newStatus.ModificationTime)
								assert.Zero(t, newStatus.Errors)
								assert.Zero(t, newStatus.ErrorCount)

								newTimeInfo := newJob.TimeInfo()
								assert.NotZero(t, newTimeInfo.Created)
								assert.NotEqual(t, j.TimeInfo().Created, newTimeInfo.Created)
								assert.Zero(t, newTimeInfo.Start)
								assert.Zero(t, newTimeInfo.End)
								assert.Equal(t, j.TimeInfo().DispatchBy, utility.BSONTime(newTimeInfo.DispatchBy))
								assert.Equal(t, j.TimeInfo().WaitUntil, utility.BSONTime(newTimeInfo.WaitUntil))
								assert.Equal(t, j.TimeInfo().MaxTime, newTimeInfo.MaxTime)
							}
						},
						"RetryingSucceedsWithScopedJob": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (remoteQueue, amboy.RetryHandler, error)) {
							q, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{
								MaxRetryAttempts: 1,
							})
							require.NoError(t, err)

							require.NoError(t, rh.Start(ctx))

							j := newMockRetryableJob("id")
							scopes := []string{"scope"}
							j.SetScopes(scopes)
							j.SetShouldApplyScopesOnEnqueue(true)
							j.UpdateRetryInfo(amboy.JobRetryOptions{
								NeedsRetry: utility.TruePtr(),
							})
							now := utility.BSONTime(time.Now())
							j.UpdateTimeInfo(amboy.JobTimeInfo{
								Start:      now,
								End:        now,
								WaitUntil:  now.Add(time.Hour),
								DispatchBy: now.Add(2 * time.Hour),
								MaxTime:    time.Minute,
							})
							j.SetStatus(amboy.JobStatusInfo{
								Completed:         true,
								ModificationCount: 10,
								ModificationTime:  now,
								Owner:             q.ID(),
							})

							require.NoError(t, q.Put(ctx, j))
							require.Equal(t, 1, q.Stats(ctx).Total)

							require.NoError(t, rh.Put(ctx, j))

							jobRetried := make(chan struct{})
							go func() {
								defer close(jobRetried)
								for {
									select {
									case <-ctx.Done():
										return
									default:
										if _, ok := q.GetAttempt(ctx, j.ID(), 1); ok {
											return
										}
									}
								}
							}()

							select {
							case <-ctx.Done():
								require.FailNow(t, "context is done before job could retry")
							case <-jobRetried:
								oldJob, ok := q.GetAttempt(ctx, j.ID(), 0)
								require.True(t, ok, "old job should still exist")

								oldRetryInfo := oldJob.RetryInfo()
								assert.True(t, oldRetryInfo.Retryable)
								assert.False(t, oldRetryInfo.NeedsRetry)
								assert.Zero(t, oldRetryInfo.CurrentAttempt)
								assert.NotZero(t, oldRetryInfo.Start)
								assert.NotZero(t, oldRetryInfo.End)
								assert.Equal(t, scopes, oldJob.Scopes())
								assert.Equal(t, bsonJobStatusInfo(j.Status()), oldJob.Status())
								assert.Equal(t, bsonJobTimeInfo(j.TimeInfo()), oldJob.TimeInfo())

								newJob, ok := q.GetAttempt(ctx, j.ID(), 1)
								require.True(t, ok, "new job should have been enqueued")

								newRetryInfo := newJob.RetryInfo()
								assert.True(t, newRetryInfo.Retryable)
								assert.False(t, newRetryInfo.NeedsRetry)
								assert.Equal(t, 1, newRetryInfo.CurrentAttempt)
								assert.Zero(t, newRetryInfo.Start)
								assert.Zero(t, newRetryInfo.End)

								newStatus := newJob.Status()
								assert.False(t, newStatus.InProgress)
								assert.False(t, newStatus.Completed)
								assert.Zero(t, newStatus.Owner)
								assert.Zero(t, newStatus.ModificationCount)
								assert.Zero(t, newStatus.ModificationTime)
								assert.Zero(t, newStatus.Errors)
								assert.Zero(t, newStatus.ErrorCount)

								newTimeInfo := newJob.TimeInfo()
								assert.NotZero(t, newTimeInfo.Created)
								assert.NotEqual(t, j.TimeInfo().Created, newTimeInfo.Created)
								assert.Zero(t, newTimeInfo.Start)
								assert.Zero(t, newTimeInfo.End)
								assert.Equal(t, scopes, newJob.Scopes())
								assert.Equal(t, j.TimeInfo().DispatchBy, utility.BSONTime(newTimeInfo.DispatchBy))
								assert.Equal(t, j.TimeInfo().WaitUntil, utility.BSONTime(newTimeInfo.WaitUntil))
								assert.Equal(t, j.TimeInfo().MaxTime, newTimeInfo.MaxTime)
							}
						},
						"RetryingPopulatesOptionsInNewJob": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (remoteQueue, amboy.RetryHandler, error)) {
							q, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{
								MaxRetryAttempts: 1,
							})
							require.NoError(t, err)

							require.NoError(t, rh.Start(ctx))

							j := newMockRetryableJob("id")
							now := utility.BSONTime(time.Now())
							j.UpdateRetryInfo(amboy.JobRetryOptions{
								NeedsRetry:  utility.TruePtr(),
								MaxAttempts: utility.ToIntPtr(5),
								WaitUntil:   utility.ToTimeDurationPtr(time.Hour),
								DispatchBy:  utility.ToTimeDurationPtr(2 * time.Hour),
							})
							j.UpdateTimeInfo(amboy.JobTimeInfo{
								Start:      now,
								End:        now,
								WaitUntil:  now.Add(-time.Minute),
								DispatchBy: now.Add(2 * time.Minute),
								MaxTime:    time.Minute,
							})
							j.SetStatus(amboy.JobStatusInfo{
								Completed:         true,
								ModificationCount: 10,
								ModificationTime:  now,
								Owner:             q.ID(),
							})

							require.NoError(t, q.Put(ctx, j))

							require.NoError(t, rh.Put(ctx, j))

							jobRetried := make(chan struct{})
							go func() {
								defer close(jobRetried)
								for {
									select {
									case <-ctx.Done():
										return
									default:
										if _, ok := q.GetAttempt(ctx, j.ID(), 1); ok {
											return
										}
									}
								}
							}()

							select {
							case <-ctx.Done():
								require.FailNow(t, "context is done before job could retry")
							case <-jobRetried:
								oldJob, ok := q.GetAttempt(ctx, j.ID(), 0)
								require.True(t, ok, "old job should still exist")
								assert.False(t, oldJob.RetryInfo().NeedsRetry)
								assert.Zero(t, oldJob.RetryInfo().CurrentAttempt)
								assert.NotZero(t, oldJob.RetryInfo().Start)
								assert.NotZero(t, oldJob.RetryInfo().End)
								assert.Equal(t, bsonJobStatusInfo(j.Status()), oldJob.Status())
								assert.Equal(t, bsonJobTimeInfo(j.TimeInfo()), oldJob.TimeInfo())

								newJob, ok := q.GetAttempt(ctx, j.ID(), 1)
								require.True(t, ok, "new job should have been enqueued")

								assert.True(t, newJob.RetryInfo().Retryable)
								assert.False(t, newJob.RetryInfo().NeedsRetry)
								assert.Equal(t, 1, newJob.RetryInfo().CurrentAttempt)
								assert.Zero(t, newJob.RetryInfo().Start)
								assert.Zero(t, newJob.RetryInfo().End)
								assert.Equal(t, j.RetryInfo().MaxAttempts, newJob.RetryInfo().MaxAttempts)
								assert.Equal(t, j.RetryInfo().DispatchBy, newJob.RetryInfo().DispatchBy)
								assert.Equal(t, j.RetryInfo().WaitUntil, newJob.RetryInfo().WaitUntil)

								assert.NotZero(t, newJob.TimeInfo().Created)
								assert.NotEqual(t, j.TimeInfo().Created, newJob.TimeInfo().Created)
								assert.Zero(t, newJob.TimeInfo().Start)
								assert.Zero(t, newJob.TimeInfo().End)
								assert.NotEqual(t, j.TimeInfo().DispatchBy, utility.BSONTime(newJob.TimeInfo().DispatchBy))
								assert.WithinDuration(t, now.Add(j.RetryInfo().DispatchBy), newJob.TimeInfo().DispatchBy, time.Minute)
								assert.NotEqual(t, j.TimeInfo().WaitUntil, utility.BSONTime(newJob.TimeInfo().WaitUntil))
								assert.WithinDuration(t, now.Add(j.RetryInfo().WaitUntil), newJob.TimeInfo().WaitUntil, time.Minute)
								assert.Equal(t, j.TimeInfo().MaxTime, newJob.TimeInfo().MaxTime)
							}
						},
						"RetryingFailsIfJobIsNotInQueue": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (remoteQueue, amboy.RetryHandler, error)) {
							q, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{
								MaxRetryAttempts: 1,
							})
							require.NoError(t, err)

							require.NoError(t, rh.Start(ctx))

							j := newMockRetryableJob("id")
							now := utility.BSONTime(time.Now())
							j.UpdateRetryInfo(amboy.JobRetryOptions{
								NeedsRetry: utility.TruePtr(),
							})
							j.SetStatus(amboy.JobStatusInfo{
								Completed:         true,
								ModificationCount: 10,
								ModificationTime:  now,
								Owner:             q.ID(),
							})

							require.NoError(t, rh.Put(ctx, j))

							jobProcessed := make(chan struct{})
							go func() {
								defer close(jobProcessed)
								for {
									select {
									case <-ctx.Done():
										return
									default:
										if !j.RetryInfo().End.IsZero() {
											return
										}
									}
								}
							}()

							select {
							case <-ctx.Done():
								require.FailNow(t, "context is done before job could retry")
							case <-jobProcessed:
								assert.Zero(t, q.Stats(ctx).Total, "queue state should not be modified when retrying job is missing from queue")
							}
						},
						"RetryNoopsIfJobRetryIsAlreadyInQueue": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (remoteQueue, amboy.RetryHandler, error)) {
							q, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{
								MaxRetryAttempts: 1,
							})
							require.NoError(t, err)

							require.NoError(t, rh.Start(ctx))

							j := newMockRetryableJob("id")
							now := utility.BSONTime(time.Now())
							j.UpdateRetryInfo(amboy.JobRetryOptions{
								NeedsRetry:     utility.TruePtr(),
								CurrentAttempt: utility.ToIntPtr(0),
							})
							j.SetStatus(amboy.JobStatusInfo{
								Completed:         true,
								ModificationCount: 10,
								ModificationTime:  now,
								Owner:             q.ID(),
							})

							require.NoError(t, q.Put(ctx, j))

							ji, err := registry.MakeJobInterchange(j, amboy.JSON)
							require.NoError(t, err)
							retryJob, err := ji.Resolve(amboy.JSON)
							require.NoError(t, err)
							retryJob.UpdateRetryInfo(amboy.JobRetryOptions{
								NeedsRetry:     utility.FalsePtr(),
								CurrentAttempt: utility.ToIntPtr(1),
							})

							require.NoError(t, q.Put(ctx, retryJob))

							retryProcessed := make(chan struct{})
							go func() {
								defer close(retryProcessed)
								for {
									select {
									case <-ctx.Done():
										return
									default:
										if j, ok := q.GetAttempt(ctx, j.ID(), 0); ok && !j.RetryInfo().End.IsZero() {
											return
										}
									}
								}
							}()

							require.NoError(t, rh.Put(ctx, j))

							select {
							case <-ctx.Done():
								require.FailNow(t, "context is done before job could retry")
							case <-retryProcessed:
								assert.Equal(t, 2, q.Stats(ctx).Total)
								storedJob, ok := q.GetAttempt(ctx, j.ID(), 1)
								require.True(t, ok)
								assert.Equal(t, bsonJobTimeInfo(retryJob.TimeInfo()), bsonJobTimeInfo(storedJob.TimeInfo()))
								assert.Equal(t, bsonJobStatusInfo(retryJob.Status()), bsonJobStatusInfo(storedJob.Status()))
								assert.Equal(t, retryJob.RetryInfo(), retryJob.RetryInfo())
							}
						},
						"RetryNoopsIfJobInQueueDoesNotNeedToRetry": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (remoteQueue, amboy.RetryHandler, error)) {
							q, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{
								MaxRetryAttempts: 1,
							})
							require.NoError(t, err)

							require.NoError(t, rh.Start(ctx))

							j := newMockRetryableJob("id")
							now := utility.BSONTime(time.Now())
							j.SetStatus(amboy.JobStatusInfo{
								Completed:         true,
								ModificationCount: 10,
								ModificationTime:  now,
								Owner:             q.ID(),
							})

							require.NoError(t, q.Put(ctx, j))

							j.UpdateRetryInfo(amboy.JobRetryOptions{
								NeedsRetry: utility.TruePtr(),
							})

							retryProcessed := make(chan struct{})
							go func() {
								defer close(retryProcessed)
								for {
									select {
									case <-ctx.Done():
										return
									default:
										if j, ok := q.GetAttempt(ctx, j.ID(), 0); ok && !j.RetryInfo().End.IsZero() {
											return
										}
									}
								}
							}()

							require.NoError(t, rh.Put(ctx, j))

							select {
							case <-ctx.Done():
								require.FailNow(t, "context is done before job could retry")
							case <-retryProcessed:
								assert.Equal(t, 1, q.Stats(ctx).Total)
								storedJob, ok := q.GetAttempt(ctx, j.ID(), 0)
								require.True(t, ok)
								assert.False(t, storedJob.RetryInfo().NeedsRetry)

								_, ok = q.GetAttempt(ctx, j.ID(), 1)
								assert.False(t, ok, "job should not have retried")
							}
						},
						// "": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (remoteQueue, amboy.RetryHandler, error)) {},
					} {
						t.Run(testName, func(t *testing.T) {
							tctx, tcancel := context.WithTimeout(ctx, 30*time.Second)
							defer tcancel()

							require.NoError(t, mDriver.getCollection().Database().Drop(tctx))
							defer func() {
								assert.NoError(t, mDriver.getCollection().Database().Drop(tctx))
							}()

							makeQueueAndRetryHandler := func(opts amboy.RetryHandlerOptions) (remoteQueue, amboy.RetryHandler, error) {
								q, err := makeQueue(10)
								if err != nil {
									return nil, nil, errors.WithStack(err)
								}
								if err := q.SetDriver(driver); err != nil {
									return nil, nil, errors.WithStack(err)
								}
								rh, err := makeRetryHandler(q, opts)
								if err != nil {
									return nil, nil, errors.WithStack(err)
								}
								return q, rh, nil
							}

							testCase(tctx, t, makeQueueAndRetryHandler)
						})
					}
				})
			}
		})
	}
}
