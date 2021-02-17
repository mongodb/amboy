package queue

import (
	"context"
	"testing"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/mongodb/amboy"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBasicRetryHandler(t *testing.T) {
	q, err := newRemoteUnordered(1)
	require.NoError(t, err)
	t.Run("SucceedsWithQueue", func(t *testing.T) {
		rh, err := newBasicRetryHandler(q, amboy.RetryHandlerOptions{})
		assert.NoError(t, err)
		assert.NotZero(t, rh)
	})
	t.Run("FailsWithNilQueue", func(t *testing.T) {
		rh, err := newBasicRetryHandler(nil, amboy.RetryHandlerOptions{})
		assert.Error(t, err)
		assert.Zero(t, rh)
	})
	t.Run("FailsWithInvalidOptions", func(t *testing.T) {
		rh, err := newBasicRetryHandler(q, amboy.RetryHandlerOptions{NumWorkers: -1})
		assert.Error(t, err)
		assert.Zero(t, rh)
	})
}

func TestRetryHandlerImplementations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for rhName, makeRetryHandler := range map[string]func(q amboy.RetryableQueue, opts amboy.RetryHandlerOptions) (amboy.RetryHandler, error){
		"Basic": func(q amboy.RetryableQueue, opts amboy.RetryHandlerOptions) (amboy.RetryHandler, error) {
			return newBasicRetryHandler(q, opts)
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
				"StartNoopsAfterAlreadyStarted": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
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
				"MockPutReenqueuesJob": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					mq, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{})
					require.NoError(t, err)

					j := newMockRetryableJob("id")
					j.UpdateRetryInfo(amboy.JobRetryOptions{
						NeedsRetry: utility.ToBoolPtr(true),
					})

					var calledGet, calledSave, calledSaveAndPut bool
					mq.getJob = func(context.Context, remoteQueue, string) (amboy.Job, bool) {
						calledGet = true
						return j, true
					}
					mq.saveJob = func(context.Context, remoteQueue, amboy.Job) error {
						calledSave = true
						return nil
					}
					mq.saveAndPutJob = func(context.Context, remoteQueue, amboy.Job, amboy.Job) error {
						calledSaveAndPut = true
						return nil
					}

					require.NoError(t, rh.Start(ctx))
					require.NoError(t, rh.Put(ctx, j))
					time.Sleep(10 * time.Millisecond)

					assert.True(t, calledGet)
					assert.True(t, calledSave)
					assert.True(t, calledSaveAndPut)
				},
				"PutSucceedsButDoesNothingIfUnstarted": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					mq, rh, err := makeQueueAndRetryHandler(amboy.RetryHandlerOptions{})
					require.NoError(t, err)
					var calledMockQueue bool
					mq.getJob = func(ctx context.Context, q remoteQueue, id string) (amboy.Job, bool) {
						calledMockQueue = true
						return nil, false
					}
					mq.saveJob = func(context.Context, remoteQueue, amboy.Job) error {
						calledMockQueue = true
						return nil
					}
					mq.saveAndPutJob = func(ctx context.Context, q remoteQueue, toSave, toPut amboy.Job) error {
						calledMockQueue = true
						return nil
					}

					require.False(t, rh.Started())
					require.NoError(t, rh.Put(ctx, newMockRetryableJob("id")))
					time.Sleep(10 * time.Millisecond)

					assert.False(t, calledMockQueue)
					assert.Zero(t, mq.Stats(ctx).Total)
				},
				"MaxRetryAttemptsLimitsEnqueueAttempts": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					opts := amboy.RetryHandlerOptions{
						RetryBackoff:     time.Millisecond,
						MaxRetryAttempts: 3,
					}
					mq, rh, err := makeQueueAndRetryHandler(opts)
					require.NoError(t, err)

					j := newMockRetryableJob("id")
					j.UpdateRetryInfo(amboy.JobRetryOptions{
						NeedsRetry: utility.ToBoolPtr(true),
					})

					var getCalls, saveAndPutCalls int
					mq.getJob = func(context.Context, remoteQueue, string) (amboy.Job, bool) {
						getCalls++
						return j, true
					}
					mq.saveJob = func(context.Context, remoteQueue, amboy.Job) error {
						return nil
					}
					mq.saveAndPutJob = func(context.Context, remoteQueue, amboy.Job, amboy.Job) error {
						saveAndPutCalls++
						return errors.New("fail")
					}

					require.NoError(t, rh.Start(ctx))
					require.NoError(t, rh.Put(ctx, j))

					time.Sleep(2 * opts.RetryBackoff * time.Duration(opts.MaxRetryAttempts))

					assert.Equal(t, opts.MaxRetryAttempts, getCalls)
					assert.Equal(t, opts.MaxRetryAttempts, saveAndPutCalls)
				},
				"RetryBackoffWaitsBeforeAttemptingReenqueue": func(ctx context.Context, t *testing.T, makeQueueAndRetryHandler func(opts amboy.RetryHandlerOptions) (*mockRemoteQueue, amboy.RetryHandler, error)) {
					opts := amboy.RetryHandlerOptions{
						RetryBackoff:     time.Millisecond,
						MaxRetryAttempts: 6,
					}
					mq, rh, err := makeQueueAndRetryHandler(opts)
					require.NoError(t, err)

					j := newMockRetryableJob("id")
					j.UpdateRetryInfo(amboy.JobRetryOptions{
						NeedsRetry: utility.ToBoolPtr(true),
					})

					var getCalls, saveAndPutCalls int
					mq.getJob = func(context.Context, remoteQueue, string) (amboy.Job, bool) {
						getCalls++
						return j, true
					}
					mq.saveJob = func(context.Context, remoteQueue, amboy.Job) error {
						return nil
					}
					mq.saveAndPutJob = func(context.Context, remoteQueue, amboy.Job, amboy.Job) error {
						saveAndPutCalls++
						return errors.New("fail")
					}

					require.NoError(t, rh.Start(ctx))
					require.NoError(t, rh.Put(ctx, j))

					time.Sleep(time.Duration(opts.MaxRetryAttempts) * opts.RetryBackoff / 2)

					assert.True(t, getCalls > 1, "worker should have had time to attempt more than once")
					assert.True(t, getCalls < opts.MaxRetryAttempts, "worker should not have used up all attempts")
					assert.True(t, saveAndPutCalls > 1, "workers should have had time to attempt more than once")
					assert.True(t, saveAndPutCalls < opts.MaxRetryAttempts, "worker should not have used up all attempts")
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
						NeedsRetry: utility.ToBoolPtr(true),
					})

					var getCalls, saveAndPutCalls int
					mq.getJob = func(context.Context, remoteQueue, string) (amboy.Job, bool) {
						getCalls++
						return j, true
					}
					mq.saveJob = func(context.Context, remoteQueue, amboy.Job) error {
						return nil
					}
					mq.saveAndPutJob = func(context.Context, remoteQueue, amboy.Job, amboy.Job) error {
						saveAndPutCalls++
						return errors.New("fail")
					}

					require.NoError(t, rh.Start(ctx))
					require.NoError(t, rh.Put(ctx, j))

					time.Sleep(time.Duration(opts.MaxRetryAttempts) * opts.RetryBackoff)

					assert.True(t, getCalls > 1, "worker should have had time to attempt more than once")
					assert.True(t, getCalls < opts.MaxRetryAttempts, "worker should have aborted early before using up all attempts")
					assert.True(t, saveAndPutCalls > 1, "workers should have had time to attempt more than once")
					assert.True(t, saveAndPutCalls < opts.MaxRetryAttempts, "worker should have aborted early before using up all attempts")
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
