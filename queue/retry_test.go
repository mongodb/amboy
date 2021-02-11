package queue

import (
	"testing"

	"github.com/mongodb/amboy"
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

// kim: TODO: unit test the retry handler with the mockRemoteQueue
func TestRetryHandlerImplementations(t *testing.T) {
	// kim: TODO: setup
	// newMockRemoteQueue(nil options)
	// Set mock methods to return dummy results needed, without actually
	// modifying DB state at all.

	/*
		kim: TODO: test:
		- Retry handler fails in Start() without prior SetQueue()
		- SetQueue() fails after Start()
		- Started() is true after Start().
		- Close() succeeds without start and stops all workers.
		- Close() succeeds after start.
		- Close() is safe to call multiple times.
		- Put() succeeds on unstarted RetryHandler but doesn't trigger re-enqueue due to no workers
		- Put() succeeds on already-started RetryHandler and triggers re-enqueue from workers
		- Put() on closed RetryHandler doesn't trigger re-enqueue due to no workers.
		- Also need to test functionality of options.
	*/
}

// TestBasicRetryHandler tests functionality specific to the basicRetryHandler
// implementation.
func TestBasicRetryHandler(t *testing.T) {
	// kim: TODO: setup
	// newMockRemoteQueue(nil options)
	// Set mock methods to return dummy results needed, without actually
	// modifying DB state at all.
	/*
		kim: TODO: test:
		- tryEnqueueJob() reloads the job from the queue (mock Get() so that it just roundtrips the in-memory job).
		- tryEnqueueJob() fails when job cannot be reloaded from the queue.
		- tryEnqueueJob() fails if the job it gets back is not retryable (mock Get() so that it returns a regular non-retryable job).
		- tryEnqueueJob() no-ops if the reloaded job doesn't need to retry.
		- tryEnqueueJob() fails if the old retry info does not match the reloaded retry info (i.e. the job's persistent state changed).
		- tryEnqueueJob() fails if SaveAndPut() fails.
		- tryEnqueueJob() no-ops if SaveAndPut() returns duplicate job error.
		- tryEnqueueJob() succeeds and updates retry info of old and new job (check mock queue for expected calls and persistent state changes for old/new job state).
		- tryEnqueueJob() no-ops if the old job ID is invalid (i.e. doesn't have ".attempt-${attempt_num}")
		- When handleJob() fails to enqueue once, it tries again.
		- When handleJob() exceeds MaxRetryAttempts, it gives up.
		- When handleJob() exceeds MaxRetryTime, it gives up.
		- When context errors, waitForJob() stops.
		- When waitForJob() receives no job, it no-ops.
		- When waitForJob() receives a job, it attempts to enqueue it (requires mock queue)
		- When waitForJob() does not initially receive a job, it sleeps for RetryBackoff duration, then receives a job and enqueues it.
	*/
}

// kim: TODO: write these once there's a mock queue to use and introspect.
// func TestQueueRetryHandlerIntegration(t *testing.T) {
//     ctx, cancel := context.WithCancel(context.Background())
//     defer cancel()
//     // kim: TODO: populate options
//     dbOpts := MongoDBQueueCreationOptions{}
//     for queueName, makeQueue := range map[string]func(ctx context.Context, t *testing.T) remoteQueue{
//         // kim: TODO: need to propagate options to these
//         "MongoUnordered": func(ctx context.Context, t *testing.T) remoteQueue {
//             rq, err := NewMongoDBQueue(ctx, dbOpts)
//             require.NoError(t, err)
//             return rq
//         },
//         "MongoOrdered": func(ctx context.Context, t *testing.T) remoteQueue {
//             rq, err := NewMongoDBQueue(ctx, dbOpts)
//             require.NoError(t, err)
//             return rq
//         },
//     } {
//         t.Run(queueName, func(t *testing.T) {
//             for testName, testCase := range map[string]func(ctx context.Context, t *testing.T, makeMockQueue func(ctx context.Context, t *testing.T) *mockRemoteQueue){
//                 "InitializesUnstarted": func(ctx context.Context, t *testing.T, makeMockQueue func(ctx context.Context, t *testing.T) *mockRemoteQueue) {
//                     rq := makeMockQueue(ctx, t)
//                     rh := rq.RetryHandler()
//                     require.NotZero(t, rh)
//                     assert.False(t, rh.Started())
//                 },
//                 "": func(ctx context.Context, t *testing.T, makeMockQueue func(ctx context.Context, t *testing.T) *mockRemoteQueue) {},
//                 // "": func(ctx context.Context, t *testing.T, makeMockQueue func(ctx context.Context, t *testing.T) *mockRemoteQueue) {},
//                 // "": func(ctx context.Context, t *testing.T, makeMockQueue func(ctx context.Context, t *testing.T) *mockRemoteQueue) {},
//                 // "": func(ctx context.Context, t *testing.T, makeMockQueue func(ctx context.Context, t *testing.T) *mockRemoteQueue) {},
//                 // "": func(ctx context.Context, t *testing.T, makeMockQueue func(ctx context.Context, t *testing.T) *mockRemoteQueue) {},
//             } {
//                 t.Run(testName, func(t *testing.T) {
//                     tctx, tcancel := context.WithTimeout(ctx, 30*time.Second)
//                     defer tcancel()
//					   makeMockQueue := func(ctx context.Context, t *testing.T) {
//						   q := makeQueue(ctx, t)
//						   mq, err := newMockRemoteQueue(mockRemoteQueueOptions{queue: q})
//						   require.NoError(t, err)
//					       return mq
//					   }
//                     testCase(tctx, t, makeMockQueue)
//                 })
//             }
//         })
//     }
// }
