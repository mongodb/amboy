package queue

import (
	"context"
	"testing"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/mongodb/amboy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		})
		t.Run("FailsWithInvalidRetryableQueueOptions", func(t *testing.T) {
			opts := defaultMongoDBQueueTestOptions()
			opts.Retryable = &RetryableQueueOptions{
				StaleRetryingMonitorInterval: -time.Second,
			}
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
