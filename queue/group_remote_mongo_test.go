package queue

import (
	"context"
	"testing"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/mongodb/amboy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestMongoDBQueueGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(defaultMongoDBURI).SetConnectTimeout(time.Second))
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, client.Disconnect(ctx))
	}()

	for groupName, makeQueueGroup := range map[string]func(ctx context.Context, opts MongoDBQueueGroupOptions) (amboy.QueueGroup, error){
		"Mongo": func(ctx context.Context, opts MongoDBQueueGroupOptions) (amboy.QueueGroup, error) {
			return NewMongoDBQueueGroup(ctx, "prefix.", opts)
		},
		"MongoMerged": NewMongoDBSingleQueueGroup,
	} {
		t.Run(groupName, func(t *testing.T) {
			t.Run("Get", func(t *testing.T) {
				for tName, tCase := range map[string]func(ctx context.Context, t *testing.T, opts MongoDBQueueGroupOptions){
					"CreatesQueueWithDefaultQueueGroupOptions": func(ctx context.Context, t *testing.T, opts MongoDBQueueGroupOptions) {
						qg, err := makeQueueGroup(ctx, opts)
						require.NoError(t, err)

						q, err := qg.Get(ctx, utility.RandomString())
						require.NoError(t, err)

						assert.True(t, q.Info().Started, "queue should be started")

						runner := q.Runner()
						require.NotZero(t, runner)
						_, ok := runner.(amboy.AbortableRunner)
						assert.True(t, ok, "runner pool should be the default (abortable)")

						_, ok = q.(*remoteUnordered)
						assert.True(t, ok, "queue should be default remote queue (unordered)")
					},
					"CreatesQueueWithOverriddenOptions": func(ctx context.Context, t *testing.T, opts MongoDBQueueGroupOptions) {
						qg, err := makeQueueGroup(ctx, opts)
						require.NoError(t, err)

						queueOpts := &MongoDBQueueOptions{
							Abortable: utility.FalsePtr(),
							Ordered:   utility.TruePtr(),
						}
						q, err := qg.Get(ctx, utility.RandomString(), queueOpts)
						require.NoError(t, err)

						assert.True(t, q.Info().Started, "queue should be started")

						runner := q.Runner()
						require.NotZero(t, runner)
						_, ok := runner.(amboy.AbortableRunner)
						assert.False(t, ok, "runner pool should override the default to be unabortable")

						_, ok = q.(*remoteSimpleOrdered)
						assert.True(t, ok, "queue should be default remote queue to be ordered")
					},
					"FailsToCreateQueueWithInvalidOptions": func(ctx context.Context, t *testing.T, opts MongoDBQueueGroupOptions) {
						qg, err := makeQueueGroup(ctx, opts)
						require.NoError(t, err)

						dbOptsCopy := *opts.Queue.DB
						dbOptsCopy.LockTimeout = -time.Minute
						q, err := qg.Get(ctx, utility.RandomString(), &MongoDBQueueOptions{
							DB: &dbOptsCopy,
						})
						assert.Error(t, err)
						assert.Zero(t, q)
					},
					"IgnoresQueueOptionsIfQueueAlreadyExists": func(ctx context.Context, t *testing.T, opts MongoDBQueueGroupOptions) {
						qg, err := makeQueueGroup(ctx, opts)
						require.NoError(t, err)

						queueName := utility.RandomString()
						_, err = qg.Get(ctx, queueName)
						require.NoError(t, err)

						q, err := qg.Get(ctx, queueName, &MongoDBQueueOptions{
							Abortable: utility.FalsePtr(),
							Ordered:   utility.TruePtr(),
						})
						require.NoError(t, err)

						assert.True(t, q.Info().Started, "queue should be started")

						runner := q.Runner()
						require.NotZero(t, runner)
						_, ok := runner.(amboy.AbortableRunner)
						assert.True(t, ok, "runner pool should be the default (abortable) because the queue was created with default options")

						_, ok = q.(*remoteUnordered)
						assert.True(t, ok, "queue should be default remote queue (unordered) because the queue was created with default options")
					},
				} {
					t.Run(tName, func(t *testing.T) {
						tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
						defer cancel()

						opts := defaultMongoDBQueueGroupTestOptions()
						opts.Queue.DB.Client = client
						opts.Queue.Abortable = utility.TruePtr()
						opts.Queue.Ordered = utility.FalsePtr()

						require.NoError(t, client.Database(opts.Queue.DB.DB).Drop(ctx))
						defer func() {
							assert.NoError(t, client.Database(opts.Queue.DB.DB).Drop(ctx))
						}()

						tCase(tctx, t, opts)
					})
				}
			})
		})
	}
}
