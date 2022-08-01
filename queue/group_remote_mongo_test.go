package queue

import (
	"context"
	"regexp"
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
					"CreatesQueueWithRegexpQueueOptionsTakingPrecedenceOverDefaults": func(ctx context.Context, t *testing.T, opts MongoDBQueueGroupOptions) {
						id := utility.RandomString()
						re, err := regexp.Compile(id)
						require.NoError(t, err)
						opts.RegexpQueue = []RegexpMongoDBQueueOptions{
							{
								Regexp: *re,
								Options: MongoDBQueueOptions{
									Abortable: utility.FalsePtr(),
								},
							},
						}
						qg, err := makeQueueGroup(ctx, opts)
						require.NoError(t, err)

						q, err := qg.Get(ctx, id)
						require.NoError(t, err)

						assert.True(t, q.Info().Started, "queue should be started")

						runner := q.Runner()
						require.NotZero(t, runner)
						_, ok := runner.(amboy.AbortableRunner)
						assert.False(t, ok, "runner pool should override the default to be unabortable")

						_, ok = q.(*remoteUnordered)
						assert.True(t, ok, "queue should default to unordered remote queue")
					},
					"CreatesQueueWithPerQueueOptionsTakingPrecedenceOverDefaults": func(ctx context.Context, t *testing.T, opts MongoDBQueueGroupOptions) {
						id := utility.RandomString()
						opts.PerQueue = map[string]MongoDBQueueOptions{
							id: {
								Abortable: utility.FalsePtr(),
							},
						}
						qg, err := makeQueueGroup(ctx, opts)
						require.NoError(t, err)

						q, err := qg.Get(ctx, id)
						require.NoError(t, err)

						assert.True(t, q.Info().Started, "queue should be started")

						runner := q.Runner()
						require.NotZero(t, runner)
						_, ok := runner.(amboy.AbortableRunner)
						assert.False(t, ok, "runner pool should override the default to be unabortable")

						_, ok = q.(*remoteUnordered)
						assert.True(t, ok, "queue should default to unordered remote queue")
					},
					"CreatesQueueWithExplicitParameterOptionsTakingPrecedenceOverPerQueueOptions": func(ctx context.Context, t *testing.T, opts MongoDBQueueGroupOptions) {
						id := utility.RandomString()
						opts.PerQueue = map[string]MongoDBQueueOptions{
							id: {
								Abortable: utility.FalsePtr(),
								Ordered:   utility.TruePtr(),
							},
						}

						qg, err := makeQueueGroup(ctx, opts)
						require.NoError(t, err)

						queueOpts := &MongoDBQueueOptions{
							Abortable: utility.TruePtr(),
							Ordered:   utility.TruePtr(),
						}
						q, err := qg.Get(ctx, id, queueOpts)
						require.NoError(t, err)

						assert.True(t, q.Info().Started, "queue should be started")

						runner := q.Runner()
						require.NotZero(t, runner)
						_, ok := runner.(amboy.AbortableRunner)
						assert.True(t, ok, "runner pool should override the per-queue options to be unabortable")

						_, ok = q.(*remoteSimpleOrdered)
						assert.True(t, ok, "queue should override the per-queue options to be ordered remote queue")
					},
					"FailsToCreateQueueWithInvalidOptions": func(ctx context.Context, t *testing.T, opts MongoDBQueueGroupOptions) {
						qg, err := makeQueueGroup(ctx, opts)
						require.NoError(t, err)

						dbOptsCopy := *opts.DefaultQueue.DB
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
						opts.DefaultQueue.DB.Client = client
						opts.DefaultQueue.Abortable = utility.TruePtr()
						opts.DefaultQueue.Ordered = utility.FalsePtr()

						require.NoError(t, client.Database(opts.DefaultQueue.DB.DB).Drop(ctx))
						defer func() {
							assert.NoError(t, client.Database(opts.DefaultQueue.DB.DB).Drop(ctx))
						}()

						tCase(tctx, t, opts)
					})
				}
			})
		})
	}
}
