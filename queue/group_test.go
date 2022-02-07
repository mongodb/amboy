package queue

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type queueGroupCloser func(context.Context) error
type queueGroupConstructor func(context.Context, time.Duration) (amboy.QueueGroup, queueGroupCloser, error)

func localConstructor(ctx context.Context) (amboy.Queue, error) {
	return NewLocalLimitedSize(2, 128), nil
}

func TestQueueGroup(t *testing.T) {
	bctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := mongo.Connect(bctx, options.Client().ApplyURI(defaultMongoDBURI).SetConnectTimeout(2*time.Second))
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Disconnect(bctx)) }()

	t.Run("Constructor", func(t *testing.T) {
		for _, test := range []struct {
			name             string
			valid            bool
			localConstructor func(context.Context) (amboy.Queue, error)
			ttl              time.Duration
			skipRemote       bool
		}{
			{
				name:             "NilNegativeTime",
				localConstructor: nil,
				valid:            false,
				ttl:              -time.Minute,
				skipRemote:       true,
			},
			{
				name:             "NilZeroTime",
				localConstructor: nil,
				valid:            false,
				ttl:              0,
				skipRemote:       true,
			},
			{
				name:             "NilPositiveTime",
				localConstructor: nil,
				valid:            false,
				ttl:              time.Minute,
				skipRemote:       true,
			},
			{
				name:             "NegativeTime",
				localConstructor: localConstructor,
				valid:            false,
				ttl:              -time.Minute,
			},
			{
				name:             "ZeroTime",
				localConstructor: localConstructor,
				valid:            true,
				ttl:              0,
			},
			{
				name:             "PositiveTime",
				localConstructor: localConstructor,
				valid:            true,
				ttl:              time.Minute,
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				t.Run("Local", func(t *testing.T) {
					ctx, cancel := context.WithCancel(bctx)
					defer cancel()

					localOpts := LocalQueueGroupOptions{
						DefaultQueue: LocalQueueOptions{
							Constructor: test.localConstructor,
						},
						TTL: test.ttl,
					}
					g, err := NewLocalQueueGroup(ctx, localOpts) // nolint
					if test.valid {
						require.NotNil(t, g)
						require.NoError(t, err)
					} else {
						require.Nil(t, g)
						require.Error(t, err)
					}
				})
				if test.skipRemote {
					return
				}

				remoteTests := []struct {
					name       string
					db         string
					prefix     string
					uri        string
					workers    int
					workerFunc func(string) int
					valid      bool
				}{
					{
						name:       "AllFieldsSet",
						db:         "db",
						prefix:     "prefix",
						uri:        "uri",
						workerFunc: func(s string) int { return 1 },
						workers:    1,
						valid:      true,
					},
					{
						name:   "WorkersMissing",
						db:     "db",
						prefix: "prefix",
						uri:    "uri",
						valid:  false,
					},
					{
						name:       "WorkerFunctions",
						db:         "db",
						prefix:     "prefix",
						workerFunc: func(s string) int { return 1 },
						uri:        "uri",
						valid:      true,
					},
					{
						name:    "WorkerDefault",
						db:      "db",
						prefix:  "prefix",
						workers: 2,
						uri:     "uri",
						valid:   true,
					},
					{
						name:    "DBMissing",
						prefix:  "prefix",
						uri:     "uri",
						workers: 1,
						valid:   false,
					},
				}

				t.Run("Mongo", func(t *testing.T) {
					for _, remoteTest := range remoteTests {
						t.Run(remoteTest.name, func(t *testing.T) {
							ctx, cancel := context.WithCancel(bctx)
							defer cancel()
							mopts := MongoDBOptions{
								Client:       client,
								DB:           remoteTest.db,
								Collection:   remoteTest.prefix,
								URI:          remoteTest.uri,
								WaitInterval: time.Millisecond,
							}

							remoteOpts := MongoDBQueueGroupOptions{
								DefaultQueue: MongoDBQueueOptions{
									DB:             &mopts,
									NumWorkers:     utility.ToIntPtr(remoteTest.workers),
									WorkerPoolSize: remoteTest.workerFunc,
								},
								TTL:            test.ttl,
								PruneFrequency: test.ttl,
							}

							g, err := NewMongoDBQueueGroup(ctx, "prefix.", remoteOpts)
							if test.valid && remoteTest.valid {
								require.NoError(t, err)
								require.NotNil(t, g)
							} else {
								require.Error(t, err)
								require.Nil(t, g)
							}
						})
					}
				})
				t.Run("MongoMerged", func(t *testing.T) {
					for _, remoteTest := range remoteTests {
						t.Run(remoteTest.name, func(t *testing.T) {
							ctx, cancel := context.WithCancel(bctx)
							defer cancel()
							mopts := MongoDBOptions{
								Client:       client,
								WaitInterval: time.Millisecond,
								DB:           remoteTest.db,
								Collection:   newDriverID(),
								GroupName:    remoteTest.prefix,
								UseGroups:    true,
								URI:          remoteTest.uri,
							}

							remoteOpts := MongoDBQueueGroupOptions{
								DefaultQueue: MongoDBQueueOptions{
									DB:             &mopts,
									NumWorkers:     utility.ToIntPtr(remoteTest.workers),
									WorkerPoolSize: remoteTest.workerFunc,
								},
								TTL:            test.ttl,
								PruneFrequency: test.ttl,
							}

							g, err := NewMongoDBSingleQueueGroup(ctx, remoteOpts)
							if test.valid && remoteTest.valid {
								require.NoError(t, err)
								require.NotNil(t, g)
							} else {
								require.Error(t, err)
								require.Nil(t, g)
							}
						})
					}
				})
			})
		}
	})
	t.Run("Integration", func(t *testing.T) {
		for _, group := range []struct {
			name        string
			constructor queueGroupConstructor
		}{
			{
				name: "Local",
				constructor: func(ctx context.Context, ttl time.Duration) (amboy.QueueGroup, queueGroupCloser, error) {
					qg, err := NewLocalQueueGroup(ctx, LocalQueueGroupOptions{
						DefaultQueue: LocalQueueOptions{
							Constructor: localConstructor,
						},
						TTL: ttl,
					},
					)
					closer := func(_ context.Context) error { return nil }
					return qg, closer, err
				},
			},
			{
				name: "Mongo",
				constructor: func(ctx context.Context, ttl time.Duration) (amboy.QueueGroup, queueGroupCloser, error) {
					mopts := defaultMongoDBTestOptions()
					mopts.Client = client
					mopts.DB = "amboy_group_test"
					mopts.Collection = "prefix"
					mopts.WaitInterval = time.Millisecond

					closer := func(cctx context.Context) error {
						catcher := grip.NewBasicCatcher()
						catcher.Add(client.Database(mopts.DB).Drop(cctx))
						return catcher.Resolve()
					}

					opts := MongoDBQueueGroupOptions{
						DefaultQueue: MongoDBQueueOptions{
							NumWorkers: utility.ToIntPtr(1),
							DB:         &mopts,
						},
						TTL:            ttl,
						PruneFrequency: ttl,
					}

					if err := client.Database(mopts.DB).Drop(ctx); err != nil {
						return nil, closer, err
					}

					if err := client.Ping(ctx, nil); err != nil {
						return nil, closer, errors.Wrap(err, "server not pingable")
					}

					qg, err := NewMongoDBQueueGroup(ctx, "prefix.", opts)
					return qg, closer, err
				},
			},
			{
				name: "MongoMerged",
				constructor: func(ctx context.Context, ttl time.Duration) (amboy.QueueGroup, queueGroupCloser, error) {
					mopts := defaultMongoDBTestOptions()
					mopts.Client = client
					mopts.DB = "amboy_group_test"
					mopts.Collection = "prefix"
					mopts.UseGroups = true
					mopts.GroupName = "group"
					mopts.WaitInterval = time.Millisecond

					closer := func(cctx context.Context) error {
						catcher := grip.NewBasicCatcher()
						catcher.Add(client.Database(mopts.DB).Drop(cctx))
						return catcher.Resolve()
					}
					if ttl == 0 {
						ttl = time.Hour
					}

					opts := MongoDBQueueGroupOptions{
						DefaultQueue: MongoDBQueueOptions{
							NumWorkers: utility.ToIntPtr(1),
							DB:         &mopts,
						},
						TTL:            ttl,
						PruneFrequency: ttl,
					}

					if err := client.Database(mopts.DB).Drop(ctx); err != nil {
						return nil, closer, err
					}

					if err := client.Ping(ctx, nil); err != nil {
						return nil, closer, errors.Wrap(err, "server not pingable")
					}

					qg, err := NewMongoDBSingleQueueGroup(ctx, opts)
					return qg, closer, err
				},
			},
		} {
			t.Run(group.name, func(t *testing.T) {
				t.Run("Get", func(t *testing.T) {
					ctx, cancel := context.WithTimeout(bctx, 20*time.Second)
					defer cancel()

					g, closer, err := group.constructor(ctx, 0)
					defer func() { require.NoError(t, closer(ctx)) }()
					require.NoError(t, err)
					require.NotNil(t, g)
					defer g.Close(ctx)

					q1, err := g.Get(ctx, "one")
					require.NoError(t, err)
					require.NotNil(t, q1)
					require.True(t, q1.Info().Started)

					q2, err := g.Get(ctx, "two")
					require.NoError(t, err)
					require.NotNil(t, q2)
					require.True(t, q2.Info().Started)

					j1 := job.NewShellJob("true", "")
					j2 := job.NewShellJob("true", "")
					j3 := job.NewShellJob("true", "")

					// Add j1 to q1. Add j2 and j3 to q2.
					require.NoError(t, q1.Put(ctx, j1))
					require.NoError(t, q2.Put(ctx, j2))
					require.NoError(t, q2.Put(ctx, j3))

					require.True(t, amboy.WaitInterval(ctx, q1, 100*time.Millisecond))
					require.True(t, amboy.WaitInterval(ctx, q2, 100*time.Millisecond))

					resultsQ1 := []amboy.Job{}
					for result := range q1.Results(ctx) {
						resultsQ1 = append(resultsQ1, result)
					}
					resultsQ2 := []amboy.Job{}
					for result := range q2.Results(ctx) {
						resultsQ2 = append(resultsQ2, result)
					}

					require.True(t, assert.Len(t, resultsQ1, 1, "first") && assert.Len(t, resultsQ2, 2, "second"))

					// Try getting the queues again
					q1, err = g.Get(ctx, "one")
					require.NoError(t, err)
					require.NotNil(t, q1)

					q2, err = g.Get(ctx, "two")
					require.NoError(t, err)
					require.NotNil(t, q2)

					// The queues should be the same, i.e., contain the jobs we expect
					resultsQ1 = []amboy.Job{}
					for result := range q1.Results(ctx) {
						resultsQ1 = append(resultsQ1, result)
					}
					resultsQ2 = []amboy.Job{}
					for result := range q2.Results(ctx) {
						resultsQ2 = append(resultsQ2, result)
					}
					require.Len(t, resultsQ1, 1)
					require.Len(t, resultsQ2, 2)
				})
				t.Run("Put", func(t *testing.T) {
					ctx, cancel := context.WithCancel(bctx)
					defer cancel()

					g, closer, err := group.constructor(ctx, 0)
					defer func() { require.NoError(t, closer(ctx)) }()

					require.NoError(t, err)
					require.NotNil(t, g)

					defer g.Close(ctx)

					q1, err := g.Get(ctx, "one")
					require.NoError(t, err)
					require.NotNil(t, q1)
					if !q1.Info().Started {
						require.NoError(t, q1.Start(ctx))
					}

					q2, err := localConstructor(ctx)
					require.NoError(t, err)
					require.Error(t, g.Put(ctx, "one", q2), "cannot add queue to existing index")
					if !q2.Info().Started {
						require.NoError(t, q2.Start(ctx))
					}

					q3, err := localConstructor(ctx)
					require.NoError(t, err)
					require.NoError(t, g.Put(ctx, "three", q3))
					if !q3.Info().Started {
						require.NoError(t, q3.Start(ctx))
					}

					q4, err := localConstructor(ctx)
					require.NoError(t, err)
					require.NoError(t, g.Put(ctx, "four", q4))
					if !q4.Info().Started {
						require.NoError(t, q4.Start(ctx))
					}

					j1 := job.NewShellJob("true", "")
					j2 := job.NewShellJob("true", "")
					j3 := job.NewShellJob("true", "")

					// Add j1 to q3. Add j2 and j3 to q4.
					require.NoError(t, q3.Put(ctx, j1))
					require.NoError(t, q4.Put(ctx, j2))
					require.NoError(t, q4.Put(ctx, j3))

					amboy.WaitInterval(ctx, q3, 10*time.Millisecond)
					amboy.WaitInterval(ctx, q4, 10*time.Millisecond)

					resultsQ3 := []amboy.Job{}
					for result := range q3.Results(ctx) {
						resultsQ3 = append(resultsQ3, result)
					}
					resultsQ4 := []amboy.Job{}
					for result := range q4.Results(ctx) {
						resultsQ4 = append(resultsQ4, result)
					}
					require.Len(t, resultsQ3, 1)
					require.Len(t, resultsQ4, 2)

					// Try getting the queues again
					q3, err = g.Get(ctx, "three")
					require.NoError(t, err)
					require.NotNil(t, q3)

					q4, err = g.Get(ctx, "four")
					require.NoError(t, err)
					require.NotNil(t, q4)

					// The queues should be the same, i.e., contain the jobs we expect
					resultsQ3 = []amboy.Job{}
					for result := range q3.Results(ctx) {
						resultsQ3 = append(resultsQ3, result)
					}
					resultsQ4 = []amboy.Job{}
					for result := range q4.Results(ctx) {
						resultsQ4 = append(resultsQ4, result)
					}
					require.Len(t, resultsQ3, 1)
					require.Len(t, resultsQ4, 2)
				})
				t.Run("Prune", func(t *testing.T) {
					if runtime.GOOS == "windows" && group.name == "Mongo" {
						t.Skip("legacy implementation performs poorly on windows")
					}

					ctx, cancel := context.WithTimeout(bctx, 10*time.Second)
					defer cancel()

					ttl := time.Second
					if group.name == "Mongo" && utility.StringSliceContains([]string{"windows", "darwin"}, runtime.GOOS) {
						// The tests are particularly slow on MacOS/Windows, so
						// the queues need extra time to run all the jobs before
						// being pruned.
						ttl = 5 * time.Second
					}

					g, closer, err := group.constructor(ctx, ttl)
					defer func() { require.NoError(t, closer(ctx)) }()
					require.NoError(t, err)
					require.NotNil(t, g)
					defer g.Close(ctx)

					q1, err := g.Get(ctx, "five")
					require.NoError(t, err)
					require.NotNil(t, q1)

					q2, err := g.Get(ctx, "six")
					require.NoError(t, err)
					require.NotNil(t, q2)

					j1 := job.NewShellJob("true", "")
					j2 := job.NewShellJob("true", "")
					j3 := job.NewShellJob("true", "")

					// Add j1 to q1. Add j2 and j3 to q2.
					require.NoError(t, q1.Put(ctx, j1))
					require.NoError(t, q2.Put(ctx, j2))
					require.NoError(t, q2.Put(ctx, j3))

					require.True(t, amboy.WaitInterval(ctx, q2, 10*time.Millisecond))
					require.True(t, amboy.WaitInterval(ctx, q1, 10*time.Millisecond))

					// Queues should have completed work
					require.True(t, q1.Stats(ctx).IsComplete())
					require.True(t, q2.Stats(ctx).IsComplete())
					assert.Equal(t, 1, q1.Stats(ctx).Completed)
					assert.Equal(t, 2, q2.Stats(ctx).Completed)

					require.Equal(t, 2, g.Len())

					time.Sleep(ttl + time.Second)
					require.NoError(t, g.Prune(ctx))

					require.Equal(t, 0, g.Len())
				})
				t.Run("PruneWithTTL", func(t *testing.T) {
					ctx, cancel := context.WithTimeout(bctx, 40*time.Second)
					defer cancel()

					g, closer, err := group.constructor(ctx, 3*time.Second)
					defer func() { require.NoError(t, closer(ctx)) }()
					require.NoError(t, err)
					require.NotNil(t, g)
					defer g.Close(ctx)

					q1, err := g.Get(ctx, "seven")
					require.NoError(t, err)
					require.NotNil(t, q1)

					q2, err := g.Get(ctx, "eight")
					require.NoError(t, err)
					require.NotNil(t, q2)

					j1 := job.NewShellJob("true", "")
					j2 := job.NewShellJob("true", "")
					j3 := job.NewShellJob("true", "")

					// Add j1 to q1. Add j2 and j3 to q2.
					require.NoError(t, q1.Put(ctx, j1))
					require.NoError(t, q2.Put(ctx, j2))
					require.NoError(t, q2.Put(ctx, j3))

					amboy.WaitInterval(ctx, q1, 100*time.Millisecond)
					amboy.WaitInterval(ctx, q2, 100*time.Millisecond)

					// Queues should have completed work
					assert.True(t, q1.Stats(ctx).IsComplete())
					assert.True(t, q2.Stats(ctx).IsComplete())
					assert.Equal(t, 1, q1.Stats(ctx).Completed)
					assert.Equal(t, 2, q2.Stats(ctx).Completed)

					require.Equal(t, 2, g.Len())

					// this is just a way for tests that
					// prune more quickly to avoid a long sleep.
					for i := 0; i < 30; i++ {
						time.Sleep(time.Second)

						require.NoError(t, ctx.Err())
						if g.Len() == 0 {
							break
						}
					}

					require.Equal(t, 0, g.Len())
				})
				t.Run("Close", func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()

					g, closer, err := group.constructor(ctx, 0)
					defer func() { require.NoError(t, closer(ctx)) }()
					require.NoError(t, err)
					require.NotNil(t, g)

					q1, err := g.Get(ctx, "nine")
					require.NoError(t, err)
					require.NotNil(t, q1)

					q2, err := g.Get(ctx, "ten")
					require.NoError(t, err)
					require.NotNil(t, q2)

					j1 := job.NewShellJob("true", "")
					j2 := job.NewShellJob("true", "")
					j3 := job.NewShellJob("true", "")

					// Add j1 to q1. Add j2 and j3 to q2.
					require.NoError(t, q1.Put(ctx, j1))
					require.NoError(t, q2.Put(ctx, j2))
					require.NoError(t, q2.Put(ctx, j3))

					amboy.WaitInterval(ctx, q1, 10*time.Millisecond)
					amboy.WaitInterval(ctx, q2, 10*time.Millisecond)

					require.NoError(t, g.Close(ctx))
				})
			})
		}
		t.Run("PruneSmokeTest", func(t *testing.T) {
			ctx, cancel := context.WithCancel(bctx)
			defer cancel()
			mopts := MongoDBOptions{
				Client:       client,
				DB:           "amboy_group_test",
				WaitInterval: time.Millisecond,
				URI:          defaultMongoDBURI,
			}
			collPrefix := "gen"

			for i := 0; i < 10; i++ {
				_, err := client.Database("amboy_group_test").Collection(fmt.Sprintf("%s-%d.jobs", collPrefix, i)).InsertOne(ctx, bson.M{"foo": "bar"})
				require.NoError(t, err)
			}
			remoteOpts := MongoDBQueueGroupOptions{
				DefaultQueue: MongoDBQueueOptions{
					NumWorkers: utility.ToIntPtr(1),
					DB:         &mopts,
				},
				TTL:            time.Second,
				PruneFrequency: time.Second,
			}
			_, err := NewMongoDBQueueGroup(ctx, collPrefix, remoteOpts)
			require.NoError(t, err)
			time.Sleep(time.Second)
			for i := 0; i < 10; i++ {
				count, err := client.Database("amboy_group_test").Collection(fmt.Sprintf("%s-%d.jobs", collPrefix, i)).CountDocuments(ctx, bson.M{})
				require.NoError(t, err)
				require.Zero(t, count, fmt.Sprintf("gen-%d.jobs not dropped", i))
			}
		})
	})
}
