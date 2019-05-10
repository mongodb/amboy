package queue

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	mgo "gopkg.in/mgo.v2"
)

func TestQueueSmoke(t *testing.T) {
	bctx, bcancel := context.WithCancel(context.Background())
	defer bcancel()

	type closer func(context.Context) error
	noopCloser := closer(func(_ context.Context) error { return nil })

	session, err := mgo.DialWithTimeout("mongodb://localhost:27017", time.Second)
	require.NoError(t, err)
	defer session.Close()

	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017").SetConnectTimeout(time.Second))
	require.NoError(t, err)
	require.NoError(t, client.Connect(bctx))

	defer func() { require.NoError(t, client.Disconnect(bctx)) }()

	for _, test := range []struct {
		Name        string
		Constructor func(context.Context, int) (amboy.Queue, closer, error)

		MaxSize int

		// Explict Support Flags
		OrderedSupported    bool
		OrderedStartsBefore bool
		WaitUntilSupported  bool

		// Support Disabled
		SkipUnordered bool
	}{
		{
			Name: "Local",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				return NewLocalUnordered(size), noopCloser, nil
			},
		},
		{
			Name: "LocalSingle",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				q := NewLocalUnordered(size)
				runner := pool.NewSingle()
				if err := runner.SetQueue(q); err != nil {
					return nil, noopCloser, err
				}
				if err := q.SetRunner(runner); err != nil {
					return nil, noopCloser, err
				}
				return q, noopCloser, nil
			},
		},
		{
			Name: "RateLimitedSimple",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				q := NewLocalUnordered(size)

				runner, err := pool.NewSimpleRateLimitedWorkers(size, 10*time.Millisecond, q)
				if err != nil {
					return nil, noopCloser, err
				}

				if err := q.SetRunner(runner); err != nil {
					return nil, noopCloser, err

				}

				return q, noopCloser, nil
			},
		},
		{
			Name: "RateLimitedAverage",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				q := NewLocalUnordered(size)

				runner, err := pool.NewMovingAverageRateLimitedWorkers(size, 100, time.Second, q)
				if err != nil {
					return nil, noopCloser, err
				}

				if err := q.SetRunner(runner); err != nil {
					return nil, noopCloser, err

				}

				return q, noopCloser, nil
			},
		},
		{
			Name: "RemoteUnorderedInternal",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				q := NewRemoteUnordered(size)
				d := NewInternalDriver()
				closer := func(ctx context.Context) error {
					d.Close()
					return nil
				}
				if err := q.SetDriver(d); err != nil {
					return nil, closer, err
				}

				return q, closer, nil
			},
		},
		{
			Name:             "RemoteOrderedInternal",
			OrderedSupported: true,
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				q := NewSimpleRemoteOrdered(size)
				d := NewInternalDriver()

				closer := func(ctx context.Context) error {
					d.Close()
					return nil
				}

				if err := q.SetDriver(d); err != nil {
					return nil, closer, err
				}

				return q, closer, nil
			},
		},
		{
			Name:                "RemoteOrderedInternalSingle",
			OrderedSupported:    true,
			OrderedStartsBefore: true,
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				q := NewSimpleRemoteOrdered(size)
				d := NewInternalDriver()

				runner := pool.NewSingle()

				closer := func(ctx context.Context) error {
					d.Close()
					return nil
				}

				if err := runner.SetQueue(q); err != nil {
					return nil, closer, err
				}

				if err := q.SetRunner(runner); err != nil {
					return nil, closer, err
				}

				if err := q.SetDriver(d); err != nil {
					return nil, closer, err
				}

				return q, closer, nil
			},
		},
		{
			Name:                "AdaptiveOrdering",
			OrderedSupported:    true,
			OrderedStartsBefore: true,
			WaitUntilSupported:  true,
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				q := NewAdaptiveOrderedLocalQueue(size, defaultLocalQueueCapcity)

				return q, noopCloser, nil
			},
		},
		{
			Name:                "LocalOrdered",
			OrderedStartsBefore: false,
			OrderedSupported:    true,
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				return NewLocalOrdered(size), noopCloser, nil
			},
		},
		{
			Name:                "LocalOrderedSingle",
			OrderedStartsBefore: false,
			OrderedSupported:    true,
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				q := NewLocalOrdered(size)
				runner := pool.NewSingle()

				if err := runner.SetQueue(q); err != nil {
					return nil, noopCloser, err
				}

				if err := q.SetRunner(runner); err != nil {
					return nil, noopCloser, err
				}

				return q, noopCloser, nil
			},
		},
		{
			Name: "Priority",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				return NewLocalPriorityQueue(size, defaultLocalQueueCapcity), noopCloser, nil
			},
		},
		{
			Name: "PrioritySingle",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				q := NewLocalPriorityQueue(size, defaultLocalQueueCapcity)
				runner := pool.NewSingle()

				if err := runner.SetQueue(q); err != nil {
					return nil, noopCloser, err
				}

				if err := q.SetRunner(runner); err != nil {
					return nil, noopCloser, err
				}

				return q, noopCloser, nil
			},
		},
		{
			Name: "RemotePriority",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				q := NewRemoteUnordered(size)
				d := NewPriorityDriver()

				closer := func(ctx context.Context) error {
					d.Close()
					return nil
				}
				if err := q.SetDriver(d); err != nil {
					return nil, closer, err
				}

				return q, closer, nil
			},
		},
		{
			Name: "LimitedSize",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				q := NewLocalLimitedSize(size, 1024*size)

				return q, noopCloser, nil
			},
		},
		{
			Name: "Shuffled",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				q := NewShuffledLocal(size, defaultLocalQueueCapcity)

				return q, noopCloser, nil
			},
		},
		{
			Name: "ShuffledSingle",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				q := NewShuffledLocal(size, defaultLocalQueueCapcity)
				runner := pool.NewSingle()

				if err := runner.SetQueue(q); err != nil {
					return nil, noopCloser, err
				}

				if err := q.SetRunner(runner); err != nil {
					return nil, noopCloser, err
				}

				return q, noopCloser, nil
			},
		},
		{
			Name:               "Mgo",
			WaitUntilSupported: true,
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				name := strings.Replace(uuid.NewV4().String(), "-", ".", -1)
				opts := DefaultMongoDBOptions()
				opts.DB = "amboy_test"
				q := NewRemoteUnordered(1)
				driver, err := OpenNewMgoDriver(ctx, name, opts, session.Clone())
				if err != nil {
					return nil, noopCloser, err
				}

				d := driver.(*mgoDriver)
				closer := func(ctx context.Context) error {
					d.Close()
					return cleanupMgo(opts.DB, name, session.Clone())
				}
				if err := q.SetDriver(d); err != nil {
					return nil, closer, err
				}

				return q, closer, nil
			},
		},
		{
			Name:               "Mongo",
			WaitUntilSupported: true,
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				name := strings.Replace(uuid.NewV4().String(), "-", ".", -1)
				opts := DefaultMongoDBOptions()
				opts.DB = "amboy_test"
				q := NewRemoteUnordered(1)
				driver, err := OpenNewMongoDriver(ctx, name, opts, client)
				if err != nil {
					return nil, noopCloser, err
				}

				d := driver.(*mongoDriver)
				closer := func(ctx context.Context) error {
					d.Close()

					if err := client.Database(opts.DB).Collection(addJobsSuffix(name)).Drop(ctx); err != nil {
						return errors.WithStack(err)
					}

					return nil
				}
				if err := q.SetDriver(d); err != nil {
					return nil, closer, err
				}

				return q, closer, nil
			},
		},
		{
			Name:    "SQSFifo",
			MaxSize: 4,
			Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
				q, err := NewSQSFifoQueue(randomString(4), size)
				if err != nil {
					return nil, noopCloser, err
				}
				if err := q.SetRunner(pool.NewAbortablePool(size, q)); err != nil {
					return nil, noopCloser, err
				}

				return q, noopCloser, nil
			},
		},
		// {
		// 	Name:                "MongoOrdered",
		// 	OrderedStartsBefore: true,
		// 	OrderedSupported:    true,
		// 	Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
		// 		name := strings.Replace(uuid.NewV4().String(), "-", ".", -1)
		// 		opts := DefaultMongoDBOptions()
		// 		opts.DB = "amboy_test"
		// 		q := NewSimpleRemoteOrdered(size)
		// 		driver, err := OpenNewMongoDriver(ctx, name, opts, client)
		// 		if err != nil {
		// 			return nil, noopCloser, err
		// 		}

		// 		d := driver.(*mongoDriver)
		// 		closer := func(ctx context.Context) error {
		// 			d.Close()

		// 			if err := client.Database(opts.DB).Collection(addJobsSuffix(name)).Drop(ctx); err != nil {
		// 				return errors.WithStack(err)
		// 			}

		// 			return nil
		// 		}
		// 		if err := q.SetDriver(d); err != nil {
		// 			return nil, closer, err
		// 		}

		// 		return q, closer, nil
		// 	},
		// },
		// {
		// 	Name:                "MgoOrdered",
		// 	WaitUntilSupported:  true,
		// 	OrderedStartsBefore: true,
		// 	OrderedSupported:    true,
		// 	Constructor: func(ctx context.Context, size int) (amboy.Queue, closer, error) {
		// 		name := strings.Replace(uuid.NewV4().String(), "-", ".", -1)
		// 		opts := DefaultMongoDBOptions()
		// 		opts.DB = "amboy_test"
		// 		q := NewSimpleRemoteOrdered(size)
		// 		driver, err := OpenNewMgoDriver(ctx, name, opts, session.Clone())
		// 		if err != nil {
		// 			return nil, noopCloser, err
		// 		}

		// 		d := driver.(*mgoDriver)
		// 		closer := func(ctx context.Context) error {
		// 			d.Close()
		// 			return cleanupMgo(opts.DB, name, session.Clone())
		// 		}
		// 		if err := q.SetDriver(d); err != nil {
		// 			return nil, closer, err
		// 		}

		// 		return q, closer, nil
		// 	},
		// },
	} {
		t.Run(test.Name, func(t *testing.T) {
			for _, size := range []struct {
				Name string
				Size int
			}{
				{Name: "Single", Size: 1},
				{Name: "Double", Size: 2},
				{Name: "Quad", Size: 4},
				{Name: "Octo", Size: 8},
				{Name: "DoubleOcto", Size: 16},
				{Name: "QuadDoubleOcto", Size: 32},
				{Name: "OctoOcto", Size: 64},
			} {
				if test.MaxSize > 0 && size.Size > test.MaxSize {
					continue
				}
				t.Run(size.Name, func(t *testing.T) {
					if !test.SkipUnordered {
						t.Run("UnorderedWorkload", func(t *testing.T) {
							ctx, cancel := context.WithCancel(bctx)
							defer cancel()

							q, qcloser, err := test.Constructor(ctx, size.Size)
							defer func() { require.NoError(t, qcloser(ctx)) }()
							require.NoError(t, err)

							if test.OrderedSupported && !test.OrderedStartsBefore {
								// pass
							} else {
								require.NoError(t, q.Start(ctx))
							}

							testNames := []string{"test", "second", "workers", "forty-two", "true", "false", ""}
							numJobs := size.Size * len(testNames)

							wg := &sync.WaitGroup{}

							for i := 0; i < size.Size; i++ {
								wg.Add(1)
								go func(num int) {
									defer wg.Done()
									for _, name := range testNames {
										cmd := fmt.Sprintf("echo %s.%d", name, num)
										j := job.NewShellJob(cmd, "")
										assert.NoError(t, q.Put(j),
											fmt.Sprintf("with %d workers", num))
										_, ok := q.Get(j.ID())
										assert.True(t, ok)
									}
								}(i)
							}
							wg.Wait()
							if test.OrderedSupported && !test.OrderedStartsBefore {
								require.NoError(t, q.Start(ctx))
							}
							time.Sleep(100 * time.Millisecond)

							amboy.WaitCtxInterval(ctx, q, 100*time.Millisecond)

							assert.Equal(t, numJobs, q.Stats().Total, fmt.Sprintf("with %d workers", size.Size))

							amboy.WaitCtxInterval(ctx, q, 100*time.Millisecond)

							grip.Infof("workers complete for %d worker smoke test", size.Size)
							assert.Equal(t, numJobs, q.Stats().Completed, fmt.Sprintf("%+v", q.Stats()))
							for result := range q.Results(ctx) {
								assert.True(t, result.Status().Completed, fmt.Sprintf("with %d workers", size.Size))

								// assert that we had valid time info persisted
								ti := result.TimeInfo()
								assert.NotZero(t, ti.Start)
								assert.NotZero(t, ti.End)
							}

							statCounter := 0
							for stat := range q.JobStats(ctx) {
								statCounter++
								assert.True(t, stat.ID != "")
							}
							assert.Equal(t, numJobs, statCounter, fmt.Sprintf("want jobStats for every job"))

							grip.Infof("completed results check for %d worker smoke test", size.Size)
						})
					}
					if test.OrderedSupported {
						t.Run("OrderedWorkload", func(t *testing.T) {
							ctx, cancel := context.WithCancel(bctx)
							defer cancel()

							q, qcloser, err := test.Constructor(ctx, size.Size)
							defer func() { require.NoError(t, qcloser(ctx)) }()
							require.NoError(t, err)

							var lastJobName string

							testNames := []string{"amboy", "cusseta", "jasper", "sardis", "dublin"}

							numJobs := size.Size / 2 * len(testNames)

							tempDir, err := ioutil.TempDir("", strings.Join([]string{"amboy-ordered-queue-smoke-test",
								uuid.NewV4().String()}, "-"))
							require.NoError(t, err)
							defer os.RemoveAll(tempDir)

							if test.OrderedStartsBefore {
								require.NoError(t, q.Start(ctx))
							}
							for i := 0; i < size.Size/2; i++ {
								for _, name := range testNames {
									fn := filepath.Join(tempDir, fmt.Sprintf("%s.%d", name, i))
									cmd := fmt.Sprintf("echo %s", fn)
									j := job.NewShellJob(cmd, fn)
									if lastJobName != "" {
										require.NoError(t, j.Dependency().AddEdge(lastJobName))
									}
									lastJobName = j.ID()

									require.NoError(t, q.Put(j))
								}
							}

							if !test.OrderedStartsBefore {
								require.NoError(t, q.Start(ctx))
							}

							require.Equal(t, numJobs, q.Stats().Total, fmt.Sprintf("with %d workers", size.Size))
							amboy.WaitCtxInterval(ctx, q, 50*time.Millisecond)
							require.Equal(t, numJobs, q.Stats().Completed, fmt.Sprintf("%+v", q.Stats()))
							for result := range q.Results(ctx) {
								require.True(t, result.Status().Completed, fmt.Sprintf("with %d workers", size.Size))
							}

							statCounter := 0
							for stat := range q.JobStats(ctx) {
								statCounter++
								require.True(t, stat.ID != "")
							}
							require.Equal(t, statCounter, numJobs)
						})
					}
					if test.WaitUntilSupported {
						t.Run("WaitUntil", func(t *testing.T) {
							ctx, cancel := context.WithCancel(bctx)
							defer cancel()

							q, qcloser, err := test.Constructor(ctx, size.Size)
							defer func() { require.NoError(t, qcloser(ctx)) }()
							require.NoError(t, err)

							require.NoError(t, q.Start(ctx))

							testNames := []string{"test", "second", "workers", "forty-two", "true", "false", ""}
							numJobs := size.Size * len(testNames)

							wg := &sync.WaitGroup{}

							for i := 0; i < size.Size; i++ {
								wg.Add(1)
								go func(num int) {
									defer wg.Done()
									for _, name := range testNames {
										cmd := fmt.Sprintf("echo %s.%d.a", name, num)
										j := job.NewShellJob(cmd, "")
										ti := j.TimeInfo()
										require.Zero(t, ti.WaitUntil)
										require.NoError(t, q.Put(j), fmt.Sprintf("(a) with %d workers", num))
										_, ok := q.Get(j.ID())
										require.True(t, ok)

										cmd = fmt.Sprintf("echo %s.%d.b", name, num)
										j2 := job.NewShellJob(cmd, "")
										j2.UpdateTimeInfo(amboy.JobTimeInfo{
											WaitUntil: time.Now().Add(time.Hour),
										})
										ti2 := j2.TimeInfo()
										require.NotZero(t, ti2.WaitUntil)
										require.NoError(t, q.Put(j2), fmt.Sprintf("(b) with %d workers", num))
										_, ok = q.Get(j2.ID())
										require.True(t, ok)
									}
								}(i)
							}
							wg.Wait()

							require.Equal(t, numJobs*2, q.Stats().Total, fmt.Sprintf("with %d workers", size.Size))

							// wait for things to finish
							time.Sleep(time.Second)

							completed := 0
							for result := range q.Results(ctx) {
								status := result.Status()
								ti := result.TimeInfo()

								if status.Completed || status.InProgress {
									completed++
									require.Zero(t, ti.WaitUntil)
									continue
								}

								require.NotZero(t, ti.WaitUntil)
							}
							assert.True(t, completed > 0)

						})

					}
				})
			}
		})
	}
}
