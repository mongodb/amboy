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
	"github.com/mongodb/grip/level"
	"github.com/mongodb/grip/send"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	mgo "gopkg.in/mgo.v2"
)

const defaultLocalQueueCapcity = 10000

func init() {
	grip.SetName("amboy.queue.tests")
	grip.Error(grip.SetSender(send.MakeNative()))

	lvl := grip.GetSender().Level()
	lvl.Threshold = level.Error
	_ = grip.GetSender().SetLevel(lvl)

	job.RegisterDefaultJobs()
}

func newDriverID() string { return strings.Replace(uuid.NewV4().String(), "-", ".", -1) }

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
		Constructor func(context.Context, int) (amboy.Queue, error)

		MaxSize int

		// Explict Support Flags
		OrderedSupported    bool
		OrderedStartsBefore bool
		WaitUntilSupported  bool

		// Support Disabled
		SkipUnordered bool

		IsRemote bool
	}{
		{
			Name:        "Local",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, error) { return NewLocalUnordered(size), nil },
		},
		{
			Name:                "AdaptiveOrdering",
			OrderedSupported:    true,
			OrderedStartsBefore: true,
			WaitUntilSupported:  true,
			Constructor: func(ctx context.Context, size int) (amboy.Queue, error) {
				return NewAdaptiveOrderedLocalQueue(size, defaultLocalQueueCapcity), nil
			},
		},
		{
			Name:                "LocalOrdered",
			OrderedStartsBefore: false,
			OrderedSupported:    true,
			Constructor:         func(ctx context.Context, size int) (amboy.Queue, error) { return NewLocalOrdered(size), nil },
		},
		{
			Name: "Priority",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, error) {
				return NewLocalPriorityQueue(size, defaultLocalQueueCapcity), nil
			},
		},
		{
			Name: "LimitedSize",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, error) {
				return NewLocalLimitedSize(size, 1024*size), nil
			},
		},
		{
			Name: "Shuffled",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, error) {
				return NewShuffledLocal(size, defaultLocalQueueCapcity), nil
			},
		},
		{
			Name:        "RemoteUnordered",
			IsRemote:    true,
			Constructor: func(ctx context.Context, size int) (amboy.Queue, error) { return NewRemoteUnordered(size), nil },
		},
		{
			Name:             "RemoteOrdered",
			IsRemote:         true,
			OrderedSupported: true,
			Constructor:      func(ctx context.Context, size int) (amboy.Queue, error) { return NewSimpleRemoteOrdered(size), nil },
		},
		{
			Name:    "SQSFifo",
			MaxSize: 4,
			Constructor: func(ctx context.Context, size int) (amboy.Queue, error) {
				return NewSQSFifoQueue(randomString(4), size)
			},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			for _, driver := range []struct {
				Name               string
				SetDriver          func(context.Context, amboy.Queue, string) (closer, error)
				SupportsLocal      bool
				SupportsMulti      bool
				WaitUntilSupported bool
				SkipOrdered        bool
			}{
				{
					Name:          "No",
					SupportsLocal: true,
					SetDriver:     func(ctx context.Context, q amboy.Queue, name string) (closer, error) { return noopCloser, nil },
				},
				{
					Name: "Internal",
					SetDriver: func(ctx context.Context, q amboy.Queue, name string) (closer, error) {
						remote, ok := q.(Remote)
						if !ok {
							return nil, errors.New("invalid queue type")
						}

						d := NewInternalDriver()
						closer := func(ctx context.Context) error {
							d.Close()
							return nil
						}

						return closer, remote.SetDriver(d)
					},
				},
				{
					Name: "Priority",
					SetDriver: func(ctx context.Context, q amboy.Queue, name string) (closer, error) {
						remote, ok := q.(Remote)
						if !ok {
							return nil, errors.New("invalid queue type")
						}

						d := NewPriorityDriver()
						closer := func(ctx context.Context) error {
							d.Close()
							return nil
						}

						return closer, remote.SetDriver(d)
					},
				},
				{
					Name:               "Mgo",
					WaitUntilSupported: true,
					SupportsMulti:      true,
					SetDriver: func(ctx context.Context, q amboy.Queue, name string) (closer, error) {
						remote, ok := q.(Remote)
						if !ok {
							return nil, errors.New("invalid queue type")
						}
						opts := DefaultMongoDBOptions()
						opts.DB = "amboy_test"

						driver, err := OpenNewMgoDriver(ctx, name, opts, session.Clone())
						if err != nil {
							return nil, err
						}

						d := driver.(*mgoDriver)
						closer := func(ctx context.Context) error {
							d.Close()
							return session.DB(opts.DB).C(addJobsSuffix(name)).DropCollection()
						}

						return closer, remote.SetDriver(d)
					},
				},
				{
					Name:               "Mongo",
					WaitUntilSupported: true,
					SupportsMulti:      true,
					SetDriver: func(ctx context.Context, q amboy.Queue, name string) (closer, error) {
						remote, ok := q.(Remote)
						if !ok {
							return nil, errors.New("invalid queue type")
						}

						opts := DefaultMongoDBOptions()
						opts.DB = "amboy_test"

						driver, err := OpenNewMongoDriver(ctx, name, opts, client)
						if err != nil {
							return nil, err
						}

						d := driver.(*mongoDriver)
						closer := func(ctx context.Context) error {
							d.Close()
							return client.Database(opts.DB).Collection(addJobsSuffix(name)).Drop(ctx)
						}

						return closer, remote.SetDriver(d)

					},
				},
				// {
				// 	Name: "MongoGroup",
				// 	SetDriver: func(ctx context.Context, q amboy.Queue) (closer, error) {}
				// },
			} {
				if test.IsRemote == driver.SupportsLocal {
					continue
				}
				if test.OrderedSupported && driver.SkipOrdered {
					continue
				}

				t.Run(driver.Name+"Driver", func(t *testing.T) {
					for _, runner := range []struct {
						Name      string
						SetPool   func(amboy.Queue, int) error
						SkipMulti bool
						MinSize   int
						MaxSize   int
					}{
						{
							Name:    "Default",
							SetPool: func(q amboy.Queue, _ int) error { return nil },
						},
						{
							Name:      "Single",
							SkipMulti: true,
							MaxSize:   1,
							SetPool: func(q amboy.Queue, _ int) error {
								runner := pool.NewSingle()
								if err := runner.SetQueue(q); err != nil {
									return err
								}

								return q.SetRunner(runner)
							},
						},
						{
							Name:    "Abortable",
							MinSize: 4,
							SetPool: func(q amboy.Queue, size int) error { return q.SetRunner(pool.NewAbortablePool(size, q)) },
						},
						{
							Name:      "RateLimitedSimple",
							MinSize:   4,
							MaxSize:   32,
							SkipMulti: true,
							SetPool: func(q amboy.Queue, size int) error {
								runner, err := pool.NewSimpleRateLimitedWorkers(size, 10*time.Millisecond, q)
								if err != nil {
									return nil
								}

								return q.SetRunner(runner)
							},
						},
						{
							Name:      "RateLimitedAverage",
							MinSize:   4,
							MaxSize:   16,
							SkipMulti: true,
							SetPool: func(q amboy.Queue, size int) error {
								runner, err := pool.NewMovingAverageRateLimitedWorkers(size, size*100, 10*time.Millisecond, q)
								if err != nil {
									return nil
								}

								return q.SetRunner(runner)
							},
						},
					} {
						t.Run(runner.Name+"Pool", func(t *testing.T) {
							for _, size := range []struct {
								Name string
								Size int
							}{
								{Name: "One", Size: 1},
								{Name: "Two", Size: 2},
								{Name: "Four", Size: 4},
								{Name: "Eight", Size: 8},
								{Name: "Sixteen", Size: 16},
								{Name: "ThirtyTwo", Size: 32},
								{Name: "SixtyFour", Size: 64},
							} {
								if test.MaxSize > 0 && size.Size > test.MaxSize {
									continue
								}

								if runner.MinSize > 0 && runner.MinSize > size.Size {
									continue
								}

								if runner.MaxSize > 0 && runner.MaxSize < size.Size {
									continue
								}

								t.Run(size.Name, func(t *testing.T) {
									if !test.SkipUnordered {
										t.Run("Unordered", func(t *testing.T) {
											ctx, cancel := context.WithCancel(bctx)
											defer cancel()

											q, err := test.Constructor(ctx, size.Size)
											require.NoError(t, err)
											require.NoError(t, runner.SetPool(q, size.Size))
											dcloser, err := driver.SetDriver(ctx, q, newDriverID())
											defer func() { require.NoError(t, dcloser(ctx)) }()
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
									if test.OrderedSupported && !driver.SkipOrdered {
										t.Run("Ordered", func(t *testing.T) {
											ctx, cancel := context.WithCancel(bctx)
											defer cancel()

											q, err := test.Constructor(ctx, size.Size)
											require.NoError(t, err)
											require.NoError(t, runner.SetPool(q, size.Size))

											dcloser, err := driver.SetDriver(ctx, q, newDriverID())
											defer func() { require.NoError(t, dcloser(ctx)) }()
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
									if test.WaitUntilSupported || driver.WaitUntilSupported {
										t.Run("WaitUntil", func(t *testing.T) {
											ctx, cancel := context.WithCancel(bctx)
											defer cancel()

											q, err := test.Constructor(ctx, size.Size)
											require.NoError(t, err)
											require.NoError(t, runner.SetPool(q, size.Size))

											dcloser, err := driver.SetDriver(ctx, q, newDriverID())
											defer func() { require.NoError(t, dcloser(ctx)) }()
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
									t.Run("OneExecution", func(t *testing.T) {
										if test.Name == "LocalOrdered" {
											t.Skip("topological sort deadlocks")
										}
										ctx, cancel := context.WithCancel(bctx)
										defer cancel()

										q, err := test.Constructor(ctx, size.Size)
										require.NoError(t, err)
										require.NoError(t, runner.SetPool(q, size.Size))

										dcloser, err := driver.SetDriver(ctx, q, newDriverID())
										defer func() { require.NoError(t, dcloser(ctx)) }()
										require.NoError(t, err)

										mockJobCounters.Reset()
										count := 40

										if !test.OrderedSupported || test.OrderedStartsBefore {
											require.NoError(t, q.Start(ctx))
										}

										for i := 0; i < count; i++ {
											j := newMockJob()
											jobID := fmt.Sprintf("%d.%d.mock.single-exec", i, job.GetNumber())
											j.SetID(jobID)
											assert.NoError(t, q.Put(j))
										}

										if test.OrderedSupported && !test.OrderedStartsBefore {
											fmt.Println("starting")
											require.NoError(t, q.Start(ctx))
											fmt.Println("started")
										}

										amboy.WaitCtxInterval(ctx, q, 10*time.Millisecond)
										assert.Equal(t, count, mockJobCounters.Count())
									})

									if test.IsRemote {
										t.Run("Multiple", func(t *testing.T) {
											for _, multi := range []struct {
												Name            string
												Setup           func(context.Context, amboy.Queue, amboy.Queue) (closer, error)
												MultipleDrivers bool
											}{
												{
													Name:            "TwoDrivers",
													MultipleDrivers: true,
													Setup: func(ctx context.Context, qOne, qTwo amboy.Queue) (closer, error) {
														catcher := grip.NewBasicCatcher()
														driverID := newDriverID()
														dcloserOne, err := driver.SetDriver(ctx, qOne, driverID)
														catcher.Add(err)

														dcloserTwo, err := driver.SetDriver(ctx, qTwo, driverID)
														catcher.Add(err)

														closer := func(ctx context.Context) error {
															err := dcloserOne(ctx)
															grip.Info(dcloserTwo(ctx))
															return err
														}

														return closer, catcher.Resolve()
													},
												},
												{
													Name: "OneDriver",
													Setup: func(ctx context.Context, qOne, qTwo amboy.Queue) (closer, error) {
														catcher := grip.NewBasicCatcher()
														driverID := newDriverID()
														dcloserOne, err := driver.SetDriver(ctx, qOne, driverID)
														catcher.Add(err)

														rq := qOne.(Remote)
														catcher.Add(qTwo.(Remote).SetDriver(rq.Driver()))

														return dcloserOne, catcher.Resolve()
													},
												},
											} {
												if multi.MultipleDrivers && !driver.SupportsMulti && size.Name != "Single" {
													continue
												}
												t.Run(multi.Name, func(t *testing.T) {
													ctx, cancel := context.WithCancel(bctx)
													defer cancel()

													qOne, err := test.Constructor(ctx, size.Size)
													require.NoError(t, err)
													qTwo, err := test.Constructor(ctx, size.Size)
													require.NoError(t, err)
													require.NoError(t, runner.SetPool(qOne, size.Size))
													require.NoError(t, runner.SetPool(qTwo, size.Size))

													closer, err := multi.Setup(ctx, qOne, qTwo)
													require.NoError(t, err)
													defer func() { require.NoError(t, closer(ctx)) }()

													assert.NoError(t, qOne.Start(ctx))
													assert.NoError(t, qTwo.Start(ctx))

													num := 200
													adderProcs := 4

													wg := &sync.WaitGroup{}
													for o := 0; o < adderProcs; o++ {
														wg.Add(1)
														go func(o int) {
															defer wg.Done()
															// add a bunch of jobs: half to one queue and half to the other.
															for i := 0; i < num; i++ {
																cmd := fmt.Sprintf("echo %d.%d", o, i)
																j := job.NewShellJob(cmd, "")
																if i%2 == 0 {
																	assert.NoError(t, qOne.Put(j))
																	assert.Error(t, qOne.Put(j))
																	continue
																}
																assert.NoError(t, qTwo.Put(j))
															}
														}(o)
													}
													wg.Wait()

													num = num * adderProcs

													grip.Info("added jobs to queues")

													// check that both queues see all jobs
													statsOne := qOne.Stats()
													statsTwo := qTwo.Stats()

													require.Equal(t, statsOne.Total, num)
													require.Equal(t, statsTwo.Total, num)

													grip.Infof("before wait statsOne: %+v", statsOne)
													grip.Infof("before wait statsTwo: %+v", statsTwo)

													// wait for all jobs to complete.
													amboy.WaitCtxInterval(ctx, qOne, 100*time.Millisecond)
													amboy.WaitCtxInterval(ctx, qTwo, 100*time.Millisecond)

													grip.Infof("after wait statsOne: %+v", qOne.Stats())
													grip.Infof("after wait statsTwo: %+v", qTwo.Stats())

													// check that all the results in the queues are are completed,
													// and unique
													firstCount := 0
													results := make(map[string]struct{})
													for result := range qOne.Results(ctx) {
														firstCount++
														assert.True(t, result.Status().Completed)
														results[result.ID()] = struct{}{}
													}

													secondCount := 0
													// make sure that all of the results in the second queue match
													// the results in the first queue.
													for result := range qTwo.Results(ctx) {
														secondCount++
														assert.True(t, result.Status().Completed)
														results[result.ID()] = struct{}{}
													}

													assert.Equal(t, firstCount, secondCount)
													assert.Equal(t, len(results), firstCount)
												})
											}

										})
									}
								})

							}

						})

					}
				})
			}
		})
	}
}
