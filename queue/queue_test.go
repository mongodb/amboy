package queue

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mongodb/amboy/job"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/level"
	"github.com/mongodb/grip/send"
	uuid "github.com/satori/go.uuid"
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

	session, err := mgo.DialWithTimeout("mongodb://localhost:27017", time.Second)
	require.NoError(t, err)
	defer session.Close()

	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017").SetConnectTimeout(time.Second))
	require.NoError(t, err)
	require.NoError(t, client.Connect(bctx))

	defer func() { require.NoError(t, client.Disconnect(bctx)) }()

	for _, test := range DefaultQueueTestCases() {
		if test.Skip {
			continue
		}

		t.Run(test.Name, func(t *testing.T) {
			for _, driver := range DefaultDriverTestCases(client, session) {
				if test.IsRemote == driver.SupportsLocal {
					continue
				}
				if test.OrderedSupported && driver.SkipOrdered {
					continue
				}

				t.Run(driver.Name+"Driver", func(t *testing.T) {
					for _, runner := range DefaultPoolTestCases() {
						if test.IsRemote && runner.SkipRemote {
							continue
						}

						t.Run(runner.Name+"Pool", func(t *testing.T) {
							for _, size := range DefaultSizeTestCases() {
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
											UnorderedTest(bctx, t, test, driver, runner, size)
										})
									}
									if test.OrderedSupported && !driver.SkipOrdered {
										t.Run("Ordered", func(t *testing.T) {
											OrderedTest(bctx, t, test, driver, runner, size)
										})
									}
									if test.WaitUntilSupported || driver.WaitUntilSupported {
										t.Run("WaitUntil", func(t *testing.T) {
											WaitUntilTest(bctx, t, test, driver, runner, size)
										})
									}
									t.Run("OneExecution", func(t *testing.T) {
										OneExecutionTest(bctx, t, test, driver, runner, size)
									})

									if test.IsRemote && !runner.SkipMulti {
										for _, multi := range DefaultMultipleExecututionTestCases(driver) {
											if multi.MultipleDrivers && !driver.SupportsMulti && size.Name != "Single" {
												continue
											}
											t.Run(multi.Name, func(t *testing.T) {
												MultiExecutionTest(bctx, t, test, driver, runner, size, multi)
											})
										}

										t.Run("ManyQueues", func(t *testing.T) {
											ManyQueueTest(bctx, t, test, driver, runner, size)
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
