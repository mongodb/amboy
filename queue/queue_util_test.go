package queue

import (
	"context"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	mgo "gopkg.in/mgo.v2"
)

type QueueTestCase struct {
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
	Skip     bool
}

func DefaultQueueTestCases() []QueueTestCase {
	return []QueueTestCase{
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
			Skip:    true,
			Constructor: func(ctx context.Context, size int) (amboy.Queue, error) {
				return NewSQSFifoQueue(randomString(4), size)
			},
		},
	}
}

type TestCloser func(context.Context) error

type DriverTestCase struct {
	Name               string
	SetDriver          func(context.Context, amboy.Queue, string) (TestCloser, error)
	Constructor        func(context.Context, string, int) ([]Driver, TestCloser, error)
	SupportsLocal      bool
	SupportsMulti      bool
	WaitUntilSupported bool
	SkipOrdered        bool
}

func DefaultDriverTestCases(client *mongo.Client, session *mgo.Session) []DriverTestCase {
	return []DriverTestCase{
		{
			Name:          "No",
			SupportsLocal: true,
			Constructor: func(ctx context.Context, name string, size int) ([]Driver, TestCloser, error) {
				return nil, func(_ context.Context) error { return nil }, errors.New("not supported")
			},
			SetDriver: func(ctx context.Context, q amboy.Queue, name string) (TestCloser, error) {
				return func(_ context.Context) error { return nil }, nil
			},
		},
		{
			Name: "Internal",
			Constructor: func(ctx context.Context, name string, size int) ([]Driver, TestCloser, error) {
				return nil, func(_ context.Context) error { return nil }, errors.New("not supported")
			},
			SetDriver: func(ctx context.Context, q amboy.Queue, name string) (TestCloser, error) {
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
			Constructor: func(ctx context.Context, name string, size int) ([]Driver, TestCloser, error) {
				return nil, func(_ context.Context) error { return nil }, errors.New("not supported")
			},
			SetDriver: func(ctx context.Context, q amboy.Queue, name string) (TestCloser, error) {
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
			Constructor: func(ctx context.Context, name string, size int) ([]Driver, TestCloser, error) {
				opts := DefaultMongoDBOptions()
				opts.DB = "amboy_test"

				var err error
				out := make([]Driver, size)
				catcher := grip.NewBasicCatcher()
				for i := 0; i < size; i++ {
					out[i], err = OpenNewMgoDriver(ctx, name, opts, session.Clone())
					catcher.Add(err)
				}
				closer := func(ctx context.Context) error {
					for _, d := range out {
						if d != nil {
							d.(*mgoDriver).Close()
						}
					}
					return session.DB(opts.DB).C(addJobsSuffix(name)).DropCollection()
				}

				return out, closer, catcher.Resolve()
			},
			SetDriver: func(ctx context.Context, q amboy.Queue, name string) (TestCloser, error) {
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
			Constructor: func(ctx context.Context, name string, size int) ([]Driver, TestCloser, error) {
				opts := DefaultMongoDBOptions()
				opts.DB = "amboy_test"

				var err error
				out := make([]Driver, size)
				catcher := grip.NewBasicCatcher()
				for i := 0; i < size; i++ {
					out[i], err = OpenNewMongoDriver(ctx, name, opts, client)
					catcher.Add(err)
				}
				closer := func(ctx context.Context) error {
					for _, d := range out {
						if d != nil {
							d.(*mongoDriver).Close()
						}
					}
					return client.Database(opts.DB).Collection(addJobsSuffix(name)).Drop(ctx)
				}

				return out, closer, catcher.Resolve()
			},
			SetDriver: func(ctx context.Context, q amboy.Queue, name string) (TestCloser, error) {
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
		{
			Name:               "MongoGroup",
			WaitUntilSupported: true,
			SupportsMulti:      true,
			Constructor: func(ctx context.Context, name string, size int) ([]Driver, TestCloser, error) {
				opts := DefaultMongoDBOptions()
				opts.DB = "amboy_test"

				out := []Driver{}
				catcher := grip.NewBasicCatcher()
				for i := 0; i < size; i++ {
					driver, err := OpenNewMongoGroupDriver(ctx, name, opts, "one", client)
					catcher.Add(err)
					out = append(out, driver)
					driver, err = OpenNewMongoGroupDriver(ctx, name, opts, "two", client)
					catcher.Add(err)
					out = append(out, driver)
				}
				closer := func(ctx context.Context) error {
					for _, d := range out {
						if d != nil {
							d.(*mongoGroupDriver).Close()
						}
					}
					return client.Database(opts.DB).Drop(ctx)
				}

				return out, closer, catcher.Resolve()
			},
			SetDriver: func(ctx context.Context, q amboy.Queue, name string) (TestCloser, error) {
				remote, ok := q.(Remote)
				if !ok {
					return nil, errors.New("invalid queue type")
				}

				opts := DefaultMongoDBOptions()
				opts.DB = "amboy_test"

				driver, err := OpenNewMongoGroupDriver(ctx, name, opts, "three", client)
				if err != nil {
					return nil, err
				}

				d := driver.(*mongoDriver)
				closer := func(ctx context.Context) error {
					d.Close()
					return client.Database(opts.DB).Drop(ctx)
				}

				return closer, remote.SetDriver(d)
			},
		},
		{
			Name:               "MgoGroup",
			WaitUntilSupported: true,
			SupportsMulti:      true,
			Constructor: func(ctx context.Context, name string, size int) ([]Driver, TestCloser, error) {
				opts := DefaultMongoDBOptions()
				opts.DB = "amboy_test"

				out := []Driver{}
				catcher := grip.NewBasicCatcher()
				for i := 0; i < size; i++ {
					driver, err := OpenNewMgoGroupDriver(ctx, name, opts, "four", session.Clone())
					catcher.Add(err)
					out = append(out, driver)
					driver, err = OpenNewMgoGroupDriver(ctx, name, opts, "five", session.Clone())
					catcher.Add(err)
					out = append(out, driver)
				}
				closer := func(ctx context.Context) error {
					for _, d := range out {
						if d != nil {
							d.(*mongoGroupDriver).Close()
						}
					}
					return session.DB(opts.DB).DropDatabase()
				}

				return out, closer, catcher.Resolve()
			},
			SetDriver: func(ctx context.Context, q amboy.Queue, name string) (TestCloser, error) {
				remote, ok := q.(Remote)
				if !ok {
					return nil, errors.New("invalid queue type")
				}

				opts := DefaultMongoDBOptions()
				opts.DB = "amboy_test"

				driver, err := OpenNewMgoGroupDriver(ctx, name, opts, "six", session.Clone())
				if err != nil {
					return nil, err
				}

				d := driver.(*mongoDriver)
				closer := func(ctx context.Context) error {
					d.Close()
					return session.DB(opts.DB).DropDatabase()
				}

				return closer, remote.SetDriver(d)
			},
		},
	}
}

type PoolTestCase struct {
	Name       string
	SetPool    func(amboy.Queue, int) error
	SkipRemote bool
	SkipMulti  bool
	MinSize    int
	MaxSize    int
}

func DefaultPoolTestCases() []PoolTestCase {
	return []PoolTestCase{
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
			Name:    "RateLimitedSimple",
			MinSize: 4,
			MaxSize: 32,
			SetPool: func(q amboy.Queue, size int) error {
				runner, err := pool.NewSimpleRateLimitedWorkers(size, 10*time.Millisecond, q)
				if err != nil {
					return nil
				}

				return q.SetRunner(runner)
			},
		},
		{
			Name:       "RateLimitedAverage",
			MinSize:    4,
			MaxSize:    16,
			SkipMulti:  true,
			SkipRemote: true,
			SetPool: func(q amboy.Queue, size int) error {
				runner, err := pool.NewMovingAverageRateLimitedWorkers(size, size*100, 10*time.Millisecond, q)
				if err != nil {
					return nil
				}

				return q.SetRunner(runner)
			},
		},
	}
}

type SizeTestCase struct {
	Name string
	Size int
}

func DefaultSizeTestCases() []SizeTestCase {
	return []SizeTestCase{
		{Name: "One", Size: 1},
		{Name: "Two", Size: 2},
		{Name: "Four", Size: 4},
		{Name: "Eight", Size: 8},
		{Name: "Sixteen", Size: 16},
		{Name: "ThirtyTwo", Size: 32},
		{Name: "SixtyFour", Size: 64},
	}
}

type MultipleExecutionTestCase struct {
	Name            string
	Setup           func(context.Context, amboy.Queue, amboy.Queue) (TestCloser, error)
	MultipleDrivers bool
}

func DefaultMultipleExecututionTestCases(driver DriverTestCase) []MultipleExecutionTestCase {
	return []MultipleExecutionTestCase{
		{
			Name:            "TwoDrivers",
			MultipleDrivers: true,
			Setup: func(ctx context.Context, qOne, qTwo amboy.Queue) (TestCloser, error) {
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
			Setup: func(ctx context.Context, qOne, qTwo amboy.Queue) (TestCloser, error) {
				catcher := grip.NewBasicCatcher()
				driverID := newDriverID()
				dcloserOne, err := driver.SetDriver(ctx, qOne, driverID)
				catcher.Add(err)

				rq := qOne.(Remote)
				catcher.Add(qTwo.(Remote).SetDriver(rq.Driver()))

				return dcloserOne, catcher.Resolve()
			},
		},
	}

}
