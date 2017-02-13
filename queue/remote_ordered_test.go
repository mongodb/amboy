package queue

import (
	"testing"
	"time"

	mgo "gopkg.in/mgo.v2"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/dependency"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/amboy/queue/driver"
	"github.com/mongodb/amboy/registry"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/suite"
	"github.com/tychoish/grip"
	"golang.org/x/net/context"
)

func init() {
	registry.AddDependencyType("mock", func() dependency.Manager { return dependency.NewMock() })
}

type SimpleRemoteOrderedSuite struct {
	queue             amboy.Queue
	tearDown          func() error
	driver            driver.Driver
	driverConstructor func() driver.Driver
	canceler          context.CancelFunc
	suite.Suite
}

func TestSimpleRemoteOrderedSuite(t *testing.T) {
	suite.Run(t, new(SimpleRemoteOrderedSuite))
}

func (s *SimpleRemoteOrderedSuite) SetupSuite() {
	name := "test-" + uuid.NewV4().String()
	uri := "mongodb://localhost"
	s.driverConstructor = func() driver.Driver {
		return driver.NewMongoDB(name, driver.DefaultMongoDBOptions())
	}

	s.tearDown = func() error {
		session, err := mgo.Dial(uri)
		defer session.Close()
		if err != nil {
			return err
		}

		err = session.DB("amboy").C(name + ".jobs").DropCollection()
		if err != nil {
			return err
		}

		err = session.DB("amboy").C(name + ".locks").DropCollection()
		if err != nil {
			return err
		}

		return nil
	}
}

func (s *SimpleRemoteOrderedSuite) SetupTest() {
	ctx, canceler := context.WithCancel(context.Background())
	s.driver = s.driverConstructor()
	s.canceler = canceler
	s.NoError(s.driver.Open(ctx))
	queue := NewSimpleRemoteOrdered(2)
	s.NoError(queue.SetDriver(s.driver))
	s.queue = queue
}

func (s *SimpleRemoteOrderedSuite) TearDownTest() {
	// this order is important, running teardown before canceling
	// the context to prevent closing the connection before
	// running the teardown procedure, given that some connection
	// resources may be shared in the driver.
	grip.CatchError(s.tearDown())
	s.canceler()
}

func (s *SimpleRemoteOrderedSuite) TestQueueSkipsCompletedJobs() {
	j := job.NewShellJob("echo hello", "")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	j.MarkComplete()
	s.True(j.Completed())

	s.NoError(s.queue.Start(ctx))
	s.NoError(s.queue.Put(j))

	amboy.WaitCtx(ctx, s.queue)

	stat := s.queue.Stats()

	s.Equal(stat.Total, 1)
	s.Equal(stat.Completed, 1)
}

func (s *SimpleRemoteOrderedSuite) TestQueueSkipsUnresolvedJobs() {
	j := job.NewShellJob("echo hello", "")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.False(j.Completed())
	mockDep := dependency.NewMock()
	mockDep.Response = dependency.Unresolved
	s.Equal(mockDep.State(), dependency.Unresolved)
	j.SetDependency(mockDep)

	s.NoError(s.queue.Start(ctx))
	s.NoError(s.queue.Put(j))

	amboy.WaitCtx(ctx, s.queue)

	stat := s.queue.Stats()

	s.Equal(stat.Total, 1)
	s.Equal(stat.Completed, 0)
}

func (s *SimpleRemoteOrderedSuite) TestQueueSkipsBlockedJobsWithNoEdges() {
	j := job.NewShellJob("echo hello", "")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.False(j.Completed())
	mockDep := dependency.NewMock()
	mockDep.Response = dependency.Blocked
	s.Equal(mockDep.State(), dependency.Blocked)
	s.Equal(len(mockDep.Edges()), 0)
	j.SetDependency(mockDep)

	s.NoError(s.queue.Start(ctx))
	s.NoError(s.queue.Put(j))

	amboy.WaitCtx(ctx, s.queue)

	stat := s.queue.Stats()

	s.Equal(stat.Total, 1)
	s.Equal(stat.Completed, 0)
}

func (s *SimpleRemoteOrderedSuite) TestQueueSkipsBlockedJobsWithManyEdges() {
	j := job.NewShellJob("echo hello", "")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.False(j.Completed())
	mockDep := dependency.NewMock()
	mockDep.Response = dependency.Blocked
	s.NoError(mockDep.AddEdge("foo"))
	s.NoError(mockDep.AddEdge("bar"))
	s.NoError(mockDep.AddEdge("bas"))
	s.Equal(mockDep.State(), dependency.Blocked)
	s.Equal(len(mockDep.Edges()), 3)
	j.SetDependency(mockDep)

	s.NoError(s.queue.Start(ctx))
	s.NoError(s.queue.Put(j))

	amboy.WaitCtx(ctx, s.queue)

	stat := s.queue.Stats()

	s.Equal(stat.Total, 1)
	s.Equal(stat.Completed, 0)
}

func (s *SimpleRemoteOrderedSuite) TestQueueSkipsBlockedJobsWithOneEdge() {
	j := job.NewShellJob("echo hello", "")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.False(j.Completed())
	mockDep := dependency.NewMock()
	mockDep.Response = dependency.Blocked
	s.NoError(mockDep.AddEdge("foo"))
	s.Equal(mockDep.State(), dependency.Blocked)
	s.Equal(len(mockDep.Edges()), 1)
	j.SetDependency(mockDep)

	s.NoError(s.queue.Start(ctx))
	s.NoError(s.queue.Put(j))

	amboy.WaitCtx(ctx, s.queue)

	stat := s.queue.Stats()

	s.Equal(stat.Total, 1)
	s.Equal(stat.Completed, 0)
}
