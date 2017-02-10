package driver

import (
	"fmt"
	"testing"
	"time"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tychoish/grip"
	"golang.org/x/net/context"
	"gopkg.in/mgo.v2"
)

type MongoDBLockSuite struct {
	lock        *MongoDBJobLock
	require     *require.Assertions
	uri         string
	dbName      string
	session     *mgo.Session
	collections []*mgo.Collection
	suite.Suite
}

func TestMongoDBLockSuite(t *testing.T) {
	suite.Run(t, new(MongoDBLockSuite))
}

func (s *MongoDBLockSuite) SetupSuite() {
	s.require = s.Require()
	s.uri = "mongodb://localhost:27017"
	s.dbName = "amboy"
	ses, err := mgo.Dial(s.uri)
	s.require.NoError(err)
	s.session = ses
}

func (s *MongoDBLockSuite) SetupTest() {
	ctx := context.Background()

	name := uuid.NewV4().String()

	lock, err := NewMongoDBJobLock(ctx, name, s.dbName, name, s.session)
	s.NoError(err)
	s.lock = lock
}

func (s *MongoDBLockSuite) TearDownSuite() {
	session, err := mgo.Dial(s.uri)
	if !s.NoError(err) {
		defer session.Close()
	}

	for _, coll := range s.collections {
		grip.CatchWarning(coll.DropCollection())
	}

}

func (s *MongoDBLockSuite) TestIsLockedElsewhere() {
	ctx := context.Background()
	s.lock.Lock(ctx)

	s.Equal(machineName, s.lock.last.Host)
	s.NotEqual(s.lock.last.Host, "foo")
	s.NotEqual("foo", machineName)
	machineName = "foo"

	s.True(s.lock.IsLockedElsewhere(ctx), fmt.Sprintf("%+v", s.lock.last))
}

func (s *MongoDBLockSuite) TestIsNotLockedElsewhere() {
	ctx := context.Background()
	s.lock.Lock(ctx)

	s.Equal(machineName, s.lock.last.Host)
	s.False(s.lock.IsLockedElsewhere(ctx), fmt.Sprintf("%+v", s.lock.last))

	s.lock.Unlock(ctx)
	s.False(s.lock.IsLockedElsewhere(ctx), fmt.Sprintf("%+v", s.lock.last))
}

func (s *MongoDBLockSuite) TestGettingLockWithCanceledContext() {
	ctx, cancel := context.WithCancel(context.Background())

	s.lock.Lock(ctx)
	s.True(s.lock.IsLocked(ctx))

	s.lock.Unlock(ctx)
	s.False(s.lock.IsLocked(ctx))
	cancel()

	// if the context is canceled, or there are any errors
	// contacting, we assume it's locked (elsewhere.)
	s.True(s.lock.IsLocked(ctx))
	s.lock.Unlock(ctx)
	s.True(s.lock.IsLocked(ctx))
}

func (s *MongoDBLockSuite) TestCancelLockAquisitionProcess() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.False(s.lock.IsLocked(ctx))
	s.lock.Lock(ctx)
	s.True(s.lock.IsLocked(ctx))

	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	s.lock.Lock(ctx)
	s.True(s.lock.IsLocked(context.Background()))
}

func (s *MongoDBLockSuite) TestReleaseIfExperiedReturns() {
	s.lock.last.Locked = true
	s.lock.last.Time = time.Now().AddDate(-1, 0, 0)
	s.NoError(s.lock.releaseIfExpired(1 * time.Second))
}
