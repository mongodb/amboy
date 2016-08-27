package remote

import (
	"testing"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tychoish/grip"
	"golang.org/x/net/context"
	"gopkg.in/mgo.v2"
)

type LockSuite struct {
	name            string
	lock            RemoteJobLock
	lockConstructor func() RemoteJobLock
	tearDown        func()
	require         *require.Assertions
	suite.Suite
}

// This suite of tests should test the interface of the RemoteJobLock
// without depending on the implementation details of the lock
// itself. All lock implementations should pass this suite.

func TestLocalLockSuite(t *testing.T) {
	s := new(LockSuite)
	s.lockConstructor = func() RemoteJobLock {
		return NewLocalJobLock(uuid.NewV4().String())
	}

	suite.Run(t, s)
}

func TestMongoDBLockInterfaceSuite(t *testing.T) {
	s := new(LockSuite)
	s.name = uuid.NewV4().String()
	session, err := mgo.Dial("mongodb://localhost:27017")
	if err != nil {
		grip.Alert("failed to connect to local mongodb")
		t.FailNow()
	}
	coll := session.DB("amboy").C(s.name + ".lock")
	ctx, cancel := context.WithCancel(context.Background())
	s.lockConstructor = func() RemoteJobLock {
		lock, err := NewMongoDBJobLock(ctx, uuid.NewV4().String(), coll)
		s.NoError(err)
		return lock
	}
	s.tearDown = func() {
		cancel()
		s.NoError(coll.DropCollection())
		session.Close()
	}

	suite.Run(t, s)
}

func (s *LockSuite) SetupSuite() {
	s.require = s.Require()
}

func (s *LockSuite) TearDownSuite() {
	if s.tearDown != nil {
		s.tearDown()
	}
}

func (s *LockSuite) SetupTest() {
	s.lock = s.lockConstructor()
}

func (s *LockSuite) TestInitalValues() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.False(s.lock.IsLocked(ctx))
	s.False(s.lock.IsLockedElsewhere(ctx))
}

func (s *LockSuite) TestLockMethodAffectsIsLockedValue() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.False(s.lock.IsLocked(ctx))
	s.lock.Lock(ctx)

	s.True(s.lock.IsLocked(ctx))
	s.False(s.lock.IsLockedElsewhere(ctx))
}

func (s *LockSuite) TestUnlockMethodAffectsIsLockedValue() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.lock.Lock(ctx)

	s.True(s.lock.IsLocked(ctx))
	s.lock.Unlock(ctx)
	s.False(s.lock.IsLocked(ctx))
}
