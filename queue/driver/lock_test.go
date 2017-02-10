package driver

import (
	"testing"

	"gopkg.in/mgo.v2"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tychoish/grip"
	"golang.org/x/net/context"
)

// This suite of tests should test the interface of the JobLock
// without depending on the implementation details of the lock
// itself. All lock implementations should pass this suite.

type LockSuite struct {
	name            string
	lock            JobLock
	lockConstructor func() JobLock
	tearDown        func()
	require         *require.Assertions
	suite.Suite
}

// Each lock implementation should run this suite of tests.

func TestLocalLockSuite(t *testing.T) {
	s := new(LockSuite)

	s.name = "test"
	s.lockConstructor = func() JobLock {
		return NewInternalLock(uuid.NewV4().String())
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
	ctx, cancel := context.WithCancel(context.Background())
	collName := s.name + ".lock"
	dbName := "amboy"

	s.lockConstructor = func() JobLock {
		lock, err := NewMongoDBJobLock(ctx, uuid.NewV4().String(), dbName, collName, session)
		s.NoError(err)
		return lock
	}
	s.tearDown = func() {
		cancel()
		s.NoError(session.DB("amboy").C(collName).DropCollection())
		session.Close()
	}

	suite.Run(t, s)
}

// The implementation of the suite and its cases:

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
