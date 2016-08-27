package remote

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/net/context"
)

type LocalDriverSuite struct {
	driver  *LocalDriver
	require *require.Assertions
	suite.Suite
}

func TestLocalDriverSuite(t *testing.T) {
	suite.Run(t, new(LocalDriverSuite))
}

func (s *LocalDriverSuite) SetupSuite() {
	s.require = s.Require()
}

func (s *LocalDriverSuite) SetupTest() {
	s.driver = NewLocalDriver()
}

func (s *LocalDriverSuite) TestLocalDriverImplementsDriverInterface() {
	s.Implements((*Driver)(nil), s.driver)
}

func (s *LocalDriverSuite) TestLocalDriverInitialValues() {
	stats := s.driver.Stats()
	s.Equal(0, stats.Complete)
	s.Equal(0, stats.Locked)
	s.Equal(0, stats.Total)
	s.Equal(0, stats.Unlocked)

	s.Len(s.driver.jobs.m, 0)
	s.Len(s.driver.jobs.dispatched, 0)
	s.Len(s.driver.locks.m, 0)
}

func (s *LocalDriverSuite) TestOpenShouldReturnNil() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.NoError(s.driver.Open(ctx))
}

func (s *LocalDriverSuite) TestOpenShouldReturnNilOnSuccessiveCalls() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	before := s.driver.Stats()
	for i := 0; i < 200; i++ {
		s.NoError(s.driver.Open(ctx))
	}
	after := s.driver.Stats()

	s.Equal(before, after)
}

func (s *LocalDriverSuite) TestCloseShouldBeANoop() {
	before := s.driver.Stats()
	for i := 0; i < 200; i++ {
		s.driver.Close()
	}
	after := s.driver.Stats()

	s.Equal(before, after)
}
