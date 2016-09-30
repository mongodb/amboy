package gimlet

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type GripLoggingSuite struct {
	require *require.Assertions
	suite.Suite
}

func TestGripLoggingSuite(t *testing.T) {
	suite.Run(t, new(GripLoggingSuite))
}

func (s *GripLoggingSuite) SetupSuite() {
	s.require = s.Require()
}

func (s *GripLoggingSuite) TestLoggableMethodIsTrueForAllNonNilValues() {
	for obj := range []interface{}{true, false, 1, 100, "the", map[string]string{"a": "one"}} {
		m := NewJSONMessage(obj)
		s.True(m.Loggable())
	}
}

func (s *GripLoggingSuite) TestLoggableMethodIsFalseForNilValues() {
	m := NewJSONMessage(nil)
	s.False(m.Loggable())
}

func (s *GripLoggingSuite) TestRawMethodGivesInternalData() {
	for obj := range []interface{}{true, false, 1, 100, "the", map[string]string{"a": "one"}} {
		m := NewJSONMessage(obj)
		s.Equal(m.Raw(), obj)
	}
}

func (s *GripLoggingSuite) TestMarshalPrettyHelperAlwaysProduceNewLineTerminatedOutput() {
	for obj := range []interface{}{true, false, 1, 100, "the", map[string]string{"a": "one"}} {
		m := NewJSONMessage(obj)
		out, err := m.MarshalPretty()
		s.NoError(err)
		s.True(bytes.HasSuffix(out, []byte("\n")))
	}
}

func (s *GripLoggingSuite) TestResolveMethodReturnsErrorWhenPassingNonMarshalableData() {
	m := NewJSONMessage(make(chan struct{}))
	out := m.Resolve()
	s.True(strings.HasPrefix(out, "problem marshaling message."))
}

func (s *GripLoggingSuite) TestResolveMethodReturnsJsonFormatedString() {
	m := NewJSONMessage(map[string]int{"a": 1})
	out := m.Resolve()
	s.Equal("{\"a\":1}", out)
}
