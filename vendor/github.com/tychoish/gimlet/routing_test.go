package gimlet

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RoutingSuite struct {
	app     *APIApp
	require *require.Assertions
	suite.Suite
}

func TestRoutingSuite(t *testing.T) {
	suite.Run(t, new(RoutingSuite))
}

func (s *RoutingSuite) SetupSuite() {
	s.require = s.Require()
}

func (s *RoutingSuite) SetupTest() {
	s.app = NewApp()
}

func (s *RoutingSuite) TestRouteConstructorAlwaysAddsPrefix() {
	for idx, n := range []string{"foo", "bar", "baz", "f", "e1"} {
		s.True(!strings.HasPrefix(n, "/"))
		r := s.app.AddRoute(n)
		s.Len(s.app.routes, idx+1)
		s.True(strings.HasPrefix(r.route, "/"), r.route)
	}
}

func (s *RoutingSuite) TestPutMethod() {
	r := s.app.AddRoute("/work").Put()
	s.Len(s.app.routes, 1)
	s.Len(r.methods, 1)
	s.Equal(r.methods[0], PUT)
	s.Equal(r.methods[0].String(), "PUT")
}

func (s *RoutingSuite) TestDeleteMethod() {
	r := s.app.AddRoute("/work").Delete()
	s.Len(s.app.routes, 1)
	s.Len(r.methods, 1)
	s.Equal(r.methods[0], DELETE)
	s.Equal(r.methods[0].String(), "DELETE")
}

func (s *RoutingSuite) TestGetMethod() {
	r := s.app.AddRoute("/work").Get()
	s.Len(s.app.routes, 1)
	s.Len(r.methods, 1)
	s.Equal(r.methods[0], GET)
	s.Equal(r.methods[0].String(), "GET")
}

func (s *RoutingSuite) TestPatchMethod() {
	r := s.app.AddRoute("/work").Patch()
	s.Len(s.app.routes, 1)
	s.Len(r.methods, 1)
	s.Equal(r.methods[0], PATCH)
	s.Equal(r.methods[0].String(), "PATCH")
}

func (s *RoutingSuite) TestPostMethod() {
	r := s.app.AddRoute("/work").Post()
	s.Len(s.app.routes, 1)
	s.Len(r.methods, 1)
	s.Equal(r.methods[0], POST)
	s.Equal(r.methods[0].String(), "POST")
}
