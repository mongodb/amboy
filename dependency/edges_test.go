package dependency

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type EdgeHandlerSuite struct {
	edges *JobEdges
	suite.Suite
}

func TestEdgeHandlerSuite(t *testing.T) {
	suite.Run(t, new(EdgeHandlerSuite))
}

func (s *EdgeHandlerSuite) SetupTest() {
	s.edges = NewJobEdges()
}

func (s *EdgeHandlerSuite) TestAddEdgePersistsInternallyAsExpected() {
	s.Len(s.edges.Edges(), 0)

	name := "test-one"
	s.edges.AddEdge(name)
	edges := s.edges.Edges()
	s.Equal(edges[0], name)

	s.Equal(s.edges.TaskEdges[0], name)

	edge, ok := s.edges.edgesSet[name]
	s.True(ok)
	s.True(edge)
}

func (s *EdgeHandlerSuite) TestInternalEdgesSetIsMaintained() {
	s.Len(s.edges.Edges(), 0)

	name := "test-one"
	s.edges.AddEdge(name)
	s.Len(s.edges.edgesSet, 1)
	s.Len(s.edges.Edges(), 1)

	// replace the internal set tracker
	s.edges.edgesSet = make(map[string]bool)
	s.Len(s.edges.edgesSet, 0)

	// add another edge, potentially with the same edge, shouldn't
	// reduce a duplicate edge in the set or the list
	s.edges.AddEdge(name)
	s.Len(s.edges.edgesSet, 1)
	s.Len(s.edges.TaskEdges, 1)
	s.Len(s.edges.Edges(), 1)
}

func (s *EdgeHandlerSuite) TestTaskEdgeTracking() {
	// edge defaults to empty
	s.Len(s.edges.Edges(), 0)

	s.edges.AddEdge("foo")
	s.Len(s.edges.Edges(), 1)

	// make sure the internals look like we expect.
	s.Len(s.edges.edgesSet, 1)
	exists, ok := s.edges.edgesSet["foo"]
	s.True(exists)
	s.True(ok)
}
