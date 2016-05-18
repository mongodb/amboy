package dependency

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

// CreatesFileDependencySuite tests the dependency.Manager
// implementation that checks for the existence of a file. If the file
// exist the dependency becomes a noop.
type CreatesFileDependencySuite struct {
	dep      *CreatesFileDependency
	packages []string
	suite.Suite
}

func TestCreatesFileDependencySuite(t *testing.T) {
	suite.Run(t, new(CreatesFileDependencySuite))
}

func (s *CreatesFileDependencySuite) SetupSuite() {
	s.packages = []string{"job", "dependency", "queue", "pool", "build", "registry"}
}

func (s *CreatesFileDependencySuite) SetupTest() {
	s.dep = NewCreatesFileInstance()
}

func (s *CreatesFileDependencySuite) TestInstanceImplementsManagerInterface() {
	s.Implements((*Manager)(nil), s.dep)
}

func (s *CreatesFileDependencySuite) TestConstructorCreatesObjectWithFileNameSet() {
	for _, dir := range s.packages {
		dep := NewCreatesFileDependency(dir)
		s.Equal(dir, dep.FileName)
	}
}

func (s *CreatesFileDependencySuite) TestDependencyWithoutFileSetReportsReady() {
	s.Equal(s.dep.FileName, "")
	s.Equal(s.dep.State(), Ready)

	s.dep.FileName = " \\[  ]"
	s.Equal(s.dep.State(), Ready)

	s.dep.FileName = "foo"
	s.Equal(s.dep.State(), Ready)

	s.dep.FileName = " "
	s.Equal(s.dep.State(), Ready)

	s.dep.FileName = ""
	s.Equal(s.dep.State(), Ready)
}

func (s *CreatesFileDependencySuite) TestAmboyPackageDirectoriesExistAndReportPassedState() {
	for _, dir := range s.packages {
		dep := NewCreatesFileDependency("../" + dir)
		s.Equal(dep.State(), Passed, dir)
	}

}

func (s *CreatesFileDependencySuite) TestCreatesDependencyTestReportsExpectedType() {
	t := s.dep.Type()
	s.Equal(t.Name, "create-file")
	s.Equal(t.Version, 0)
}
