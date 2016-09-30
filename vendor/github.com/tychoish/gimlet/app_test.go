package gimlet

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/level"
)

// APIAppSuite contains tests of the APIApp system. Tests of the route
// methods are ostly handled in other suites.
type APIAppSuite struct {
	app *APIApp
	suite.Suite
}

func TestAPIAppSuite(t *testing.T) {
	suite.Run(t, new(APIAppSuite))
}

func (s *APIAppSuite) SetupTest() {
	s.app = NewApp()
	grip.SetThreshold(level.Info)
}

func (s *APIAppSuite) TestDefaultValuesAreSet() {
	s.Len(s.app.middleware, 3)
	s.Len(s.app.routes, 0)
	s.Equal(s.app.port, 3000)
	s.True(s.app.StrictSlash)
	s.False(s.app.isResolved)
	s.Equal(s.app.defaultVersion, -1)
}

func (s *APIAppSuite) TestRouterGetterReturnsErrorWhenUnresovled() {
	s.False(s.app.isResolved)

	_, err := s.app.Router()
	s.Error(err)
}

func (s *APIAppSuite) TestDefaultVersionSetter() {
	s.Equal(s.app.defaultVersion, -1)
	s.app.SetDefaultVersion(-2)
	s.Equal(s.app.defaultVersion, -1)

	s.app.SetDefaultVersion(0)
	s.Equal(s.app.defaultVersion, 0)

	s.app.SetDefaultVersion(1)
	s.Equal(s.app.defaultVersion, 1)

	for idx := range [100]int{} {
		s.app.SetDefaultVersion(idx)
		s.Equal(s.app.defaultVersion, idx)
	}
}

func (s *APIAppSuite) TestMiddleWearResetEmptiesList() {
	s.Len(s.app.middleware, 3)
	s.app.ResetMiddleware()
	s.Len(s.app.middleware, 0)
}

func (s *APIAppSuite) TestMiddleWearAdderAddsItemToList() {
	s.Len(s.app.middleware, 3)
	s.app.AddMiddleware(NewAppLogger())
	s.Len(s.app.middleware, 4)
}

func (s *APIAppSuite) TestPortSetterDoesNotAllowImpermisableValues() {
	s.Equal(s.app.port, 3000)

	for _, port := range []int{0, -1, -2000, 99999, 65536, 1000, 100, 1023} {
		err := s.app.SetPort(port)
		s.Equal(s.app.port, 3000)
		s.Error(err)
	}

	for _, port := range []int{1025, 65535, 50543, 8080, 8000} {
		err := s.app.SetPort(port)
		s.Equal(s.app.port, port)
		s.NoError(err)
	}
}

func (s *APIAppSuite) TestAddAppReturnsErrorIfOuterAppIsResolved() {
	newApp := NewApp()
	err := newApp.Resolve()
	s.NoError(err)
	s.True(newApp.isResolved)

	// if you attempt use AddApp on an app that is already
	// resolved, it returns an error.
	s.Error(newApp.AddApp(s.app))
}

func (s *APIAppSuite) TestAddAppReturnsNoErrorIfInnerAppIsResolved() {
	newApp := NewApp()
	err := s.app.Resolve()
	s.NoError(err)
	s.True(s.app.isResolved)

	s.NoError(newApp.AddApp(s.app))
}

func (s *APIAppSuite) TestRouteMergingInIfVersionsAreTheSame() {
	subApp := NewApp()
	s.Len(subApp.routes, 0)
	route := subApp.AddRoute("/foo")
	s.Len(subApp.routes, 1)

	s.Len(s.app.routes, 0)
	err := s.app.AddApp(subApp)
	s.NoError(err)

	s.Len(s.app.routes, 1)
	s.Equal(s.app.routes[0], route)
}

func (s *APIAppSuite) TestRouteMergingInWithDifferntVersions() {
	// If the you have two apps with different default versions,
	// routes in the sub-app that don't have a version set, should
	// get their version set to whatever the value of the sub
	// app's default value at the time of merging the apps.
	subApp := NewApp()
	subApp.SetDefaultVersion(2)
	s.NotEqual(s.app.defaultVersion, subApp.defaultVersion)

	// add a route to the first app
	s.Len(subApp.routes, 0)
	route := subApp.AddRoute("/foo").Version(3)
	s.Equal(route.version, 3)
	s.Len(subApp.routes, 1)

	// try adding to second app, to the first, with one route
	s.Len(s.app.routes, 0)
	err := s.app.AddApp(subApp)
	s.NoError(err)
	s.Len(s.app.routes, 1)
	s.Equal(s.app.routes[0], route)

	nextApp := NewApp()
	s.Len(nextApp.routes, 0)
	nextRoute := nextApp.AddRoute("/bar")
	s.Len(nextApp.routes, 1)
	s.Equal(nextRoute.version, -1)
	nextApp.SetDefaultVersion(3)
	s.Equal(nextRoute.version, -1)

	// make sure the default value of nextApp is on the route in the subApp
	err = s.app.AddApp(nextApp)
	s.NoError(err)
	s.Equal(s.app.routes[1], nextRoute)

	// this is the meaningful validation here.
	s.Equal(s.app.routes[1].version, 3)
}

func (s *APIAppSuite) TestRouterReturnsRouterInstanceWhenResolved() {
	s.False(s.app.isResolved)
	r, err := s.app.Router()
	s.Nil(r)
	s.Error(err)

	s.app.AddRoute("/foo").Version(1)
	s.NoError(s.app.Resolve())
	s.True(s.app.isResolved)

	r, err = s.app.Router()
	s.NotNil(r)
	s.NoError(err)
}

func (s *APIAppSuite) TestResolveEncountersErrorsWithAnInvalidRoot() {
	s.False(s.app.isResolved)

	s.app.AddRoute("/foo").Version(-10)
	s.Error(s.app.Resolve())

}

func (s *APIAppSuite) TestSetPortToExistingValueIsANoOp() {
	port := s.app.port

	s.Equal(port, s.app.port)
	s.NoError(s.app.SetPort(port))
	s.Equal(port, s.app.port)
}
