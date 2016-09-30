package gimlet

import (
	"net/http"
	"strings"

	"github.com/tychoish/grip"
)

// APIRoute is a object that represents each route in the application
// and includes the route and associate internal metadata for the
// route.
type APIRoute struct {
	route   string
	methods []httpMethod
	handler http.HandlerFunc
	version int
}

// AddRoute is the primary method for creating and registering a new route with an
// application. Use as the root of a method chain, passing this method
// the path of the route.
func (a *APIApp) AddRoute(r string) *APIRoute {
	route := &APIRoute{route: r, version: -1}

	// data validation and cleanup
	if !strings.HasPrefix(route.route, "/") {
		route.route = "/" + route.route
	}

	a.routes = append(a.routes, route)

	return route
}

// IsValid checks if a route has is valid. Current implementation only
// makes sure that the version of the route is method.
func (r *APIRoute) IsValid() bool {
	return r.version >= 0
}

// Version allows you to specify an integer for the version of this
// route. Version is chainable.
func (r *APIRoute) Version(version int) *APIRoute {
	if version < 0 {
		grip.Warningf("%d is not a valid version", version)
	} else {
		r.version = version
	}
	return r
}

// Handler makes it possible to register an http.HandlerFunc with a
// route. Chainable. The common pattern for implementing these
// functions is to write functions and methods in your application
// that *return* handler fucntions, so you can pass application state
// or other data into to the handlers when the applications start,
// without relying on either global state *or* running into complex
// typing issues.
func (r *APIRoute) Handler(h http.HandlerFunc) *APIRoute {
	r.handler = h

	return r
}

// Get is a chainable method to add a handler for the GET method to
// the current route. Routes may specify multiple methods.
func (r *APIRoute) Get() *APIRoute {
	r.methods = append(r.methods, GET)
	return r
}

// Put is a chainable method to add a handler for the PUT method to
// the current route. Routes may specify multiple methods.
func (r *APIRoute) Put() *APIRoute {
	r.methods = append(r.methods, PUT)
	return r
}

// Post is a chainable method to add a handler for the POST method to
// the current route. Routes may specify multiple methods.
func (r *APIRoute) Post() *APIRoute {
	r.methods = append(r.methods, POST)
	return r
}

// Delete is a chainable method to add a handler for the DELETE method
// to the current route. Routes may specify multiple methods.
func (r *APIRoute) Delete() *APIRoute {
	r.methods = append(r.methods, DELETE)
	return r
}

// Patch is a chainable method to add a handler for the PATCH method
// to the current route. Routes may specify multiple methods.
func (r *APIRoute) Patch() *APIRoute {
	r.methods = append(r.methods, PATCH)
	return r
}
