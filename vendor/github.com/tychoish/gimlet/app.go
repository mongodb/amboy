// Package gimlet is a toolkit for building JSON/HTTP interfaces (e.g. REST).
//
// Gimlet builds on standard library and common tools for building web
// applciations (e.g. Negroni and gorilla,) and is only concerned with
// JSON/HTTP interfaces, and omits support for aspects of HTTP
// applications outside of the scope of JSON APIs (e.g. templating,
// sessions.) Gimilet attempts to provide minimal convinences on top
// of great infrastucture so that your application can omit
// boilerplate and you don't have to build potentially redundant
// infrastructure.
package gimlet

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/phyber/negroni-gzip/gzip"
	"github.com/tychoish/grip"
	"github.com/tylerb/graceful"
	"github.com/urfave/negroni"
)

// APIApp is a structure representing a single API service.
type APIApp struct {
	StrictSlash    bool
	isResolved     bool
	defaultVersion int
	port           int
	router         *mux.Router
	address        string
	routes         []*APIRoute
	middleware     []negroni.Handler
}

// NewApp returns a pointer to an application instance. These
// instances have reasonable defaults and include middleware to:
// recover from panics in handlers, log information about the request,
// and gzip compress all data. Users must specify a default version
// for new methods.
func NewApp() *APIApp {
	a := &APIApp{
		StrictSlash:    true,
		defaultVersion: -1, // this is the same as having no version prepended to the path.
		port:           3000,
	}

	a.AddMiddleware(negroni.NewRecovery())
	a.AddMiddleware(NewAppLogger())
	a.AddMiddleware(gzip.Gzip(gzip.DefaultCompression))

	return a
}

// SetDefaultVersion allows you to specify a default version for the
// application. Default versions must be 0 (no version,) or larger.
func (a *APIApp) SetDefaultVersion(version int) {
	if version < 0 {
		grip.Warningf("%d is not a valid version", version)
	} else {
		a.defaultVersion = version
		grip.Noticef("Set default api version to /v%d/", version)
	}
}

// Router is the getter for an APIApp's router object. If the
// application isn't resloved, then the error return value is non-nil.
func (a *APIApp) Router() (*mux.Router, error) {
	if a.isResolved {
		return a.router, nil
	}
	return nil, errors.New("application is not resolved")
}

// AddApp allows you to combine App instances, by taking one app and
// add its routes to the current app. Returns a non-nill error value
// if the current app is resolved. If the apps have different default
// versions set, the versions on the second app are explicitly set.
func (a *APIApp) AddApp(app *APIApp) error {
	// if we've already resolved then it has to be an error
	if a.isResolved {
		return errors.New("cannot merge an app into a resolved app")
	}

	// this is a weird case, so worth a warning, but not worth exiting
	if app.isResolved {
		grip.Warningln("merging a resolved app into an unresolved app may be an error.",
			"Continuing cautiously.")
	}
	// this is incredibly straightforward, just add the added routes to our routes list.
	if app.defaultVersion == a.defaultVersion {
		a.routes = append(a.routes, app.routes...)
		return nil
	}

	// This makes sure that instance default versions are
	// respected in routes when merging instances. This covers the
	// case where you assemble v1 and v2 of an api in different
	// places in your code and want to merge them in later.
	for _, route := range app.routes {
		if route.version == -1 {
			route.Version(app.defaultVersion)
		}
		a.routes = append(a.routes, route)
	}

	return nil
}

// AddMiddleware adds a negroni handler as middleware to the end of
// the current list of middleware handlers.
func (a *APIApp) AddMiddleware(m negroni.Handler) {
	a.middleware = append(a.middleware, m)
}

// Resolve processes the data in an application instance, including
// all routes and creats a mux.Router object for the application
// instance.
func (a *APIApp) Resolve() error {
	a.router = mux.NewRouter().StrictSlash(a.StrictSlash)

	var hasErrs bool
	for _, route := range a.routes {
		if !route.IsValid() {
			hasErrs = true
			grip.Errorf("%d is an invalid api version. not adding route for %s",
				route.version, route.route)
			continue
		}

		var methods []string
		for _, m := range route.methods {
			methods = append(methods, strings.ToLower(m.String()))
		}

		if route.version > 0 {
			versionedRoute := fmt.Sprintf("/v%d%s", route.version, route.route)
			a.router.HandleFunc(versionedRoute, route.handler).Methods(methods...)
			grip.Debugln("added route for:", versionedRoute)
		}

		if route.version == a.defaultVersion || route.version == 0 {
			a.router.HandleFunc(route.route, route.handler).Methods(methods...)
			grip.Debugln("added route for:", route.route)
		}
	}

	a.isResolved = true

	if hasErrs {
		return errors.New("encountered errors resolving routes")
	}

	return nil
}

// ResetMiddleware removes *all* middleware handlers from the current
// application.
func (a *APIApp) ResetMiddleware() {
	a.middleware = []negroni.Handler{}
}

// Run configured API service on the configured port. If you Registers
// middlewear for gziped responses and graceful shutdown with a 10
// second timeout.
func (a *APIApp) Run() error {
	var err error
	if !a.isResolved {
		err = a.Resolve()
	}

	n := negroni.New()
	for _, m := range a.middleware {
		n.Use(m)
	}

	n.UseHandler(a.router)

	listenOn := strings.Join([]string{a.address, strconv.Itoa(a.port)}, ":")
	grip.Noticeln("starting app on:", listenOn)

	graceful.Run(listenOn, 10*time.Second, n)
	return err
}

// SetPort allows users to configure a default port for the API
// service. Defaults to 3000, and return errors will refuse to set the
// port to something unreasonable.
func (a *APIApp) SetPort(port int) error {
	defaultPort := 3000

	if port == a.port {
		grip.Warningf("port is already set to %d", a.port)
	} else if port <= 0 {
		a.port = defaultPort
		return fmt.Errorf("%d is not a valid port numbaer, using %d", port, defaultPort)
	} else if port > 65535 {
		a.port = defaultPort
		return fmt.Errorf("port %d is too large, using default port (%d)", port, defaultPort)
	} else if port < 1024 {
		a.port = defaultPort
		return fmt.Errorf("port %d is too small, using default port (%d)", port, defaultPort)
	} else {
		a.port = port
	}

	return nil
}
