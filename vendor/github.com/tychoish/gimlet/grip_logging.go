package gimlet

import (
	"net/http"
	"time"

	"github.com/tychoish/grip"
	"github.com/urfave/negroni"
)

// AppLogging provides a Negroni-compatible middleware to send all
// logging using the grip packages logging. This defaults to using
// systemd logging, but gracefully falls back to use go standard
// library logging, with some additional helpers and configurations to
// support configurable level-based logging. This particular
// middlewear resembles the basic tracking provided by Negroni's
// standard logging system.
type AppLogging struct {
	grip.Journaler
}

// NewAppLogger creates an logging middlear instance suitable for use
// with Negroni. Sets the logging configuration to be the same as the
// default global grip logging object.
func NewAppLogger() *AppLogging {
	l := &AppLogging{grip.NewJournaler("gimlet")}

	// use the default sender for grip's standard logger.
	l.SetSender(grip.Sender())

	return l
}

// Logs the request path, the beginning of every request as well as
// the duration upon completion and the status of the response.
func (l *AppLogging) ServeHTTP(rw http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	start := time.Now()
	l.Infof("Started %s %s", r.Method, r.URL.Path)

	next(rw, r)

	res := rw.(negroni.ResponseWriter)
	l.Infof("Completed %v %s in %v", res.Status(), http.StatusText(res.Status()), time.Since(start))
}
