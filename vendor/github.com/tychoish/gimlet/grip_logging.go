package gimlet

import (
	"net/http"
	"time"

	"github.com/mongodb/grip"
	"github.com/mongodb/grip/logging"
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
	l := &AppLogging{&logging.Grip{grip.GetSender()}}

	return l
}

// Logs the request path, the beginning of every request as well as
// the duration upon completion and the status of the response.
func (l *AppLogging) ServeHTTP(rw http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	start := time.Now()
	id := getNumber()
	l.Infof("[id=%d] started %s %s", id, r.Method, r.URL.Path)

	next(rw, r)

	res := rw.(negroni.ResponseWriter)
	l.Infof("[id=%d] completed %v %s in %v", id, res.Status(),
		http.StatusText(res.Status()), time.Since(start))
}
