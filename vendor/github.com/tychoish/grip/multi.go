/*
Error Catcher

The MutiCatcher type makes it possible to collect from a group of
operations and then aggregate them as a single error.
*/
package grip

import (
	"errors"
	"strings"
)

// TODO: make a new catcher package, leave constructors in this
// package, use this Catcher interface, and write implementations that
// don't translate errors into string internally.
//
// type Catcher interface {
// 	Add(error)
// 	Len() int
// 	HasErrors() bool
// 	String() string
// 	Resolve() error
// }

// MultiCatcher provides an interface to collect and coalesse error
// messages within a function or other sequence of operations. Used to
// implement a kind of "continue on error"-style operations
type MultiCatcher struct {
	errs []string
}

// NewCatcher returns a Catcher instance that you can use to capture
// error messages and aggregate the errors.
func NewCatcher() *MultiCatcher {
	return &MultiCatcher{}
}

// Add takes an error object and, if it's non-nil, adds it to the
// internal collection of errors.
func (c *MultiCatcher) Add(err error) {
	if err != nil {
		c.errs = append(c.errs, err.Error())
	}
}

// Len returns the number of errors stored in the collector.
func (c *MultiCatcher) Len() int {
	return len(c.errs)
}

// HasErrors returns true if the collector has ingested errors, and
// false otherwise.
func (c *MultiCatcher) HasErrors() bool {
	return c.Len() > 0
}

// String implements the Stringer interface, and returns a comma
// separated string of the string representation of each error object
// in the collector.
func (c *MultiCatcher) String() string {
	return strings.Join(c.errs, ", ")
}

// Resolve returns a final error object for the Catcher. If there are
// no errors, it returns nil, and returns an error object with the
// string form of all error objects in the collector.
func (c *MultiCatcher) Resolve() (err error) {
	if c.Len() == 0 {
		err = nil
	} else if c.Len() == 1 {
		err = errors.New(c.errs[0])
	} else {
		err = errors.New(c.String())
	}

	c = NewCatcher()
	return err
}
