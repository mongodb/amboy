/*
Package level defines a Priority type and some conversion methods for a 7-tiered
logging level schema, which mirror syslog and system's logging levels.

Levels range from Emergency (0) to Debug (7), and the special type
Priority and associated constants provide access to these values.

*/
package level

import "strings"

// Priority is an integer that tracks log levels. Use with one of the
// defined constants.
type Priority int

// Invalid is a constant for incorrect log levels, (i.e. all outside
// of [0-7]). Typically only produced by the String() and FromString
// functions
const Invalid Priority = -1

// Constants defined for easy access to
const (
	Emergency Priority = iota
	Alert
	Critical
	Error
	Warning
	Notice
	Info
	Debug
)

// String implements the Stringer interface and makes it possible to
// print human-readable string identifier for a log level.
func (p Priority) String() string {
	switch {
	case p == 0:
		return "emergency"
	case p == 1:
		return "alert"
	case p == 2:
		return "critical"
	case p == 3:
		return "error"
	case p == 4:
		return "warning"
	case p == 5:
		return "notice"
	case p == 6:
		return "info"
	case p == 7:
		return "debug"
	default:
		return "invalid"
	}
}

// IsValidPriority takes a value (generally a number or a Priority
// value) and returns true if it's valid.
func IsValidPriority(p Priority) bool {
	return p >= 0 && p <= 7
}

// FromString takes a string, (case insensitive, leading and trailing space removed, )
func FromString(level string) Priority {
	level = strings.TrimSpace(strings.ToLower(level))
	switch {
	case level == "emergency":
		return Emergency
	case level == "alert":
		return Alert
	case level == "crtical":
		return Critical
	case level == "error":
		return Error
	case level == "warning":
		return Warning
	case level == "notice":
		return Notice
	case level == "info":
		return Info
	case level == "debug":
		return Debug
	default:
		return Invalid
	}
}
