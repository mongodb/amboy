package message

import (
	"fmt"
	"strings"
)

type lineMessenger struct {
	Lines []interface{} `yaml:"lines" json:"lines" bson:"lines"`
}

// NewLinesMessage is a basic constructor for a type that, given a
// bunch of arguments, calls fmt.Sprintln() on the arguemnts passed to
// the constructor during the Resolve() operation. Use in combination
// with Compose[*] logging methods.
func NewLinesMessage(args ...interface{}) Composer {
	return &lineMessenger{
		Lines: args,
	}
}

func (l *lineMessenger) Loggable() bool {
	return len(l.Lines) > 0
}

func (l *lineMessenger) Resolve() string {
	return strings.Trim(fmt.Sprintln(l.Lines...), "\n")
}

func (l *lineMessenger) Raw() interface{} {
	return l
}
