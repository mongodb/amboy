package message

import (
	"fmt"
	"strings"

	"github.com/tychoish/grip/level"
)

type lineMessenger struct {
	Lines []interface{} `yaml:"lines" json:"lines" bson:"`
	Base  `bson:"metadata" json:"metadata" yaml:"metadata"`
}

// NewLinesMessage is a basic constructor for a type that, given a
// bunch of arguments, calls fmt.Sprintln() on the arguemnts passed to
// the constructor during the Resolve() operation. Use in combination
// with Compose[*] logging methods.
func NewLinesMessage(p level.Priority, args ...interface{}) Composer {
	m := &lineMessenger{
		Lines: args,
	}
	m.SetPriority(p)
	return m
}

func NewLines(args ...interface{}) Composer {
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
	_ = l.Collect()
	return l
}
