package message

import (
	"fmt"
	"strings"

	"github.com/tychoish/grip/level"
)

type errorWrapMessage struct {
	base     string
	args     []interface{}
	err      error
	Message  string `bson:"message,omitempty" json:"message,omitempty" yaml:"message,omitempty"`
	Extended string `bson:"extended,omitempty" json:"extended,omitempty" yaml:"extended,omitempty"`
	Base     `bson:"metadata" json:"metadata" yaml:"metadata"`
}

func NewErrorWrapMessage(p level.Priority, err error, base string, args ...interface{}) Composer {
	m := &errorWrapMessage{
		base: base,
		args: args,
		err:  err,
	}
	m.SetPriority(p)

	return m
}

func NewErrorWrapDefault(err error, base string, args ...interface{}) Composer {
	return &errorWrapMessage{
		base: base,
		args: args,
		err:  err,
	}
}

func (m *errorWrapMessage) Resolve() string {
	if m.Message != "" {
		return m.Message
	}

	lines := []string{}

	if m.base != "" {
		lines = append(lines, fmt.Sprintf(m.base, m.args...))
	}

	if m.err != nil {
		lines = append(lines, fmt.Sprintf("%+v", m.err))
	}

	if len(lines) > 0 {
		m.Message = strings.Join(lines, "\n")
	} else {
		m.Message = "<none>"

	}

	return m.Message
}
func (m *errorWrapMessage) Raw() interface{} {
	_ = m.Resolve()
	_ = m.Collect()

	return m
}

func (m *errorWrapMessage) Loggable() bool {
	if m.err == nil {
		return false
	}

	return true
}
