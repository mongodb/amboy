package slogger

import (
	"fmt"
	"strings"

	"github.com/tychoish/grip/message"
)

type StackError struct {
	message.Composer
	Stacktrace []string
	message    string
}

func NewStackError(messageFmt string, args ...interface{}) *StackError {
	return &StackError{
		Composer:   message.NewFormatted(messageFmt, args...),
		Stacktrace: stacktrace(),
	}
}

func (s *StackError) Raw() interface{} {
	return &struct {
		Message    string      `bson:"message" json:"message" yaml:"message"`
		Stacktrace []string    `bson:"stacktrace" json:"stacktrace" yaml:"stacktrace"`
		Metadata   interface{} `bson:"metadata" json:"metadata" yaml:"metadata"`
	}{
		Message:    s.Composer.Resolve(),
		Stacktrace: s.Stacktrace,
		Metadata:   s.Composer.Raw(),
	}
}

func (s *StackError) Error() string { return s.Resolve() }
func (s *StackError) Resolve() string {
	if !s.Composer.Loggable() {
		return ""
	}

	if s.message != "" {
		return s.message
	}

	s.message = fmt.Sprintf("%s\n\t%s",
		s.Composer.Resolve(),
		strings.Join(s.Stacktrace, "\n\t"))

	return s.message
}
