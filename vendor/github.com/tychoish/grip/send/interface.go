// Package send provides an interface for defining "senders" for
// different logging backends, as well as basic implementations for
// common logging approaches to use with the Grip logging
// interface. Backends currently include: syslog, systemd's journal,
// standard output, and file baased methods.
package send

import (
	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/message"
)

// The Sender interface describes how the Journaler type's method in
// primary "grip" package's methods interact with a logging output
// method. The Journaler type provides Sender() and SetSender()
// methods that allow client code to swap logging backend
// implementations dependency-injection style.
type Sender interface {
	// returns the name of the logging system. Typically this corresponds directly with
	Name() string
	SetName(string)

	// returns a constant for the type of the sender. Used by the
	// loggers as part of their dependency injection mechanism.
	Type() SenderType

	// Method that actually sends messages (the string) to the
	// logging capture system. The Send() method filters out
	// logged messages based on priority, typically using the
	// generic ShouldLogMessage() function.
	Send(level.Priority, message.Composer)

	// Sets the logger's threshold level. Messages of lower
	// priority should be dropped.
	SetThresholdLevel(level.Priority) error
	// Retrieves the threshold level for the logger.
	ThresholdLevel() level.Priority

	// Sets the default level, which is used in conversion of
	// logging types, and for "default" logging methods.
	SetDefaultLevel(level.Priority) error
	// Retreives the default level for the logger.
	DefaultLevel() level.Priority

	// Takes a key/value pair and stores the values in a mapping
	// structure in the Sender interface. Used, primarily, by the
	// systemd logger, but may be useful in the implementation of
	// other componentized loggers.
	AddOption(string, string)

	// If the logging sender holds any resources that require
	// desecration, they should be cleaned up tin the Close()
	// method. Close() is called by the SetSender() method before
	// changing loggers.
	Close()
}

// ShouldLogMessage provides a sender-independent method for
// determining if a message should be logged. Considers a priority
// value for a message, the default and threshould priorities from the
// sender, and the likability of a message provided by the Composer
// interface to determine if a message is loggable.
func ShouldLogMessage(s Sender, p level.Priority, m message.Composer) bool {
	// higher p numbers are "lower priority" than lower ones
	// (e.g. Emergency=0, Debug=7)
	if p > s.ThresholdLevel() {
		return false
	}
	if !m.Loggable() {
		return false
	}

	return true
}
