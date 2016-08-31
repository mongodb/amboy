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
	// generic MessageInfo.ShouldLog() function.
	Send(level.Priority, message.Composer)

	// Sets the logger's threshold level. Messages of lower
	// priority should be dropped.
	SetThresholdLevel(level.Priority) error
	// Retrieves the threshold level for the logger.
	ThresholdLevel() level.Priority

	// Sets the default level, which is used in conversion ofS
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

// LevelInfo provides a sender-independent structure for storing
// information about a sender's configured log levels.
type LevelInfo struct {
	defaultLevel   level.Priority
	thresholdLevel level.Priority
}

// NewLevelInfo builds a level info object based on the default and
// threshold levels specified.
func NewLevelInfo(d level.Priority, t level.Priority) LevelInfo {
	return LevelInfo{
		defaultLevel:   d,
		thresholdLevel: t,
	}
}

// MessageInfo provides a sender-independent method for determining if
// a message should be logged. Stores all of the information, and
// provides a ShouldLog method that senders can use if a message is logabble.
type MessageInfo struct {
	aboveThreshold bool
	loggable       bool
}

// ShouldLog returns true when the message, according to the
// information contained in the MessageInfo structure should be
// logged.
func (m MessageInfo) ShouldLog() bool {
	return m.loggable && m.aboveThreshold
}

// GetMessageInfo takes the sender's configured LevelInfo, a priority
// for the message, and a MessageComposer object and returns a
// MessageInfo.
func GetMessageInfo(info LevelInfo, level level.Priority, m message.Composer) MessageInfo {
	return MessageInfo{
		loggable:       m.Loggable(),
		aboveThreshold: level < info.thresholdLevel,
	}
}
