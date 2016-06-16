/*
Conditional Logging

The Conditional logging methods take two arguments, a Boolean, and a
message argument. Messages can be strings, objects that implement the
MessageComposer interface, or errors. If condition boolean is true,
the threshold level is met, and the message to log is not an empty
string, then it logs the resolved message.

Use conditional logging methods to potentially suppress log messages
based on situations orthogonal to log level, with "log sometimes" or
"log rarely" semantics. Combine with MessageComposers to to avoid
expensive message building operations.
*/
package grip

// Default-level Conditional Methods

func DefaultWhen(conditional bool, m interface{}) {
	std.DefaultWhen(conditional, m)
}
func DefaultWhenln(conditional bool, msg ...interface{}) {
	std.DefaultWhenln(conditional, msg...)
}
func DefaultWhenf(conditional bool, msg string, args ...interface{}) {
	std.DefaultWhenf(conditional, msg, args...)
}

// Emergency-level Conditional Methods

func EmergencyWhen(conditional bool, m interface{}) {
	std.EmergencyWhen(conditional, m)
}
func EmergencyWhenln(conditional bool, msg ...interface{}) {
	std.EmergencyWhenln(conditional, msg...)
}
func EmergencyWhenf(conditional bool, msg string, args ...interface{}) {
	std.EmergencyWhenf(conditional, msg, args...)
}
func EmergencyPanicWhen(conditional bool, msg interface{}) {
	std.EmergencyPanicWhen(conditional, msg)
}
func EmergencyPanicWhenln(conditional bool, msg ...interface{}) {
	std.EmergencyFatalWhenln(conditional, msg...)
}
func EmergencyPanicWhenf(conditional bool, msg string, args ...interface{}) {
	std.EmergencyPanicWhenf(conditional, msg, args...)
}
func EmergencyFatalWhen(conditional bool, msg interface{}) {
	std.EmergencyFatalWhen(conditional, msg)
}
func EmergencyFatalWhenln(conditional bool, msg ...interface{}) {
	std.EmergencyFatalWhenln(conditional, msg...)
}
func EmergencyFatalWhenf(conditional bool, msg string, args ...interface{}) {
	std.EmergencyFatalWhenf(conditional, msg, args...)
}

// Alert-Level Conditional Methods

func AlertWhen(conditional bool, m interface{}) {
	std.AlertWhen(conditional, m)
}
func AlertWhenln(conditional bool, msg ...interface{}) {
	std.AlertWhenln(conditional, msg...)
}
func AlertWhenf(conditional bool, msg string, args ...interface{}) {
	std.AlertWhenf(conditional, msg, args...)
}
func AlertPanicWhen(conditional bool, msg interface{}) {
	std.AlertPanicWhen(conditional, msg)
}
func AlertPanicWhenln(conditional bool, msg ...interface{}) {
	std.AlertPanicWhenln(conditional, msg...)
}
func AlertPanicWhenf(conditional bool, msg string, args ...interface{}) {
	std.AlertPanicWhenf(conditional, msg, args...)
}
func AlertFatalWhen(conditional bool, msg interface{}) {
	std.AlertFatalWhen(conditional, msg)
}
func AlertFatalWhenln(conditional bool, msg ...interface{}) {
	std.AlertFatalWhenln(conditional, msg...)
}
func AlertFatalWhenf(conditional bool, msg string, args ...interface{}) {
	std.AlertFatalWhenf(conditional, msg, args)
}

// Critical-level Conditional Methods

func CriticalWhen(conditional bool, m interface{}) {
	std.CriticalWhen(conditional, m)
}
func CriticalWhenln(conditional bool, msg ...interface{}) {
	std.CriticalWhenln(conditional, msg...)
}
func CriticalWhenf(conditional bool, msg string, args ...interface{}) {
	std.CriticalWhenf(conditional, msg, args...)
}
func CriticalPanicWhen(conditional bool, msg interface{}) {
	std.CriticalPanicWhen(conditional, msg)
}
func CriticalPanicWhenln(conditional bool, msg ...interface{}) {
	std.CriticalPanicWhenln(conditional, msg...)
}
func CriticalPanicWhenf(conditional bool, msg string, args ...interface{}) {
	std.CriticalPanicWhenf(conditional, msg, args...)
}
func CriticalFatalWhen(conditional bool, msg interface{}) {
	std.CriticalFatalWhen(conditional, msg)
}
func CriticalFatalWhenln(conditional bool, msg ...interface{}) {
	std.CriticalFatalWhenln(conditional, msg...)
}
func CriticalFatalWhenf(conditional bool, msg string, args ...interface{}) {
	std.CriticalFatalWhenf(conditional, msg, args...)
}

// Error-level Conditional Methods

func ErrorWhen(conditional bool, m interface{}) {
	std.ErrorWhen(conditional, m)
}
func ErrorWhenln(conditional bool, msg ...interface{}) {
	std.ErrorWhenln(conditional, msg...)
}
func ErrorWhenf(conditional bool, msg string, args ...interface{}) {
	std.ErrorWhenf(conditional, msg, args...)
}
func ErrorPanicWhen(conditional bool, msg interface{}) {
	std.ErrorPanicWhen(conditional, msg)
}
func ErrorPanicWhenln(conditional bool, msg ...interface{}) {
	std.ErrorPanicWhenln(conditional, msg...)
}
func ErrorPanicWhenf(conditional bool, msg string, args ...interface{}) {
	std.ErrorPanicWhenf(conditional, msg, args...)
}
func ErrorFatalWhen(conditional bool, msg interface{}) {
	std.ErrorFatalWhen(conditional, msg)
}
func ErrorFatalWhenln(conditional bool, msg ...interface{}) {
	std.ErrorFatalWhenln(conditional, msg...)
}
func ErrorFatalWhenf(conditional bool, msg string, args ...interface{}) {
	std.ErrorFatalWhenf(conditional, msg, args...)
}

// Warning-level Conditional Methods

func WarningWhen(conditional bool, m interface{}) {
	std.WarningWhen(conditional, m)
}
func WarningWhenln(conditional bool, msg ...interface{}) {
	std.WarningWhenln(conditional, msg...)
}
func WarningWhenf(conditional bool, msg string, args ...interface{}) {
	std.WarningWhenf(conditional, msg, args...)
}

// Notice-level Conditional Methods

func NoticeWhen(conditional bool, m interface{}) {
	std.NoticeWhen(conditional, m)
}
func NoticeWhenln(conditional bool, msg ...interface{}) {
	std.NoticeWhenln(conditional, msg...)
}
func NoticeWhenf(conditional bool, msg string, args ...interface{}) {
	std.NoticeWhenf(conditional, msg, args...)
}

// Info-level Conditional Methods

func InfoWhen(conditional bool, message interface{}) {
	std.InfoWhen(conditional, message)
}
func InfoWhenln(conditional bool, msg ...interface{}) {
	std.InfoWhenln(conditional, msg...)
}
func InfoWhenf(conditional bool, msg string, args ...interface{}) {
	std.InfoWhenf(conditional, msg, args...)
}

// Debug-level conditional Methods

func DebugWhen(conditional bool, m interface{}) {
	std.DebugWhen(conditional, m)
}
func DebugWhenln(conditional bool, msg ...interface{}) {
	std.DebugWhenln(conditional, msg...)
}
func DebugWhenf(conditional bool, msg string, args ...interface{}) {
	std.DebugWhenf(conditional, msg, args...)
}
