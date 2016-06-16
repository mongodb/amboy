/*
Basic Logging

Loging helpers exist for the following levels:

   Emergency + (fatal/panic)
   Alert + (fatal/panic)
   Critical + (fatal/panic)
   Error + (fatal/panic)
   Warning
   Notice
   Info
   Debug

These methods accept both strings (message content,) or types that
implement the message.MessageComposer interface. Composer types make
it possible to delay generating a message unless the logger is over
the logging threshold. Use this to avoid expensive serialization
operations for suppressed logging operations.

All levels also have additional methods with `ln` and `f` appended to
the end of the method name which allow Println() and Printf() style
functionality. You must pass printf/println-style arguments to these methods.
*/
package grip

// default methods for sending messages at the default level.

func Default(msg interface{}) {
	std.Default(msg)
}
func Defaultf(msg string, a ...interface{}) {
	std.Defaultf(msg, a...)
}
func Defaultln(a ...interface{}) {
	std.Defaultln(a...)
}

// Leveled Logging Methods
// Emergency-level logging methods

func EmergencyFatal(msg interface{}) {
	std.EmergencyFatal(msg)
}
func Emergency(msg interface{}) {
	std.Emergency(msg)
}
func Emergencyf(msg string, a ...interface{}) {
	std.Emergencyf(msg, a...)
}
func Emergencyln(a ...interface{}) {
	std.Emergencyln(a...)
}
func EmergencyPanic(msg interface{}) {
	std.EmergencyPanic(msg)
}
func EmergencyPanicf(msg string, a ...interface{}) {
	std.EmergencyPanicf(msg, a...)
}
func EmergencyPanicln(a ...interface{}) {
	std.EmergencyPanicln(a...)
}
func EmergencyFatalf(msg string, a ...interface{}) {
	std.EmergencyFatalf(msg, a...)
}
func EmergencyFatalln(a ...interface{}) {
	std.EmergencyFatalln(a...)
}

// Alert-level logging methods

func AlertFatal(msg interface{}) {
	std.AlertFatal(msg)
}
func Alert(msg interface{}) {
	std.Alert(msg)
}
func Alertf(msg string, a ...interface{}) {
	std.Alertf(msg, a...)
}
func Alertln(a ...interface{}) {
	std.Alertln(a...)
}
func AlertPanic(msg interface{}) {
	std.AlertPanic(msg)
}
func AlertPanicf(msg string, a ...interface{}) {
	std.AlertPanicf(msg, a...)
}
func AlertPanicln(a ...interface{}) {
	std.AlertPanicln(a...)
}
func AlertFatalf(msg string, a ...interface{}) {
	std.AlertFatalf(msg, a...)
}
func AlertFatalln(a ...interface{}) {
	std.AlertFatalln(a...)
}

// Critical-level logging methods

func Critical(msg interface{}) {
	std.Critical(msg)
}
func Criticalf(msg string, a ...interface{}) {
	std.Criticalf(msg, a...)
}
func Criticalln(a ...interface{}) {
	std.Criticalln(a...)
}
func CriticalFatal(msg interface{}) {
	std.CriticalFatal(msg)
}
func CriticalFatalf(msg string, a ...interface{}) {
	std.CriticalFatalf(msg, a...)
}
func CriticalFatalln(a ...interface{}) {
	std.CriticalFatalln(a...)
}
func CriticalPanic(msg interface{}) {
	std.CriticalPanic(msg)
}
func CriticalPanicf(msg string, a ...interface{}) {
	std.CriticalPanicf(msg, a...)
}
func CriticalPanicln(a ...interface{}) {
	std.CriticalPanicln(a...)
}

// Error-level logging methods

func Error(msg interface{}) {
	std.Error(msg)
}
func Errorf(msg string, a ...interface{}) {
	std.Errorf(msg, a...)
}
func Errorln(a ...interface{}) {
	std.Errorln(a...)
}
func ErrorPanic(msg interface{}) {
	std.ErrorPanic(msg)
}
func ErrorPanicf(msg string, a ...interface{}) {
	std.ErrorPanicf(msg, a...)
}
func ErrorPanicln(a ...interface{}) {
	std.ErrorPanicln(a...)
}
func ErrorFatal(msg interface{}) {
	std.ErrorFatal(msg)
}
func ErrorFatalf(msg string, a ...interface{}) {
	std.ErrorFatalf(msg, a...)
}
func ErrorFatalln(a ...interface{}) {
	std.ErrorPanicln(a...)
}

// Warning-level logging methods

func Warning(msg interface{}) {
	std.Warning(msg)
}
func Warningf(msg string, a ...interface{}) {
	std.Warningf(msg, a...)
}
func Warningln(a ...interface{}) {
	std.Warningln(a...)
}

// Notice-level logging methods

func Notice(msg interface{}) {
	std.Notice(msg)
}
func Noticef(msg string, a ...interface{}) {
	std.Noticef(msg, a...)
}
func Noticeln(a ...interface{}) {
	std.Noticeln(a...)
}

// Info-level logging methods

func Info(msg interface{}) {
	std.Info(msg)
}
func Infof(msg string, a ...interface{}) {
	std.Infof(msg, a...)
}
func Infoln(a ...interface{}) {
	std.Infoln(a...)
}

// Debug-level logging methods

func Debug(msg interface{}) {
	std.Debug(msg)
}
func Debugf(msg string, a ...interface{}) {
	std.Debugf(msg, a...)
}
func Debugln(a ...interface{}) {
	std.Debugln(a...)
}
