// Catch Logging
//
// Logging helpers for catching and logging error messages. Helpers exist
// for the following levels, with helpers defined both globally for the
// global logger and for Journaler logging objects.
package grip

// Emergency + (fatal/panic)
// Alert + (fatal/panic)
// Critical + (fatal/panic)
// Error + (fatal/panic)
// Warning
// Notice
// Info
// Debug

func CatchDefault(err error) {
	std.CatchDefault(err)
}

// Level Emergency Catcher Logging Helpers

func CatchEmergency(err error) {
	std.CatchEmergency(err)
}
func CatchEmergencyPanic(err error) {
	std.CatchEmergency(err)
}
func CatchEmergencyFatal(err error) {
	std.CatchEmergencyFatal(err)
}

// Level Alert Catcher Logging Helpers

func CatchAlert(err error) {
	std.CatchAlert(err)
}
func CatchAlertPanic(err error) {
	std.CatchAlertPanic(err)
}
func CatchAlertFatal(err error) {
	std.CatchAlertFatal(err)
}

// Level Critical Catcher Logging Helpers

func CatchCritical(err error) {
	std.CatchCritical(err)
}
func CatchCriticalPanic(err error) {
	std.CatchCriticalPanic(err)
}
func CatchCriticalFatal(err error) {
	std.CatchCriticalFatal(err)
}

// Level Error Catcher Logging Helpers

func CatchError(err error) {
	std.CatchError(err)
}
func CatchErrorPanic(err error) {
	std.CatchErrorPanic(err)
}
func CatchErrorFatal(err error) {
	std.CatchErrorFatal(err)
}

// Level Warning Catcher Logging Helpers

func CatchWarning(err error) {
	std.CatchWarning(err)
}

// Level Notice Catcher Logging Helpers

func CatchNotice(err error) {
	std.CatchNotice(err)
}

// Level Info Catcher Logging Helpers

func CatchInfo(err error) {
	std.CatchInfo(err)
}

// Level Debug Catcher Logging Helpers

func CatchDebug(err error) {
	std.CatchDebug(err)
}
