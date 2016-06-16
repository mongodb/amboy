package grip

import "github.com/tychoish/grip/send"

// SetSender swaps send.Sender() implementations in a logging
// instance. Calls the Close() method on the existing instance before
// changing the implementation for the current instance.
func SetSender(s send.Sender) {
	std.SetSender(s)
}

// Sender returns the current Journaler's sender instance. Use this in
// combination with SetSender to have multiple Journaler instances
// backed by the same send.Sender instance.
func Sender() send.Sender {
	return std.Sender()
}

// CloneSender, for the trivially constructable Sender
// implementations, makes a new instance of this type for the logging
// instance. For unsupported sender implementations, the method
// injects the sender itself into the Journaler instance.
func CloneSender(s send.Sender) {
	std.CloneSender(s)
}

// UseNativeLogger configures the standard grip package logger to use
// a native, standard output, logging instance, without changing the
// configuration of the Journaler.
func UseNativeLogger() error {
	return std.UseNativeLogger()
}

// UseSystemdLogger configures the standard grip package logger to use
// the systemd loggerwithout changing the configuration of the
// Journaler.
func UseSystemdLogger() error {
	return std.UseSystemdLogger()
}

// UseFileLogger configures the standard grip package logger to use a
// file-based logger that writes all log output to a file, based on
// the go standard library logging methods.
func UseFileLogger(filename string) error {
	return std.UseFileLogger(filename)
}
