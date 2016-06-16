package logging

import "github.com/tychoish/grip/send"

// SetSender swaps send.Sender() implementations in a logging
// instance. Calls the Close() method on the existing instance before
// changing the implementation for the current instance.
func (g *Grip) SetSender(s send.Sender) {
	g.sender.Close()
	g.sender = s
}

// Sender returns the current Journaler's sender instance. Use this in
// combination with SetSender() to have multiple Journaler instances
// backed by the same send.Sender instance.
func (g *Grip) Sender() send.Sender {
	return g.sender
}

// CloneSender, for the trivially constructable Sender
// implementations, makes a new instance of this type for the logging
// instance. For unsupported sender implementations, just injects the
// sender itself into the Grip instance.
func (g *Grip) CloneSender(s send.Sender) {
	switch {
	case s.Type() == send.Native:
		g.UseNativeLogger()
	case s.Type() == send.Systemd:
		g.UseSystemdLogger()
	default:
		g.SetSender(s)
	}
}

// UseNativeLogger sets the Journaler to use a native, standard
// output, logging instance, without changing the configuration of the
// Journaler.
func (g *Grip) UseNativeLogger() error {
	// name, threshold, default
	sender, err := send.NewNativeLogger(g.name, g.sender.ThresholdLevel(), g.sender.DefaultLevel())
	g.SetSender(sender)
	return err
}

// UseSystemdLogger set the Journaler to use the systemd loggerwithout
// changing the configuration of the Journaler.
func (g *Grip) UseSystemdLogger() error {
	// name, threshold, default
	sender, err := send.NewJournaldLogger(g.name, g.sender.ThresholdLevel(), g.sender.DefaultLevel())
	if err != nil {
		if g.Sender().Name() == "bootstrap" {
			g.SetSender(sender)
		}
		return err
	}
	g.SetSender(sender)
	return nil
}

// UseFileLogger creates a file-based logger that writes all log
// output to a file, based on the standard library logging methods.
func (g *Grip) UseFileLogger(filename string) error {
	s, err := send.NewFileLogger(g.name, filename, g.sender.ThresholdLevel(), g.sender.DefaultLevel())
	if err != nil {
		if g.Sender().Name() == "bootstrap" {
			g.SetSender(s)
		}
		return err
	}
	g.SetSender(s)
	return nil
}
