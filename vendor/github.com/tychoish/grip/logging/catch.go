package logging

// Catch Logging
//
// Logging helpers for catching and logging error messages. Helpers exist
// for the following levels, with helpers defined both globally for the
// global logger and for Journaler logging objects.
import (
	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/message"
)

func (g *Grip) CatchDefault(err error) {
	g.sender.Send(g.DefaultLevel(), message.NewErrorMessage(err))
}

func (g *Grip) CatchEmergency(err error) {
	g.sender.Send(level.Emergency, message.NewErrorMessage(err))
}
func (g *Grip) CatchEmergencyPanic(err error) {
	g.sendPanic(level.Emergency, message.NewErrorMessage(err))
}
func (g *Grip) CatchEmergencyFatal(err error) {
	g.sendFatal(level.Emergency, message.NewErrorMessage(err))
}

func (g *Grip) CatchAlert(err error) {
	g.sender.Send(level.Alert, message.NewErrorMessage(err))
}
func (g *Grip) CatchAlertPanic(err error) {
	g.sendPanic(level.Alert, message.NewErrorMessage(err))
}
func (g *Grip) CatchAlertFatal(err error) {
	g.sendFatal(level.Alert, message.NewErrorMessage(err))
}

func (g *Grip) CatchCritical(err error) {
	g.sender.Send(level.Critical, message.NewErrorMessage(err))
}
func (g *Grip) CatchCriticalPanic(err error) {
	g.sendPanic(level.Critical, message.NewErrorMessage(err))
}
func (g *Grip) CatchCriticalFatal(err error) {
	g.sendFatal(level.Critical, message.NewErrorMessage(err))
}

func (g *Grip) CatchError(err error) {
	g.sender.Send(level.Error, message.NewErrorMessage(err))
}
func (g *Grip) CatchErrorPanic(err error) {
	g.sendPanic(level.Error, message.NewErrorMessage(err))
}
func (g *Grip) CatchErrorFatal(err error) {
	g.sendFatal(level.Error, message.NewErrorMessage(err))
}

func (g *Grip) CatchWarning(err error) {
	g.sender.Send(level.Warning, message.NewErrorMessage(err))
}

func (g *Grip) CatchNotice(err error) {
	g.sender.Send(level.Notice, message.NewErrorMessage(err))
}

func (g *Grip) CatchInfo(err error) {
	g.sender.Send(level.Info, message.NewErrorMessage(err))
}

func (g *Grip) CatchDebug(err error) {
	g.sender.Send(level.Debug, message.NewErrorMessage(err))
}
