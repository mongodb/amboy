/*
Package logging provides the primary implementation of the Journaler
interface (which is cloned in public functions in the grip interface
itself.)

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
*/
package logging

import (
	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/message"
)

func (g *Grip) Default(msg interface{}) {
	g.sender.Send(g.sender.DefaultLevel(), message.ConvertToComposer(msg))
}
func (g *Grip) Defaultf(msg string, a ...interface{}) {
	g.sender.Send(g.sender.DefaultLevel(), message.NewFormatedMessage(msg, a...))
}
func (g *Grip) Defaultln(a ...interface{}) {
	g.sender.Send(g.sender.DefaultLevel(), message.NewLinesMessage(a...))
}

func (g *Grip) Emergency(msg interface{}) {
	g.sender.Send(level.Emergency, message.ConvertToComposer(msg))
}
func (g *Grip) Emergencyf(msg string, a ...interface{}) {
	g.sender.Send(level.Emergency, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) Emergencyln(a ...interface{}) {
	g.sender.Send(level.Emergency, message.NewLinesMessage(a...))
}
func (g *Grip) EmergencyPanic(msg interface{}) {
	g.sendPanic(level.Emergency, message.ConvertToComposer(msg))
}
func (g *Grip) EmergencyPanicf(msg string, a ...interface{}) {
	g.sendPanic(level.Emergency, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) EmergencyPanicln(a ...interface{}) {
	g.sendPanic(level.Emergency, message.NewLinesMessage(a...))
}
func (g *Grip) EmergencyFatal(msg interface{}) {
	g.sendFatal(level.Emergency, message.ConvertToComposer(msg))
}
func (g *Grip) EmergencyFatalf(msg string, a ...interface{}) {
	g.sendFatal(level.Emergency, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) EmergencyFatalln(a ...interface{}) {
	g.sendFatal(level.Emergency, message.NewLinesMessage(a...))
}

func (g *Grip) Alert(msg interface{}) {
	g.sender.Send(level.Alert, message.ConvertToComposer(msg))
}
func (g *Grip) Alertf(msg string, a ...interface{}) {
	g.sender.Send(level.Alert, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) Alertln(a ...interface{}) {
	g.sender.Send(level.Alert, message.NewLinesMessage(a...))
}
func (g *Grip) AlertPanic(msg interface{}) {
	g.sendFatal(level.Alert, message.ConvertToComposer(msg))
}
func (g *Grip) AlertPanicf(msg string, a ...interface{}) {
	g.sendPanic(level.Alert, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) AlertPanicln(a ...interface{}) {
	g.sendPanic(level.Alert, message.NewLinesMessage(a...))
}
func (g *Grip) AlertFatal(msg interface{}) {
	g.sendFatal(level.Alert, message.ConvertToComposer(msg))
}
func (g *Grip) AlertFatalf(msg string, a ...interface{}) {
	g.sendFatal(level.Alert, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) AlertFatalln(a ...interface{}) {
	g.sendFatal(level.Alert, message.NewLinesMessage(a...))
}

func (g *Grip) Critical(msg interface{}) {
	g.sender.Send(level.Critical, message.ConvertToComposer(msg))
}
func (g *Grip) Criticalf(msg string, a ...interface{}) {
	g.sender.Send(level.Critical, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) Criticalln(a ...interface{}) {
	g.sender.Send(level.Critical, message.NewLinesMessage(a...))
}
func (g *Grip) CriticalFatal(msg interface{}) {
	g.sendFatal(level.Critical, message.ConvertToComposer(msg))
}
func (g *Grip) CriticalFatalf(msg string, a ...interface{}) {
	g.sender.Send(level.Critical, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) CriticalFatalln(a ...interface{}) {
	g.sendFatal(level.Critical, message.NewLinesMessage(a...))
}
func (g *Grip) CriticalPanic(msg interface{}) {
	g.sendPanic(level.Critical, message.ConvertToComposer(msg))
}
func (g *Grip) CriticalPanicf(msg string, a ...interface{}) {
	g.sendPanic(level.Critical, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) CriticalPanicln(a ...interface{}) {
	g.sendPanic(level.Critical, message.NewLinesMessage(a...))
}

func (g *Grip) Error(msg interface{}) {
	g.sender.Send(level.Error, message.ConvertToComposer(msg))
}
func (g *Grip) Errorf(msg string, a ...interface{}) {
	g.sender.Send(level.Error, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) Errorln(a ...interface{}) {
	g.sender.Send(level.Error, message.NewLinesMessage(a...))
}
func (g *Grip) ErrorFatal(msg interface{}) {
	g.sendFatal(level.Error, message.ConvertToComposer(msg))
}
func (g *Grip) ErrorFatalf(msg string, a ...interface{}) {
	g.sendFatal(level.Error, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) ErrorFatalln(a ...interface{}) {
	g.sendFatal(level.Error, message.NewLinesMessage(a...))
}
func (g *Grip) ErrorPanic(msg interface{}) {
	g.sendFatal(level.Error, message.ConvertToComposer(msg))
}
func (g *Grip) ErrorPanicf(msg string, a ...interface{}) {
	g.sendPanic(level.Error, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) ErrorPanicln(a ...interface{}) {
	g.sendPanic(level.Error, message.NewLinesMessage(a...))
}

func (g *Grip) Warning(msg interface{}) {
	g.sender.Send(level.Warning, message.ConvertToComposer(msg))
}
func (g *Grip) Warningf(msg string, a ...interface{}) {
	g.sender.Send(level.Warning, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) Warningln(a ...interface{}) {
	g.sender.Send(level.Warning, message.NewLinesMessage(a...))
}

func (g *Grip) Notice(msg interface{}) {
	g.sender.Send(level.Notice, message.ConvertToComposer(msg))
}
func (g *Grip) Noticef(msg string, a ...interface{}) {
	g.sender.Send(level.Notice, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) Noticeln(a ...interface{}) {
	g.sender.Send(level.Notice, message.NewLinesMessage(a...))
}

func (g *Grip) Info(msg interface{}) {
	g.sender.Send(level.Info, message.ConvertToComposer(msg))
}
func (g *Grip) Infof(msg string, a ...interface{}) {
	g.sender.Send(level.Info, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) Infoln(a ...interface{}) {
	g.sender.Send(level.Info, message.NewLinesMessage(a...))
}

func (g *Grip) Debug(msg interface{}) {
	g.sender.Send(level.Debug, message.ConvertToComposer(msg))
}
func (g *Grip) Debugf(msg string, a ...interface{}) {
	g.sender.Send(level.Debug, message.NewFormatedMessage(msg, a...))
}
func (g *Grip) Debugln(a ...interface{}) {
	g.sender.Send(level.Debug, message.NewLinesMessage(a...))
}
