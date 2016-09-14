package logging

import (
	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/message"
)

// Internal helpers to manage sending interaction

func (g *Grip) conditionalSend(priority level.Priority, conditional bool, m interface{}) {
	if !conditional {
		return
	}

	g.sender.Send(priority, message.ConvertToComposer(m))
	return
}

func (g *Grip) conditionalSendPanic(priority level.Priority, conditional bool, m interface{}) {
	if !conditional {
		return
	}

	g.sendPanic(priority, message.ConvertToComposer(m))
}

func (g *Grip) conditionalSendFatal(priority level.Priority, conditional bool, m interface{}) {
	if !conditional {
		return
	}

	g.sendFatal(priority, message.ConvertToComposer(m))
}

/////////////

func (g *Grip) DefaultWhen(conditional bool, m interface{}) {
	g.conditionalSend(g.sender.DefaultLevel(), conditional, m)
}
func (g *Grip) DefaultWhenln(conditional bool, msg ...interface{}) {
	g.DefaultWhen(conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) DefaultWhenf(conditional bool, msg string, args ...interface{}) {
	g.DefaultWhen(conditional, message.NewFormatedMessage(msg, args...))
}

/////////////

func (g *Grip) EmergencyWhen(conditional bool, m interface{}) {
	g.conditionalSend(level.Emergency, conditional, m)
}
func (g *Grip) EmergencyWhenln(conditional bool, msg ...interface{}) {
	g.EmergencyWhen(conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) EmergencyWhenf(conditional bool, msg string, args ...interface{}) {
	g.EmergencyWhen(conditional, message.NewFormatedMessage(msg, args...))
}
func (g *Grip) EmergencyPanicWhen(conditional bool, msg interface{}) {
	g.conditionalSendPanic(level.Emergency, conditional, msg)
}
func (g *Grip) EmergencyPanicWhenln(conditional bool, msg ...interface{}) {
	g.conditionalSendPanic(level.Emergency, conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) EmergencyPanicWhenf(conditional bool, msg string, args ...interface{}) {
	g.conditionalSendPanic(level.Emergency, conditional, message.NewFormatedMessage(msg, args...))
}
func (g *Grip) EmergencyFatalWhen(conditional bool, msg interface{}) {
	g.conditionalSendFatal(level.Emergency, conditional, msg)
}
func (g *Grip) EmergencyFatalWhenln(conditional bool, msg ...interface{}) {
	g.conditionalSendFatal(level.Emergency, conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) EmergencyFatalWhenf(conditional bool, msg string, args ...interface{}) {
	g.conditionalSendFatal(level.Emergency, conditional, message.NewFormatedMessage(msg, args...))
}

/////////////

func (g *Grip) AlertWhen(conditional bool, m interface{}) {
	g.conditionalSend(level.Alert, conditional, m)
}
func (g *Grip) AlertWhenln(conditional bool, msg ...interface{}) {
	g.AlertWhen(conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) AlertWhenf(conditional bool, msg string, args ...interface{}) {
	g.AlertWhen(conditional, message.NewFormatedMessage(msg, args...))
}
func (g *Grip) AlertPanicWhen(conditional bool, msg interface{}) {
	g.conditionalSendPanic(level.Alert, conditional, msg)
}
func (g *Grip) AlertPanicWhenln(conditional bool, msg ...interface{}) {
	g.conditionalSendPanic(level.Alert, conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) AlertPanicWhenf(conditional bool, msg string, args ...interface{}) {
	g.conditionalSendPanic(level.Alert, conditional, message.NewFormatedMessage(msg, args...))
}
func (g *Grip) AlertFatalWhen(conditional bool, msg interface{}) {
	g.conditionalSendFatal(level.Alert, conditional, msg)
}
func (g *Grip) AlertFatalWhenln(conditional bool, msg ...interface{}) {
	g.conditionalSendFatal(level.Alert, conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) AlertFatalWhenf(conditional bool, msg string, args ...interface{}) {
	g.conditionalSendFatal(level.Alert, conditional, message.NewFormatedMessage(msg, args...))
}

/////////////

func (g *Grip) CriticalWhen(conditional bool, m interface{}) {
	g.conditionalSend(level.Critical, conditional, m)
}
func (g *Grip) CriticalWhenln(conditional bool, msg ...interface{}) {
	g.CriticalWhen(conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) CriticalWhenf(conditional bool, msg string, args ...interface{}) {
	g.CriticalWhen(conditional, message.NewFormatedMessage(msg, args...))
}
func (g *Grip) CriticalPanicWhen(conditional bool, msg interface{}) {
	g.conditionalSendPanic(level.Critical, conditional, msg)
}
func (g *Grip) CriticalPanicWhenln(conditional bool, msg ...interface{}) {
	g.conditionalSendPanic(level.Critical, conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) CriticalPanicWhenf(conditional bool, msg string, args ...interface{}) {
	g.conditionalSendPanic(level.Critical, conditional, message.NewFormatedMessage(msg, args...))
}
func (g *Grip) CriticalFatalWhen(conditional bool, msg interface{}) {
	g.conditionalSendFatal(level.Critical, conditional, msg)
}
func (g *Grip) CriticalFatalWhenln(conditional bool, msg ...interface{}) {
	g.conditionalSendFatal(level.Critical, conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) CriticalFatalWhenf(conditional bool, msg string, args ...interface{}) {
	g.conditionalSendFatal(level.Critical, conditional, message.NewFormatedMessage(msg, args...))
}

/////////////

func (g *Grip) ErrorWhen(conditional bool, m interface{}) {
	g.conditionalSend(level.Error, conditional, m)
}
func (g *Grip) ErrorWhenln(conditional bool, msg ...interface{}) {
	g.ErrorWhen(conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) ErrorWhenf(conditional bool, msg string, args ...interface{}) {
	g.ErrorWhen(conditional, message.NewFormatedMessage(msg, args...))
}
func (g *Grip) ErrorPanicWhen(conditional bool, msg interface{}) {
	g.conditionalSendPanic(level.Error, conditional, msg)
}
func (g *Grip) ErrorPanicWhenln(conditional bool, msg ...interface{}) {
	g.conditionalSendPanic(level.Error, conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) ErrorPanicWhenf(conditional bool, msg string, args ...interface{}) {
	g.conditionalSendPanic(level.Error, conditional, message.NewFormatedMessage(msg, args...))
}
func (g *Grip) ErrorFatalWhen(conditional bool, msg interface{}) {
	g.conditionalSendFatal(level.Error, conditional, msg)
}
func (g *Grip) ErrorFatalWhenln(conditional bool, msg ...interface{}) {
	g.conditionalSendFatal(level.Error, conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) ErrorFatalWhenf(conditional bool, msg string, args ...interface{}) {
	g.conditionalSendFatal(level.Error, conditional, message.NewFormatedMessage(msg, args...))
}

/////////////

func (g *Grip) WarningWhen(conditional bool, m interface{}) {
	g.conditionalSend(level.Warning, conditional, m)
}
func (g *Grip) WarningWhenln(conditional bool, msg ...interface{}) {
	g.WarningWhen(conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) WarningWhenf(conditional bool, msg string, args ...interface{}) {
	g.WarningWhen(conditional, message.NewFormatedMessage(msg, args...))
}

/////////////

func (g *Grip) NoticeWhen(conditional bool, m interface{}) {
	g.conditionalSend(level.Notice, conditional, m)
}
func (g *Grip) NoticeWhenln(conditional bool, msg ...interface{}) {
	g.NoticeWhen(conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) NoticeWhenf(conditional bool, msg string, args ...interface{}) {
	g.NoticeWhen(conditional, message.NewFormatedMessage(msg, args...))
}

/////////////

func (g *Grip) InfoWhen(conditional bool, message interface{}) {
	g.conditionalSend(level.Info, conditional, message)
}
func (g *Grip) InfoWhenln(conditional bool, msg ...interface{}) {
	g.InfoWhen(conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) InfoWhenf(conditional bool, msg string, args ...interface{}) {
	g.InfoWhen(conditional, message.NewFormatedMessage(msg, args...))
}

/////////////

func (g *Grip) DebugWhen(conditional bool, m interface{}) {
	g.conditionalSend(level.Debug, conditional, m)
}
func (g *Grip) DebugWhenln(conditional bool, msg ...interface{}) {
	g.DebugWhen(conditional, message.NewLinesMessage(msg...))
}
func (g *Grip) DebugWhenf(conditional bool, msg string, args ...interface{}) {
	g.DebugWhen(conditional, message.NewFormatedMessage(msg, args...))
}
