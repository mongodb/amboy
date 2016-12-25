package slogger

import "github.com/tychoish/grip/level"

type Level uint8

const (
	OFF Level = iota
	DEBUG
	INFO
	WARN
	ERROR
)

func (l Level) String() string {
	switch l {
	case OFF:
		return "off"
	case DEBUG:
		return "debug"
	case INFO:
		return "info"
	case WARN:
		return "warn"
	default:
		return ""
	}
}

func (l Level) Priority() level.Priority {
	switch l {
	case OFF:
		return level.Invalid
	case DEBUG:
		return level.Debug
	case INFO:
		return level.Info
	case WARN:
		return level.Warning
	default:
		return level.Notice
	}
}

func convertFromPriority(l level.Priority) Level {
	switch l {
	case level.Emergency, level.Alert, level.Critical, level.Error, level.Warning:
		return WARN
	case level.Notice, level.Info:
		return INFO
	case level.Debug:
		return DEBUG
	case level.Invalid:
		return OFF
	default:
		return INFO
	}
}
