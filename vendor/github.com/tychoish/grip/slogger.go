package grip

import (
	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/send"
	"github.com/tychoish/grip/slogger"
)

func NewJournalerFromSlogger(logger *slogger.Logger) (Journaler, error) {
	l := send.LevelInfo{level.Debug, level.Debug}

	j := NewJournaler(logger.Name)
	j.GetSender().SetLevel(l)

	sender, err := send.NewMultiSender(logger.Name, l, logger.Appenders)
	if err != nil {
		return nil, err
	}

	if err := j.SetSender(sender); err != nil {
		return nil, err
	}

	return j, nil
}
