package message

import (
	"fmt"
	"os"
	"time"

	"github.com/tychoish/grip/level"
)

type Base struct {
	Level    level.Priority `bson:"level,omitempty" json:"level,omitempty" yaml:"level,omitempty"`
	Hostname string         `bson:"hostname,omitempty" json:"hostname,omitempty" yaml:"hostname,omitempty"`
	Time     time.Time      `bson:"time,omitempty" json:"time,omitempty" yaml:"time,omitempty"`
	Process  string         `bson:"process,omitempty" json:"process,omitempty" yaml:"process,omitempty"`
	Logger   string         `bson:"logger,omitempty" json:"logger,omitempty" yaml:"logger,omitempty"`
}

func (b *Base) Collect() error {
	var err error
	b.Hostname, err = os.Hostname()
	if err != nil {
		return err
	}

	b.Time = time.Now()
	b.Process = os.Args[0]

	return nil
}

func (b *Base) Priority() level.Priority {
	return b.Level
}

func (b *Base) SetPriority(l level.Priority) error {
	if !level.IsValidPriority(l) {
		return fmt.Errorf("%s (%d) is not a valid priority", l, l)
	}

	b.Level = l

	return nil
}
