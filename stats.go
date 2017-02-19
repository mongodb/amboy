package amboy

import (
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/sometimes"
)

// QueueStats is a simple structure that the Stats() method in the
// Queue interface returns and tracks the state of the queue, and
// provides a common format for different Queue implementations to
// report on their state.
type QueueStats struct {
	Running   int `bson:"running" json:"running" yaml:"running"`
	Completed int `bson:"completed" json:"completed" yaml:"completed"`
	Pending   int `bson:"pending" json:"pending" yaml:"pending"`
	Blocked   int `bson:"blocked" json:"blocked" yaml:"blocked"`
	Total     int `bson:"total" json:"total" yaml:"total"`
}

func (s QueueStats) isComplete() bool {
	grip.DebugWhenf(sometimes.Fifth(),
		"%d jobs complete of %d total (blocked=%d, running=%d)",
		s.Completed, s.Total, s.Blocked, s.Running)

	if s.Total == s.Completed {
		return true
	}

	if s.Total <= s.Completed+s.Blocked {
		return true
	}

	return false
}
