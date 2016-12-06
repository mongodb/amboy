package amboy

import "github.com/tychoish/grip"

// QueueStats is a simple structure that the Stats() method in the
// Queue interface returns and tracks the state of the queue, and
// provides a common format for different Queue implementations to
// report on their state.
type QueueStats struct {
	Running   int `bson:"running" json:"running" yaml:"running"`
	Completed int `bson:"completed" json:"completed" yaml:"completed"`
	Pending   int `bson:"pending" json:"pending" yaml:"pending"`
	Total     int `bson:"total" json:"total" yaml:"total"`
}

func (s QueueStats) isComplete() bool {
	grip.Debugf("%d jobs complete of %d total", s.Completed, s.Total)
	return s.Total == s.Completed
}
