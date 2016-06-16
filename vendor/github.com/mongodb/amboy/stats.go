package amboy

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
