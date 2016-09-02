package driver

import (
	"github.com/mongodb/amboy"
	"golang.org/x/net/context"
)

// Driver describes the interface between a queue and an out of
// process persistence layer, like a database.
type Driver interface {
	Open(context.Context) error
	Close()

	Put(amboy.Job) error
	Get(string) (amboy.Job, error)
	Reload(amboy.Job) amboy.Job
	Save(amboy.Job) error

	GetLock(context.Context, amboy.Job) (JobLock, error)

	Jobs() <-chan amboy.Job
	Next() amboy.Job

	Stats() Stats
}

// Stats is a common structure that queues' storage Drivers use
// to report on the current state of their managed jobs and locks.
type Stats struct {
	Locked   int `bson:"locked" json:"locked" yaml:"locked"`
	Unlocked int `bson:"unlocked" json:"unlocked" yaml:"unlocked"`
	Total    int `bson:"total" json:"total" yaml:"total"`
	Complete int `bson:"complete" json:"complete" yaml:"complete"`
	Pending  int `bson:"pending" json:"pending" yaml:"pending"`
}

// JobLock defines an interface for Queue objects to interact
// with a lock that's persisted externally. This lock can't protect
// resources within a process (use sync.Mutex instead.)
type JobLock interface {
	Name() string
	Lock(context.Context)
	Unlock(context.Context)
	IsLocked(context.Context) bool
	IsLockedElsewhere(context.Context) bool
}
