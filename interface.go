package amboy

import "github.com/tychoish/amboy/dependency"

// Job describes a unit of work. Implementations of Job instances are
// the content of the Queue. The amboy/job package contains several
// general purpose and example implementations. Jobs are responsible,
// primarily via their Dependency property, for determining: if they
// need to run, and what Jobs they depend on.
type Job interface {
	// Provides a unique identifier for a job. Queues may error if
	// two jobs have different IDs.
	ID() string

	// The primary execution method for the job. Should toggle the
	// completed state for the job. While Runner implementations
	// *do* collect errors from tasks, which is accessible from
	// the queue, in general tasks should maintain their own
	// success/failure state, and not rely on the return value of
	// this function.
	Run() error

	// Returns true if the job has been completed. Jobs that
	// encountered errors are, often, also complete.
	Completed() bool

	// Returns a pointer to a JobType object that Queue
	// implementations can use to de-serialize tasks.
	Type() JobType

	// Provides access to the job's dependency information, and
	// allows queues to override a dependency (e.g. in a force
	// build state, or as part of serializing dependency objects
	// with jobs.)
	SetDependency(dependency.Manager)
	Dependency() dependency.Manager
}

// JobType contains information about the type of a job, which queues
// can use to serialize objects. All Job implementations must store
// and produce instances of this type that identify the type and
// implementation version.
type JobType struct {
	Name    string `json:"name" bson:"name" yaml:"name"`
	Version int    `json:"version" bson:"version" yaml:"version"`
}

// Queue describes a very simple Job queue interface that allows users
// to define Job objects, add them to a worker queue and execute tasks
// from that queue. Queue implementations may run locally or as part
// of a distributed application, with multiple workers and submitter
// Queue instances, which can support different job dispatching and
// organization properties.
type Queue interface {
	// Used to add a job to the queue. Should only error if the
	// Queue cannot accept jobs.
	Put(Job) error

	// Given a job id, get that job. The second return value is a
	// Boolean, which indicates if the named job had been
	// registered by a Queue.
	Get(string) (Job, bool)

	// Returns the next job in the queue.
	Next() (Job, error)

	// Makes it possible to detect if a Queue has started
	// dispatching jobs to runners.
	Started() bool

	// Used to mark a Job complete and remove it from the pending
	// work of the queue.
	Complete(Job)

	// Returns a channel that produces completed Job objects.
	Results() <-chan Job

	// Returns an object that contains statistics about the
	// current state of the Queue.
	Stats() *QueueStats

	// Getter for the Runner implementation embedded in the Queue
	// instance.
	Runner() Runner

	// Setter for the Runner implementation embedded in the Queue
	// instance. Permits runtime substitution of interfaces, but
	// implementations are not expected to permit users to change
	// runner implementations after starting the Queue.
	SetRunner(Runner) error

	// Begins the execution of the job Queue, using the embedded
	// Runner.
	Start() error

	// Waits for all jobs to complete.
	Wait()

	// Close calls ensures that all jobs are complete and cleans
	// up resources used by the Queue (e.g. channels, workers.)
	Close()

	// Closed returns true when workers should exit.
	Closed() bool
}

// Runner describes a simple worker interface for executing jobs in
// the context of a Queue. Used by queue implementations to run
// tasks. Generally Queue implementations will spawn a runner as part
// of their constructor or Start() methods, but client code can inject
// alternate Runner implementations, as required.
type Runner interface {
	// Reports if the pool has started.
	Started() bool

	// Provides a method to change or set the pointer to the
	// enclosing Queue object after instance creation. Runner
	// implementations may not be able to change their Queue
	// association after starting.
	SetQueue(Queue) error

	// Returns the current size of the runner pool. Some
	// implementations may not have a meaningful concept of size.
	Size() int

	// Allows callers to change the size of of the runner,
	// i.e. the number of workers. Runners are not obligated to
	// support changing the size after their start method is
	// called.
	SetSize(int) error

	// Prepares the runner implementation to begin doing work, if
	// any is required (e.g. starting workers.) Typically called
	// by the enclosing Queue object's Start() method.
	Start() error

	// Blocks until all worker threads have exited. Typically only
	// called in the context of the Close() method of an enclosing
	// Queue object.
	Wait()

	// Error returns an error if *any* Job.Run operation produced
	// an error. Typically it's more effective for Jobs to track
	// or handle their own errors internally.
	Error() error
}
