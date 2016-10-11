package job

import (
	"fmt"
	"strings"
	"sync"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/dependency"
	"github.com/mongodb/amboy/priority"
	"github.com/mongodb/amboy/registry"
	"github.com/pkg/errors"
	"github.com/tychoish/grip"
)

// Group is a structure for running collections of Job objects at the
// same time, as a single Job. Use Groups to isolate several Jobs from
// other Jobs in the queue, and ensure that several Jobs run on a
// single system.
type Group struct {
	Name     string                              `bson:"name" json:"name" yaml:"name"`
	Complete bool                                `bson:"complete" json:"complete" yaml:"complete"`
	Errors   []error                             `bson:"errors" json:"errors" yaml:"errors"`
	Jobs     map[string]*registry.JobInterchange `bson:"jobs" json:"jobs" yaml:"jobs"`
	T        amboy.JobType                       `bson:"type" json:"type" yaml:"type"`
	dep      dependency.Manager
	mutex    sync.RWMutex

	priority.Value
	// It might be feasible to make a Queue implementation that
	// implements the Job interface so that we can eliminate this
	// entirely.
}

// NewGroup creates a new, empty Group object.
func NewGroup(name string) *Group {
	g := newGroupInstance()
	g.Name = name

	return g
}

// newGroupInstance is a common constructor for the public NewGroup
// constructior and the registry.JobFactory constructor.
func newGroupInstance() *Group {
	return &Group{
		Jobs: make(map[string]*registry.JobInterchange),
		dep:  dependency.NewAlways(),
		T: amboy.JobType{
			Name:    "group",
			Version: 0,
			Format:  amboy.BSON,
		},
	}
}

// Add is not part of the Job interface, but allows callers to append
// jobs to the Group. Returns an error if a job with the same ID()
// value already exists in the group.
func (g *Group) Add(j amboy.Job) error {
	name := j.ID()

	g.mutex.Lock()
	defer g.mutex.Unlock()
	_, exists := g.Jobs[name]
	if exists {
		return fmt.Errorf("job named '%s', already exists in Group %s",
			name, g.Name)
	}

	job, err := registry.MakeJobInterchange(j)
	if err != nil {
		return err
	}

	g.Jobs[name] = job
	return nil
}

// ID returns a (hopefully) unique identifier for the job, based on
// the name specified to the constructor and in this implementation,
// the name passed to the constructor and an internal counter.
func (g *Group) ID() string {
	return g.Name
}

// Run executes the jobs. Provides "continue on error" semantics for
// Jobs in the Group. Returns an error if: the Group has already
// run, or if any of the constituent Jobs produce an error *or* if
// there are problems with the JobInterchange converters.
func (g *Group) Run() {
	if g.Complete {
		g.Errors = append(g.Errors, fmt.Errorf("Group '%s' has already executed", g.Name))

		return
	}

	wg := &sync.WaitGroup{}

	g.mutex.RLock()
	for _, job := range g.Jobs {
		runnableJob, err := registry.ConvertToJob(job)
		if err != nil {
			g.Errors = append(g.Errors, err)
			continue
		}

		depState := runnableJob.Dependency().State()
		if depState == dependency.Passed {
			grip.Infof("skipping job %s because of dependency", runnableJob.ID())
			continue
		} else if depState == dependency.Blocked || depState == dependency.Unresolved {
			grip.Warningf("dispatching blocked/unresolved job %s", runnableJob.ID())
		}

		wg.Add(1)
		go func(j amboy.Job, group *Group) {
			defer wg.Done()

			j.Run()

			// after the task completes, add the issue
			// back to Jobs map so that we preserve errors
			// idiomatically for Groups.

			jobErr := j.Error()
			if jobErr != nil {
				g.Errors = append(g.Errors, jobErr)
			}

			job, err := registry.MakeJobInterchange(j)
			if err != nil {
				g.Errors = append(g.Errors, err)
				return
			}

			if jobErr != nil {
				return
			}

			group.mutex.Lock()
			defer group.mutex.Unlock()
			group.Jobs[j.ID()] = job
		}(runnableJob, g)
	}
	g.mutex.RUnlock()

	wg.Wait()

	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.Complete = true
}

func (g *Group) Error() error {
	if len(g.Errors) == 0 {
		return nil
	}

	var outputs []string

	for _, err := range g.Errors {
		outputs = append(outputs, fmt.Sprintf("%+v", err))
	}

	return errors.New(strings.Join(outputs, "\n"))
}

// Completed returns true when the job has executed.
func (g *Group) Completed() bool {
	g.mutex.RLock()
	defer g.mutex.RUnlock()

	return g.Complete
}

// Type returns a JobType object for this Job, which reports the kind
// of job and the version of the Job when it was created.
func (g *Group) Type() amboy.JobType {
	return g.T
}

// Dependency returns the dependency object for this task.
func (g *Group) Dependency() dependency.Manager {
	return g.dep
}

// SetDependency allows you to configure the dependency.Manager
// instance for this object. If you want to swap different dependency
// instances you can as long as the new instance is of the "Always"
// type.
func (g *Group) SetDependency(d dependency.Manager) {
	if d == nil || d.Type().Name != "always" {
		grip.Warningf("group job types must have 'always' dependency types, '%s' is invalid",
			d.Type().Name)
		return
	}

	g.dep = d
}
