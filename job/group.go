package job

import (
	"fmt"
	"sync"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/dependency"
	"github.com/mongodb/amboy/registry"
	"github.com/tychoish/grip"
)

func init() {
	registry.AddJobType("group", groupJobFactory)
}

// Group is a structure for running collections of Job objects at
// the same time, as a single Job. Use Job groups to isolate several
// jobs from other Jobs in the queue, and ensure that several jobs run
// on a single system.
type Group struct {
	counter  int
	Complete bool
	Name     string
	Jobs     map[string]*registry.JobInterchange
	D        dependency.Manager
	l        *sync.RWMutex
	// It might be feasible to make a Queue implementation that
	// implements the Job interface so that we can eliminate this
	// entirely.
}

// NewGroup creates a new, empty Group object.
func NewGroup(name string) *Group {
	return &Group{
		counter: GetNumber(),
		Name:    name,
		Jobs:    make(map[string]*registry.JobInterchange),
		l:       &sync.RWMutex{},
		D:       dependency.NewAlways(),
	}
}

func groupJobFactory() amboy.Job {
	// to produce Job objects for the registry/JobInterchange
	// mechanism.
	return &Group{
		Jobs: make(map[string]*registry.JobInterchange),
	}
}

// Add is not part of the Job interface, but allows callers to append
// jobs to the Group. Returns an error if a job with the same ID()
// value already exists in the group.
func (g *Group) Add(j amboy.Job) error {
	name := j.ID()

	g.l.Lock()
	defer g.l.Unlock()
	_, exists := g.Jobs[name]
	if exists {
		return fmt.Errorf("job named '%s', already exists in Group %s",
			name, g.Name)
	}

	g.Jobs[name] = registry.MakeJobInterchange(j)
	return nil
}

// ID returns a (hopefully) unique identifier for the job, based on,
// in this implementation, the name passed to the constructor and an internal counter.
func (g *Group) ID() string {
	return fmt.Sprintf("%s-%d", g.Name, g.counter)
}

// Run executes the jobs. Provides "continue on error" semantics for
// Jobs in the Group. Returns an error if: the Group has already
// run, or if any of the constituent Jobs produce an error *or* if
// there are problems with the JobInterchange converters.
func (g *Group) Run() error {
	if g.Complete {
		return fmt.Errorf("Group '%s' has already executed", g.Name)
	}

	catcher := grip.NewCatcher()
	wg := &sync.WaitGroup{}
	for _, job := range g.Jobs {
		runnableJob, err := registry.ConvertToJob(job)
		if err != nil {
			catcher.Add(err)
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
			catcher.Add(j.Run())

			// after the task completes, add the issue
			// back to Jobs map so that we preserve errors
			// idiomatically for Groups.
			group.l.Lock()
			defer group.l.Unlock()
			group.Jobs[j.ID()] = registry.MakeJobInterchange(j)
			wg.Done()
		}(runnableJob, g)
	}
	wg.Wait()
	g.Complete = true
	return catcher.Resolve()
}

// Completed returns true when the job has executed.
func (g *Group) Completed() bool {
	return g.Complete
}

// Type returns a JobType object for this Job, which reports the kind
// of job and the version of the Job when it was created.
func (g *Group) Type() amboy.JobType {
	return amboy.JobType{
		Name:    "group",
		Version: 0,
	}
}

// Dependency returns the dependency object for this task.
func (g *Group) Dependency() dependency.Manager {
	return g.D
}

// SetDependency allows you to configure the dependency.Manager
// instance for this object. If you want to swap different dependency
// instances you can as long as the new instance is of the "Always"
// type.
func (g *Group) SetDependency(d dependency.Manager) {
	if d.Type().Name == "always" {
		g.D = d
	} else {
		grip.Warning("repo building jobs should take 'always'-run dependencies.")
	}
}
