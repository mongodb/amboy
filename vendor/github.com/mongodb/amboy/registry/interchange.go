package registry

import (
	"fmt"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/dependency"
)

// DependencyInterchange objects are a standard form for
// dependency.Manager objects. Amboy (should) only pass
// DependencyInterchange objects between processes, which have the
// type information in easy to access and index-able locations.
type DependencyInterchange struct {
	Type       string             `json:"type" bson:"type" yaml:"type"`
	Version    int                `json:"version" bson:"version" yaml:"version"`
	Dependency dependency.Manager `json:"dependency" bson:"dependency" yaml:"dependency"`
}

// MakeDependencyInterchange converts a dependency.Manager document to
// its DependencyInterchange format.
func MakeDependencyInterchange(d dependency.Manager) *DependencyInterchange {
	return &DependencyInterchange{
		Type:       d.Type().Name,
		Version:    d.Type().Version,
		Dependency: d,
	}
}

// ConvertToDependency uses the registry to convert a
// DependencyInterchange object to the correct dependnecy.Manager
// type.
func ConvertToDependency(d *DependencyInterchange) (dependency.Manager, error) {
	factory, err := GetDependencyFactory(d.Type)
	if err != nil {
		return nil, err
	}

	dep := factory()

	if dep.Type().Version != d.Version {
		return nil, fmt.Errorf("dependency '%s' (version=%d) does not match the current version (%d) for the dependency type '%s'",
			d.Type, d.Version, dep.Type().Version, dep.Type().Name)
	}

	// this works, because we want to use all the data from the
	// interchange object, but want to use the type information
	// associated with the object that we produced with the
	// factory.
	dep = d.Dependency

	return dep, err
}

////////////////////////////////////////////////////////////////////////////////////////////////////

// JobInterchange provides a consistent way to describe and reliably
// serialize Job objects between different queue
// instances. Interchange is also used internally as part of JobGroup
// Job type.
type JobInterchange struct {
	Name       string                 `json:"name" bson:"name" yaml:"name"`
	Type       string                 `json:"type" bson:"type" yaml:"type"`
	Version    int                    `json:"version" bson:"version" yaml:"version"`
	Job        amboy.Job              `json:"job" bson:"job" yaml:"job"`
	Dependency *DependencyInterchange `json:"dependency" bson:"dependency" yaml:"dependency"`
}

// MakeJobInterchange changes a Job interface into a JobInterchange
// structure, for easier serialization.
func MakeJobInterchange(j amboy.Job) *JobInterchange {
	return &JobInterchange{
		Name:       j.ID(),
		Type:       j.Type().Name,
		Version:    j.Type().Version,
		Job:        j,
		Dependency: MakeDependencyInterchange(j.Dependency()),
	}
}

// ConvertToJob reverses the process of ConvertToInterchange and
// converts the interchange format to a Job object using the types in
// the registry. Returns an error if the job type of the
// JobInterchange object isn't registered or the current version of
// the job produced by the registry is *not* the same as the version
// of the Job.
func ConvertToJob(j *JobInterchange) (amboy.Job, error) {
	factory, err := GetJobFactory(j.Type)
	if err != nil {
		return nil, err
	}

	job := factory()

	if job.Type().Version != j.Version {
		return nil, fmt.Errorf("job '%s' (version=%d) does not match the current version (%d) for the job type '%s'",
			j.Name, j.Version, job.Type().Version, j.Type)
	}

	dep, err := ConvertToDependency(j.Dependency)
	if err != nil {
		return nil, err
	}

	// this works, because we want to use all the data from the
	// interchange object, but want to use the type information
	// associated with the object that we produced with the
	// factory.
	job = j.Job
	job.SetDependency(dep)

	return job, nil
}
