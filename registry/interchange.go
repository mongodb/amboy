package registry

import (
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/dependency"
	"github.com/pkg/errors"
)

// JobInterchange provides a consistent way to describe and reliably
// serialize Job objects between different queue
// instances. Interchange is also used internally as part of JobGroup
// Job type.
type JobInterchange struct {
	Name       string                 `json:"name" bson:"_id" yaml:"name"`
	Type       string                 `json:"type" bson:"type" yaml:"type"`
	Version    int                    `json:"version" bson:"version" yaml:"version"`
	Priority   int                    `json:"priority" bson:"priority" yaml:"priority"`
	Job        *rawJob                `json:"job,omitempty" bson:"job,omitempty" yaml:"job,omitempty"`
	Dependency *DependencyInterchange `json:"dependency,omitempty" bson:"dependency,omitempty" yaml:"dependency,omitempty"`
	Status     amboy.JobStatusInfo    `bson:"status" json:"status" yaml:"status"`
	TimeInfo   amboy.JobTimeInfo      `bson:"time_info" json:"time_info" yaml:"time_info"`
}

// MakeJobInterchange changes a Job interface into a JobInterchange
// structure, for easier serialization.
func MakeJobInterchange(j amboy.Job, f amboy.Format) (*JobInterchange, error) {
	typeInfo := j.Type()

	if typeInfo.Version < 0 {
		return nil, errors.New("cannot use jobs with versions less than 0 with job interchange")
	}

	dep, err := makeDependencyInterchange(j.Dependency(), f)
	if err != nil {
		return nil, err
	}

	data, err := amboy.ConvertTo(f, j)
	if err != nil {
		return nil, err
	}

	output := &JobInterchange{
		Name:     j.ID(),
		Type:     typeInfo.Name,
		Version:  typeInfo.Version,
		Priority: j.Priority(),
		Status:   j.Status(),
		TimeInfo: j.TimeInfo(),
		Job: &rawJob{
			Body:     data,
			typeName: typeInfo.Name,
			job:      j,
		},
		Dependency: dep,
	}

	return output, nil
}

// ConvertToJob reverses the process of ConvertToInterchange and
// converts the interchange format to a Job object using the types in
// the registry. Returns an error if the job type of the
// JobInterchange object isn't registered or the current version of
// the job produced by the registry is *not* the same as the version
// of the Job.
func ConvertToJob(j *JobInterchange, f amboy.Format) (amboy.Job, error) {
	factory, err := GetJobFactory(j.Type)
	if err != nil {
		return nil, err
	}

	job := factory()

	if job.Type().Version != j.Version {
		return nil, errors.Errorf("job '%s' (version=%d) does not match the current version (%d) for the job type '%s'",
			j.Name, j.Version, job.Type().Version, j.Type)
	}

	dep, err := convertToDependency(j.Dependency, f)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	j.Job.typeName = j.Type
	err = amboy.ConvertFrom(f, j.Job.Body, job)
	if err != nil {
		return nil, errors.Wrap(err, "converting job body")
	}

	job.SetDependency(dep)
	job.SetPriority(j.Priority)
	job.SetStatus(j.Status)
	job.UpdateTimeInfo(j.TimeInfo)

	return job, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////

// DependencyInterchange objects are a standard form for
// dependency.Manager objects. Amboy (should) only pass
// DependencyInterchange objects between processes, which have the
// type information in easy to access and index-able locations.
type DependencyInterchange struct {
	Type       string         `json:"type" bson:"type" yaml:"type"`
	Version    int            `json:"version" bson:"version" yaml:"version"`
	Edges      []string       `bson:"edges" json:"edges" yaml:"edges"`
	Dependency *rawDependency `json:"dependency" bson:"dependency" yaml:"dependency"`
}

// MakeDependencyInterchange converts a dependency.Manager document to
// its DependencyInterchange format.
func makeDependencyInterchange(d dependency.Manager, f amboy.Format) (*DependencyInterchange, error) {
	typeInfo := d.Type()

	data, err := amboy.ConvertTo(f, d)
	if err != nil {
		return nil, err
	}

	output := &DependencyInterchange{
		Type:    typeInfo.Name,
		Version: typeInfo.Version,
		Edges:   d.Edges(),
		Dependency: &rawDependency{
			Body:     data,
			typeName: typeInfo.Name,
			dep:      d,
		},
	}

	return output, nil
}

// convertToDependency uses the registry to convert a
// DependencyInterchange object to the correct dependnecy.Manager
// type.
func convertToDependency(d *DependencyInterchange, f amboy.Format) (dependency.Manager, error) {
	factory, err := GetDependencyFactory(d.Type)
	if err != nil {
		return nil, err
	}

	dep := factory()

	if dep.Type().Version != d.Version {
		return nil, errors.Errorf("dependency '%s' (version=%d) does not match the current version (%d) for the dependency type '%s'",
			d.Type, d.Version, dep.Type().Version, dep.Type().Name)
	}
	d.Dependency.typeName = d.Type

	// this works, because we want to use all the data from the
	// interchange object, but want to use the type information
	// associated with the object that we produced with the
	// factory.
	err = amboy.ConvertFrom(f, d.Dependency.Body, dep)
	if err != nil {
		return nil, errors.Wrap(err, "converting dependency")
	}

	return dep, nil
}
