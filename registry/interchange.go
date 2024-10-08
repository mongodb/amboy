package registry

import (
	"runtime/debug"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/dependency"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/pkg/errors"
)

// JobInterchange provides a consistent way to describe and reliably
// serialize Job objects between different queue
// instances. Interchange is also used internally as part of JobGroup
// Job type.
type JobInterchange struct {
	Name             string                 `bson:"_id" json:"name" yaml:"name"`
	Type             string                 `json:"type" bson:"type" yaml:"type"`
	Group            string                 `bson:"group,omitempty" json:"group,omitempty" yaml:"group,omitempty"`
	Version          int                    `json:"version" bson:"version" yaml:"version"`
	Status           amboy.JobStatusInfo    `bson:"status" json:"status" yaml:"status"`
	Scopes           []string               `bson:"scopes,omitempty" json:"scopes,omitempty" yaml:"scopes,omitempty"`
	EnqueueScopes    []string               `bson:"enqueue_scopes,omitempty" json:"enqueue_scopes,omitempty" yaml:"enqueue_scopes,omitempty"`
	EnqueueAllScopes bool                   `bson:"enqueue_all_scopes,omitempty" json:"enqueue_all_scopes,omitempty" yaml:"enqueue_all_scopes,omitempty"`
	RetryInfo        amboy.JobRetryInfo     `bson:"retry_info" json:"retry_info,omitempty" yaml:"retry_info,omitempty"`
	TimeInfo         amboy.JobTimeInfo      `bson:"time_info" json:"time_info,omitempty" yaml:"time_info,omitempty"`
	Job              rawJob                 `json:"job" bson:"job" yaml:"job"`
	Dependency       *DependencyInterchange `json:"dependency,omitempty" bson:"dependency,omitempty" yaml:"dependency,omitempty"`
}

// MakeJobInterchange changes a Job interface into a JobInterchange
// structure, for easier serialization. This will truncate job errors if they
// will take up an unreasonably large amount of space in its serialized form.
func MakeJobInterchange(j amboy.Job, f amboy.Format) (*JobInterchange, error) {
	typeInfo := j.Type()

	if typeInfo.Version < 0 {
		return nil, errors.New("cannot use jobs with versions less than 0 with job interchange")
	}

	dep, err := makeDependencyInterchange(f, j.Dependency())
	if err != nil {
		return nil, err
	}

	data, err := convertTo(f, j)
	if err != nil {
		return nil, err
	}

	status := j.Status()
	truncatedErrs, isTruncated := truncateJobErrors(status.Errors)
	status.Errors = truncatedErrs
	grip.WarningWhen(isTruncated, message.Fields{
		"message":        "job errors were too large and had to be truncated to reduce job interchange to a reasonable size",
		"job_id":         j.ID(),
		"truncated_errs": truncatedErrs,
		"stack":          string(debug.Stack()),
	})

	output := &JobInterchange{
		Name:             j.ID(),
		Type:             typeInfo.Name,
		Version:          typeInfo.Version,
		Status:           status,
		TimeInfo:         j.TimeInfo(),
		EnqueueScopes:    j.EnqueueScopes(),
		EnqueueAllScopes: j.EnqueueAllScopes(),
		RetryInfo:        j.RetryInfo(),
		Job:              data,
		Dependency:       dep,
	}

	return output, nil
}

// truncateJobErrors truncates the errors in the job status if the total size of
// the errors is too large.
func truncateJobErrors(errs []string) (truncated []string, isTruncated bool) {
	totalLength := 0
	for _, err := range errs {
		totalLength += len(err)
	}

	const maxLengthPerError = 1000
	const maxNumErrors = 100
	const maxTotalErrorLength = maxNumErrors * maxLengthPerError

	if totalLength <= maxTotalErrorLength {
		return errs, false
	}

	truncatedErrs := errs
	if len(errs) > maxNumErrors {
		truncatedErrs = errs[:maxNumErrors]
	}
	for i := range truncatedErrs {
		if len(truncatedErrs[i]) > maxLengthPerError {
			truncatedErrs[i] = truncatedErrs[i][:maxLengthPerError]
		}
	}

	return truncatedErrs, true
}

// Resolve reverses the process of ConvertToInterchange and
// converts the interchange format to a Job object using the types in
// the registry. Returns an error if the job type of the
// JobInterchange object isn't registered or the current version of
// the job produced by the registry is *not* the same as the version
// of the Job.
func (j *JobInterchange) Resolve(f amboy.Format) (amboy.Job, error) {
	factory, err := GetJobFactory(j.Type)
	if err != nil {
		return nil, err
	}

	job := factory()

	if job.Type().Version != j.Version {
		return nil, errors.Errorf("job '%s' (version=%d) does not match the current version (%d) for the job type '%s'",
			j.Name, j.Version, job.Type().Version, j.Type)
	}

	dep, err := convertToDependency(f, j.Dependency)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	err = convertFrom(f, j.Job, job)
	if err != nil {
		return nil, errors.Wrap(err, "converting job body")
	}

	job.SetDependency(dep)
	job.SetStatus(j.Status)
	job.SetEnqueueScopes(j.EnqueueScopes...)
	job.SetEnqueueAllScopes(j.EnqueueAllScopes)
	job.UpdateTimeInfo(j.TimeInfo)
	job.UpdateRetryInfo(j.RetryInfo.Options())

	return job, nil
}

// Raw returns the serialized version of the job.
func (j *JobInterchange) Raw() []byte { return j.Job }

////////////////////////////////////////////////////////////////////////////////////////////////////

// DependencyInterchange objects are a standard form for
// dependency.Manager objects. Amboy (should) only pass
// DependencyInterchange objects between processes, which have the
// type information in easy to access and index-able locations.
type DependencyInterchange struct {
	Type       string        `json:"type" bson:"type" yaml:"type"`
	Version    int           `json:"version" bson:"version" yaml:"version"`
	Edges      []string      `bson:"edges" json:"edges" yaml:"edges"`
	Dependency rawDependency `json:"dependency" bson:"dependency" yaml:"dependency"`
}

// MakeDependencyInterchange converts a dependency.Manager document to
// its DependencyInterchange format.
func makeDependencyInterchange(f amboy.Format, d dependency.Manager) (*DependencyInterchange, error) {
	typeInfo := d.Type()

	data, err := convertTo(f, d)
	if err != nil {
		return nil, err
	}

	output := &DependencyInterchange{
		Type:       typeInfo.Name,
		Version:    typeInfo.Version,
		Edges:      d.Edges(),
		Dependency: data,
	}

	return output, nil
}

// convertToDependency uses the registry to convert a
// DependencyInterchange object to the correct dependnecy.Manager
// type.
func convertToDependency(f amboy.Format, d *DependencyInterchange) (dependency.Manager, error) {
	factory, err := GetDependencyFactory(d.Type)
	if err != nil {
		return nil, err
	}

	dep := factory()

	if dep.Type().Version != d.Version {
		return nil, errors.Errorf("dependency '%s' (version=%d) does not match the current version (%d) for the dependency type '%s'",
			d.Type, d.Version, dep.Type().Version, dep.Type().Name)
	}

	// this works, because we want to use all the data from the
	// interchange object, but want to use the type information
	// associated with the object that we produced with the
	// factory.
	err = convertFrom(f, d.Dependency, dep)
	if err != nil {
		return nil, errors.Wrap(err, "converting dependency")
	}

	return dep, nil
}
