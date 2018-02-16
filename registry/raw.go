package registry

import (
	"github.com/mongodb/amboy"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2/bson"
)

type rawJob struct {
	Body     []byte `bson:"body" json:"body" yaml:"body"`
	typeName string
	job      interface{}
}

// Set == Unmarshal
func (j *rawJob) SetBSON(r bson.Raw) error { j.Body = r.Data; return nil }

// Get == Marshal
func (j *rawJob) GetBSON() (interface{}, error) {
	if j.job != nil {
		return j.job, nil
	}

	factory, err := GetJobFactory(j.typeName)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	job := factory()

	if err = amboy.ConvertFrom(amboy.BSON, j.Body, job); err != nil {
		return nil, errors.WithStack(err)
	}

	j.job = job
	return j.job, nil
}

func (j *rawJob) UnmasrhalJSON(data []byte) error { j.Body = data; return nil }
func (j *rawJob) MarshalJSON() ([]byte, error) {
	if j.Body != nil {
		return j.Body, nil
	}

	if j.job == nil {
		return nil, errors.New("no job defined")
	}

	body, err := amboy.ConvertTo(amboy.JSON, j.job)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	j.Body = body

	return j.Body, nil
}

func (j *rawJob) UnmasrhalYAML(data []byte) error { j.Body = data; return nil }
func (j *rawJob) MarshalYAML() ([]byte, error)    { return j.Body, nil }

////////////////////////////////////////////////////////////////////////

type rawDependency struct {
	Body     []byte `bson:"body" json:"body" yaml:"body"`
	typeName string
	dep      interface{}
}

// Set == Unmarshal
func (d *rawDependency) SetBSON(r bson.Raw) error { d.Body = r.Data; return nil }

// Get == Marshal
func (d *rawDependency) GetBSON() (interface{}, error) {
	if d.dep != nil {
		return d.dep, nil
	}

	factory, err := GetDependencyFactory(d.typeName)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	dep := factory()

	if err = amboy.ConvertFrom(amboy.BSON, d.Body, dep); err != nil {
		return nil, errors.WithStack(err)
	}

	d.dep = dep

	return d.dep, nil
}

func (d *rawDependency) UnmasrhalJSON(data []byte) error { d.Body = data; return nil }
func (d *rawDependency) MarshalJSON() ([]byte, error) {
	if len(d.Body) > 0 {
		return d.Body, nil
	}

	if d.dep == nil {
		return nil, errors.New("no dependency defined")
	}

	body, err := amboy.ConvertTo(amboy.JSON, d.dep)

	if err != nil {
		return nil, errors.WithStack(err)
	}

	d.Body = body

	return d.Body, nil

}

func (d *rawDependency) UnmasrhalYAML(data []byte) error { d.Body = data; return nil }
func (d *rawDependency) MarshalYAML() ([]byte, error)    { return d.Body, nil }
