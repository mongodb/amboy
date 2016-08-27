package registry

// This file has a mock implementation of a job. Used in other tests.

import (
	"errors"
	"fmt"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/dependency"
)

func init() {
	AddJobType("test", jobTestFactory)
}

type JobTest struct {
	Name       string
	Content    string
	complete   bool
	shouldFail bool
	T          amboy.JobType
	dep        dependency.Manager
}

func NewTestJob(content string) *JobTest {
	id := fmt.Sprintf("%s-%s", content+"-job", content)

	return &JobTest{
		Name:    id,
		Content: content,
		dep:     dependency.NewAlways(),
		T: amboy.JobType{
			Name:    "test",
			Format:  amboy.BSON,
			Version: 0,
		},
	}
}

func jobTestFactory() amboy.Job {
	return &JobTest{
		T: amboy.JobType{
			Name:    "test",
			Format:  amboy.BSON,
			Version: 0,
		},
	}
}

func (f *JobTest) ID() string {
	return f.Name
}

func (f *JobTest) Run() {
	f.complete = true
}

func (f *JobTest) Error() error {
	if f.shouldFail {
		return errors.New("poisoned task")
	}

	return nil
}

func (f *JobTest) Completed() bool {
	return f.complete
}

func (f *JobTest) Type() amboy.JobType {
	return f.T
}

func (f *JobTest) Dependency() dependency.Manager {
	return f.dep
}

func (f *JobTest) SetDependency(d dependency.Manager) {
	f.dep = d
}

func (f *JobTest) Export() ([]byte, error) {
	return amboy.ConvertTo(f.Type().Format, f)
}

func (f *JobTest) Import(data []byte) error {
	return amboy.ConvertFrom(f.Type().Format, data, f)
}
