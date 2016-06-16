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
	D          dependency.Manager
	T          amboy.JobType
}

func NewTestJob(content string) *JobTest {
	id := fmt.Sprintf("%s-%s", content+"-job", content)

	return &JobTest{
		Name:    id,
		Content: content,
		D:       dependency.NewAlways(),
		T: amboy.JobType{
			Name:    "test",
			Version: 0,
		},
	}
}

func jobTestFactory() amboy.Job {
	return &JobTest{
		T: amboy.JobType{
			Name:    "test",
			Version: 0,
		},
	}
}

func (f *JobTest) ID() string {
	return f.Name
}

func (f *JobTest) Run() error {
	f.complete = true

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
	return f.D
}

func (f *JobTest) SetDependency(d dependency.Manager) {
	f.D = d
}
