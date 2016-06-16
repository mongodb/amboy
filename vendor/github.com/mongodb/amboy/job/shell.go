package job

import (
	"fmt"
	"os/exec"
	"strings"
	"sync"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/dependency"
	"github.com/mongodb/amboy/registry"
	"github.com/tychoish/grip"
)

// ShellJob is an amboy.Job implementation that runs shell commands in
// the context of an amboy.Job object.
type ShellJob struct {
	Name       string             `bson:"name" json:"name" yaml:"name"`
	IsComplete bool               `bson:"is_complete" json:"is_complete" yaml:"is_complete"`
	Command    string             `bson:"command" json:"command" yaml:"command"`
	Output     string             `bson:"output" json:"output" yaml:"output"`
	WorkingDir string             `bson:"working_dir" json:"working_dir" yaml:"working_dir"`
	Env        map[string]string  `bson:"env" json:"env" yaml:"env"`
	D          dependency.Manager `bson:"dependency" json:"dependency" yaml:"dependency"`
	T          amboy.JobType      `bson:"type" json:"type" yaml:"type"`
	args       []string
	*sync.RWMutex
}

func init() {
	registry.AddJobType("shell", shellJobFactory)
}

// NewShellJob takes the command, as a string along with the name of a
// file that the command would create, and returns a pointer to a
// ShellJob object. If the "creates" argument is an empty string then
// the command always runs, otherwise only if the file specified does
// not exist. You can change the dependency with the SetDependency
// argument.
func NewShellJob(cmd string, creates string) *ShellJob {
	j := NewShellJobInstance()

	j.Command = cmd
	j.getArgsFromCommand()

	if creates != "" {
		j.D = dependency.NewCreatesFile(creates)
	}

	return j
}

// NewShellJobInstance returns a pointer to an initialized ShellJob
// instance, but does not set the command or the name. Use when the
// command is not known at creation time.
func NewShellJobInstance() *ShellJob {
	return &ShellJob{
		Env: make(map[string]string),
		D:   dependency.NewAlways(),
		T: amboy.JobType{
			Name:    "shell",
			Version: 0,
		},
		RWMutex: &sync.RWMutex{},
	}
}

func shellJobFactory() amboy.Job {
	return NewShellJobInstance()
}

// ID returns a string identifier for the job.
func (j *ShellJob) ID() string {
	if j.Name == "" {
		j.getArgsFromCommand()

		j.RLock()
		defer j.RUnlock()
		if len(j.args) == 0 {
			j.Name = fmt.Sprintf("%d.shell-job", GetNumber())
		} else {
			j.Name = fmt.Sprintf("%s-%d.shell-job", j.args[0], GetNumber())
		}
	}

	return j.Name
}

func (j *ShellJob) getArgsFromCommand() {
	j.Lock()
	defer j.Unlock()

	j.args = strings.Split(j.Command, " ")
}

// Run executes the shell commands. Add keys to the Env map to modify
// the environment, or change the value of the WorkingDir property to
// set the working directory for this command. Captures output into
// the Output attribute, and returns the error value of the command.
func (j *ShellJob) Run() error {
	grip.Debugf("running %s", j.Command)

	j.getArgsFromCommand()

	j.RLock()
	cmd := exec.Command(j.args[0], j.args[1:]...)
	j.RUnlock()

	cmd.Dir = j.WorkingDir
	cmd.Env = j.getEnVars()

	output, err := cmd.CombinedOutput()

	j.Lock()
	defer j.Unlock()

	j.Output = strings.TrimSpace(string(output))
	j.IsComplete = true

	return err
}

// Completed returns true if the command has already run.
func (j *ShellJob) Completed() bool {
	j.RLock()
	defer j.RUnlock()

	return j.IsComplete
}

func (j *ShellJob) getEnVars() []string {
	if len(j.Env) == 0 {
		return []string{}
	}

	output := make([]string, len(j.Env))

	for k, v := range j.Env {
		output = append(output, strings.Join([]string{k, v}, "="))
	}

	return output
}

// Dependency returns the dependency object for the job.
func (j *ShellJob) Dependency() dependency.Manager {
	return j.D
}

// SetDependency allows you to modify the dependency for the job.
func (j *ShellJob) SetDependency(d dependency.Manager) {
	j.D = d
}

// Type returns the JobType object for this instance.
func (j *ShellJob) Type() amboy.JobType {
	return j.T
}
