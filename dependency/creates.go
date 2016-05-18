package dependency

import "os"

// CreatesFileDependency describes a task that is only ready to run if
// a specific file doesn't exist. Specify that file as the FileName
// attribute, or to the NewCreatesFileDependency constructor.
type CreatesFileDependency struct {
	FileName string   `bson:"file_name" json:"file_name" yaml:"file_name"`
	T        TypeInfo `bson:"type" json:"type" yaml:"type"`

	*JobEdges
}

// NewCreatesFileInstance builds a new CreatesFileDependency object,
// used in the factory function for the registry, and for cases when
// the dependent file is not known at construction time.
func NewCreatesFileInstance() *CreatesFileDependency {
	c := &CreatesFileDependency{
		T: TypeInfo{
			Name:    "create-file",
			Version: 0,
		},
		JobEdges: NewJobEdges(),
	}

	return c
}

// NewCreatesFileDependency constructs a CreatesFileDependency object
// in cases when the dependent FileName is known at object creation
// time.
func NewCreatesFileDependency(name string) *CreatesFileDependency {
	c := NewCreatesFileInstance()
	c.FileName = name

	return c
}

// State returns Ready if the dependent file does not exist or is not
// specified, and Passed if the file *does* exist. Jobs with Ready
// states should be executed, while those with Passed states should be
// a no-op.
func (d *CreatesFileDependency) State() State {
	if d.FileName == "" {
		return Ready
	}

	if _, err := os.Stat(d.FileName); os.IsNotExist(err) {
		return Ready
	}

	return Passed
}

// Type returns the type information on the "creates-file" Manager
// implementation.
func (d *CreatesFileDependency) Type() TypeInfo {
	return d.T
}
