package dependency

import "github.com/mongodb/amboy/registry"

// TypeInfo describes the type information that every dependency
// implementation should provide in its Type() implementation.
type TypeInfo struct {
	Name    string `json:"name" bson:"name" yaml:"name"`
	Version int    `json:"version" bson:"version" yaml:"version"`
}

// Manager objects provide a way for Jobs and queues to communicate
// about dependencies between multiple Jobs. While some, indeed many
// Job implementations, will have dependencies that *always* trigger
// rebuilds, others will be able to specify a dependency that a queue
// implementation can use to order Jobs.
type Manager interface {
	// Reports the state of the dependency, and allows calling
	// tasks to determine if the dependencies for a Job have been
	// satisfied.
	State() State

	// Computes and returns a list of Job IDs that this task
	// depends on. While the State() method is ultimately
	// responsible for determining if a Dependency is resolved,
	// the Edges() function provides Queue implementations with a
	// way of (potentially) dependencies.
	Edges() []string

	// Adds new edges to the dependency manager.
	AddEdge(string) error

	// Returns a pointer to a DependencyType object, which is used
	// for serializing Dependency objects, when needed.
	Type() TypeInfo
}

type CallbackCheck func([]string) State

type CallbackManager struct {
	CallbackName string   `bson:"function_name" json:"function_name" yaml:"function_name"`
	T            TypeInfo `bson:"type" json:"type" yaml:"type"`
	JobEdges     `bson:"edges" json:"edges" yaml:"edges"`
}

func NewCallbackManager(name string) *CallbackManager {
	return &CallbackManager{
		CallbackName: name,
		T: TypeInfo{
			Version: 1,
			Name:    CheckCallback,
		},
	}
}

func (d *CallbackManager) Type() TypeInfo { return d.T }
func (d *CallbackManager) State() State {
	cb := registry.GetCallbackFactory(d.CallbackName)
	return cb(c.Edges())
}
