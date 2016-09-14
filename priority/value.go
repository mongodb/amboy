/*
Package priority

Provides a type for representing priority information in Jobs:  the "Value"
type, which jobs should compose to satisfy the priority methods of the
amboy.Job interface.
*/
package priority

// Value stores priority information for a job, and is intended to be
// composed by Job implementations with simple priority storage
// requirements.
type Value struct {
	P int `bson:"priority" json:"priority" yaml:"priority"`
}

// Priority returns the priority value, and is part of the amboy.Job
// interface.
func (v *Value) Priority() int {
	return v.P
}

// SetPriority allows users to set the priority of a job, and is part
// of the amboy.Job interface.
func (v *Value) SetPriority(p int) {
	v.P = p
}
