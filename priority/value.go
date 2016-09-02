/*
Priority

Provides a type for representing priority information in Jobs:  the "Value"
type, which jobs should compose to satisfy the priority methods of the
amboy.Job interface.
*/
package priority

type Value struct {
	P int `bson:"priority" json:"priority" yaml:"priority"`
}

func (v *Value) Priority() int {
	return v.P
}

func (v *Value) SetPriority(p int) {
	v.P = p
}
