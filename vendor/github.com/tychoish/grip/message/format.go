package message

import "fmt"

type formatMessenger struct {
	base string
	args []interface{}
}

// NewFormatedMessage takes arguments as fmt.Sprintf(), and returns
// an object that only runs the format operation as part of the
// Resolve() method.
func NewFormatedMessage(base string, args ...interface{}) Composer {
	return &formatMessenger{base, args}
}

func (f *formatMessenger) Resolve() string {
	return fmt.Sprintf(f.base, f.args...)
}

func (f *formatMessenger) Loggable() bool {
	return f.base != ""
}

func (f *formatMessenger) Raw() interface{} {
	return &struct {
		Message  string `json:"message" bson:"message" yaml:"message"`
		Loggable bool   `json:"loggable" bson:"loggable" yaml:"loggable"`
	}{
		Message:  f.Resolve(),
		Loggable: f.Loggable(),
	}
}
