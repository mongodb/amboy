package message

type errorMessage struct {
	Err error `json:"error" bson:"error" yaml:"error"`
}

// NewErrorMessage takes an error object and returns a Composer
// instance that only renders a loggable message when the error is
// non-nil.
func NewErrorMessage(err error) Composer {
	return &errorMessage{err}
}

func (e *errorMessage) Resolve() string {
	if e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *errorMessage) Loggable() bool {
	return e.Err != nil
}

func (e *errorMessage) Raw() interface{} {
	return e
}
