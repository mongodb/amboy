package message

type stringMessage struct {
	content string
}

// NewDefaultMessage provides a Composer interface around a single
// string, which are always logable unless the string is empty.
func NewDefaultMessage(message string) Composer {
	return &stringMessage{message}
}

func (s *stringMessage) Resolve() string {
	return s.content
}

func (s *stringMessage) Loggable() bool {
	return s.content != ""
}

func (s *stringMessage) Raw() interface{} {
	return &struct {
		Message  string `json:"message" bson:"message" yaml:"message"`
		Loggable bool   `json:"loggable" bson:"loggable" yaml:"loggable"`
	}{
		Message:  s.Resolve(),
		Loggable: s.Loggable(),
	}
}
