package message

// Composer defines an interface with a "Resolve()" method that
// returns the message in string format. Objects that implement this
// interface, in combination to the Compose[*] operations, the
// Resolve() method is only caled if the priority of the method is
// greater than the threshold priority. This makes it possible to
// defer building log messages (that may be somewhat expensive to
// generate) until it's certain that we're going to be outputting the
// message.
type Composer interface {
	// Returns the content of the message as a string for use in
	// line-printing logging engines.
	Resolve() string

	// A "raw" format of the logging output for use by some Sender
	// implementations that write logged items to interfaces that
	// accept JSON or another structured.
	Raw() interface{}

	// Returns "true" when the message has content and should be
	// logged, and false otherwise. When false, the sender can
	// (and should!) ignore messages even if they are otherwise
	// above the logging threshold.
	Loggable() bool
}

// ConvertToComposer can coerce unknown objects into Composer
// instances, as possible.
func ConvertToComposer(message interface{}) Composer {
	switch message := message.(type) {
	case Composer:
		return message
	case string:
		return NewDefaultMessage(message)
	case []interface{}:
		return NewLinesMessage(message)
	case error:
		return NewErrorMessage(message)
	default:
		return NewFormatedMessage("%+v", message)
	}
}
