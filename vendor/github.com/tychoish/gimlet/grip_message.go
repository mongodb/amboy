package gimlet

import (
	"encoding/json"
	"fmt"
)

// JSONMessage is an implementation of the grip/message.Composer
// interface, used so that we can log all incoming and outgoing Json
// content in debug mode, without needing to serialize structs under
// normal operation. Also contains a MarshalPretty() method which is
// used in rendering JSON into the response objects.
type JSONMessage struct {
	data interface{}
}

// NewJSONMessage constructs a new JSONMessage object.
func NewJSONMessage(data interface{}) *JSONMessage {
	return &JSONMessage{data}
}

// Resolve returns a string form of the message. Part of the Composer
// interface.
func (m *JSONMessage) Resolve() string {
	out, err := json.Marshal(m.data)
	if err != nil {
		return fmt.Sprintf("problem marshaling message. Error: %+v", err)
	}
	return string(out)
}

// Loggable is a method that allows allows the logging sender method
// to avoid sending messages that don't have content or are otherwise
// not worth sending, apart from the priority mechanism. In this
// implementation this is always true as long as the data is
// non-nil. May be useful in the future to modify this form to
// suppress sensitive data.
func (m *JSONMessage) Loggable() bool {
	return m.data != nil
}

// Raw returns the data without seralizing it first. Useful for
// logging mechanisms that handle a raw format for insertion into a
// database or posting to a service.
func (m *JSONMessage) Raw() interface{} {
	return m.data
}

// MarshalPretty is a helper method to simplify calls to
// json.MarshalIndent(). This is not part of the Composer interface.
func (m *JSONMessage) MarshalPretty() ([]byte, error) {
	response, err := json.MarshalIndent(m.data, "", "  ")
	response = append(response, []byte("\n")...)
	return response, err
}
