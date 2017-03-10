package gimlet

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"

	"github.com/mongodb/grip"
)

// WriteTextResponse writes data to the response body with the given
// code as plain text after attempting to convert the data to a byte
// array.
func WriteTextResponse(w http.ResponseWriter, code int, data interface{}) {
	var out []byte

	switch data := data.(type) {
	case []byte:
		out = data
	case string:
		out = []byte(data)
	case error:
		out = []byte(data.Error())
	case []string:
		out = []byte(strings.Join(data, "\n"))
	case fmt.Stringer:
		out = []byte(data.String())
	case *bytes.Buffer:
		out = data.Bytes()
	default:
		out = []byte(fmt.Sprintf("%v", data))
	}

	w.Header().Set("Content-Type", "plain/text; charset=utf-8")
	w.WriteHeader(code)
	size, err := w.Write(out)
	if err != nil {
		grip.Warningf("encountered error %s writing a %d response", err.Error(), size)
	}
}

// WriteText writes the data, converted to text as possible, to the response body, with a successful
// status code.
func WriteText(w http.ResponseWriter, data interface{}) {
	// 200
	WriteTextResponse(w, http.StatusOK, data)
}

// WriteErrorText write the data, converted to text as possible, to the response body with a
// bad-request (e.g. 400) response code.
func WriteErrorText(w http.ResponseWriter, data interface{}) {
	// 400
	WriteTextResponse(w, http.StatusBadRequest, data)
}

// WriteErrorText write the data, converted to text as possible, to the response body with an
// internal server error (e.g. 500) response code.
func WriteInternalErrorText(w http.ResponseWriter, data interface{}) {
	// 500
	WriteTextResponse(w, http.StatusInternalServerError, data)
}
