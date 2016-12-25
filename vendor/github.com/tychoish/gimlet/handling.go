package gimlet

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/tychoish/grip"
)

// WriteJSONResponse writes a JSON document to the body of an HTTP
// request, setting the return status of to 500 if the JSON
// seralization process encounters an error, otherwise return
func WriteJSONResponse(w http.ResponseWriter, code int, data interface{}) {
	j := &JSONMessage{data: data}

	out, err := j.MarshalPretty()

	if err != nil {
		grip.CatchDebug(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	grip.Debug(j)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	size, err := w.Write(out)
	if err == nil {
		grip.Debugf("response object was %d", size)
	} else {
		grip.Warningf("encountered error %s writing a %d response", err.Error(), size)
	}
}

// WriteJSON is a helper method to write JSON data to the body of an
// HTTP request and return 200 (successful.)
func WriteJSON(w http.ResponseWriter, data interface{}) {
	// 200
	WriteJSONResponse(w, http.StatusOK, data)
}

// WriteErrorJSON is a helper method to write JSON data to the body of
// an HTTP request and return 400 (user error.)
func WriteErrorJSON(w http.ResponseWriter, data interface{}) {
	// 400
	WriteJSONResponse(w, http.StatusBadRequest, data)
}

// WriteInternalErrorJSON is a helper method to write JSON data to the
// body of an HTTP request and return 500 (internal error.)
func WriteInternalErrorJSON(w http.ResponseWriter, data interface{}) {
	// 500
	WriteJSONResponse(w, http.StatusInternalServerError, data)
}

// GetJSON parses JSON from a io.ReadCloser (e.g. http/*Request.Body
// or http/*Response.Body) into an object specified by the
// request. Used in handler functiosn to retreve and parse data
// submitted by the client.
func GetJSON(r io.ReadCloser, data interface{}) error {
	defer r.Close()

	bytes, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	return json.Unmarshal(bytes, data)
}

// GetVars is a helper method that processes an http.Request and
// returns a map of strings to decoded strings for all arguments
// passed to the method in the URL. Use this helper function when
// writing handler functions.
func GetVars(r *http.Request) map[string]string {
	return mux.Vars(r)
}
