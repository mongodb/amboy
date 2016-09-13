package rest

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/tychoish/gimlet"
	"github.com/tychoish/grip"
)

type jobStatusResponse struct {
	Exists      bool        `bson:"job_exists" json:"job_exists" yaml:"job_exists"`
	Completed   bool        `bson:"completed" json:"completed" yaml:"completed"`
	ID          string      `bson:"id,omitempty" json:"id,omitempty" yaml:"id,omitempty"`
	JobsPending int         `bson:"jobs_pending,omitempty" json:"jobs_pending,omitempty" yaml:"jobs_pending,omitempty"`
	Error       string      `bson:"error,omitempty" json:"error,omitempty" yaml:"error,omitempty"`
	Job         interface{} `bson:"job,omitempty" json:"job,omitempty" yaml:"job,omitempty"`
}

func (s *Service) getJobStatusResponse(name string) (*jobStatusResponse, error) {
	var msg string
	var err error

	resp := &jobStatusResponse{}
	resp.JobsPending = s.queue.Stats().Pending
	resp.ID = name

	if name == "" {
		msg = fmt.Sprintf("did not specify job name: %s", name)
		err = errors.New(msg)
		resp.Error = msg

		return resp, err
	}

	j, exists := s.queue.Get(name)
	resp.Exists = exists

	if !exists {
		msg = fmt.Sprintf("could not recover job '%s'", name)
		err = errors.New(msg)
		resp.Error = msg

		return resp, err
	}

	resp.Exists = true
	resp.Completed = j.Completed()
	resp.Job = j

	return resp, nil
}

// JobStatus is a http.HandlerFunc that writes a job status document to the request.
func (s *Service) JobStatus(w http.ResponseWriter, r *http.Request) {
	name := gimlet.GetVars(r)["name"]

	response, err := s.getJobStatusResponse(name)
	if err != nil {
		grip.Error(err)
		gimlet.WriteErrorJSON(w, response)
		return
	}

	gimlet.WriteJSON(w, response)
}
