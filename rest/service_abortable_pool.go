package rest

import (
	"net/http"

	"github.com/evergreen-ci/gimlet"
	"github.com/mongodb/amboy"
	"github.com/pkg/errors"
)

type ManagementService struct {
	pool amboy.AbortableRunner
}

func NewManagementService(p amboy.AbortableRunner) *ManagementService {
	return &ManagementService{
		pool: p,
	}
}

func (s *ManagementService) App() *gimlet.APIApp {
	app := gimlet.NewApp()

	app.AddRoute("/jobs/list").Version(1).Get().Handler(s.ListJobs)
	app.AddRoute("/jobs/abort").Version(1).Delete().Handler(s.AbortAllJobs)
	app.AddRoute("/job/{name}").Version(1).Get().Handler(s.GetJobStatus)
	app.AddRoute("/job/{name}").Version(1).Delete().Handler(s.AbortRunningJob)

	return app
}

func (s *ManagementService) ListJobs(rw http.ResponseWriter, r *http.Request) {
	jobs := s.pool.RunningJobs()

	gimlet.WriteJSON(rw, jobs)
}

func (s *ManagementService) AbortAllJobs(rw http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	s.pool.AbortAll(ctx)

	gimlet.WriteJSON(rw, `{}`)
}

func (s *ManagementService) GetJobStatus(rw http.ResponseWriter, r *http.Request) {
	name := gimlet.GetVars(r)["name"]

	if !s.pool.IsRunning(name) {
		gimlet.WriteJSONResponse(rw, http.StatusNotFound,
			map[string]string{
				"name":   name,
				"status": "running",
			})
		return
	}

	gimlet.WriteJSON(rw, map[string]string{
		"name":   name,
		"status": "not running",
	})
}

func (s *ManagementService) AbortRunningJob(rw http.ResponseWriter, r *http.Request) {
	name := gimlet.GetVars(r)["name"]
	ctx := r.Context()
	err := s.pool.Abort(ctx, name)
	if err != nil {
		gimlet.WriteResponse(rw, gimlet.MakeJSONErrorResponder(errors.Wrapf(err,
			"problem aborting job '%s'", name)))
	}

	gimlet.WriteJSON(rw, map[string]string{
		"name":   name,
		"status": "aborted",
	})
}
