package rest

import (
	"net/http"
	"time"

	"github.com/evergreen-ci/gimlet"
	"github.com/mongodb/amboy/reporting"
	"github.com/pkg/errors"
)

type ReportingService struct {
	reporter reporting.Reporter
}

func NewReportingService(r reporting.Reporter) *ReportingService {
	return &ReportingService{
		reporter: r,
	}
}

func (s *ReportingService) App() *gimlet.APIApp {
	app := gimlet.NewApp()

	app.AddRoute("/status/{filter}").Version(1).Get().Handler(s.GetJobStatus)
	app.AddRoute("/status/{filter}/{type}").Version(1).Get().Handler(s.GetJobStatusByType)
	app.AddRoute("/timing/{filter}/{seconds}").Version(1).Get().Handler(s.GetRecentTimings)
	app.AddRoute("/errors/{filter}/{seconds}").Version(1).Get().Handler(s.GetRecentErrors)
	app.AddRoute("/errors/{filter}/{type}/{seconds}").Version(1).Get().Handler(s.GetRecentErrorsByType)

	return app
}

func (s *ReportingService) GetJobStatus(rw http.ResponseWriter, r *http.Request) {
	filter := reporting.CounterFilter(gimlet.GetVars(r)["filter"])
	ctx := r.Context()

	err := filter.Validate()
	if err != nil {
		gimlet.WriteResponse(rw, gimlet.MakeJSONErrorResponder(err))
		return
	}

	data, err := s.reporter.JobStatus(ctx, filter)
	if err != nil {
		gimlet.WriteResponse(rw, gimlet.MakeJSONErrorResponder(err))
		return
	}

	gimlet.WriteJSON(rw, data)
}

func (s *ReportingService) GetJobStatusByType(rw http.ResponseWriter, r *http.Request) {
	vars := gimlet.GetVars(r)
	filter := reporting.CounterFilter(vars["filter"])
	jobType := vars["type"]

	if err := filter.Validate(); err != nil {
		gimlet.WriteResponse(rw, gimlet.MakeJSONErrorResponder(err))
		return
	}

	ctx := r.Context()
	data, err := s.reporter.JobIDsByState(ctx, jobType, filter)
	if err != nil {
		gimlet.WriteResponse(rw, gimlet.MakeJSONErrorResponder(err))
		return
	}

	gimlet.WriteJSON(rw, data)
}

func (s *ReportingService) GetRecentTimings(rw http.ResponseWriter, r *http.Request) {
	vars := gimlet.GetVars(r)
	dur, err := time.ParseDuration(vars["seconds"])
	if err != nil {
		gimlet.WriteResponse(rw, gimlet.MakeJSONErrorResponder(errors.Wrapf(err,
			"problem parsing duration from %s", vars["seconds"])))
		return
	}

	filter := reporting.RuntimeFilter(vars["filter"])
	if err := filter.Validate(); err != nil {
		gimlet.WriteResponse(rw, gimlet.MakeJSONErrorResponder(err))
		return
	}

	ctx := r.Context()
	data, err := s.reporter.RecentTiming(ctx, dur, filter)
	if err != nil {
		gimlet.WriteResponse(rw, gimlet.MakeJSONErrorResponder(err))
		return
	}

	gimlet.WriteJSON(rw, data)
}

func (s *ReportingService) GetRecentErrors(rw http.ResponseWriter, r *http.Request) {
	vars := gimlet.GetVars(r)

	dur, err := time.ParseDuration(vars["seconds"])
	if err != nil {
		gimlet.WriteResponse(rw, gimlet.MakeJSONErrorResponder(errors.Wrapf(err,
			"problem parsing duration from %s", vars["seconds"])))
		return
	}

	filter := reporting.ErrorFilter(vars["filter"])
	if err := filter.Validate(); err != nil {
		gimlet.WriteResponse(rw, gimlet.MakeJSONErrorResponder(err))
		return
	}

	ctx := r.Context()
	data, err := s.reporter.RecentErrors(ctx, dur, filter)
	if err != nil {
		gimlet.WriteResponse(rw, gimlet.MakeJSONErrorResponder(err))
		return
	}

	gimlet.WriteJSON(rw, data)
}

func (s *ReportingService) GetRecentErrorsByType(rw http.ResponseWriter, r *http.Request) {
	vars := gimlet.GetVars(r)
	jobType := vars["type"]

	dur, err := time.ParseDuration(vars["seconds"])
	if err != nil {
		gimlet.WriteResponse(rw, gimlet.MakeJSONErrorResponder(errors.Wrapf(err,
			"problem parsing duration from %s", vars["seconds"])))
		return
	}

	filter := reporting.ErrorFilter(vars["filter"])
	if err := filter.Validate(); err != nil {
		gimlet.WriteResponse(rw, gimlet.MakeJSONErrorResponder(err))
		return
	}

	ctx := r.Context()
	data, err := s.reporter.RecentJobErrors(ctx, jobType, dur, filter)
	if err != nil {
		gimlet.WriteResponse(rw, gimlet.MakeJSONErrorResponder(err))
		return
	}

	gimlet.WriteJSON(rw, data)
}
