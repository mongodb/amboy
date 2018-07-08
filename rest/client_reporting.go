package rest

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/evergreen-ci/gimlet"
	"github.com/mongodb/amboy/reporting"
	"github.com/pkg/errors"
)

type ReportingClient struct {
	client *http.Client
	url    string
}

func NewReportingClient(url string) *ReportingClient {
	return NewReportingClientFromExisting(&http.Client{}, url)
}

func NewReportingClientFromExisting(client *http.Client, url string) *ReportingClient {
	return &ReportingClient{
		client: client,
		url:    url,
	}
}

func (c *ReportingClient) doRequest(ctx context.Context, path string, out interface{}) error {
	req, err := http.NewRequest(http.MethodGet, c.url+path, nil)
	if err != nil {
		return errors.Wrap(err, "problem building request")
	}

	req = req.WithContext(ctx)
	resp, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "error processing request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("found '%s' for request to '%s' on '%s'",
			http.StatusText(resp.StatusCode), path, c.url)
	}

	if err = gimlet.GetJSON(resp.Body, out); err != nil {
		return errors.Wrap(err, "problem reading response")
	}

	return nil
}

func (c *ReportingClient) JobStatus(ctx context.Context, filter reporting.CounterFilter) (*reporting.JobStatusReport, error) {
	out := &reporting.JobStatusReport{}

	if err := c.doRequest(ctx, "/status/"+string(filter), out); err != nil {
		return nil, errors.WithStack(err)
	}

	return out, nil
}

func (c *ReportingClient) RecentTiming(ctx context.Context, dur time.Duration, filter reporting.RuntimeFilter) (*reporting.JobRuntimeReport, error) {
	out := &reporting.JobRuntimeReport{}

	path := fmt.Sprintf("/timing/%s/%d", string(filter), int64(dur.Round(time.Second).Seconds()))

	if err := c.doRequest(ctx, path, out); err != nil {
		return nil, errors.Wrap(err, "problem with request")
	}

	return out, nil
}

func (c *ReportingClient) JobIDsByState(ctx context.Context, jobType string, filter reporting.CounterFilter) (*reporting.JobReportIDs, error) {
	out := &reporting.JobReportIDs{}

	if err := c.doRequest(ctx, fmt.Sprintf("/status/%s/%s", string(filter), jobType), out); err != nil {
		return nil, errors.Wrap(err, "problem with request")
	}

	return out, nil
}

func (c *ReportingClient) RecentErrors(ctx context.Context, dur time.Duration, filter reporting.ErrorFilter) (*reporting.JobErrorsReport, error) {
	out := &reporting.JobErrorsReport{}

	path := fmt.Sprintf("/errors/%s/%d", string(filter), int64(dur.Round(time.Second).Seconds()))
	if err := c.doRequest(ctx, path, out); err != nil {
		return nil, errors.Wrap(err, "problem with request")
	}

	return out, nil
}

func (c *ReportingClient) RecentJobErrors(ctx context.Context, jobType string, dur time.Duration, filter reporting.
	ErrorFilter) (*reporting.JobErrorsReport, error) {
	out := &reporting.JobErrorsReport{}

	path := fmt.Sprintf("/errors/%s/%s/%d", string(filter), jobType, int64(dur.Round(time.Second).Seconds()))
	if err := c.doRequest(ctx, path, out); err != nil {
		return nil, errors.Wrap(err, "problem with request")
	}

	return out, nil
}
