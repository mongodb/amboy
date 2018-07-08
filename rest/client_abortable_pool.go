package rest

import (
	"context"
	"net/http"

	"github.com/evergreen-ci/gimlet"
	"github.com/pkg/errors"
)

type ManagementClient struct {
	client *http.Client
	url    string
}

func NewManagementClient(url string) *ManagementClient {
	return NewManagementClientFromExisting(&http.Client{}, url)
}

func NewManagementClientFromExisting(client *http.Client, url string) *ManagementClient {
	return &ManagementClient{
		client: client,
		url:    url,
	}
}

func (c *ManagementClient) ListJobs(ctx context.Context) ([]string, error) {
	req, err := http.NewRequest(http.MethodGet, c.url+"/v1/jobs/list", nil)
	if err != nil {
		return nil, errors.Wrap(err, "problem building request")
	}

	req = req.WithContext(ctx)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "error processing request")
	}
	defer resp.Body.Close()
	out := []string{}
	if err = gimlet.GetJSON(resp.Body, out); err != nil {
		return nil, errors.Wrap(err, "problem reading response")
	}

	return out, nil
}

func (c *ManagementClient) AbortAllJobs(ctx context.Context) error {
	req, err := http.NewRequest(http.MethodDelete, c.url+"/v1/jobs/abort", nil)
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
		return errors.New("failed to abort jobs")
	}

	return nil
}

func (c *ManagementClient) IsRunning(ctx context.Context, job string) (bool, error) {
	req, err := http.NewRequest(http.MethodGet, c.url+"/v1/jobs/"+job, nil)
	if err != nil {
		return false, errors.Wrap(err, "problem building request")
	}

	req = req.WithContext(ctx)
	resp, err := c.client.Do(req)
	if err != nil {
		return false, errors.Wrap(err, "error processing request")
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}

	return true, nil
}

func (c *ManagementClient) AbortJob(ctx context.Context, job string) error {
	req, err := http.NewRequest(http.MethodDelete, c.url+"/v1/jobs/"+job, nil)
	if err != nil {
		return errors.Wrap(err, "problem building request")
	}

	req = req.WithContext(ctx)
	resp, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "error processing request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		rerr := &gimlet.ErrorResponse{}
		if err := gimlet.GetJSON(resp.Body, rerr); err != nil {
			return errors.Wrapf(err, "problem reading error response with %s",
				http.StatusText(resp.StatusCode))

		}
		return errors.Wrap(rerr, "remove server returned error")
	}

	return nil
}
