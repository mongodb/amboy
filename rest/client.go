package rest

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/registry"
	"github.com/pkg/errors"
	"github.com/tychoish/gimlet"
	"github.com/tychoish/grip"
	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"
)

const (
	defaultClientPort int = 3000
	maxClientPort         = 65535
)

// Client provides an interface for interacting with a remote amboy
// Service.
type Client struct {
	host   string
	prefix string
	port   int
	client *http.Client
}

// NewClient takes host, port, and URI prefix information and
// constructs a new Client.
func NewClient(host string, port int, prefix string) (*Client, error) {
	c := &Client{client: &http.Client{}}

	return c.initClient(host, port, prefix)
}

// NewClientFromExisting takes an existing http.Client object and
// produces a new Client object.
func NewClientFromExisting(client *http.Client, host string, port int, prefix string) (*Client, error) {
	if client == nil {
		return nil, errors.New("must use a non-nil existing client")
	}

	c := &Client{client: client}

	return c.initClient(host, port, prefix)
}

// Copy takes an existing Client object and returns a new client
// object with the same settings that uses a *new* http.Client.
func (c *Client) Copy() *Client {
	new := &Client{}
	*new = *c
	new.client = &http.Client{}

	return new
}

func (c *Client) initClient(host string, port int, prefix string) (*Client, error) {
	var err error

	err = c.SetHost(host)
	if err != nil {
		return nil, err
	}

	err = c.SetPort(port)
	if err != nil {
		return nil, err
	}

	err = c.SetPrefix(prefix)
	if err != nil {
		return nil, err
	}

	return c, nil
}

////////////////////////////////////////////////////////////////////////
//
// Configuration Interface
//
////////////////////////////////////////////////////////////////////////

// Client returns a pointer to embedded http.Client object.
func (c *Client) Client() *http.Client {
	return c.client
}

// SetHost allows callers to change the hostname (including leading
// "http(s)") for the Client. Returns an error if the specified host
// does not start with "http".
func (c *Client) SetHost(h string) error {
	if !strings.HasPrefix(h, "http") {
		return errors.Errorf("host '%s' is malformed. must start with 'http'", h)
	}

	if strings.HasSuffix(h, "/") {
		h = h[:len(h)-1]
	}

	c.host = h

	return nil
}

// Host returns the current host.
func (c *Client) Host() string {
	return c.host
}

// SetPort allows callers to change the port used for the client. If
// the port is invalid, returns an error and sets the port to the
// default value. (3000)
func (c *Client) SetPort(p int) error {
	if p <= 0 || p >= maxClientPort {
		c.port = defaultClientPort
		return errors.Errorf("cannot set the port to %d, using %d instead", p, defaultClientPort)
	}

	c.port = p
	return nil
}

// Port returns the current port value for the Client.
func (c *Client) Port() int {
	return c.port
}

// SetPrefix allows callers to modify the prefix, for this client,
func (c *Client) SetPrefix(p string) error {
	c.prefix = strings.Trim(p, "/")
	return nil
}

// Prefix accesses the prefix for the client, The prefix is the part
// of the URI between the end-point and the hostname, of the API.
func (c *Client) Prefix() string {
	return c.prefix
}

func (c *Client) getURL(endpoint string) string {
	var url []string

	if c.port == 80 || c.port == 0 {
		url = append(url, c.host)
	} else {
		url = append(url, fmt.Sprintf("%s:%d", c.host, c.port))
	}

	if c.prefix != "" {
		url = append(url, c.prefix)
	}

	if endpoint = strings.Trim(endpoint, "/"); endpoint != "" {
		url = append(url, endpoint)
	}

	return strings.Join(url, "/")
}

////////////////////////////////////////////////////////////////////////
//
// Operations that Interact with the Remote API.
//
////////////////////////////////////////////////////////////////////////

func (c *Client) getStats(ctx context.Context) (*status, error) {
	resp, err := ctxhttp.Get(ctx, c.client, c.getURL("/v1/status"))

	if err != nil {
		return nil, err
	}

	s := &status{}
	err = gimlet.GetJSON(resp.Body, s)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// Running is true when the underlying queue is running and accepting
// jobs, and false when the queue is not runner or if there's a
// problem connecting to the queue.
func (c *Client) Running(ctx context.Context) (bool, error) {
	s, err := c.getStats(ctx)
	if err != nil {
		return false, err
	}

	return s.QueueRunning, nil
}

// PendingJobs reports on the total number of jobs currently dispatched
// by the queue to workers.
func (c *Client) PendingJobs(ctx context.Context) (int, error) {
	s, err := c.getStats(ctx)
	if err != nil {
		return -1, err
	}

	return s.PendingJobs, nil
}

// SubmitJob adds a job to a remote queue connected to the rest interface.
func (c *Client) SubmitJob(ctx context.Context, j amboy.Job) (string, error) {
	ji, err := registry.MakeJobInterchange(j)
	if err != nil {
		return "", err
	}

	b, err := amboy.ConvertTo(amboy.JSON, ji)
	if err != nil {
		return "", err
	}

	resp, err := ctxhttp.Post(ctx, c.client, c.getURL("/v1/job/create"),
		"application/json", bytes.NewBuffer(b))
	if err != nil {
		return "", err
	}

	cjr := createResponse{}
	err = gimlet.GetJSON(resp.Body, &cjr)
	if err != nil {
		return "", err
	}

	if cjr.Error != "" {
		return "", errors.Errorf("service reported error: '%s'", cjr.Error)
	}

	return cjr.ID, nil
}

func (c *Client) jobStatus(ctx context.Context, name string) (*jobStatusResponse, error) {
	resp, err := ctxhttp.Get(ctx, c.client, c.getURL(fmt.Sprintf("/v1/job/status/%s", name)))
	if err != nil {
		return nil, err
	}

	s := &jobStatusResponse{}
	err = gimlet.GetJSON(resp.Body, s)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// JobComplete checks the stats of a job, by name, and returns true if
// that job is complete. When false, check the second return value to
// ensure that the job exists in the remote queue.
func (c *Client) JobComplete(ctx context.Context, name string) (bool, error) {
	st, err := c.jobStatus(ctx, name)
	if err != nil {
		return false, err
	}

	return st.Completed, nil
}

func waitForOutcome(ctx context.Context, outcome func() (bool, error)) bool {
	timer := time.NewTimer(0)
	factor := 0
	for {
		// TODO replace this interval with some exponential
		// backoff+jitter.
		if factor <= 50 {
			factor++
		}

		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			complete, err := outcome()
			if err != nil {
				grip.CatchError(err)
				timer.Reset(time.Duration(factor) * 100 * time.Millisecond)
				if factor < 10 {
					continue
				}
				return false
			}

			if !complete {
				timer.Reset(time.Duration(factor) * 100 * time.Millisecond)
				continue
			}

			return true
		}
	}
}

// Wait blocks until the job identified by the name argument is
// complete. Does not handle the case where a job does not exist.
func (c *Client) Wait(ctx context.Context, name string) bool {
	return waitForOutcome(ctx, func() (bool, error) {
		s, err := c.jobStatus(ctx, name)
		if err != nil || !s.Exists {
			return false, err
		}

		if !s.Completed {
			grip.Debugf("job %s is not completed", name)
			return false, nil
		}

		return true, nil
	})
}

// WaitAll waits for *all* pending jobs in the queue to complete.
func (c *Client) WaitAll(ctx context.Context) bool {
	return waitForOutcome(ctx, func() (bool, error) {
		s, err := c.getStats(ctx)
		if err != nil || !s.QueueRunning {
			return false, err
		}

		if s.PendingJobs != 0 {
			grip.Debugf("%d pending jobs, waiting", s.PendingJobs)
			return false, nil
		}

		return true, nil
	})
}
