package rest

import (
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/queue"
	"github.com/mongodb/amboy/registry"
	"github.com/pkg/errors"
	"github.com/tychoish/gimlet"
	"github.com/tychoish/grip"
	"golang.org/x/net/context"
)

// Service is used as a place holder for application state and configuration.
type Service struct {
	queue           amboy.Queue
	closer          context.CancelFunc
	registeredTypes []string
	app             *gimlet.APIApp
}

// NewService constructs a new service object. Use the Open() method
// to initialize the service, and the Run() method to start the
// service.
func NewService() *Service {
	service := &Service{}

	for name := range registry.JobTypeNames() {
		service.registeredTypes = append(service.registeredTypes, name)
	}

	return service
}

// Open populates the application and starts the underlying
// queue. This method sets and initializes a LocalLimitedSize queue
// implementation.
func (s *Service) Open(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	s.closer = cancel

	s.queue = queue.NewLocalLimitedSize(2, 256)
	grip.CatchAlert(s.queue.Start(ctx))

	s.addRoutes()
}

// Close releases resources (i.e. the queue) associated with the
// service.
func (s *Service) Close() {
	if s.closer != nil {
		s.closer()
	}
}

// Queue provides access to the underlying queue object for the service.
func (s *Service) Queue() amboy.Queue {
	return s.queue
}

// SetQueue allows callers to inject an alternate queue implementation.
func (s *Service) SetQueue(q amboy.Queue) error {
	if s.closer != nil {
		return errors.New("cannot set a new queue, Service is already open")
	}

	s.queue = q
	return nil
}

// App provides access to the gimplet.APIApp instance which builds and
// orchestrates the REST API. Use this method if you want to combine
// the routes in this Service with another service, or add additional
// routes to support other application functionality.
func (s *Service) App() *gimlet.APIApp {
	if s.app == nil {
		s.app = gimlet.NewApp()
		s.app.SetDefaultVersion(0)
	}

	return s.app
}

// Run starts the REST service. All errors are logged.
func (s *Service) Run() {
	grip.CatchAlert(s.App().Run())
}

func (s *Service) addRoutes() {
	app := s.App()

	app.AddRoute("/").Version(0).Get().Handler(s.Status)
	app.AddRoute("/status").Version(1).Get().Handler(s.Status)
	app.AddRoute("/job/create").Version(1).Post().Handler(s.Create)
	app.AddRoute("/job/status/{name}").Version(1).Get().Handler(s.JobStatus)
}
