package rest

import (
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/queue"
	"github.com/mongodb/amboy/registry"
	"github.com/tychoish/gimlet"
	"golang.org/x/net/context"
)

// Service is used as a place holder for application state and configuration.
// TODO: define application state and configuration as a component of
// this struct.
type Service struct {
	queue           amboy.Queue
	closer          context.CancelFunc
	registeredTypes []string
	app             *gimlet.APIApp
}

func NewService() *Service {
	service := &Service{}

	for name := range registry.JobTypeNames() {
		service.registeredTypes = append(service.registeredTypes, name)
	}

	return service
}

func (s *Service) Open(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	s.closer = cancel

	s.queue = queue.NewLocalLimitedSize(2, 256)
	s.queue.Start(ctx)

	s.addRoutes()
}

func (s *Service) Close() {
	if s.closer != nil {
		s.closer()
	}
}

func (s *Service) App() *gimlet.APIApp {
	if s.app == nil {
		s.app = gimlet.NewApp()
		s.app.SetDefaultVersion(0)
	}

	return s.app
}

func (s *Service) Run() {
	s.App().Run()
}

func (s *Service) addRoutes() {
	app := s.App()

	app.AddRoute("/").Version(0).Get().Handler(s.Status)
	app.AddRoute("/status").Version(1).Get().Handler(s.Status)
	app.AddRoute("/job/create").Version(1).Post().Handler(s.Create)
	app.AddRoute("/job/status/{name}").Version(1).Get().Handler(s.JobStatus)
}
