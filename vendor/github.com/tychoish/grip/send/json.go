package send

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/tychoish/grip/message"
)

type jsonLogger struct {
	logger *log.Logger
	closer func() error
	*base
}

func NewJSONConsoleLogger(name string, l LevelInfo) (Sender, error) {
	s := MakeJSONConsoleLogger()
	if err := s.SetLevel(l); err != nil {
		return nil, err
	}

	s.SetName(name)

	return s, nil
}

func MakeJSONConsoleLogger() Sender {
	s := &jsonLogger{
		base:   newBase(""),
		closer: func() error { return nil },
	}

	s.reset = func() {
		s.logger = log.New(os.Stdout, "", 0)
	}

	return s
}

// Log-to-file constructors

func NewJSONFileLogger(name, file string, l LevelInfo) (Sender, error) {
	s, err := MakeJSONFileLogger(file)
	if err != nil {
		return nil, err
	}

	if err := s.SetLevel(l); err != nil {
		return nil, err
	}

	s.SetName(name)

	return s, nil
}

func MakeJSONFileLogger(file string) (Sender, error) {
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("error opening logging file, %s", err.Error())
	}

	s := &jsonLogger{
		base: newBase(""),
		closer: func() error {
			return f.Close()
		},
	}

	s.reset = func() {
		s.logger = log.New(f, "", 0)
	}

	return s, nil
}

// Implementation of required methods not implemented in BASE

func (s *jsonLogger) Type() SenderType { return Json }
func (s *jsonLogger) Send(m message.Composer) {
	if s.level.ShouldLog(m) {
		out, err := json.Marshal(m.Raw())
		if err != nil {
			errMsg, _ := json.Marshal(message.NewError(err).Raw())
			s.logger.Println(errMsg)

			out, err = json.Marshal(message.NewDefaultMessage(m.Priority(), m.Resolve()).Raw())
			if err == nil {
				s.logger.Println(string(out))
			}

			return
		}

		s.logger.Println(string(out))
	}
}
