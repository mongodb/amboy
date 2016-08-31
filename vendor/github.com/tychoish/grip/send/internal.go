package send

import (
	"fmt"
	"sync"

	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/message"
)

// InternalSender implements a Sender object that makes it possible to
// access logging messages, in the InternalMessage format without
// logging to an output method. The Send method does not filter out
// under-priority and unloggable messages. Used  for testing
// purposes.
type InternalSender struct {
	name    string
	options map[string]string
	level   LevelInfo
	output  chan *InternalMessage

	sync.RWMutex
}

// InternalMessage provides a complete representation of all
// information associated with a logging event.
type InternalMessage struct {
	Message  message.Composer
	Priority level.Priority
	Logged   bool
	Rendered string
}

// NewInternalLogger creates and returns a Sender implementation that
// does not log messages, but converts them to the InternalMessage
// format and puts them into an internal channel, that allows you to
// access the massages via the extra "GetMessage" method. Useful for
// testing.
func NewInternalLogger(thresholdLevel, defaultLevel level.Priority) (*InternalSender, error) {
	l := &InternalSender{
		output:  make(chan *InternalMessage, 100),
		options: make(map[string]string),
	}

	err := l.SetDefaultLevel(defaultLevel)
	if err != nil {
		return l, err
	}

	err = l.SetThresholdLevel(thresholdLevel)
	return l, err
}

func (s *InternalSender) GetMessage() *InternalMessage {
	return <-s.output
}

func (s *InternalSender) Send(p level.Priority, m message.Composer) {
	o := &InternalMessage{
		Message:  m,
		Priority: p,
		Rendered: m.Resolve(),
		Logged:   GetMessageInfo(s.level, p, m).ShouldLog(),
	}

	s.output <- o
}

func (s *InternalSender) Name() string {
	return s.name
}

func (s *InternalSender) SetName(n string) {
	s.name = n
}

func (s *InternalSender) ThresholdLevel() level.Priority {
	s.RLock()
	defer s.RUnlock()

	return s.level.thresholdLevel
}

func (s *InternalSender) SetThresholdLevel(p level.Priority) error {
	s.Lock()
	defer s.Unlock()

	if level.IsValidPriority(p) {
		s.level.thresholdLevel = p
		return nil
	}
	return fmt.Errorf("%s (%d) is not a valid priority value (0-6)", p, int(p))
}

func (s *InternalSender) DefaultLevel() level.Priority {
	s.RLock()
	defer s.RUnlock()

	return s.level.defaultLevel
}

func (s *InternalSender) SetDefaultLevel(p level.Priority) error {
	s.Lock()
	defer s.Unlock()

	if level.IsValidPriority(p) {
		s.level.defaultLevel = p
		return nil
	}
	return fmt.Errorf("%s (%d) is not a valid priority value (0-6)", p, int(p))

}

func (s *InternalSender) AddOption(key, value string) {
	s.Lock()
	defer s.Unlock()

	s.options[key] = value
}

func (s *InternalSender) Close() {
	close(s.output)
}

func (s *InternalSender) Type() SenderType {
	return Internal
}
