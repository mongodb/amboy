package send

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/coreos/go-systemd/journal"
	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/message"
)

type systemdJournal struct {
	name     string
	level    LevelInfo
	options  map[string]string
	fallback *log.Logger

	sync.RWMutex
}

// NewJournaldLogger creates a Sender object that writes log messages
// to the system's systemd journald logging facility. If there's an
// error with the sending to the journald, messages fallback to
// writing to standard output.
func NewJournaldLogger(name string, thresholdLevel, defaultLevel level.Priority) (Sender, error) {
	s := &systemdJournal{
		name:    name,
		options: make(map[string]string),
	}

	err := s.SetDefaultLevel(defaultLevel)
	if err != nil {
		return s, err
	}

	err = s.SetThresholdLevel(thresholdLevel)
	if err != nil {
		return s, err
	}

	s.createFallback()
	return s, nil
}

func (s *systemdJournal) Name() string {
	return s.name
}

func (s *systemdJournal) createFallback() {
	s.fallback = log.New(os.Stdout, strings.Join([]string{"[", s.name, "] "}, ""), log.LstdFlags)
}

func (s *systemdJournal) SetName(name string) {
	s.name = name
	s.createFallback()
}

func (s *systemdJournal) Send(p level.Priority, m message.Composer) {
	if !GetMessageInfo(s.level, p, m).ShouldLog() {
		return
	}

	msg := m.Resolve()
	err := journal.Send(msg, s.convertPrioritySystemd(p), s.options)
	if err != nil {
		s.fallback.Println("systemd journaling error:", err.Error())
		s.fallback.Printf("[p=%s]: %s\n", p, msg)
	}
}

func (s *systemdJournal) SetDefaultLevel(p level.Priority) error {
	s.Lock()
	defer s.Unlock()

	if level.IsValidPriority(p) {
		s.level.defaultLevel = p
		return nil
	}

	return fmt.Errorf("%s (%d) is not a valid priority value (0-6)", p, (p))
}

func (s *systemdJournal) SetThresholdLevel(p level.Priority) error {
	s.Lock()
	defer s.Unlock()

	if level.IsValidPriority(p) {
		s.level.thresholdLevel = p
		return nil
	}

	return fmt.Errorf("%s (%d) is not a valid priority value (0-6)", p, (p))
}

func (s *systemdJournal) DefaultLevel() level.Priority {
	s.RLock()
	defer s.RUnlock()

	return s.level.defaultLevel
}

func (s *systemdJournal) ThresholdLevel() level.Priority {
	s.RLock()
	defer s.RUnlock()

	return s.level.thresholdLevel
}

func (s *systemdJournal) AddOption(key, value string) {
	s.options[key] = value
}

func (s *systemdJournal) convertPrioritySystemd(p level.Priority) journal.Priority {
	switch {
	case p == level.Emergency:
		return journal.PriEmerg
	case p == level.Alert:
		return journal.PriAlert
	case p == level.Critical:
		return journal.PriCrit
	case p == level.Error:
		return journal.PriErr
	case p == level.Warning:
		return journal.PriWarning
	case p == level.Notice:
		return journal.PriNotice
	case p == level.Info:
		return journal.PriInfo
	case p == level.Debug:
		return journal.PriDebug
	default:
		return s.convertPrioritySystemd(s.level.defaultLevel)
	}
}

func (s *systemdJournal) Close() {
	return
}

func (s *systemdJournal) Type() SenderType {
	return Systemd
}
