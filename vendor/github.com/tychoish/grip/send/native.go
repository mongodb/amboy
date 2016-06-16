package send

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/message"
)

type nativeLogger struct {
	name           string
	defaultLevel   level.Priority
	thresholdLevel level.Priority
	options        map[string]string
	logger         *log.Logger
	template       string

	*sync.RWMutex
}

// NewNativeLogger creates a new Sender interface that writes all
// loggable messages to a standard output logger that uses Go's
// standard library logging system.
func NewNativeLogger(name string, thresholdLevel, defaultLevel level.Priority) (Sender, error) {
	l := &nativeLogger{
		name:     name,
		template: "[p=%d]: %s\n",
		RWMutex:  &sync.RWMutex{},
	}
	l.createLogger()
	err := l.SetDefaultLevel(defaultLevel)
	if err != nil {
		return l, err
	}

	err = l.SetThresholdLevel(thresholdLevel)

	return l, err
}

func (n *nativeLogger) createLogger() {
	n.logger = log.New(os.Stdout, strings.Join([]string{"[", n.name, "] "}, ""), log.LstdFlags)
}

func (n *nativeLogger) Send(p level.Priority, m message.Composer) {
	if !ShouldLogMessage(n, p, m) {
		return
	}

	n.logger.Printf(n.template, int(p), m.Resolve())
}

func (n *nativeLogger) Name() string {
	return n.name
}

func (n *nativeLogger) SetName(name string) {
	n.name = name
	n.createLogger()
}

func (n *nativeLogger) AddOption(key, value string) {
	n.options[key] = value
}

func (n *nativeLogger) DefaultLevel() level.Priority {
	n.RLock()
	defer n.RUnlock()

	return n.defaultLevel
}

func (n *nativeLogger) ThresholdLevel() level.Priority {
	n.RLock()
	defer n.RUnlock()

	return n.thresholdLevel
}

func (n *nativeLogger) SetDefaultLevel(p level.Priority) error {
	n.Lock()
	defer n.Unlock()

	if level.IsValidPriority(p) {
		n.defaultLevel = p
		return nil
	}

	return fmt.Errorf("%s (%d) is not a valid priority value (0-6)", p, int(p))
}

func (n *nativeLogger) SetThresholdLevel(p level.Priority) error {
	n.Lock()
	defer n.Unlock()

	if level.IsValidPriority(p) {
		n.thresholdLevel = p
		return nil
	}

	return fmt.Errorf("%s (%d) is not a valid priority value (0-6)", p, int(p))
}

func (n *nativeLogger) Close() {
	return
}

func (n *nativeLogger) Type() SenderType {
	return Native
}
