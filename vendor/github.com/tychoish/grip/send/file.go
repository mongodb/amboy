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

type fileLogger struct {
	name  string
	level LevelInfo

	options  map[string]string
	template string

	logger  *log.Logger
	fileObj *os.File
	*sync.RWMutex
}

// NewFileLogger creates a Sender implementation that writes log
// output to a file. Returns an error but falls back to a standard
// output logger if there's problems with the file. Internally using
// the go standard library logging system.
func NewFileLogger(name, filePath string, thresholdLevel, defaultLevel level.Priority) (Sender, error) {
	l := &fileLogger{
		name:     name,
		options:  make(map[string]string),
		template: "[p=%d]: %s\n",
		RWMutex:  &sync.RWMutex{},
	}

	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		l, _ := NewNativeLogger(name, thresholdLevel, defaultLevel)
		return l, fmt.Errorf("error opening logging file, %s, falling back to stdOut logging", err.Error())
	}
	l.fileObj = f
	l.createLogger()

	err = l.SetDefaultLevel(defaultLevel)
	if err != nil {
		return l, err
	}

	err = l.SetThresholdLevel(thresholdLevel)
	return l, err
}

func (f *fileLogger) createLogger() {
	f.logger = log.New(f.fileObj, strings.Join([]string{"[", f.name, "] "}, ""), log.LstdFlags)
}

func (f *fileLogger) Close() {
	_ = f.fileObj.Close()
}

func (f *fileLogger) Send(p level.Priority, m message.Composer) {
	if !GetMessageInfo(f.level, p, m).ShouldLog() {
		return
	}

	f.logger.Printf(f.template, int(p), m.Resolve())
}

func (f *fileLogger) Name() string {
	return f.name
}

func (f *fileLogger) SetName(name string) {
	f.name = name
	f.createLogger()
}

func (f *fileLogger) AddOption(key, value string) {
	f.Lock()
	defer f.Unlock()

	f.options[key] = value
}

func (f *fileLogger) DefaultLevel() level.Priority {
	f.RLock()
	defer f.RUnlock()

	return f.level.defaultLevel
}

func (f *fileLogger) ThresholdLevel() level.Priority {
	f.RLock()
	defer f.RUnlock()

	return f.level.thresholdLevel
}

func (f *fileLogger) SetDefaultLevel(p level.Priority) error {
	f.Lock()
	defer f.Unlock()

	if level.IsValidPriority(p) {
		f.level.defaultLevel = p
		return nil
	}

	return fmt.Errorf("%s (%d) is not a valid priority value (0-6)", p, int(p))
}

func (f *fileLogger) SetThresholdLevel(p level.Priority) error {
	f.Lock()
	defer f.Unlock()

	if level.IsValidPriority(p) {
		f.level.thresholdLevel = p
		return nil
	}

	return fmt.Errorf("%s (%d) is not a valid priority value (0-6)", p, int(p))
}

func (f *fileLogger) Type() SenderType {
	return File
}
