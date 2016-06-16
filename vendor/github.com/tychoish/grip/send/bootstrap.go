package send

import (
	"fmt"
	"sync"

	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/message"
)

type bootstrapLogger struct {
	defaultLevel   level.Priority
	thresholdLevel level.Priority
	*sync.RWMutex
}

// NewBootstrapLogger returns a minimal, default composer
// implementation, used by the Journaler instances, for storing basic
// threhsold level configuration during journaler creation. Not
// functional as a sender for general use.
func NewBootstrapLogger(thresholdLevel, defaultLevel level.Priority) Sender {
	b := &bootstrapLogger{}
	b.RWMutex = &sync.RWMutex{}
	err := b.SetDefaultLevel(defaultLevel)
	if err != nil {
		fmt.Println(err.Error())
	}

	err = b.SetThresholdLevel(thresholdLevel)
	if err != nil {
		fmt.Println(err.Error())
	}

	return b
}

func (b *bootstrapLogger) Name() string {
	return "bootstrap"
}

func (b *bootstrapLogger) Send(_ level.Priority, _ message.Composer) {
	return
}

func (b *bootstrapLogger) SetName(_ string) {
	return
}

func (b *bootstrapLogger) AddOption(_, _ string) {
	return
}

func (b *bootstrapLogger) SetDefaultLevel(l level.Priority) error {
	b.Lock()
	defer b.Unlock()
	if level.IsValidPriority(l) {
		b.defaultLevel = l
		return nil
	}

	return fmt.Errorf("%s (%d) is not a valid priority value (0-6)", l, int(l))
}

func (b *bootstrapLogger) SetThresholdLevel(l level.Priority) error {
	b.Lock()
	defer b.Unlock()

	if level.IsValidPriority(l) {
		b.thresholdLevel = l
		return nil
	}

	return fmt.Errorf("%s (%d) is not a valid priority value (0-6)", l, int(l))
}

func (b *bootstrapLogger) DefaultLevel() level.Priority {
	b.RLock()
	defer b.RUnlock()

	return b.defaultLevel
}

func (b *bootstrapLogger) ThresholdLevel() level.Priority {
	b.RLock()
	defer b.RUnlock()

	return b.thresholdLevel
}

func (b *bootstrapLogger) Close() {
	return
}

func (b *bootstrapLogger) Type() SenderType {
	return Bootstrap
}
