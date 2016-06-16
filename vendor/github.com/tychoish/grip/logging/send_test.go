package logging

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/message"
	"github.com/tychoish/grip/send"
)

type GripInternalSuite struct {
	grip *Grip
	name string
	suite.Suite
}

func TestGripSuite(t *testing.T) {
	suite.Run(t, new(GripInternalSuite))
}

func (s *GripInternalSuite) SetupSuite() {
	s.name = "test"
	s.grip = NewGrip(s.name)
	s.Equal(s.grip.Name(), s.name)
}

func (s *GripInternalSuite) SetupTest() {
	s.grip.SetName(s.name)
	s.grip.SetSender(send.NewBootstrapLogger(s.grip.ThresholdLevel(), s.grip.DefaultLevel()))
}

func (s *GripInternalSuite) TestPanicSenderActuallyPanics() {
	// both of these are in anonymous functions so that the defers
	// cover the correct area.

	func() {
		// first make sure that the defualt send method doesn't panic
		defer func() {
			s.Nil(recover())
		}()

		s.grip.Sender().Send(s.grip.DefaultLevel(), message.NewLinesMessage("foo"))
	}()

	func() {
		// call a panic function with a recoverer set.
		defer func() {
			s.NotNil(recover())
		}()

		s.grip.sendPanic(s.grip.DefaultLevel(), message.NewLinesMessage("foo"))
	}()
}

func (s *GripInternalSuite) TestPanicSenderRespectsTThreshold() {
	s.True(level.Debug > s.grip.DefaultLevel())

	// test that there is a no panic if the message isn't "logabble"
	defer func() {
		s.Nil(recover())
	}()

	s.grip.sendPanic(level.Debug, message.NewLinesMessage("foo"))
}

func (s *GripInternalSuite) TestConditionalSend() {
	// because sink is an internal type (implementation of
	// sender,) and "GetMessage" isn't in the interface, though it
	// is exported, we can't pass the sink between functions.
	sink, err := send.NewInternalLogger(s.grip.ThresholdLevel(), s.grip.DefaultLevel())
	s.NoError(err)
	s.grip.SetSender(sink)

	msg := message.NewLinesMessage("foo")
	msgTwo := message.NewLinesMessage("bar")

	// when the conditional argument is true, it should work
	s.grip.conditionalSend(level.Emergency, true, msg)
	s.Equal(sink.GetMessage().Message, msg)

	// when the conditional argument is true, it should work, and the channel is fifo
	s.grip.conditionalSend(level.Emergency, false, msgTwo)
	s.grip.conditionalSend(level.Emergency, true, msg)
	s.Equal(sink.GetMessage().Message, msg)

	// change the order
	s.grip.conditionalSend(level.Emergency, true, msg)
	s.grip.conditionalSend(level.Emergency, false, msgTwo)
	s.Equal(sink.GetMessage().Message, msg)
}

func (s *GripInternalSuite) TestConditionalSendPanic() {
	sink, err := send.NewInternalLogger(s.grip.ThresholdLevel(), s.grip.DefaultLevel())
	s.NoError(err)
	s.grip.SetSender(sink)

	msg := message.NewLinesMessage("foo")

	// first if the conditional is false, it can't panic.
	s.NotPanics(func() {
		s.grip.conditionalSendPanic(level.Emergency, false, msg)
	})

	// next, if the conditional is true it should panic
	s.Panics(func() {
		s.grip.conditionalSendPanic(level.Emergency, true, msg)
	})
}

func (s *GripInternalSuite) TestConditionalSendFatalDoesNotExitIfNotLoggable() {
	msg := message.NewLinesMessage("foo")
	s.grip.conditionalSendFatal(s.grip.DefaultLevel(), false, msg)

	s.True(level.Debug > s.grip.DefaultLevel())
	s.grip.conditionalSendFatal(level.Debug, true, msg)
}

// This testing method uses the technique outlined in:
// http://stackoverflow.com/a/33404435 to test a function that exits
// since it's impossible to "catch" an os.Exit
func TestSendFatalExits(t *testing.T) {
	grip := NewGrip("test")
	if os.Getenv("SHOULD_CRASH") == "1" {
		grip.sendFatal(grip.DefaultLevel(), message.NewLinesMessage("foo"))
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestSendFatalExits")
	cmd.Env = append(os.Environ(), "SHOULD_CRASH=1")
	err := cmd.Run()
	if err == nil {
		t.Errorf("sendFatal should have exited 0, instead: %s", err.Error())
	}
}
