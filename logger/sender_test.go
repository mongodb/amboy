package logger

import (
	"context"
	"testing"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/queue"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/level"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/send"
	"github.com/stretchr/testify/suite"
)

// this suite is lifted directly from the grip sender's test suite
type SenderSuite struct {
	senders  map[string]send.Sender
	mock     *send.InternalSender
	queue    amboy.Queue
	canceler context.CancelFunc
	suite.Suite
}

func TestSenderSuite(t *testing.T) {
	suite.Run(t, new(SenderSuite))
}

func (s *SenderSuite) SetupTest() {
	l := send.LevelInfo{Default: level.Info, Threshold: level.Notice}

	ctx := context.Background()

	ctx, s.canceler = context.WithCancel(ctx)
	var err error
	s.mock, err = send.NewInternalLogger("internal", l)
	s.Require().NoError(err)
	s.senders = map[string]send.Sender{}

	s.senders["single"], err = NewQueueBackedSender(ctx, s.mock, 2, 128)
	s.Require().NoError(err)

	s.queue = queue.NewLocalLimitedSize(4, 128)
	s.NoError(s.queue.Start(ctx))
	s.Require().True(s.queue.Info().Started)

	s.senders["single-shared"] = MakeQueueSender(ctx, s.queue, s.mock)
}

func (s *SenderSuite) TearDownTest() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	amboy.WaitInterval(ctx, s.queue, 100*time.Millisecond)

	for _, sender := range s.senders {
		s.NoError(sender.Close())
	}
	s.NoError(s.mock.Close())
	if s.canceler != nil {
		s.canceler()
	}
}

func (s *SenderSuite) TearDownSuite() {
	for _, sender := range s.senders {
		s.NoError(sender.Close())
	}
}

func (s *SenderSuite) TestSenderImplementsInterface() {
	// this actually won't catch the error; the compiler will in
	// the fixtures, but either way we need to make sure that the
	// tests actually enforce this.
	for name, sender := range s.senders {
		s.Implements((*send.Sender)(nil), sender, name)
	}
}

func (s *SenderSuite) TestNameSetterRoundTrip() {
	for n, sender := range s.senders {
		for i := 0; i < 100; i++ {
			name := utility.MakeRandomString(6)
			s.NotEqual(sender.Name(), name, n)
			sender.SetName(name)
			s.Equal(sender.Name(), name, n)
		}
	}
}

func (s *SenderSuite) TestLevelSetterRejectsInvalidSettings() {
	levels := []send.LevelInfo{
		{Default: level.Invalid, Threshold: level.Invalid},
		{Default: level.Priority(-10), Threshold: level.Priority(-1)},
		{Default: level.Debug, Threshold: level.Priority(-1)},
		{Default: level.Priority(800), Threshold: level.Priority(-2)},
	}

	for n, sender := range s.senders {
		s.NoError(sender.SetLevel(send.LevelInfo{Default: level.Debug, Threshold: level.Alert}))
		for _, l := range levels {
			s.True(sender.Level().Valid(), n)
			s.False(l.Valid(), n)
			s.Error(sender.SetLevel(l), n)
			s.True(sender.Level().Valid(), n)
			s.NotEqual(sender.Level(), l, n)
		}

	}
}

func (s *SenderSuite) TestFlush() {
	for t, sender := range s.senders {
		for i := 0; i < 10; i++ {
			sender.Send(message.ConvertToComposer(level.Error, "message"))
		}
		s.Require().NoError(sender.Flush(context.Background()), t)
		for i := 0; i < 10; i++ {
			m, ok := s.mock.GetMessageSafe()
			s.Require().True(ok, t)
			s.Equal("message", m.Message.String(), t)
		}
	}
}

func (s *SenderSuite) TestCloserShouldUsusallyNoop() {
	for t, sender := range s.senders {
		s.NoError(sender.Close(), t)
	}
}

func (s *SenderSuite) TestBasicNoopSendTest() {
	for name, sender := range s.senders {
		grip.Info(name)
		for i := -10; i <= 110; i += 5 {
			m := message.NewDefaultMessage(level.Priority(i), "hello world! "+utility.MakeRandomString(5))
			sender.Send(m)
		}
	}
}
