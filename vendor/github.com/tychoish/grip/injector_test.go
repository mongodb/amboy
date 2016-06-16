package grip

import (
	"os"

	"github.com/tychoish/grip/send"
)

func (s *GripSuite) TestSenderGetterReturnsExpectedJournaler() {
	grip := NewJournaler("sender_swap")
	s.Equal(grip.Name(), "sender_swap")
	s.Equal(grip.Sender().Name(), "bootstrap")

	err := grip.UseNativeLogger()
	s.NoError(err)

	s.Equal(grip.Name(), "sender_swap")
	s.NotEqual(grip.Sender().Name(), "bootstrap")
	ns, _ := send.NewNativeLogger("native_sender", s.grip.Sender().ThresholdLevel(), s.grip.Sender().DefaultLevel())
	defer ns.Close()
	s.IsType(grip.Sender(), ns)

	err = grip.UseFileLogger("foo")
	s.NoError(err)

	defer func() { std.CatchError(os.Remove("foo")) }()

	s.Equal(grip.Name(), "sender_swap")
	s.NotEqual(grip.Sender(), ns)
	fs, _ := send.NewFileLogger("file_sender", "foo", s.grip.Sender().ThresholdLevel(), s.grip.Sender().DefaultLevel())
	defer fs.Close()
	s.IsType(grip.Sender(), fs)
}
