package send

import "github.com/tychoish/grip/message"

// this file contains tools to support the slogger interface

type WriteStringer interface {
	WriteString(str string) (int, error)
}

type streamLogger struct {
	fobj WriteStringer
	*base
}

func NewStreamLogger(name string, ws WriteStringer, l LevelInfo) (Sender, error) {
	s := MakeStreamLogger(ws)

	if err := s.SetLevel(l); err != nil {
		return nil, err
	}

	s.SetName(name)

	return s, nil
}

func MakeStreamLogger(ws WriteStringer) Sender {
	return &streamLogger{
		fobj: ws,
		base: newBase(""),
	}
}

func (s *streamLogger) Type() SenderType { return Stream }
func (s *streamLogger) Send(m message.Composer) {
	if s.level.ShouldLog(m) {
		_, _ = s.fobj.WriteString(m.Resolve())
	}
}
