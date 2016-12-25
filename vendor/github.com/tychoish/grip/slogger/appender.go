package slogger

import (
	"bytes"
	"os"
	"strings"

	"github.com/tychoish/grip/level"
	"github.com/tychoish/grip/send"
)

// Appender is the slogger equivalent of a send.Sender, and this
// provides the same public interface for Appenders, in terms of Grip's
// senders.

type Appender interface {
	Append(log *Log) error
}

func StdOutAppender() send.Sender {
	s, _ := send.NewStreamLogger("", os.Stdout, send.LevelInfo{Default: level.Debug, Threshold: level.Debug})
	return s
}

func StdErrAppender() send.Sender {
	s, _ := send.NewStreamLogger("", os.Stderr, send.LevelInfo{Default: level.Debug, Threshold: level.Debug})
	return s
}

func DevNullAppender() (send.Sender, error) {
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		return nil, err
	}

	return send.NewStreamLogger("", devNull, send.LevelInfo{Default: level.Debug, Threshold: level.Debug})
}

type stringAppender struct {
	buf *bytes.Buffer
}

func (s stringAppender) WriteString(str string) (int, error) {
	if !strings.HasSuffix(str, "\n") {
		str += "\n"
	}
	return s.buf.WriteString(str)
}

func NewStringAppender(buffer *bytes.Buffer) send.Sender {
	s, _ := send.NewStreamLogger("", stringAppender{buffer}, send.LevelInfo{Default: level.Debug, Threshold: level.Debug})
	return s
}

func LevelFilter(threshold Level, sender send.Sender) send.Sender {
	l := sender.Level()
	l.Threshold = threshold.Priority()
	sender.SetLevel(l)

	return sender
}

type SenderAppender struct {
	send.Sender
}

func (s SenderAppender) Append(log *Log) error {
	s.Send(log)

	return nil
}
