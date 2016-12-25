package send

type SenderType int

const (
	Custom SenderType = iota
	Systemd
	Native
	Json
	Syslog
	Internal
	Multi
	File
	Slack
	Xmpp
	Stream
	Bootstrap
)

func (t SenderType) String() string {
	switch t {
	case Systemd:
		return "systemd"
	case Native:
		return "native"
	case Syslog:
		return "syslog"
	case Internal:
		return "internal"
	case File:
		return "file"
	case Bootstrap:
		return "bootstrap"
	case Custom:
		return "custom"
	case Slack:
		return "slack"
	case Xmpp:
		return "xmpp"
	case Json:
		return "json"
	case Stream:
		return "stream"
	case Multi:
		return "multi"
	default:
		return "native"
	}
}
