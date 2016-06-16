package grip

import (
	"os"
	"runtime"
	"strings"
)

var std = NewJournaler("grip")

func init() {
	if !strings.Contains(os.Args[0], "go-build") {
		std.SetName(os.Args[0])
	}

	if ev := os.Getenv("GRIP_USE_STDOUT"); ev != "" {
		err := std.UseNativeLogger()
		std.CatchAlert(err)
	} else if ev := os.Getenv("GRIP_USE_JOURNALD"); ev != "" {
		err := std.UseSystemdLogger()
		std.CatchAlert(err)
	}

	if std.Sender().Name() == "bootstrap" {
		if runtime.GOOS == "linux" {
			err := std.UseSystemdLogger()
			if err != nil {
				// native logger can't/shouldn't throw
				// and there's no good fallback
				_ = std.UseNativeLogger()
			}
		} else {
			_ = std.UseNativeLogger()
		}
	}
}
