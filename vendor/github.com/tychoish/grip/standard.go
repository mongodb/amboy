package grip

import (
	"os"
	"strings"
)

var std = NewJournaler("grip")

func init() {
	if !strings.Contains(os.Args[0], "go-build") {
		std.SetName(os.Args[0])
	}

	std.CatchAlert(std.UseNativeLogger())
}
