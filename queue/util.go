package queue

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

// RandomString returns a cryptographically random string.
func randomString(x int) string {
	b := make([]byte, x)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func AddJobsSuffix(s string) string {
	return s + ".jobs"
}

func TrimJobsSuffix(s string) string {
	return strings.TrimSuffix(s, ".jobs")
}
