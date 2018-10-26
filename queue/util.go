package queue

import (
	"crypto/rand"
	"encoding/hex"
)

// RandomString returns a cryptographically random string.
func RandomString(x int) string {
	b := make([]byte, x)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
