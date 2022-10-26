package queue

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"

	"github.com/mongodb/amboy"
)

// randomString returns a cryptographically random string.
func randomString(x int) string {
	b := make([]byte, x)
	_, _ = rand.Read(b) // nolint
	return hex.EncodeToString(b)
}

// addJobsSuffix adds the expected collection suffix for the non-grouped queue
// if it doesn't already have the suffix.
func addJobsSuffix(s string) string {
	if strings.HasSuffix(s, ".jobs") {
		return s
	}
	return s + ".jobs"
}

func trimJobsSuffix(s string) string {
	return strings.TrimSuffix(s, ".jobs")
}

// addGroupSuffix adds the expected collection suffix for a queue group if it
// doesn't already have the suffix.
func addGroupSuffix(s string) string {
	if strings.HasSuffix(s, ".group") {
		return s
	}
	return s + ".group"
}

func isDispatchable(stat amboy.JobStatusInfo, lockTimeout time.Duration) bool {
	if isStaleJob(stat, lockTimeout) {
		return true
	}
	if stat.Completed {
		return false
	}
	if stat.InProgress {
		return false
	}

	return true
}

func isStaleJob(stat amboy.JobStatusInfo, lockTimeout time.Duration) bool {
	return stat.InProgress && time.Since(stat.ModificationTime) > lockTimeout
}
