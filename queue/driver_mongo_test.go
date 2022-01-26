package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMongoDBOptions(t *testing.T) {
	t.Run("DefaultOptionsAreValid", func(t *testing.T) {
		opts := defaultMongoDBTestOptions()
		assert.NoError(t, opts.Validate())
	})
}
