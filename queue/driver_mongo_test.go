package queue

import (
	"testing"
	"time"

	"github.com/mongodb/amboy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestMongoDBOptions(t *testing.T) {
	client, err := mongo.NewClient(options.Client().ApplyURI(defaultMongoDBURI))
	require.NoError(t, err)

	t.Run("Validate", func(t *testing.T) {
		t.Run("SucceedsWithDefaultOptions", func(t *testing.T) {
			opts := defaultMongoDBTestOptions()
			assert.NoError(t, opts.Validate())
		})
		t.Run("SucceedsWithURIButMissingClient", func(t *testing.T) {
			opts := defaultMongoDBTestOptions()
			opts.Client = nil
			opts.URI = defaultMongoDBURI
			assert.NoError(t, opts.Validate())
		})
		t.Run("SucceedsWithClientButMissingURI", func(t *testing.T) {
			opts := defaultMongoDBTestOptions()
			opts.Client = client
			opts.URI = ""
			assert.NoError(t, opts.Validate())
		})
		t.Run("FailsWithMissingURIAndClient", func(t *testing.T) {
			opts := defaultMongoDBTestOptions()
			opts.Client = nil
			opts.URI = ""
			assert.Error(t, opts.Validate())
		})
		t.Run("FailsWithMissingCollection", func(t *testing.T) {
			opts := defaultMongoDBTestOptions()
			opts.Collection = ""
			assert.Error(t, opts.Validate())
		})
		t.Run("FailsWithInvalidSampleSize", func(t *testing.T) {
			opts := defaultMongoDBTestOptions()
			opts.SampleSize = -50
			assert.Error(t, opts.Validate())
		})
		t.Run("FailsWithInvalidLockTimeout", func(t *testing.T) {
			opts := defaultMongoDBTestOptions()
			opts.LockTimeout = -time.Minute
			assert.Error(t, opts.Validate())
		})
		t.Run("SetsDefaultLockTimeout", func(t *testing.T) {
			opts := defaultMongoDBTestOptions()
			opts.LockTimeout = 0
			assert.NoError(t, opts.Validate())
			assert.Equal(t, amboy.LockTimeout, opts.LockTimeout)
		})
		t.Run("FailsWhenUsingGroupsWithoutGroupName", func(t *testing.T) {
			opts := defaultMongoDBTestOptions()
			opts.UseGroups = true
			opts.GroupName = ""
			assert.Error(t, opts.Validate())
		})
	})
}
