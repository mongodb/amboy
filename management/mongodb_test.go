package management

import (
	"context"
	"testing"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/mongodb/amboy/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func defaultMongoDBTestOptions() queue.MongoDBOptions {
	opts := queue.DefaultMongoDBOptions()
	opts.DB = "amboy_test"
	opts.Collection = "test." + utility.RandomString()
	return opts
}

func TestMongoDBConstructors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := mongo.NewClient(options.Client().ApplyURI(defaultMongoDBTestOptions().URI).SetConnectTimeout(time.Second))
	require.NoError(t, err)
	require.NoError(t, client.Connect(ctx))

	t.Run("MissingClientShouldError", func(t *testing.T) {
		opts := defaultMongoDBTestOptions()
		conf := DBQueueManagerOptions{Options: opts}

		db, err := MakeDBQueueManager(ctx, conf)
		assert.Error(t, err)
		assert.Zero(t, db)
	})
	t.Run("InvalidDBOptionsShouldError", func(t *testing.T) {
		opts := defaultMongoDBTestOptions()
		opts.Collection = ""
		conf := DBQueueManagerOptions{Options: opts}

		db, err := MakeDBQueueManager(ctx, conf)
		assert.Error(t, err)
		assert.Zero(t, db)
	})
	t.Run("BuildNewConnector", func(t *testing.T) {
		opts := defaultMongoDBTestOptions()
		opts.Client = client
		opts.Collection = t.Name()
		conf := DBQueueManagerOptions{Options: opts}

		db, err := MakeDBQueueManager(ctx, conf)
		assert.NoError(t, err)
		assert.NotNil(t, db)

		r, ok := db.(*dbQueueManager)
		require.True(t, ok)
		require.NotNil(t, r)
		assert.NotZero(t, r.collection)
	})
	t.Run("DialWithNewConstructor", func(t *testing.T) {
		opts := defaultMongoDBTestOptions()
		opts.Collection = t.Name()
		conf := DBQueueManagerOptions{Options: opts}

		r, err := NewDBQueueManager(ctx, conf)
		assert.NoError(t, err)
		assert.NotNil(t, r)
	})
	t.Run("DialWithBadURI", func(t *testing.T) {
		opts := defaultMongoDBTestOptions()
		opts.Collection = t.Name()
		opts.URI = "mongodb://lochost:26016"
		conf := DBQueueManagerOptions{Options: opts}

		r, err := NewDBQueueManager(ctx, conf)
		assert.Error(t, err)
		assert.Nil(t, r)
	})
}
