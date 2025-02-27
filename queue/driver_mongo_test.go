package queue

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mongodb/amboy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestMongoDBOptions(t *testing.T) {
	client, err := mongo.Connect(options.Client().ApplyURI(defaultMongoDBURI))
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

func TestPutMany(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := defaultMongoDBTestOptions()
	opts.SkipQueueIndexBuilds = true
	opts.SkipReportingIndexBuilds = true
	opts.Collection = "jobs.jobs"
	client, err := mongo.Connect(options.Client().ApplyURI(opts.URI))
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, client.Disconnect(ctx))
	}()
	opts.Client = client
	defer func() {
		assert.NoError(t, client.Database(opts.DB).Drop(ctx))
	}()

	driver, err := openNewMongoDriver(ctx, opts)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, driver.Close(ctx))
	}()

	for testName, test := range map[string]func(*testing.T){
		"NoJobs": func(t *testing.T) {
			err = driver.PutMany(ctx, []amboy.Job{})
			assert.NoError(t, err)
		},
		"NilJobs": func(t *testing.T) {
			err = driver.PutMany(ctx, nil)
			assert.NoError(t, err)
		},
		"DistinctJobs": func(t *testing.T) {
			j0 := newMockJob()
			j0.SetID("j0")
			j1 := newMockJob()
			j1.SetID("j1")
			err = driver.PutMany(ctx, []amboy.Job{j0, j1})
			assert.NoError(t, err)
		},
		"DuplicateJob": func(t *testing.T) {
			j0 := newMockJob()
			j0.SetID("j0")
			j0Dup := newMockJob()
			j0Dup.SetID("j0")
			err = driver.PutMany(ctx, []amboy.Job{j0, j0Dup})
			assert.Error(t, err)
			assert.True(t, amboy.IsDuplicateJobError(err))
		},
		"OtherError": func(t *testing.T) {
			var sb strings.Builder
			for i := 0; i < 16000000; i++ {
				sb.WriteString("a")
			}
			j0 := newMockJob()
			j0.SetID(sb.String())
			err = driver.PutMany(ctx, []amboy.Job{j0})
			assert.Error(t, err)
			assert.False(t, amboy.IsDuplicateJobError(err))
		},
		"MixOfErrors": func(t *testing.T) {
			var sb strings.Builder
			for i := 0; i < 16000000; i++ {
				sb.WriteString("a")
			}
			j0 := newMockJob()
			j0.SetID(sb.String())
			j1 := newMockJob()
			j1.SetID("j1")
			j1Dup := newMockJob()
			j1Dup.SetID("j1")
			err = driver.PutMany(ctx, []amboy.Job{j0, j1, j1Dup})
			assert.Error(t, err)
			assert.False(t, amboy.IsDuplicateJobError(err))
		},
		"DuplicateScopes": func(t *testing.T) {
			_, err := client.Database(opts.DB).Collection(opts.Collection).Indexes().CreateOne(ctx, mongo.IndexModel{
				Keys: bson.D{
					bson.E{
						Key:   "scopes",
						Value: 1,
					},
				},
				Options: options.Index().SetUnique(true).SetPartialFilterExpression(bson.M{"scopes": bson.M{"$exists": true}}),
			})
			require.NoError(t, err)

			j0 := newMockJob()
			j0.SetID("j0")
			j0.SetScopes([]string{"scope"})
			j0.SetEnqueueAllScopes(true)
			j1 := newMockJob()
			j1.SetID("j1")
			j1.SetScopes([]string{"scope"})
			j1.SetEnqueueAllScopes(true)
			err = driver.PutMany(ctx, []amboy.Job{j0, j1})
			assert.Error(t, err)
			assert.True(t, amboy.IsDuplicateJobError(err))
			assert.True(t, amboy.IsDuplicateJobScopeError(err))
		},
		"DuplicateAndDuplicateScopes": func(t *testing.T) {
			_, err := client.Database(opts.DB).Collection(opts.Collection).Indexes().CreateOne(ctx, mongo.IndexModel{
				Keys: bson.D{
					bson.E{
						Key:   "scopes",
						Value: 1,
					},
				},
				Options: options.Index().SetUnique(true).SetPartialFilterExpression(bson.M{"scopes": bson.M{"$exists": true}}),
			})
			require.NoError(t, err)

			j0 := newMockJob()
			j0.SetID("j0")
			j0.SetScopes([]string{"scope"})
			j0.SetEnqueueAllScopes(true)
			j1 := newMockJob()
			j1.SetID("j1")
			j1.SetScopes([]string{"scope"})
			j1.SetEnqueueAllScopes(true)
			j1Dup := newMockJob()
			j1Dup.SetID("j1")
			err = driver.PutMany(ctx, []amboy.Job{j0, j1, j1Dup})
			assert.Error(t, err)
			assert.True(t, amboy.IsDuplicateJobError(err))
			assert.True(t, amboy.IsDuplicateJobScopeError(err))
		},
	} {
		require.NoError(t, client.Database(opts.DB).Collection(opts.Collection).Drop(ctx))
		t.Run(testName, test)
	}
}
