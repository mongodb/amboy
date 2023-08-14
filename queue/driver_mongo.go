package queue

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/registry"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

// MongoDBOptions represents options for creating a MongoDB driver to
// communicate MongoDB-specific settings about the driver's behavior and
// operation.
type MongoDBOptions struct {
	// Client is the MongoDB client used to connect a MongoDB queue driver to
	// its MongoDB-backed storage if no MongoDB URI is specified. Either Client
	// or URI must be set.
	Client *mongo.Client
	// URI is used to connect to MongoDB if no client is specified. Either
	// Client or URI must be set.
	URI string
	// DB is the name of the database in which the driver operates.
	DB string
	// Collection is the collection name in which the driver manages jobs.
	Collection string
	// GroupName is the namespace for the jobs in the queue managed by this
	// driver when the collection is shared by multiple queues. This is used to
	// ensure that jobs within each queue are isolated from each other by
	// namespace.
	GroupName string
	// UseGroups determines if the jobs in this collection could be in different
	// queues. If true, the driver will ensure that jobs are isolated between
	// the different queues using GroupName, and GroupName must be set.
	UseGroups bool
	// Priority determines if the queue obeys priority ordering of jobs.
	Priority bool
	// CheckWaitUntil determines if jobs that have not met their wait until time
	// yet should be filtered from consideration for dispatch. If true, any job
	// whose wait until constraint has not been reached yet will be filtered.
	CheckWaitUntil bool
	// CheckDispatchBy determines if jobs that have already exceeded their
	// dispatch by deadline should be filtered from consideration for dispatch.
	// If true, any job whose dispatch by deadline has been passed will be
	// filtered.
	CheckDispatchBy bool
	// SkipQueueIndexBuilds determines if indexes required for regular queue
	// operations should be built before using the driver.
	SkipQueueIndexBuilds bool
	// SkipReportingIndexBuilds determines if indexes related to reporting job
	// state should be built before using the driver.
	SkipReportingIndexBuilds bool
	// PreferredIndexes allows users to explicitly use a particular index to
	// fulfill driver queries.
	PreferredIndexes PreferredIndexOptions
	// Format is the internal format used to store jobs in the DB. The default
	// value is amboy.BSON.
	Format amboy.Format
	// WaitInterval is the duration that the driver will wait in between checks
	// for the next available job when no job is currently available to
	// dispatch.
	WaitInterval time.Duration
	// TTL sets the number of seconds for a TTL index on the "info.created"
	// field. If set to zero, the TTL index will not be created and
	// and documents may live forever in the database.
	TTL time.Duration
	// LockTimeout determines how long the queue will wait for a job that's
	// already been dispatched to finish without receiving a lock ping
	// indicating liveliness. If the lock timeout has been exceeded without a
	// lock ping, it will be considered stale and will be re-dispatched. If set,
	// this overrides the default job lock timeout.
	LockTimeout time.Duration
	// SampleSize is the maximum number of jobs per set of jobs checked.
	// If it samples from the available jobs, the order of next jobs are randomized.
	// By default, the driver does not sample from the next available jobs.
	// SampleSize cannot be used if Priority is true.
	SampleSize int
}

// PreferredIndexOptions provide options to explicitly set the index for use in
// specific scenarios. If an index is not explicitly given, the index will be
// picked automatically.
type PreferredIndexOptions struct {
	// NextJob determines the index pattern that will be used for requesting the
	// next job in the queue.
	// For the queue group, the driver will implicitly include the group, so it
	// does not need to be included in the index pattern.
	NextJob bson.D
}

// defaultMongoDBURI is the default URI to connect to a MongoDB instance.
const defaultMongoDBURI = "mongodb://localhost:27017"

// DefaultMongoDBOptions constructs a new options object with default
// values: connecting to a MongoDB instance on localhost, using the
// "amboy" database, and *not* using priority ordering of jobs.
func DefaultMongoDBOptions() MongoDBOptions {
	return MongoDBOptions{
		URI:                      defaultMongoDBURI,
		DB:                       "amboy",
		Priority:                 false,
		UseGroups:                false,
		CheckWaitUntil:           true,
		CheckDispatchBy:          false,
		SkipQueueIndexBuilds:     false,
		SkipReportingIndexBuilds: false,
		PreferredIndexes:         PreferredIndexOptions{},
		WaitInterval:             time.Second,
		Format:                   amboy.BSON,
		LockTimeout:              amboy.LockTimeout,
		SampleSize:               0,
	}
}

// Validate validates that the required options are given and sets fields that
// are unspecified and have a default value.
func (opts *MongoDBOptions) Validate() error {
	catcher := grip.NewBasicCatcher()
	catcher.NewWhen(opts.URI == "" && opts.Client == nil, "must specify connection URI or an existing client")
	catcher.NewWhen(opts.DB == "", "must specify database")
	catcher.NewWhen(opts.Collection == "", "must specify collection")
	catcher.NewWhen(opts.SampleSize < 0, "sample rate cannot be negative")
	catcher.NewWhen(opts.Priority && opts.SampleSize > 0, "cannot sample next jobs when ordering them by priority")
	catcher.NewWhen(opts.LockTimeout < 0, "lock timeout cannot be negative")
	catcher.NewWhen(opts.GroupName == "" && opts.UseGroups, "cannot use groups without a group name")
	if opts.LockTimeout == 0 {
		opts.LockTimeout = amboy.LockTimeout
	}
	if !opts.Format.IsValid() {
		opts.Format = amboy.BSON
	}
	return catcher.Resolve()
}

type mongoDriver struct {
	client     *mongo.Client
	ownsClient bool
	opts       MongoDBOptions
	collection string
	instanceID string
	mu         sync.RWMutex
	cancel     context.CancelFunc
	closed     bool
	dispatcher Dispatcher
}

// newMongoDriver constructs a MongoDB-backed queue driver instance without a
// MongoDB client. Callers must follow this with a call to Open before using the
// driver to set up the client.
func newMongoDriver(opts MongoDBOptions) (*mongoDriver, error) {
	host, _ := os.Hostname() // nolint

	if err := opts.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid driver options")
	}

	instanceIDParts := []string{opts.Collection}
	if opts.GroupName != "" {
		instanceIDParts = append(instanceIDParts, opts.GroupName)
	}
	instanceIDParts = append(instanceIDParts, host, uuid.New().String())

	return &mongoDriver{
		collection: opts.Collection,
		opts:       opts,
		instanceID: strings.Join(instanceIDParts, "."),
	}, nil
}

// openNewMongoDriver constructs a new MongoDB-backed queue driver instance
// using the client given in the MongoDB options. Use this if creating the
// driver with an existing client given in the MongoDB options; the client must
// already have established a connection to the DB. The returned driver does not
// take ownership of the lifetime of the client.
func openNewMongoDriver(ctx context.Context, opts MongoDBOptions) (*mongoDriver, error) {
	d, err := newMongoDriver(opts)
	if err != nil {
		return nil, errors.Wrap(err, "creating driver")
	}

	if err := d.start(ctx, clientStartOptions{client: opts.Client}); err != nil {
		return nil, errors.Wrap(err, "starting driver")
	}

	return d, nil
}

func (d *mongoDriver) ID() string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.instanceID
}

// Open opens a new owned connection for the driver. If a connection has already
// been previously established for the driver, this is a no-op.
func (d *mongoDriver) Open(ctx context.Context) error {
	d.mu.RLock()
	if d.cancel != nil {
		d.mu.RUnlock()
		return nil
	}
	d.mu.RUnlock()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(d.opts.URI))
	if err != nil {
		return errors.Wrapf(err, "opening connection to DB at URI '%s", d.opts.URI)
	}

	return errors.Wrap(d.start(ctx, clientStartOptions{
		client:     client,
		ownsClient: true,
	}), "starting driver")
}

type clientStartOptions struct {
	client     *mongo.Client
	ownsClient bool
}

func (d *mongoDriver) start(ctx context.Context, opts clientStartOptions) error {
	if opts.client == nil {
		return errors.New("cannot start the driver without a client")
	}

	d.mu.Lock()
	dCtx, cancel := context.WithCancel(ctx)
	d.cancel = cancel

	d.client = opts.client
	d.ownsClient = opts.ownsClient
	d.mu.Unlock()

	startAt := time.Now()
	go func() {
		<-dCtx.Done()
		grip.Info(message.Fields{
			"message":   "closing session for driver",
			"driver_id": d.instanceID,
			"uptime":    time.Since(startAt),
			"span":      time.Since(startAt).String(),
			"service":   "amboy.queue.mdb",
			"is_group":  d.opts.UseGroups,
			"group":     d.opts.GroupName,
		})
	}()

	if err := d.setupDB(ctx); err != nil {
		return errors.Wrap(err, "setting up database")
	}

	return nil
}

func (d *mongoDriver) getCollection() *mongo.Collection {
	db := d.client.Database(d.opts.DB)
	if d.opts.UseGroups {
		return db.Collection(addGroupSuffix(d.collection))
	}

	return db.Collection(addJobsSuffix(d.collection))
}

func (d *mongoDriver) setupDB(ctx context.Context) error {
	indexes := []mongo.IndexModel{}
	if !d.opts.SkipQueueIndexBuilds {
		indexes = append(indexes, d.queueIndexes()...)
	}
	if !d.opts.SkipReportingIndexBuilds {
		indexes = append(indexes, d.reportingIndexes()...)
	}

	if len(indexes) > 0 {
		_, err := d.getCollection().Indexes().CreateMany(ctx, indexes)
		return errors.Wrap(err, "building indexes")
	}

	return nil
}

func (d *mongoDriver) queueIndexes() []mongo.IndexModel {
	var indexes []mongo.IndexModel

	retrying := d.ensureGroupIndexPrefix(bson.D{
		bson.E{
			Key:   "status.completed",
			Value: bsonx.Int32(1),
		},
		bson.E{
			Key:   "retry_info.retryable",
			Value: bsonx.Int32(1),
		},
		bson.E{
			Key:   "retry_info.needs_retry",
			Value: bsonx.Int32(1),
		},
		bson.E{
			Key:   "status.mod_ts",
			Value: bsonx.Int32(-1),
		},
	})
	indexes = append(indexes, mongo.IndexModel{
		Keys: retrying,
		// We have to shorten the index name because the index name length
		// is limited to 127 bytes for MongoDB 4.0.
		// Source: https://docs.mongodb.com/manual/reference/limits/#Index-Name-Length
		// TODO: this only affects tests. Remove the custom index name once
		// CI tests have upgraded to MongoDB 4.2+.
		Options: options.Index().SetName("retrying_jobs"),
	})

	retryableJobIDAndAttempt := d.ensureGroupIndexPrefix(bson.D{
		bson.E{
			Key:   "retry_info.base_job_id",
			Value: bsonx.Int32(1),
		},
		bson.E{
			Key:   "retry_info.current_attempt",
			Value: bsonx.Int32(-1),
		},
	})
	indexes = append(indexes, mongo.IndexModel{Keys: retryableJobIDAndAttempt})

	scopes := d.ensureGroupIndexPrefix(bson.D{
		bson.E{
			Key:   "scopes",
			Value: bsonx.Int32(1),
		},
	})
	indexes = append(indexes, mongo.IndexModel{
		Keys:    scopes,
		Options: options.Index().SetUnique(true).SetPartialFilterExpression(bson.M{"scopes": bson.M{"$exists": true}}),
	})

	if d.opts.TTL > 0 {
		ttl := int32(d.opts.TTL / time.Second)
		indexes = append(indexes, mongo.IndexModel{
			Keys: bson.D{
				{
					Key:   "time_info.created",
					Value: bsonx.Int32(1),
				},
			},
			Options: options.Index().SetExpireAfterSeconds(ttl),
		})
	}

	makePrimary := func() bson.D {
		primary := d.ensureGroupIndexPrefix(bson.D{
			bson.E{
				Key:   "status.completed",
				Value: bsonx.Int32(1),
			},
			bson.E{
				Key:   "status.in_prog",
				Value: bsonx.Int32(1),
			},
		})
		if d.opts.Priority {
			primary = append(primary, bson.E{
				Key:   "priority",
				Value: bsonx.Int32(1),
			})
		}
		return primary
	}

	if d.opts.CheckWaitUntil {
		waitUntil := append(makePrimary(), bson.E{
			Key:   "time_info.wait_until",
			Value: bsonx.Int32(1),
		})
		indexes = append(indexes, mongo.IndexModel{Keys: waitUntil})
	}
	if d.opts.CheckDispatchBy {
		dispatchBy := append(makePrimary(), bson.E{
			Key:   "time_info.dispatch_by",
			Value: bsonx.Int32(1),
		})
		indexes = append(indexes, mongo.IndexModel{Keys: dispatchBy})
	}
	if !d.opts.CheckWaitUntil && !d.opts.CheckDispatchBy {
		indexes = append(indexes, mongo.IndexModel{Keys: makePrimary()})
	}

	return indexes
}

func (d *mongoDriver) reportingIndexes() []mongo.IndexModel {
	completedInProgModTs := d.ensureGroupIndexPrefix(bson.D{
		bson.E{
			Key:   "status.completed",
			Value: bsonx.Int32(1),
		},
		bson.E{
			Key:   "status.in_prog",
			Value: bsonx.Int32(1),
		},
		bson.E{
			Key:   "status.mod_ts",
			Value: bsonx.Int32(1),
		},
	})
	completedEnd := d.ensureGroupIndexPrefix(bson.D{
		bson.E{
			Key:   "status.completed",
			Value: bsonx.Int32(1),
		},
		bson.E{
			Key:   "time_info.end",
			Value: bsonx.Int32(1),
		},
	})
	completedCreated := d.ensureGroupIndexPrefix(bson.D{
		bson.E{
			Key:   "status.completed",
			Value: bsonx.Int32(1),
		},
		bson.E{
			Key:   "time_info.created",
			Value: bsonx.Int32(1),
		},
	})
	typeCompletedInProgModTs := d.ensureGroupIndexPrefix(bson.D{
		bson.E{
			Key:   "type",
			Value: bsonx.Int32(1),
		},
		bson.E{
			Key:   "status.completed",
			Value: bsonx.Int32(1),
		},
		bson.E{
			Key:   "status.in_prog",
			Value: bsonx.Int32(1),
		},
		bson.E{
			Key:   "status.mod_ts",
			Value: bsonx.Int32(1),
		},
	})
	typeCompletedEnd := d.ensureGroupIndexPrefix(bson.D{
		bson.E{
			Key:   "type",
			Value: bsonx.Int32(1),
		},
		bson.E{
			Key:   "status.completed",
			Value: bsonx.Int32(1),
		},
		bson.E{
			Key:   "time_info.end",
			Value: bsonx.Int32(1),
		},
	})

	return []mongo.IndexModel{
		{Keys: completedInProgModTs},
		{Keys: completedEnd},
		{Keys: completedCreated},
		{Keys: typeCompletedInProgModTs},
		{Keys: typeCompletedEnd},
	}
}

// Close closes the resources for the driver. If it initialized a new client, it
// is disconnected.
func (d *mongoDriver) Close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}

	d.closed = true

	if d.cancel != nil {
		d.cancel()
		d.cancel = nil

	}

	if d.ownsClient {
		return d.client.Disconnect(ctx)
	}

	return nil
}

func buildCompoundID(n, id string) string { return fmt.Sprintf("%s.%s", n, id) }

func deconstructCompoundID(id, prefix string) string {
	return strings.TrimPrefix(id, prefix+".")
}

func (d *mongoDriver) getIDWithGroup(name string) string {
	if d.opts.UseGroups {
		name = buildCompoundID(d.opts.GroupName, name)
	}

	return name
}

func (d *mongoDriver) addMetadata(j *registry.JobInterchange) {
	d.addRetryToMetadata(j)
	d.addGroupToMetadata(j)
}

func (d *mongoDriver) removeMetadata(j *registry.JobInterchange) {
	d.removeGroupFromMetadata(j)
	d.removeRetryFromMetadata(j)
}

func (d *mongoDriver) addGroupToMetadata(j *registry.JobInterchange) {
	if !d.opts.UseGroups {
		return
	}

	j.Group = d.opts.GroupName
	j.Name = buildCompoundID(d.opts.GroupName, j.Name)
}

func (d *mongoDriver) removeGroupFromMetadata(j *registry.JobInterchange) {
	if !d.opts.UseGroups {
		return
	}

	j.Name = deconstructCompoundID(j.Name, d.opts.GroupName)
}

func (d *mongoDriver) addRetryToMetadata(j *registry.JobInterchange) {
	if !j.RetryInfo.Retryable {
		return
	}

	j.RetryInfo.BaseJobID = j.Name
	j.Name = buildCompoundID(retryAttemptPrefix(j.RetryInfo.CurrentAttempt), j.Name)
}

func (d *mongoDriver) removeRetryFromMetadata(j *registry.JobInterchange) {
	if !j.RetryInfo.Retryable {
		return
	}

	j.Name = deconstructCompoundID(j.Name, retryAttemptPrefix(j.RetryInfo.CurrentAttempt))
}

func (d *mongoDriver) modifyQueryForGroup(q bson.M) bson.M {
	if !d.opts.UseGroups {
		return q
	}

	q["group"] = d.opts.GroupName
	return q
}

func (d *mongoDriver) Get(ctx context.Context, name string) (amboy.Job, error) {
	j := &registry.JobInterchange{}

	byRetryAttempt := bson.M{
		"retry_info.current_attempt": -1,
	}
	res := d.getCollection().FindOne(ctx, d.getIDQuery(name), options.FindOne().SetSort(byRetryAttempt))
	if err := res.Err(); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, amboy.NewJobNotFoundError("no such job found")
		}
		return nil, errors.Wrap(err, "finding job")
	}

	if err := res.Decode(j); err != nil {
		return nil, errors.Wrap(err, "decoding job document into interchange job")
	}

	output, err := j.Resolve(d.opts.Format)
	if err != nil {
		return nil, errors.Wrap(err, "converting interchange job to in-memory job")
	}

	return output, nil
}

// getIDQuery matches a job ID against either a retryable job or a non-retryable
// job.
func (d *mongoDriver) getIDQuery(id string) bson.M {
	matchID := bson.M{"_id": d.getIDWithGroup(id)}

	matchRetryable := bson.M{"retry_info.base_job_id": id}
	d.modifyQueryForGroup(matchRetryable)

	return bson.M{
		"$or": []bson.M{
			matchID,
			matchRetryable,
		},
	}
}

// GetAttempt finds a retryable job matching the given job ID and execution
// attempt. This returns an error if no matching job is found.
func (d *mongoDriver) GetAttempt(ctx context.Context, id string, attempt int) (amboy.Job, error) {
	matchIDAndAttempt := bson.M{
		"retry_info.base_job_id":     id,
		"retry_info.current_attempt": attempt,
	}
	d.modifyQueryForGroup(matchIDAndAttempt)

	res := d.getCollection().FindOne(ctx, matchIDAndAttempt)
	if err := res.Err(); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, amboy.NewJobNotFoundError("no such job found")
		}
		return nil, errors.Wrap(err, "finding job attempt")
	}

	ji := &registry.JobInterchange{}
	if err := res.Decode(ji); err != nil {
		return nil, errors.Wrap(err, "decoding job document into interchange job")
	}

	j, err := ji.Resolve(d.opts.Format)
	if err != nil {
		return nil, errors.Wrap(err, "converting interchange job to in-memory job")
	}

	return j, nil
}

// GetAllAttempts finds all execution attempts of a retryable job for a given
// job ID. This returns an error if no matching job is found. The returned jobs
// are sorted by increasing attempt number.
func (d *mongoDriver) GetAllAttempts(ctx context.Context, id string) ([]amboy.Job, error) {
	matchID := bson.M{
		"retry_info.base_job_id": id,
	}
	d.modifyQueryForGroup(matchID)

	sortAttempt := bson.M{"retry_info.current_attempt": -1}

	cursor, err := d.getCollection().Find(ctx, matchID, options.Find().SetSort(sortAttempt))
	if err != nil {
		return nil, errors.Wrap(err, "finding all attempts")
	}

	jobInts := []registry.JobInterchange{}
	if err := cursor.All(ctx, &jobInts); err != nil {
		return nil, errors.Wrap(err, "decoding job document into interchange job")
	}
	if len(jobInts) == 0 {
		return nil, amboy.NewJobNotFoundError("no such job found")
	}

	jobs := make([]amboy.Job, len(jobInts))
	for i, ji := range jobInts {
		j, err := ji.Resolve(d.opts.Format)
		if err != nil {
			return nil, errors.Wrap(err, "converting interchange job to in-memory job")
		}
		jobs[len(jobs)-i-1] = j
	}

	return jobs, nil
}

func (d *mongoDriver) Put(ctx context.Context, j amboy.Job) error {
	return d.PutMany(ctx, []amboy.Job{j})
}

// PutMany enqueues multiple jobs on the queue.
// Returns an error if any of the jobs cannot be inserted. If a non-duplicate-job error
// is encountered it is returned. If every job is a duplicate job error then if any
// of them is a duplicate scope error a duplicate scope error is returned.
// If none of them is a duplicate scope error then a duplicate job error
// is returned.
func (d *mongoDriver) PutMany(ctx context.Context, jobs []amboy.Job) error {
	var jobInterchanges []any
	for _, j := range jobs {
		ji, err := registry.MakeJobInterchange(j, d.opts.Format)
		if err != nil {
			return errors.Wrap(err, "converting in-memory job to interchange job")
		}
		ji.Scopes = j.EnqueueScopes()
		d.addMetadata(ji)
		jobInterchanges = append(jobInterchanges, ji)
	}

	if _, err := d.getCollection().InsertMany(ctx, jobInterchanges, options.InsertMany().SetOrdered(false)); err != nil {
		jobIDs := make([]string, 0, len(jobs))
		for _, j := range jobs {
			jobIDs = append(jobIDs, j.ID())
		}
		if d.isMongoDupScope(err) {
			return amboy.NewDuplicateJobScopeErrorf("job scopes conflict inserting '%v'", jobIDs)
		}
		if isMongoDupKey(err) {
			return amboy.NewDuplicateJobErrorf("job already exists inserting '%v'", jobIDs)
		}

		return errors.Wrapf(err, "inserting new jobs '%v'", jobIDs)
	}

	return nil
}

func (d *mongoDriver) getAtomicQuery(jobName string, stat amboy.JobStatusInfo) bson.M {
	d.mu.RLock()
	defer d.mu.RUnlock()
	owner := d.instanceID
	timeoutTs := time.Now().Add(-d.LockTimeout())

	// The lock can be acquired if the modification time is unset (i.e. it's
	// unowned) or older than the lock timeout (i.e. the lock is stale),
	// regardless of what the other data is.
	unownedOrStaleLock := bson.M{"status.mod_ts": bson.M{"$lte": timeoutTs}}

	// The lock is actively owned by this in-memory instance of the queue if
	// owner and modification count match.
	//
	// The modification count is +1 in the case that we're looking to update the
	// job and its modification count (in the Complete case, it does not update
	// the modification count).
	activeOwnedLock := bson.M{
		"status.owner":     owner,
		"status.mod_count": bson.M{"$in": []int{stat.ModificationCount, stat.ModificationCount - 1}},
		"status.mod_ts":    bson.M{"$gt": timeoutTs},
	}

	// If we're trying to update the status to in progress or maintain that it
	// is in progress, this is only allowed if the job is not already marked
	// complete.
	if stat.InProgress {
		unownedOrStaleLock["status.completed"] = false
		activeOwnedLock["status.completed"] = false
	}

	return bson.M{
		"_id": jobName,
		"$or": []bson.M{
			unownedOrStaleLock,
			activeOwnedLock,
		},
	}
}

func isMongoDupKey(err error) bool {
	dupKeyErrs := getMongoDupKeyErrors(err)
	return dupKeyErrs.writeConcernError != nil || (len(dupKeyErrs.writeErrors) != 0 && !dupKeyErrs.hasOtherWriteErrors) || dupKeyErrs.commandError != nil
}

func (d *mongoDriver) isMongoDupScope(err error) bool {
	dupKeyErrs := getMongoDupKeyErrors(err)
	var index string
	if d.opts.UseGroups {
		index = " group_1_scopes_1 "
	} else {
		index = " scopes_1 "
	}
	if wce := dupKeyErrs.writeConcernError; wce != nil {
		if strings.Contains(wce.Message, index) {
			return true
		}
	}

	for _, werr := range dupKeyErrs.writeErrors {
		if strings.Contains(werr.Message, index) {
			return true
		}
	}

	if ce := dupKeyErrs.commandError; ce != nil {
		if strings.Contains(ce.Message, index) {
			return true
		}
	}

	return false
}

type mongoDupKeyErrors struct {
	writeConcernError   *mongo.WriteConcernError
	writeErrors         []mongo.WriteError
	hasOtherWriteErrors bool
	commandError        *mongo.CommandError
}

func getMongoDupKeyErrors(err error) mongoDupKeyErrors {
	var dupKeyErrs mongoDupKeyErrors

	if we, ok := errors.Cause(err).(mongo.WriteException); ok {
		dupKeyErrs.writeConcernError = getMongoDupKeyWriteConcernError(we.WriteConcernError)
		dupKeyErrs.writeErrors = getMongoDupKeyWriteErrors(we.WriteErrors)
		dupKeyErrs.hasOtherWriteErrors = len(we.WriteErrors) > len(dupKeyErrs.writeErrors)
	}

	if we, ok := errors.Cause(err).(mongo.BulkWriteException); ok {
		dupKeyErrs.writeConcernError = getMongoDupKeyWriteConcernError(we.WriteConcernError)
		var writeErrors mongo.WriteErrors
		for _, err := range we.WriteErrors {
			writeErrors = append(writeErrors, err.WriteError)
		}
		dupKeyErrs.writeErrors = getMongoDupKeyWriteErrors(writeErrors)
		dupKeyErrs.hasOtherWriteErrors = len(writeErrors) > len(we.WriteErrors)
	}

	if ce, ok := errors.Cause(err).(mongo.CommandError); ok {
		dupKeyErrs.commandError = getMongoDupKeyCommandError(ce)
	}

	return dupKeyErrs
}

func getMongoDupKeyWriteConcernError(wce *mongo.WriteConcernError) *mongo.WriteConcernError {
	if wce == nil {
		return nil
	}

	switch wce.Code {
	case 11000, 11001, 12582:
		return wce
	case 16460:
		if strings.Contains(wce.Message, " E11000 ") {
			return wce
		}
		return nil
	default:
		return nil
	}
}

func getMongoDupKeyWriteErrors(writeErrors mongo.WriteErrors) []mongo.WriteError {
	if len(writeErrors) == 0 {
		return nil
	}

	var werrs []mongo.WriteError
	for _, werr := range writeErrors {
		if werr.Code == 11000 {
			werrs = append(werrs, werr)
		}
	}

	return werrs
}

func getMongoDupKeyCommandError(err mongo.CommandError) *mongo.CommandError {
	switch err.Code {
	case 11000, 11001:
		return &err
	case 16460:
		if strings.Contains(err.Message, " E11000 ") {
			return &err
		}
		return nil
	default:
		return nil
	}
}

func (d *mongoDriver) Save(ctx context.Context, j amboy.Job) error {
	ji, err := d.prepareInterchange(j)
	if err != nil {
		return errors.WithStack(err)
	}

	ji.Scopes = j.Scopes()

	return errors.WithStack(d.doUpdate(ctx, ji))
}

func (d *mongoDriver) CompleteAndPut(ctx context.Context, toComplete amboy.Job, toPut amboy.Job) error {
	sess, err := d.client.StartSession()
	if err != nil {
		return errors.Wrap(err, "starting transaction session")
	}
	defer sess.EndSession(ctx)

	atomicCompleteAndPut := func(sessCtx mongo.SessionContext) (interface{}, error) {
		if err = d.Complete(sessCtx, toComplete); err != nil {
			return nil, errors.Wrap(err, "completing old job")
		}

		if err = d.Put(sessCtx, toPut); err != nil {
			return nil, errors.Wrap(err, "adding new job")
		}

		return nil, nil
	}

	if _, err = sess.WithTransaction(ctx, atomicCompleteAndPut); err != nil {
		return errors.Wrap(err, "atomic complete and put")
	}

	return nil
}

func (d *mongoDriver) Complete(ctx context.Context, j amboy.Job) error {
	ji, err := d.prepareInterchange(j)
	if err != nil {
		return errors.WithStack(err)
	}

	// It is safe to drop the scopes now in all cases except for one - if the
	// job still needs to retry, any scopes that are applied immediately on
	// enqueue must still be held because they will need to be safely
	// transferred to the retry job.
	if ji.RetryInfo.ShouldRetry() {
		ji.Scopes = j.EnqueueScopes()
	} else {
		ji.Scopes = nil
	}

	return errors.WithStack(d.doUpdate(ctx, ji))
}

func (d *mongoDriver) prepareInterchange(j amboy.Job) (*registry.JobInterchange, error) {
	stat := j.Status()
	stat.ModificationTime = time.Now()
	j.SetStatus(stat)

	ji, err := registry.MakeJobInterchange(j, d.opts.Format)
	if err != nil {
		return nil, errors.Wrap(err, "converting in-memory job to interchange job")
	}
	return ji, nil
}

func (d *mongoDriver) doUpdate(ctx context.Context, ji *registry.JobInterchange) error {
	d.addMetadata(ji)

	if ji.Status.InProgress && ji.Status.Completed {
		err := errors.New("job was found both in progress and complete")
		grip.Error(message.WrapError(err, message.Fields{
			"message":     "programmer error: a job should not be saved as both in progress and complete - manually changing in progress to false",
			"jira_ticket": "EVG-14609",
			"job_id":      ji.Name,
			"service":     "amboy.queue.mdb",
			"driver_id":   d.instanceID,
		}))
		ji.Status.InProgress = false
	}

	query := d.getAtomicQuery(ji.Name, ji.Status)

	res, err := d.getCollection().ReplaceOne(ctx, query, ji)
	if err != nil {
		if d.isMongoDupScope(err) {
			return amboy.NewDuplicateJobScopeErrorf("job scopes '%s' conflict", ji.Scopes)
		}
		return errors.Wrapf(err, "saving job '%s': %+v", ji.Name, res)
	}

	if res.MatchedCount == 0 {
		return message.WrapError(amboy.NewJobNotFoundErrorf("unmatched job"), message.Fields{
			"matched":  res.MatchedCount,
			"modified": res.ModifiedCount,
			"job_id":   ji.Name,
		})
	}

	return nil
}

func (d *mongoDriver) Jobs(ctx context.Context) <-chan amboy.Job {
	output := make(chan amboy.Job)
	go func() {
		defer func() {
			if err := recovery.HandlePanicWithError(recover(), nil, "getting jobs"); err != nil {
				grip.Error(message.WrapError(err, message.Fields{
					"message":   "failed while getting jobs from the DB",
					"operation": "job iterator",
					"service":   "amboy.queue.mdb",
					"driver_id": d.instanceID,
				}))
			}
			close(output)
		}()
		q := bson.M{}
		d.modifyQueryForGroup(q)

		iter, err := d.getCollection().Find(ctx, q, options.Find().SetSort(bson.M{"status.mod_ts": -1}))
		if err != nil {
			grip.Warning(message.WrapError(err, message.Fields{
				"message":   "problem with query",
				"driver_id": d.instanceID,
				"service":   "amboy.queue.mdb",
				"is_group":  d.opts.UseGroups,
				"group":     d.opts.GroupName,
				"operation": "job iterator",
			}))
			return
		}
		for iter.Next(ctx) {
			ji := &registry.JobInterchange{}
			if err = iter.Decode(ji); err != nil {
				grip.Warning(message.WrapError(err, message.Fields{
					"message":   "problem decoding job document into interchange job",
					"driver_id": d.instanceID,
					"service":   "amboy.queue.mdb",
					"is_group":  d.opts.UseGroups,
					"group":     d.opts.GroupName,
					"operation": "job iterator",
				}))

				continue
			}

			var j amboy.Job
			j, err = ji.Resolve(d.opts.Format)
			if err != nil {
				grip.Warning(message.WrapError(err, message.Fields{
					"message":   "problem converting interchange job to in-memory job",
					"driver_id": d.instanceID,
					"service":   "amboy.queue.mdb",
					"operation": "job iterator",
					"is_group":  d.opts.UseGroups,
					"group":     d.opts.GroupName,
				}))
				continue
			}

			select {
			case <-ctx.Done():
				return
			case output <- j:
			}
		}

		grip.Error(message.WrapError(iter.Err(), message.Fields{
			"driver_id": d.instanceID,
			"service":   "amboy.queue.mdb",
			"is_group":  d.opts.UseGroups,
			"group":     d.opts.GroupName,
			"operation": "job iterator",
			"message":   "database iterator error",
		}))
	}()

	return output
}

func (d *mongoDriver) RetryableJobs(ctx context.Context, filter retryableJobFilter) <-chan amboy.Job {
	jobs := make(chan amboy.Job)

	go func() {
		defer func() {
			if err := recovery.HandlePanicWithError(recover(), nil, "getting retryable jobs"); err != nil {
				grip.Error(message.WrapError(err, message.Fields{
					"message":   "failed while getting retryable jobs from the DB",
					"operation": "retryable job iterator",
					"service":   "amboy.queue.mdb",
					"driver_id": d.instanceID,
				}))
			}
			close(jobs)
		}()

		var q bson.M
		switch filter {
		case retryableJobAll:
			q = bson.M{"retry_info.retryable": true}
		case retryableJobAllRetrying:
			q = d.getRetryingQuery(bson.M{})
		case retryableJobActiveRetrying:
			q = d.getRetryingQuery(bson.M{})
			q["status.mod_ts"] = bson.M{"$gte": time.Now().Add(-d.LockTimeout())}
		case retryableJobStaleRetrying:
			q = d.getRetryingQuery(bson.M{})
			q["status.mod_ts"] = bson.M{"$lte": time.Now().Add(-d.LockTimeout())}
		default:
			return
		}
		d.modifyQueryForGroup(q)

		iter, err := d.getCollection().Find(ctx, q, options.Find().SetSort(bson.M{"status.mod_ts": -1}))
		if err != nil {
			grip.Warning(message.WrapError(err, message.Fields{
				"message":   "problem with query",
				"driver_id": d.instanceID,
				"service":   "amboy.queue.mdb",
				"is_group":  d.opts.UseGroups,
				"group":     d.opts.GroupName,
				"operation": "retryable job iterator",
			}))
			return
		}
		for iter.Next(ctx) {
			ji := &registry.JobInterchange{}
			if err = iter.Decode(ji); err != nil {
				grip.Warning(message.WrapError(err, message.Fields{
					"message":   "problem decoding job document into job interchange",
					"driver_id": d.instanceID,
					"service":   "amboy.queue.mdb",
					"is_group":  d.opts.UseGroups,
					"group":     d.opts.GroupName,
					"operation": "retryable job iterator",
				}))
				continue
			}

			var j amboy.Job
			j, err = ji.Resolve(d.opts.Format)
			if err != nil {
				grip.Warning(message.WrapError(err, message.Fields{
					"message":   "converting interchange job to in-memory job",
					"driver_id": d.instanceID,
					"service":   "amboy.queue.mdb",
					"operation": "retryable job iterator",
					"is_group":  d.opts.UseGroups,
					"group":     d.opts.GroupName,
				}))
				continue
			}

			select {
			case <-ctx.Done():
				return
			case jobs <- j:
			}
		}

		grip.Error(message.WrapError(iter.Err(), message.Fields{
			"driver_id": d.instanceID,
			"service":   "amboy.queue.mdb",
			"is_group":  d.opts.UseGroups,
			"group":     d.opts.GroupName,
			"operation": "retryable job iterator",
			"message":   "database iterator error",
		}))
	}()

	return jobs
}

// JobInfo returns a channel that produces information about all jobs stored in
// the DB. Job information is returned in order of decreasing modification time.
func (d *mongoDriver) JobInfo(ctx context.Context) <-chan amboy.JobInfo {
	infos := make(chan amboy.JobInfo)
	go func() {
		defer close(infos)
		q := bson.M{}
		d.modifyQueryForGroup(q)

		iter, err := d.getCollection().Find(ctx,
			q,
			&options.FindOptions{
				Sort: bson.M{"status.mod_ts": -1},
				Projection: bson.M{
					"_id":        1,
					"status":     1,
					"retry_info": 1,
					"time_info":  1,
					"type":       1,
					"version":    1,
				},
			})
		if err != nil {
			grip.Warning(message.WrapError(err, message.Fields{
				"message":   "problem with query",
				"driver_id": d.instanceID,
				"service":   "amboy.queue.mdb",
				"operation": "job info iterator",
				"is_group":  d.opts.UseGroups,
				"group":     d.opts.GroupName,
			}))
			return
		}

		for iter.Next(ctx) {
			ji := &registry.JobInterchange{}
			if err := iter.Decode(ji); err != nil {
				grip.Warning(message.WrapError(err, message.Fields{
					"message":   "problem decoding job document into interchange job",
					"driver_id": d.instanceID,
					"service":   "amboy.queue.mdb",
					"operation": "job info iterator",
					"is_group":  d.opts.UseGroups,
					"group":     d.opts.GroupName,
				}))
				continue
			}

			d.removeMetadata(ji)
			info := amboy.JobInfo{
				ID:     ji.Name,
				Status: ji.Status,
				Time:   ji.TimeInfo,
				Retry:  ji.RetryInfo,
				Type: amboy.JobType{
					Name:    ji.Type,
					Version: ji.Version,
				},
			}

			select {
			case <-ctx.Done():
				return
			case infos <- info:
			}

		}
	}()

	return infos
}

// Next is the means by which callers can dispatch and run jobs by handing out the next job available to run. If the
// driver finds an unowned job that is waiting to dispatch, it will dispatch the job, effectively giving the caller
// ownership and responsibility of running it. If there is no job that can be currently dispatched to the worker, it
// will periodically poll until a job becomes available or the given context errors.
func (d *mongoDriver) Next(ctx context.Context) amboy.Job {
	var (
		job            amboy.Job
		misses         int
		dispatchMisses int
		dispatchSkips  int
	)

	startAt := time.Now()
	defer func() {
		grip.WarningWhen(
			time.Since(startAt) > time.Second,
			message.Fields{
				"duration_secs": time.Since(startAt).Seconds(),
				"service":       "amboy.queue.mdb",
				"operation":     "next job",
				"attempts":      dispatchMisses,
				"skips":         dispatchSkips,
				"misses":        misses,
				"dispatched":    job != nil,
				"message":       "slow job dispatching operation",
				"driver_id":     d.instanceID,
				"is_group":      d.opts.UseGroups,
				"group":         d.opts.GroupName,
			})
	}()

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			misses++
			job, dispatchInfo, err := d.dispatchNextJob(ctx, startAt)
			if err != nil {
				if job != nil {
					d.dispatcher.Release(ctx, job)
				}
				grip.Warning(message.WrapError(err, message.Fields{
					"message":       "problem getting next job",
					"driver_id":     d.instanceID,
					"service":       "amboy.queue.mdb",
					"operation":     "dispatching job",
					"is_group":      d.opts.UseGroups,
					"group":         d.opts.GroupName,
					"duration_secs": time.Since(startAt).Seconds(),
				}))
				return nil
			}
			dispatchMisses += dispatchInfo.misses
			dispatchSkips += dispatchInfo.skips

			if job != nil {
				return job
			}

			timer.Reset(time.Duration(misses * rand.Intn(int(d.opts.WaitInterval))))
		}
	}
}

// dispatchAttemptInfo contains aggregate statistics on an attempt to dispatch
// jobs.
type dispatchAttemptInfo struct {
	// skips is the number of times it encountered a job that was not yet ready
	// to dispatch.
	skips int
	// misses is the number of times the dispatcher attempted to dispatch a job
	// but failed.
	misses int
}

// dispatchNextJob attempts to dispatch a job. Stale in progress jobs are given precedence over pending jobs.
func (d *mongoDriver) dispatchNextJob(ctx context.Context, startAt time.Time) (amboy.Job, dispatchAttemptInfo, error) {
	now := time.Now()
	var info dispatchAttemptInfo

	job, dispatchInfo, err := d.tryDispatchWithQuery(ctx, d.getNextStaleInProgressQuery(now), startAt)
	info.misses += dispatchInfo.misses
	info.skips += dispatchInfo.skips

	if err != nil {
		return job, info, errors.Wrap(err, "dispatching stale in progress job")
	}
	if job != nil {
		return job, info, nil
	}

	job, dispatchInfo, err = d.tryDispatchWithQuery(ctx, d.getNextPendingQuery(now), startAt)
	info.misses += dispatchInfo.misses
	info.skips += dispatchInfo.skips
	if err != nil {
		return job, info, errors.Wrap(err, "dispatching stale in progress job")
	}

	return job, info, nil
}

// getNextStaleInProgressQuery returns the query for the next available stale in-progress jobs.
func (d *mongoDriver) getNextStaleInProgressQuery(now time.Time) bson.M {
	staleBefore := now.Add(-d.LockTimeout())
	qd := d.getInProgQuery(bson.M{"status.mod_ts": bson.M{"$lte": staleBefore}})
	return d.modifyQueryForGroup(d.modifyQueryForTimeLimits(qd, now))
}

// getNextPendingQuery returns the query for the next available pending jobs.
func (d *mongoDriver) getNextPendingQuery(now time.Time) bson.M {
	qd := d.getPendingQuery(bson.M{})
	return d.modifyQueryForGroup(d.modifyQueryForTimeLimits(qd, now))
}

// modifyQueryForTimeLimits returns a new query that adds a check to the given query for jobs' time limits.
// If neither CheckWaitUntil nor CheckDispatchBy are set the original query is returned.
func (d *mongoDriver) modifyQueryForTimeLimits(q bson.M, now time.Time) bson.M {
	var timeLimits []bson.M
	if d.opts.CheckWaitUntil {
		checkWaitUntil := bson.M{"$or": []bson.M{
			{"time_info.wait_until": bson.M{"$lte": now}},
			{"time_info.wait_until": bson.M{"$exists": false}},
		}}
		timeLimits = append(timeLimits, checkWaitUntil)
	}

	if d.opts.CheckDispatchBy {
		checkDispatchBy := bson.M{"$or": []bson.M{
			{"time_info.dispatch_by": bson.M{"$gt": now}},
			{"time_info.dispatch_by": bson.M{"$exists": false}},
		}}
		timeLimits = append(timeLimits, checkDispatchBy)
	}

	if len(timeLimits) > 0 {
		q = bson.M{"$and": append(timeLimits, q)}
	}

	return q
}

// tryDispatchWithQuery attempts to dispatch a job that matches the query. If sampleSize is greater than
// zero only that number of jobs are considered and the jobs are randomly shuffled.
// If dispatching succeeds, the successfully dispatched job is returned.
func (d *mongoDriver) tryDispatchWithQuery(ctx context.Context, query bson.M, startAt time.Time) (amboy.Job, dispatchAttemptInfo, error) {
	iter, err := d.getNextCursor(ctx, query)
	if err != nil {
		return nil, dispatchAttemptInfo{}, errors.Wrap(err, "getting next job iterator")
	}

	job, dispatchInfo := d.tryDispatchFromCursor(ctx, iter, startAt)

	if err = iter.Err(); err != nil {
		return job, dispatchInfo, errors.Wrap(err, "iterator encountered an error")
	}
	if err = iter.Close(ctx); err != nil {
		return job, dispatchInfo, errors.Wrap(err, "closing iterator")
	}

	return job, dispatchInfo, nil
}

// getNextCursor returns a cursor for the jobs matching nextQuery.
// If sampleSize is greater than zero it's added as a limit and the jobs are shuffled.
func (d *mongoDriver) getNextCursor(ctx context.Context, nextQuery bson.M) (*mongo.Cursor, error) {
	pipeline := []bson.M{{"$match": nextQuery}}

	if d.opts.Priority {
		pipeline = append(pipeline, bson.M{"$sort": bson.E{Key: "priority", Value: -1}})
	}

	if d.opts.SampleSize > 0 {
		// $limit must precede $sample for performance reasons. $sample scans all input
		// documents to randomly select documents to return. Therefore, without a
		// limit on the number of jobs to consider, the cost of $sample will become
		// more expensive with the number of jobs that it must consider.
		// Source:
		// https://docs.mongodb.com/manual/reference/operator/aggregation/sample/#behavior
		pipeline = append(pipeline, bson.M{"$limit": d.opts.SampleSize})
		pipeline = append(pipeline, bson.M{"$sample": bson.M{"size": d.opts.SampleSize}})
	}

	var opts *options.AggregateOptions
	if d.opts.PreferredIndexes.NextJob != nil {
		opts = options.Aggregate().SetHint(d.ensureGroupIndexPrefix(d.opts.PreferredIndexes.NextJob))
	}
	iter, err := d.getCollection().Aggregate(ctx, pipeline, opts)
	return iter, errors.Wrap(err, "aggregating next jobs")
}

// tryDispatchFromCursor takes an iterator over the candidate Amboy jobs and attempts
// to dispatch one of them. If it succeeds, it returns the successfully
// dispatched job.
func (d *mongoDriver) tryDispatchFromCursor(ctx context.Context, iter *mongo.Cursor, startAt time.Time) (amboy.Job, dispatchAttemptInfo) {
	var dispatchInfo dispatchAttemptInfo
	for iter.Next(ctx) {

		ji := &registry.JobInterchange{}
		if err := iter.Decode(ji); err != nil {
			grip.Warning(message.WrapError(err, message.Fields{
				"message":       "problem decoding job document into interchange job",
				"driver_id":     d.instanceID,
				"service":       "amboy.queue.mdb",
				"operation":     "next job",
				"is_group":      d.opts.UseGroups,
				"group":         d.opts.GroupName,
				"duration_secs": time.Since(startAt).Seconds(),
			}))
			// try for the next thing in the iterator if we can
			continue
		}

		j, err := ji.Resolve(d.opts.Format)
		if err != nil {
			grip.Warning(message.WrapError(err, message.Fields{
				"message":       "problem converting interchange job into in-memory job",
				"driver_id":     d.instanceID,
				"service":       "amboy.queue.mdb",
				"operation":     "next job",
				"is_group":      d.opts.UseGroups,
				"group":         d.opts.GroupName,
				"duration_secs": time.Since(startAt).Seconds(),
			}))
			// try for the next thing in the iterator if we can
			continue
		}

		if j.TimeInfo().IsStale() {
			res, err := d.getCollection().DeleteOne(ctx, bson.M{"_id": ji.Name})
			msg := message.Fields{
				"message":       "found stale job",
				"driver_id":     d.instanceID,
				"service":       "amboy.queue.mdb",
				"num_deleted":   res.DeletedCount,
				"operation":     "next job",
				"job_id":        j.ID(),
				"job_type":      j.Type().Name,
				"is_group":      d.opts.UseGroups,
				"group":         d.opts.GroupName,
				"duration_secs": time.Since(startAt).Seconds(),
			}
			grip.Warning(message.WrapError(err, msg))
			grip.NoticeWhen(err == nil, msg)
			continue
		}

		lockTimeout := d.LockTimeout()
		dispatchable := isDispatchable(j.Status(), j.TimeInfo(), lockTimeout)
		if !dispatchable {
			dispatchInfo.skips++
			continue
		} else if d.isOtherJobHoldingScopes(ctx, j) {
			dispatchInfo.skips++
			continue
		}

		if err = d.dispatcher.Dispatch(ctx, j); err != nil {
			dispatchInfo.misses++
			// Log dispatch errors if they're not caused by dispatch contention.
			//
			// The job may not be found if two workers attempt to concurrently
			// dispatch the same job. This is not an error condition since Amboy
			// should gracefully handle the contention so that only one worker
			// will actually run the job.
			//
			// The duplicate scope error can occur if two workers attempt to
			// concurrently dispatch two different jobs that have the same
			// scope. This is also not an error condition and simply means
			// scopes are working as designed.
			isDueToContention := amboy.IsJobNotFoundError(err) || amboy.IsDuplicateJobScopeError(err)
			grip.DebugWhen(!isDueToContention,
				message.WrapError(err, message.Fields{
					"message":       "failed to dispatch job for reasons other than dispatch/scope contention",
					"driver_id":     d.instanceID,
					"service":       "amboy.queue.mdb",
					"operation":     "dispatch job",
					"job_id":        j.ID(),
					"job_type":      j.Type().Name,
					"scopes":        j.Scopes(),
					"stat":          j.Status(),
					"is_group":      d.opts.UseGroups,
					"group":         d.opts.GroupName,
					"dup_key":       isMongoDupKey(err),
					"duration_secs": time.Since(startAt).Seconds(),
				}),
			)
			continue
		}

		return j, dispatchInfo
	}

	return nil, dispatchInfo
}

// isOtherJobHoldingScopes checks whether or not a different job is already
// holding the scopes required for this job.
func (d *mongoDriver) isOtherJobHoldingScopes(ctx context.Context, j amboy.Job) bool {
	if len(j.Scopes()) == 0 {
		return false
	}

	query := bson.M{
		"scopes": bson.M{"$in": j.Scopes()},
		"$not":   d.getIDQuery(j.ID()),
	}
	d.modifyQueryForGroup(query)
	num, err := d.getCollection().CountDocuments(ctx, query)
	if err != nil {
		return false
	}
	return num > 0
}

func (d *mongoDriver) Stats(ctx context.Context) amboy.QueueStats {
	coll := d.getCollection()

	statusFilter := bson.M{
		"$or": []bson.M{
			d.getPendingQuery(bson.M{}),
			d.getInProgQuery(bson.M{}),
			d.getRetryingQuery(bson.M{}),
		},
	}
	d.modifyQueryForGroup(statusFilter)
	matchStatus := bson.M{"$match": statusFilter}
	groupStatuses := bson.M{
		"$group": bson.M{
			"_id": bson.M{
				"completed":   "$status.completed",
				"in_prog":     "$status.in_prog",
				"needs_retry": "$retry_info.needs_retry",
			},
			"count": bson.M{"$sum": 1},
		},
	}
	pipeline := []bson.M{matchStatus, groupStatuses}

	c, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		grip.Warning(message.WrapError(err, message.Fields{
			"driver_id":  d.instanceID,
			"service":    "amboy.queue.mdb",
			"collection": coll.Name(),
			"is_group":   d.opts.UseGroups,
			"group":      d.opts.GroupName,
			"operation":  "queue stats",
			"message":    "could not count documents by status",
		}))
		return amboy.QueueStats{}
	}
	statusGroups := []struct {
		ID struct {
			Completed  bool `bson:"completed"`
			InProg     bool `bson:"in_prog"`
			NeedsRetry bool `bson:"needs_retry"`
		} `bson:"_id"`
		Count int `bson:"count"`
	}{}
	if err := c.All(ctx, &statusGroups); err != nil {
		grip.Warning(message.WrapError(err, message.Fields{
			"driver_id":  d.instanceID,
			"service":    "amboy.queue.mdb",
			"collection": coll.Name(),
			"is_group":   d.opts.UseGroups,
			"group":      d.opts.GroupName,
			"operation":  "queue stats",
			"message":    "failed to decode counts by status",
		}))
		return amboy.QueueStats{}
	}

	var pending, inProg, retrying int
	for _, group := range statusGroups {
		if !group.ID.InProg && !group.ID.Completed {
			pending += group.Count
		}
		if group.ID.InProg {
			inProg += group.Count
		}
		if group.ID.Completed && group.ID.NeedsRetry {
			retrying += group.Count
		}
	}

	// The aggregation cannot also count all the documents in the collection
	// without a collection scan, so query it separately. Because completed is
	// calculated between two non-atomic queries, the statistics could be
	// inconsistent (i.e. you could have an incorrect count of completed jobs).

	var total int64
	if d.opts.UseGroups {
		total, err = coll.CountDocuments(ctx, bson.M{"group": d.opts.GroupName})
	} else {
		total, err = coll.EstimatedDocumentCount(ctx)
	}
	grip.Warning(message.WrapError(err, message.Fields{
		"driver_id":  d.instanceID,
		"service":    "amboy.queue.mdb",
		"collection": coll.Name(),
		"operation":  "queue stats",
		"is_group":   d.opts.UseGroups,
		"group":      d.opts.GroupName,
		"message":    "problem counting total jobs",
	}))

	completed := int(total) - pending - inProg

	return amboy.QueueStats{
		Total:     int(total),
		Pending:   pending,
		Running:   inProg,
		Completed: completed,
		Retrying:  retrying,
	}
}

// getPendingQuery modifies the query to find jobs that have not started yet.
func (d *mongoDriver) getPendingQuery(q bson.M) bson.M {
	q["status.completed"] = false
	q["status.in_prog"] = false
	return q
}

// getInProgQuery modifies the query to find jobs that are in progress.
func (d *mongoDriver) getInProgQuery(q bson.M) bson.M {
	q["status.completed"] = false
	q["status.in_prog"] = true
	return q
}

// getCompletedQuery modifies the query to find jobs that are completed.
func (d *mongoDriver) getCompletedQuery(q bson.M) bson.M {
	q["status.completed"] = true
	return q
}

// getRetryingQuery modifies the query to find jobs that are retrying.
func (d *mongoDriver) getRetryingQuery(q bson.M) bson.M {
	q = d.getCompletedQuery(q)
	q["retry_info.retryable"] = true
	q["retry_info.needs_retry"] = true
	return q
}

func (d *mongoDriver) LockTimeout() time.Duration {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.opts.LockTimeout
}

func (d *mongoDriver) Dispatcher() Dispatcher {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.dispatcher
}

func (d *mongoDriver) SetDispatcher(disp Dispatcher) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dispatcher = disp
}

// ensureGroupPrefixPattern adds the group prefix for the index if it's for a
// driver using groups and the group prefix isn't already present.
func (d *mongoDriver) ensureGroupIndexPrefix(doc bson.D) bson.D {
	if !d.opts.UseGroups {
		return doc
	}
	if len(doc) > 0 && doc[0].Key == "group" {
		return doc
	}
	doc = append([]bson.E{{Key: "group", Value: bsonx.Int32(1)}}, doc...)

	return doc
}
