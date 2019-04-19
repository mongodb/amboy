package queue

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/registry"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

type mongoGroupDriver struct {
	client           *mongo.Client
	group            string
	name             string
	mongodbURI       string
	dbName           string
	instanceID       string
	priority         bool
	respectWaitUntil bool
	mu               sync.RWMutex
	canceler         context.CancelFunc

	LockManager
}

func NewMongoGroupDriver(name, group string, opts MongoDBOptions) Driver {
	host, _ := os.Hostname()
	return &mongoGroupDriver{
		name:             name,
		group:            group,
		dbName:           opts.DB,
		mongodbURI:       opts.URI,
		priority:         opts.Priority,
		respectWaitUntil: opts.CheckWaitUntil,
		instanceID:       fmt.Sprintf("%s.%s.%s.%s", name, group, host, uuid.NewV4()),
	}
}

// OpenNewMongoGroupDriver constructs and opens a new MongoDB driver instance
// using the specified session. It is equivalent to calling
// NewMongoGroupDriver() and calling driver.Open().
func OpenNewMongoGroupDriver(ctx context.Context, name, group string, opts MongoDBOptions, client *mongo.Client) (Driver, error) {
	d := NewMongoGroupDriver(name, group, opts).(*mongoGroupDriver)

	if err := d.start(ctx, client); err != nil {
		return nil, errors.Wrap(err, "problem starting driver")
	}

	return d, nil
}

func (d *mongoGroupDriver) ID() string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.instanceID
}

func (d *mongoGroupDriver) Open(ctx context.Context) error {
	if d.canceler != nil {
		return nil
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(d.mongodbURI))
	if err != nil {
		return errors.Wrapf(err, "problem opening connection to mongodb at '%s", d.mongodbURI)
	}

	return errors.Wrap(d.start(ctx, client), "problem starting driver")
}

func (d *mongoGroupDriver) start(ctx context.Context, client *mongo.Client) error {
	d.LockManager = NewLockManager(ctx, d)

	dCtx, cancel := context.WithCancel(ctx)
	d.canceler = cancel

	d.mu.Lock()
	d.client = client
	d.mu.Unlock()

	startAt := time.Now()
	go func() {
		<-dCtx.Done()
		grip.Info(message.Fields{
			"message": "closing session for mongodb driver",
			"group":   d.group,
			"id":      d.instanceID,
			"uptime":  time.Since(startAt),
			"span":    time.Since(startAt).String(),
			"service": "amboy.queue.group.mongodb",
		})
	}()

	if err := d.setupDB(ctx); err != nil {
		return errors.Wrap(err, "problem setting up database")
	}

	return nil
}

func (d *mongoGroupDriver) getCollection() *mongo.Collection {
	return d.client.Database(d.dbName).Collection(addGroupSufix(d.name))
}

func (d *mongoGroupDriver) setupDB(ctx context.Context) error {
	keys := bsonx.Doc{
		{
			Key:   "group",
			Value: bsonx.Int32(1),
		},
		{
			Key:   "status.completed",
			Value: bsonx.Int32(1),
		},
		{
			Key:   "status.in_prog",
			Value: bsonx.Int32(1),
		},
	}
	if d.respectWaitUntil {
		keys = append(keys, bsonx.Elem{
			Key:   "time_info.wait_until",
			Value: bsonx.Int32(1),
		})
	}

	// priority must be at the end for the sort
	if d.priority {
		keys = append(keys, bsonx.Elem{
			Key:   "priority",
			Value: bsonx.Int32(1),
		})
	}

	_, err := d.getCollection().Indexes().CreateMany(ctx, []mongo.IndexModel{
		mongo.IndexModel{
			Keys: bsonx.Doc{
				{
					Key:   "group",
					Value: bsonx.Int32(1),
				},
				{
					Key:   "_id",
					Value: bsonx.Int32(1),
				},
			},
			Options: options.Index().SetUnique(true),
		},
		mongo.IndexModel{
			Keys: keys,
		},
		mongo.IndexModel{
			Keys: bsonx.Doc{
				{
					Key:   "group",
					Value: bsonx.Int32(1),
				},
				{
					Key:   "status.mod_ts",
					Value: bsonx.Int32(1),
				},
			},
		},
	})

	return errors.Wrap(err, "problem building indexes")
}

func (d *mongoGroupDriver) Close() {
	if d.canceler != nil {
		d.canceler()
	}
}

func (d *mongoGroupDriver) Get(ctx context.Context, name string) (amboy.Job, error) {
	j := &registry.JobInterchange{}

	err := d.getCollection().FindOne(ctx, bson.M{"_id": name}).Decode(j)
	if err != nil {
		return nil, errors.Wrapf(err, "GET problem fetching '%s'", name)
	}

	output, err := j.Resolve(amboy.BSON2)
	if err != nil {
		return nil, errors.Wrapf(err,
			"GET problem converting '%s' to job object", name)
	}

	return output, nil
}

func (d *mongoGroupDriver) Put(ctx context.Context, j amboy.Job) error {
	job, err := registry.MakeJobInterchange(j, amboy.BSON2)
	if err != nil {
		return errors.Wrap(err, "problem converting job to interchange format")
	}

	job.Group = d.group

	if _, err = d.getCollection().InsertOne(ctx, job); err != nil {
		return errors.Wrapf(err, "problem saving new job %s", j.ID())
	}

	return nil
}

func (d *mongoGroupDriver) Save(ctx context.Context, j amboy.Job) error {
	name := j.ID()
	stat := j.Status()
	stat.Owner = d.instanceID
	stat.ModificationCount++
	stat.ModificationTime = time.Now()
	j.SetStatus(stat)

	job, err := registry.MakeJobInterchange(j, amboy.BSON2)
	if err != nil {
		return errors.Wrap(err, "problem converting job to interchange format")
	}

	query := getAtomicQuery(d.instanceID, name, stat.ModificationCount)
	res, err := d.getCollection().ReplaceOne(ctx, query, job)
	if err != nil {
		if isMongoDupKey(err) {
			grip.Debug(message.Fields{
				"id":        d.instanceID,
				"group":     d.group,
				"service":   "amboy.queue.group.mongo",
				"operation": "save job",
				"name":      name,
				"outcome":   "duplicate key error, ignoring stale job",
			})
			return nil
		}
		return errors.Wrapf(err, "problem saving document %s: %+v", name, res)
	}

	return nil
}

func (d *mongoGroupDriver) SaveStatus(ctx context.Context, j amboy.Job, stat amboy.JobStatusInfo) error {
	id := j.ID()
	query := getAtomicQuery(d.instanceID, id, stat.ModificationCount)
	stat.Owner = d.instanceID
	stat.ModificationCount++
	stat.ModificationTime = time.Now()
	timeInfo := j.TimeInfo()

	res, err := d.getCollection().UpdateOne(ctx, query, bson.M{"$set": bson.M{"status": stat, "time_info": timeInfo}})
	if err != nil {
		return errors.Wrapf(err, "problem updating status document for %s", id)
	}

	if res.ModifiedCount != 1 {
		return errors.Errorf("did not update any status documents [matched=%d, modified=%d]", res.MatchedCount, res.ModifiedCount)
	}

	j.SetStatus(stat)

	return nil
}

func (d *mongoGroupDriver) Jobs(ctx context.Context) <-chan amboy.Job {
	output := make(chan amboy.Job)
	go func() {
		defer close(output)
		iter, err := d.getCollection().Find(ctx, bson.M{"group": d.group}, options.Find().SetSort(bson.M{"status.mod_ts": -1}))
		if err != nil {
			grip.Warning(message.WrapError(err, message.Fields{
				"id":        d.instanceID,
				"group":     d.group,
				"service":   "amboy.queue.group.mongo",
				"operation": "job iterator",
				"message":   "problem with query",
			}))
			return
		}
		var job amboy.Job
		for iter.Next(ctx) {
			j := &registry.JobInterchange{}
			if err = iter.Decode(j); err != nil {
				grip.Warning(message.WrapError(err, message.Fields{
					"id":        d.instanceID,
					"group":     d.group,
					"service":   "amboy.queue.group.mongo",
					"operation": "job iterator",
					"message":   "problem reading job from cursor",
				}))

				continue
			}

			job, err = j.Resolve(amboy.BSON)
			if err != nil {
				grip.Warning(message.WrapError(err, message.Fields{
					"id":        d.instanceID,
					"group":     d.group,
					"service":   "amboy.queue.group.mongo",
					"operation": "job iterator",
					"message":   "problem converting job obj",
				}))
				continue
			}

			output <- job
		}

		grip.Error(message.WrapError(iter.Err(), message.Fields{
			"id":        d.instanceID,
			"group":     d.group,
			"service":   "amboy.queue.group.mongo",
			"operation": "job iterator",
			"message":   "database interface error",
		}))
	}()
	return output
}

func (d *mongoGroupDriver) JobStats(ctx context.Context) <-chan amboy.JobStatusInfo {
	output := make(chan amboy.JobStatusInfo)
	go func() {
		defer close(output)

		iter, err := d.getCollection().Find(ctx,
			struct{}{},
			&options.FindOptions{
				Sort: bson.M{"status.mod_ts": -1},
				Projection: bson.M{
					"_id":    1,
					"status": 1,
				},
			})
		if err != nil {
			grip.Warning(message.WrapError(err, message.Fields{
				"id":        d.instanceID,
				"group":     d.group,
				"service":   "amboy.queue.group.mongo",
				"operation": "job status iterator",
				"message":   "problem with query",
			}))
			return
		}

		for iter.Next(ctx) {
			j := &registry.JobInterchange{}
			if err := iter.Decode(j); err != nil {
				grip.Warning(message.WrapError(err, message.Fields{
					"id":        d.instanceID,
					"service":   "amboy.queue.monto",
					"group":     d.group,
					"operation": "job status iterator",
					"message":   "problem converting job obj",
				}))
				continue
			}

			j.Status.ID = j.Name
			select {
			case <-ctx.Done():
				return
			case output <- j.Status:
			}

		}

	}()

	return output
}

func (d *mongoGroupDriver) Next(ctx context.Context) amboy.Job {
	var (
		qd  bson.M
		err error
		job amboy.Job
	)

	qd = bson.M{
		"group": d.group,
		"$or": []bson.M{
			{
				"status.completed": false,
				"status.in_prog":   false,
			},
			{
				"status.completed": false,
				"status.mod_ts":    bson.M{"$lte": time.Now().Add(-LockTimeout)},
				"status.in_prog":   true,
			},
		},
	}

	if d.respectWaitUntil {
		qd = bson.M{
			"$and": []bson.M{
				qd,
				{"$or": []bson.M{
					{"time_info.wait_until": bson.M{"$lte": time.Now()}},
					{"time_info.wait_until": bson.M{"$exists": false}}},
				},
			},
		}
	}

	opts := options.Find()
	if d.priority {
		opts.SetSort(bson.M{"priority": -1})
	}

	j := &registry.JobInterchange{}

	iter, err := d.getCollection().Find(ctx, qd, opts)
	if err != nil {
		grip.Error(message.WrapError(err, message.Fields{
			"id":        d.instanceID,
			"group":     d.group,
			"service":   "amboy.queue.group.mongo",
			"operation": "retrieving next job",
			"message":   "problem regenerating query",
		}))
		return nil
	}
	coll := d.getCollection().Name()
	for iter.Next(ctx) {
		grip.Info(message.Fields{
			"id":              d.instanceID,
			"group":           d.group,
			"service":         "amboy.queue.group.mongo",
			"operation":       "converting next job",
			"message":         "iterating",
			"collection_name": coll,
		})
		if err = iter.Decode(j); err != nil {
			grip.Warning(message.WrapError(err, message.Fields{
				"id":        d.instanceID,
				"service":   "amboy.queue.group.mongo",
				"group":     d.group,
				"operation": "converting next job",
				"message":   "problem reading document from cursor",
			}))
			// try for the next thing in the iterator if we can
			continue
		}

		job, err = j.Resolve(amboy.BSON2)
		if err != nil {
			grip.Warning(message.WrapError(err, message.Fields{
				"id":        d.instanceID,
				"group":     d.group,
				"service":   "amboy.queue.group.mongo",
				"operation": "converting document",
				"message":   "problem converting job from intermediate form",
			}))
			// try for the next thing in the iterator if we can
			continue
		}
		break
	}

	if err = iter.Err(); err != nil {
		grip.Warning(message.WrapError(err, message.Fields{
			"id":              d.instanceID,
			"group":           d.group,
			"service":         "amboy.queue.group.mongo",
			"message":         "problem reported by iterator",
			"operation":       "retrieving next job",
			"collection_name": coll,
		}))
	}

	if err = iter.Close(ctx); err != nil {
		grip.Warning(message.WrapError(err, message.Fields{
			"id":              d.instanceID,
			"group":           d.group,
			"service":         "amboy.queue.group.mongo",
			"message":         "problem closing iterator",
			"operation":       "retrieving next job",
			"collection_name": coll,
		}))
	}

	return job
}

func (d *mongoGroupDriver) Stats(ctx context.Context) amboy.QueueStats {
	coll := d.getCollection()

	numJobs, err := coll.CountDocuments(ctx, bson.M{"group": d.group})
	grip.Warning(message.WrapError(err, message.Fields{
		"id":         d.instanceID,
		"group":      d.group,
		"service":    "amboy.queue.group.mongo",
		"collection": coll.Name(),
		"operation":  "queue stats",
		"message":    "problem counting all jobs",
	}))

	pending, err := coll.CountDocuments(ctx, bson.M{"group": d.group, "status.completed": false})
	grip.Warning(message.WrapError(err, message.Fields{
		"id":         d.instanceID,
		"group":      d.group,
		"service":    "amboy.queue.group.mongo",
		"collection": coll.Name(),
		"operation":  "queue stats",
		"message":    "problem counting pending jobs",
	}))

	numLocked, err := coll.CountDocuments(ctx, bson.M{"group": d.group, "status.completed": false, "status.in_prog": true})
	grip.Warning(message.WrapError(err, message.Fields{
		"id":         d.instanceID,
		"group":      d.group,
		"service":    "amboy.queue.group.mongo",
		"collection": coll.Name(),
		"operation":  "queue stats",
		"message":    "problem counting locked jobs",
	}))

	return amboy.QueueStats{
		Total:     int(numJobs),
		Pending:   int(pending),
		Completed: int(numJobs - pending),
		Running:   int(numLocked),
	}
}
