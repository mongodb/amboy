package queue

import (
	"context"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
)

// remoteMongoQueueGroupSingle is a group of queues backed by MongoDB that are
// multiplexed into a single MongoDB collection and isolated by group
// namespaces.
type remoteMongoQueueGroupSingle struct {
	canceler context.CancelFunc
	opts     MongoDBQueueGroupOptions
	cache    GroupCache
}

// NewMongoDBSingleQueueGroup constructs a new remote queue group. If the TTL is
// 0, the queues will not be TTLed except when the client explicitly calls
// Prune.
//
// The MongoDB single remote queue group multiplexes all queues into a single
// collection. Group name as a means to namespace each queue within the
// collection and ensure isolation.
func NewMongoDBSingleQueueGroup(ctx context.Context, opts MongoDBQueueGroupOptions) (amboy.QueueGroup, error) {
	if err := opts.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid remote queue options")
	}

	if opts.Queue.DB.DB == "" {
		return nil, errors.New("no database name specified")
	}

	if opts.Queue.DB.URI == "" {
		return nil, errors.New("no mongodb uri specified")
	}

	// This must be set to ensure that the driver accounts for multiplexing the
	// jobs of several queues within a single collection.
	opts.Queue.DB.UseGroups = true

	ctx, cancel := context.WithCancel(ctx)
	g := &remoteMongoQueueGroupSingle{
		canceler: cancel,
		opts:     opts,
		cache:    NewGroupCache(opts.TTL),
	}

	if opts.PruneFrequency > 0 && opts.TTL > 0 {
		go func() {
			defer recovery.LogStackTraceAndContinue("remote queue group background prune")
			ticker := time.NewTicker(opts.PruneFrequency)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					grip.Error(message.WrapError(g.Prune(ctx), "pruning remote queue group database"))
				}
			}
		}()
	}

	if opts.BackgroundCreateFrequency > 0 {
		go func() {
			defer recovery.LogStackTraceAndContinue("remote queue group background queue creation")
			ticker := time.NewTicker(opts.BackgroundCreateFrequency)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					grip.Error(message.WrapError(g.startQueues(ctx), "starting external queues"))
				}
			}
		}()
	}

	if err := g.startQueues(ctx); err != nil {
		return nil, errors.WithStack(err)
	}

	return g, nil
}

// getQueues return all queues that are active. A queue is active if it is
// either incomplete (still has pending or in progress jobs), or if it recently
// completed a job within the TTL. Inactive queues (i.e. queues that are both
// complete and have not recently completed a job within the TTL) are not
// returned.
func (g *remoteMongoQueueGroupSingle) getQueues(ctx context.Context) ([]string, error) {
	cursor, err := g.opts.Queue.DB.Client.Database(g.opts.Queue.DB.DB).Collection(addGroupSuffix(g.opts.Queue.DB.Collection)).Aggregate(ctx,
		[]bson.M{
			{
				"$match": bson.M{
					"$or": []bson.M{
						{
							"status.completed": false,
						},
						{
							"status.completed": true,
							"status.mod_ts":    bson.M{"$gte": time.Now().Add(-g.opts.TTL)},
						},
					},
				},
			},
			{

				"$group": bson.M{
					"_id": nil,
					"groups": bson.M{
						"$addToSet": "$group",
					},
				},
			},
		},
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	out := struct {
		Groups []string `bson:"groups"`
	}{}

	catcher := grip.NewBasicCatcher()
	for cursor.Next(ctx) {
		if err = cursor.Decode(&out); err != nil {
			catcher.Add(err)
		} else {
			break
		}
	}
	catcher.Add(cursor.Err())
	catcher.Add(cursor.Close(ctx))

	return out.Groups, catcher.Resolve()
}

func (g *remoteMongoQueueGroupSingle) startQueues(ctx context.Context) error {
	queues, err := g.getQueues(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	catcher := grip.NewBasicCatcher()
	// Refresh the TTLs on all the queues that were recently accessed or still
	// have.
	for _, id := range queues {
		_, err := g.Get(ctx, id)
		catcher.Add(err)
	}

	return catcher.Resolve()
}

// Get a queue with the given id. Get sets the last accessed time to now. Note
// that this means that the time between when the queue is retrieved and when
// the caller actually performs an operation on the queue (e.g. add a job) must
// be within the TTL; otherwise, the queue might be closed before the operation
// is done.
func (g *remoteMongoQueueGroupSingle) Get(ctx context.Context, id string, opts ...amboy.QueueOptions) (amboy.Queue, error) {
	var queue remoteQueue
	var queueOpts MongoDBQueueOptions
	var err error

	switch q := g.cache.Get(id).(type) {
	case remoteQueue:
		return q, nil
	case nil:
		mdbOpts, err := getMongoDBQueueOptions(opts...)
		if err != nil {
			return nil, errors.Wrap(err, "invalid queue options")
		}
		queueOpts = mergeMongoDBQueueOptions(append([]MongoDBQueueOptions{g.opts.Queue}, mdbOpts...)...)
		if err = queueOpts.Validate(); err != nil {
			return nil, errors.Wrap(err, "invalid queue options")
		}
		// The group name has to be set to ensure the queue uses its namespace
		// within the single multiplexed collection.
		queueOpts.DB.GroupName = id
		// These settings must apply to all queues in the queue group to ensure
		// proper management of jobs within the single multiplexed collection.
		queueOpts.DB.UseGroups = g.opts.Queue.DB.UseGroups
		queueOpts.DB.Collection = g.opts.Queue.DB.Collection
		queue, err = queueOpts.buildQueue(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "constructing queue")
		}
	default:
		return q, nil
	}

	if err = g.cache.Set(id, queue, g.opts.TTL); err != nil {
		// If another thread already created and set the queue in the cache in
		// between the cache check above and now, it should be safe to throw
		// away the queue that was just constructed since it hasn't started yet.
		queue.Close(ctx)

		if q := g.cache.Get(id); q != nil {
			return q, nil
		}

		return nil, errors.Wrap(err, "caching queue")
	}

	if err := queue.Start(ctx); err != nil {
		return nil, errors.Wrap(err, "starting queue")
	}

	return queue, nil
}

func (g *remoteMongoQueueGroupSingle) Put(ctx context.Context, name string, queue amboy.Queue) error {
	return g.cache.Set(name, queue, 0)
}

func (g *remoteMongoQueueGroupSingle) Len() int { return g.cache.Len() }

func (g *remoteMongoQueueGroupSingle) Queues(ctx context.Context) []string {
	queues, err := g.getQueues(ctx)
	grip.Warning(message.WrapError(err, "problem getting active queues in queue group"))
	return queues
}

func (g *remoteMongoQueueGroupSingle) Prune(ctx context.Context) error {
	return g.cache.Prune(ctx)
}

func (g *remoteMongoQueueGroupSingle) Close(ctx context.Context) error {
	defer g.canceler()
	return g.cache.Close(ctx)
}
