package queue

import (
	"context"
	"regexp"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
)

// MongoDBQueueGroupOptions describe options to create a queue group backed by
// MongoDB.
type MongoDBQueueGroupOptions struct {
	// DefaultQueue represents default options for queues in the queue group.
	// These can be optionally overridden at the individual queue level.
	DefaultQueue MongoDBQueueOptions

	// RegexpQueue represents options for queues by IDs that match a regular
	// expression. These take precedence over the DefaultQueue options but have
	// lower precedence than the PerQueue options.
	RegexpQueue []RegexpMongoDBQueueOptions

	// PerQueue represent options for specific queues by ID. These take
	// precedence over the DefaultQueue and RegexpQueue options.
	PerQueue map[string]MongoDBQueueOptions

	// PruneFrequency is how often inactive queues are checked to see if they
	// can be pruned.
	PruneFrequency time.Duration

	// BackgroundCreateFrequency is how often active queues can have their TTLs
	// periodically refreshed in the background. A queue is active as long as it
	// either still has jobs to complete or the most recently completed job
	// finished within the TTL. This is useful in case a queue still has jobs to
	// process but a user does not explicitly access the queue - if the goal is
	// to ensure a queue is never pruned when it still has jobs to complete,
	// this should be set to a value lower than the TTL.
	BackgroundCreateFrequency time.Duration

	// TTL determines how long a queue is considered active without performing
	// useful work for being accessed by a user. After the TTL has elapsed, the
	// queue is allowed to be pruned.
	TTL time.Duration
}

// RegexpMongoDBQueueOptions represents a mapping from a regular expression to
// match named queues in a queue group to options for those queues.
type RegexpMongoDBQueueOptions struct {
	// Regexp is the regular expression to match against the queue ID.
	Regexp regexp.Regexp
	// Options are the queue options to apply to matching queues.
	Options MongoDBQueueOptions
}

func (o MongoDBQueueGroupOptions) validate() error {
	catcher := grip.NewBasicCatcher()
	catcher.NewWhen(o.TTL < 0, "TTL cannot be negative")
	catcher.NewWhen(o.TTL > 0 && o.TTL < time.Second, "if specified, TTL cannot be less than 1 second")
	catcher.NewWhen(o.PruneFrequency < 0, "prune frequency cannot be negative")
	catcher.NewWhen(o.PruneFrequency > 0 && o.TTL < time.Second, "if specified, prune frequency cannot be less than 1 second")
	catcher.NewWhen((o.TTL == 0 && o.PruneFrequency != 0) || (o.TTL != 0 && o.PruneFrequency == 0), "must specify both TTL and prune frequency or neither of them")
	catcher.Wrap(o.DefaultQueue.Validate(), "invalid default queue options")
	catcher.NewWhen(o.DefaultQueue.DB == nil || o.DefaultQueue.DB.Client == nil, "must provide a DB client for queue group operations")
	return catcher.Resolve()
}

// getQueueOptsWithPrecedence returns the merged options for the queue given by
// the ID. The order of precedence for merging conflicting options is (in order
// of increasing precedence): default options, per-queue options, parameter
// options. If additional options are passed as parameters, their precedence is
// based on the order that they're given.
func (o MongoDBQueueGroupOptions) getQueueOptsWithPrecedence(id string, opts ...MongoDBQueueOptions) MongoDBQueueOptions {
	precedenceOrderedOpts := []MongoDBQueueOptions{o.DefaultQueue}

	for _, regexpOpts := range o.RegexpQueue {
		if regexpOpts.Regexp.MatchString(id) {
			precedenceOrderedOpts = append(precedenceOrderedOpts, regexpOpts.Options)
		}
	}

	if perQueueOpts, ok := o.PerQueue[id]; ok {
		precedenceOrderedOpts = append(precedenceOrderedOpts, perQueueOpts)
	}

	precedenceOrderedOpts = append(precedenceOrderedOpts, opts...)
	return mergeMongoDBQueueOptions(precedenceOrderedOpts...)
}

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
// collection.
func NewMongoDBSingleQueueGroup(ctx context.Context, opts MongoDBQueueGroupOptions) (amboy.QueueGroup, error) {
	if opts.DefaultQueue.DB == nil {
		return nil, errors.New("must provide DB options")
	}

	// Because of the way this queue group is implemented, the driver must
	// account for job isolation between multiplexed queues within a single
	// collection, so it needs to set UseGroups.
	opts.DefaultQueue.DB.UseGroups = true
	// GroupName must be provided if UseGroups is set, but the queue's name is
	// not yet known here; the queue's name is determined when the queue is
	// generated dynamically in Get. Therefore, the GroupName set here is not
	// actually important and is only necessary to pass validation; the actual
	// queue's name will be validated when the queue is created.
	originalGroupName := opts.DefaultQueue.DB.GroupName
	opts.DefaultQueue.DB.GroupName = "placeholder"

	if err := opts.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid remote queue options")
	}

	opts.DefaultQueue.DB.GroupName = originalGroupName

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

// getQueues return all queues that are active. A queue is active if it still
// has jobs to process (still has pending, in progress, or retrying jobs), or if
// it recently completed a job within the TTL. Inactive queues (i.e. queues that
// have no jobs to process and have not recently completed a job within the
// TTL) are not returned.
func (g *remoteMongoQueueGroupSingle) getQueues(ctx context.Context) ([]string, error) {
	cursor, err := g.opts.DefaultQueue.DB.Client.Database(g.opts.DefaultQueue.DB.DB).Collection(addGroupSuffix(g.opts.DefaultQueue.DB.Collection)).Aggregate(ctx,
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
						{
							"status.completed":       true,
							"retry_info.retryable":   true,
							"retry_info.needs_retry": true,
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
	// have jobs to run.
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
	var err error

	switch q := g.cache.Get(id).(type) {
	case remoteQueue:
		return q, nil
	case nil:
		mdbOpts, err := getMongoDBQueueOptions(opts...)
		if err != nil {
			return nil, errors.Wrap(err, "getting queue options")
		}

		queueOpts := g.opts.getQueueOptsWithPrecedence(id, mdbOpts...)

		// The group name has to be set to ensure the queue uses its namespace
		// within the single multiplexed collection.
		queueOpts.DB.GroupName = id
		// These settings must apply to all queues in the queue group to ensure
		// proper management of jobs within the single multiplexed collection.
		queueOpts.DB.UseGroups = g.opts.DefaultQueue.DB.UseGroups
		queueOpts.DB.Collection = g.opts.DefaultQueue.DB.Collection

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
	grip.Warning(message.WrapError(err, "getting active queues in queue group"))
	return queues
}

func (g *remoteMongoQueueGroupSingle) Prune(ctx context.Context) error {
	return g.cache.Prune(ctx)
}

func (g *remoteMongoQueueGroupSingle) Close(ctx context.Context) error {
	defer g.canceler()
	return g.cache.Close(ctx)
}
