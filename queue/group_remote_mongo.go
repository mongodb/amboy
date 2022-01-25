package queue

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// remoteMongoQueueGroup is a group of database-backed queues.
type remoteMongoQueueGroup struct {
	canceler context.CancelFunc
	client   *mongo.Client
	mu       sync.RWMutex
	opts     MongoDBQueueGroupOptions
	queues   map[string]amboy.Queue
	ttlMap   map[string]time.Time
}

// MongoDBQueueGroupOptions describe options to create a queue group backed by
// MongoDB.
type MongoDBQueueGroupOptions struct {
	// Prefix is a string prepended to the queue collections.
	Prefix string

	// Queue represents default options for queues in the queue group. These can
	// be optionally overridden at the individual queue level.
	Queue MongoDBQueueOptions

	// Abortable controls if the queue will use an abortable pool
	// implementation. The Ordered option controls if an
	// order-respecting queue will be created, while default
	// workers sets the default number of workers new queues will
	// have if the WorkerPoolSize function is not set.
	// Abortable      bool
	// Ordered        bool
	// DefaultWorkers int

	// WorkerPoolSize determines how many works will be allocated
	// to each queue, based on the queue ID passed to it.
	// WorkerPoolSize func(string) int

	// Retryable represents the options to configure retrying jobs in the queue.
	// Retryable RetryableQueueOptions

	// PruneFrequency is how often inactive queues are checked to see if they
	// can be pruned.
	PruneFrequency time.Duration

	// BackgroundCreateFrequency is how often active queues can have their
	// TTLs periodically refreshed in the background. A queue is active as long
	// as it either still has jobs to complete or the most recently completed
	// job finished within the TTL. This is useful in case a queue still has
	// jobs to process but a user does not explicitly access the queue - if the
	// goal is to ensure a queue is never pruned when it still has jobs to
	// complete, this should be set to a value lower than the TTL.
	BackgroundCreateFrequency time.Duration

	// TTL determines how long a queue is considered active without performing
	// useful work for being accessed by a user. After the TTL has elapsed, the
	// queue is allowed to be pruned.
	TTL time.Duration
}

// kim: TODO: need to modify this constructor to be per-queue instead of global. func (opts *MongoDBQueueGroupOptions) constructor(ctx context.Context, name string) (remoteQueue, error) {
//     workers := opts.Queue.NumWorkers
//     if opts.Queue.WorkerPoolSize != nil {
//         workers = opts.Queue.WorkerPoolSize(name)
//         if workers == 0 {
//             workers = opts.Queue.NumWorkers
//         }
//     }
//
//     var q remoteQueue
//     var err error
//     qOpts := remoteOptions{
//         numWorkers: workers,
//         retryable:  opts.Queue.Retryable,
//     }
//     if opts.Queue.Ordered {
//         if q, err = newRemoteSimpleOrderedWithOptions(qOpts); err != nil {
//             return nil, errors.Wrap(err, "initializing ordered queue")
//         }
//     } else {
//         if q, err = newRemoteUnorderedWithOptions(qOpts); err != nil {
//             return nil, errors.Wrap(err, "initializing unordered queue")
//         }
//     }
//
//     if opts.Queue.Abortable {
//         p := pool.NewAbortablePool(workers, q)
//         if err = q.SetRunner(p); err != nil {
//             return nil, errors.Wrap(err, "configuring queue with runner")
//         }
//     }
//
//     return q, nil
// }

func (opts MongoDBQueueGroupOptions) validate() error {
	catcher := grip.NewBasicCatcher()
	catcher.NewWhen(opts.TTL < 0, "ttl must be greater than or equal to 0")
	catcher.NewWhen(opts.TTL > 0 && opts.TTL < time.Second, "ttl cannot be less than 1 second, unless it is 0")
	catcher.NewWhen(opts.PruneFrequency < 0, "prune frequency must be greater than or equal to 0")
	catcher.NewWhen(opts.PruneFrequency > 0 && opts.TTL < time.Second, "prune frequency cannot be less than 1 second, unless it is 0")
	catcher.NewWhen((opts.TTL == 0 && opts.PruneFrequency != 0) || (opts.TTL != 0 && opts.PruneFrequency == 0), "ttl and prune frequency must both be 0 or both be not 0")
	catcher.NewWhen(opts.Prefix == "", "prefix must be set")
	catcher.Wrap(opts.Queue.Validate(), "invalid default queue options")
	return catcher.Resolve()
}

// MongoDBQueueOptions represent options for a single queue in a queue group
// backed by MongoDB.
type MongoDBQueueOptions struct {
	// DB represents options for the MongoDB driver.
	DB *MongoDBOptions
	// NumWorkers is the default number of workers the queue should use. This
	// has lower precedence than WorkerPoolSize.
	NumWorkers *int
	// WorkerPoolSize returns the number of workers the queue should use. If
	// set, this takes precedence over NumWorkers.
	WorkerPoolSize func(string) int
	// Ordered indicates whether the queue should obey ordering the queue by
	// priority.
	Ordered *bool
	// Abortable indicates whether executing jobs can be aborted.
	Abortable *bool
	// Retryable represents options to retry jobs after they complete.
	Retryable *RetryableQueueOptions
}

// BuildQueue constructs a MongoDB-backed remote queue from the queue options.
func (o *MongoDBQueueOptions) BuildQueue(ctx context.Context, name string) (amboy.Queue, error) {
	return o.buildQueue(ctx, name)
}

func (o *MongoDBQueueOptions) buildQueue(ctx context.Context, name string) (remoteQueue, error) {
	workers := utility.FromIntPtr(o.NumWorkers)
	if o.WorkerPoolSize != nil {
		workers = o.WorkerPoolSize(name)
		if workers == 0 {
			workers = utility.FromIntPtr(o.NumWorkers)
		}
	}

	var q remoteQueue
	var err error
	var retryable RetryableQueueOptions
	if o.Retryable != nil {
		retryable = *o.Retryable
	}
	qOpts := remoteOptions{
		numWorkers: workers,
		retryable:  retryable,
	}
	if utility.FromBoolPtr(o.Ordered) {
		if q, err = newRemoteSimpleOrderedWithOptions(qOpts); err != nil {
			return nil, errors.Wrap(err, "initializing ordered queue")
		}
	} else {
		if q, err = newRemoteUnorderedWithOptions(qOpts); err != nil {
			return nil, errors.Wrap(err, "initializing unordered queue")
		}
	}

	if utility.FromBoolPtr(o.Abortable) {
		p := pool.NewAbortablePool(workers, q)
		if err = q.SetRunner(p); err != nil {
			return nil, errors.Wrap(err, "configuring queue with runner")
		}
	}

	return q, nil
}

// Validate checks that the given queue options are valid.
// kim: TODO: test
func (o *MongoDBQueueOptions) Validate() error {
	catcher := grip.NewBasicCatcher()
	catcher.NewWhen(o.NumWorkers == nil && o.WorkerPoolSize == nil, "must specify either a static number of workers or a worker pool size")
	catcher.NewWhen(utility.FromIntPtr(o.NumWorkers) <= 0, "cannot specify a negative number of workers")
	if o.Retryable != nil {
		catcher.Wrap(o.Retryable.Validate(), "invalid retryable queue options")
	}
	if o.DB != nil {
		catcher.Wrap(o.DB.Validate(), "invalid DB options")
	} else {
		catcher.New("must specify DB options")
	}
	return catcher.Resolve()
}

// getMongoDBQueueOptions resolves the given queue options into MongoDB-specific
// queue options. If the given options are not MongoDB options, this will return
// an error.
func getMongoDBQueueOptions(opts ...amboy.QueueOptions) ([]MongoDBQueueOptions, error) {
	var concreteOpts []MongoDBQueueOptions

	for _, o := range opts {
		switch concreteOpt := o.(type) {
		case *MongoDBQueueOptions:
			if concreteOpt != nil {
				concreteOpts = append(concreteOpts, *concreteOpt)
			}
		default:
			return nil, errors.Errorf("found queue options of type '%T', but they must be MongoDB options", concreteOpt)
		}
	}

	return concreteOpts, nil
}

// mergeMongoDBQueueOptions merges all the given MongoDBQueueOptions into a
// single set of options. Options are applied in the order they're specified and
// conflicting options are overwritten.
// kim: TODO: test
func mergeMongoDBQueueOptions(opts ...MongoDBQueueOptions) MongoDBQueueOptions {
	var merged MongoDBQueueOptions

	for _, o := range opts {
		if o.DB != nil {
			merged.DB = o.DB
		}
		if o.NumWorkers != nil {
			merged.NumWorkers = o.NumWorkers
		}
		if o.WorkerPoolSize != nil {
			merged.WorkerPoolSize = o.WorkerPoolSize
		}
		if o.Ordered != nil {
			merged.Ordered = o.Ordered
		}
		if o.Abortable != nil {
			merged.Abortable = o.Abortable
		}
		if o.Retryable != nil {
			merged.Retryable = o.Retryable
		}
	}

	return merged
}

type listCollectionsOutput struct {
	Name string `bson:"name"`
}

// NewMongoDBQueueGroup constructs a new remote queue group. If
// ttl is 0, the queues will not be TTLed except when the client
// explicitly calls Prune.
//
// The MongoDBRemoteQueue group creats a new collection for every queue,
// unlike the other remote queue group implementations. This is
// probably most viable for lower volume workloads; however, the
// caching mechanism may be more responsive in some situations.
func NewMongoDBQueueGroup(ctx context.Context, opts MongoDBQueueGroupOptions) (amboy.QueueGroup, error) {
	if err := opts.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid remote queue options")
	}

	if opts.Queue.DB.DB == "" {
		return nil, errors.New("no database name specified")
	}

	if opts.Queue.DB.URI == "" {
		return nil, errors.New("no mongodb uri specified")
	}

	ctx, cancel := context.WithCancel(ctx)
	g := &remoteMongoQueueGroup{
		canceler: cancel,
		opts:     opts,
		queues:   map[string]amboy.Queue{},
		ttlMap:   map[string]time.Time{},
	}

	if opts.PruneFrequency > 0 && opts.TTL > 0 {
		if err := g.Prune(ctx); err != nil {
			return nil, errors.Wrap(err, "pruning queue")
		}
	}

	if opts.PruneFrequency > 0 && opts.TTL > 0 {
		go func() {
			defer recovery.LogStackTraceAndContinue("panic in remote queue group ticker")
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
			defer recovery.LogStackTraceAndContinue("panic in remote queue group ticker")
			ticker := time.NewTicker(opts.PruneFrequency)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					grip.Error(message.WrapError(g.startQueues(ctx), "starting queues"))
				}
			}
		}()
	}

	if err := g.startQueues(ctx); err != nil {
		return nil, errors.WithStack(err)
	}

	return g, nil
}

func (g *remoteMongoQueueGroup) startQueues(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	colls, err := g.getExistingCollections(ctx, g.opts.Queue.DB.Client, g.opts.Queue.DB.DB, g.opts.Prefix)
	if err != nil {
		return errors.Wrap(err, "getting existing collections")
	}

	catcher := grip.NewBasicCatcher()
	for _, coll := range colls {
		q, err := g.startProcessingRemoteQueue(ctx, coll)
		if err != nil {
			catcher.Add(errors.Wrap(err, "starting queue"))
		} else {
			g.queues[g.idFromCollection(coll)] = q
			g.ttlMap[g.idFromCollection(coll)] = time.Now()
		}
	}

	return catcher.Resolve()
}

func (g *remoteMongoQueueGroup) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return len(g.queues)
}

func (g *remoteMongoQueueGroup) Queues(ctx context.Context) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	out, _ := g.getExistingCollections(ctx, g.opts.Queue.DB.Client, g.opts.Queue.DB.DB, g.opts.Prefix) // nolint

	return out
}

func (g *remoteMongoQueueGroup) startProcessingRemoteQueue(ctx context.Context, coll string, opts ...amboy.QueueOptions) (amboy.Queue, error) {
	coll = trimJobsSuffix(coll)
	mdbOpts, err := getMongoDBQueueOptions(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "invalid queue options")
	}
	queueOpts := mergeMongoDBQueueOptions(append([]MongoDBQueueOptions{g.opts.Queue}, mdbOpts...)...)
	if err := queueOpts.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid queue options")
	}
	q, err := queueOpts.buildQueue(ctx, coll)
	// kim: TODO: remove
	// q, err := g.opts.constructor(ctx, coll)
	if err != nil {
		return nil, errors.Wrap(err, "constructing queue")
	}

	var d remoteQueueDriver
	if queueOpts.DB.Client != nil {
		d, err = openNewMongoDriver(ctx, coll, *queueOpts.DB)
		if err != nil {
			return nil, errors.Wrap(err, "creating and opening driver")
		}
	} else {
		d, err = newMongoDriver(coll, *queueOpts.DB)
		if err != nil {
			return nil, errors.Wrap(err, "creating and opening driver")
		}
	}
	if err := q.SetDriver(d); err != nil {
		return nil, errors.Wrap(err, "setting driver")
	}
	if err := q.Start(ctx); err != nil {
		return nil, errors.Wrap(err, "starting queue")
	}
	return q, nil
}

func (g *remoteMongoQueueGroup) getExistingCollections(ctx context.Context, client *mongo.Client, db, prefix string) ([]string, error) {
	c, err := client.Database(db).ListCollections(ctx, bson.M{"name": bson.M{"$regex": fmt.Sprintf("^%s.*", prefix)}})
	if err != nil {
		return nil, errors.Wrap(err, "calling listCollections")
	}
	defer c.Close(ctx)
	var collections []string
	for c.Next(ctx) {
		elem := listCollectionsOutput{}
		if err := c.Decode(&elem); err != nil {
			return nil, errors.Wrap(err, "parsing listCollections output")
		}
		collections = append(collections, elem.Name)
	}
	if err := c.Err(); err != nil {
		return nil, errors.Wrap(err, "iterating over list collections cursor")
	}
	if err := c.Close(ctx); err != nil {
		return nil, errors.Wrap(err, "closing cursor")
	}
	return collections, nil
}

// Get a queue with the given id. Get sets the last accessed time to now. Note
// that this means that the time between when the queue is retrieved and when
// the caller actually performs an operation on the queue (e.g. add a job) must
// be within the TTL; otherwise, the queue might be closed before the operation
// is done.
func (g *remoteMongoQueueGroup) Get(ctx context.Context, id string, opts ...amboy.QueueOptions) (amboy.Queue, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if queue, ok := g.queues[id]; ok {
		g.ttlMap[id] = time.Now()
		return queue, nil
	}

	queue, err := g.startProcessingRemoteQueue(ctx, g.collectionFromID(id))
	if err != nil {
		return nil, errors.Wrap(err, "starting queue")
	}
	g.queues[id] = queue
	g.ttlMap[id] = time.Now()
	return queue, nil
}

// Put a queue with the given ID. The caller is responsible for starting the
// queue.
func (g *remoteMongoQueueGroup) Put(ctx context.Context, id string, queue amboy.Queue) error {
	g.mu.RLock()
	if _, ok := g.queues[id]; ok {
		g.mu.RUnlock()
		return errors.New("a queue already exists at this index")
	}
	g.mu.RUnlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	// Check again in case the map was modified after we released the read lock.
	if _, ok := g.queues[id]; ok {
		return errors.New("a queue already exists at this index")
	}

	g.queues[id] = queue
	g.ttlMap[id] = time.Now()
	return nil
}

// Prune queues that have no pending work, and have completed work older than the TTL.
func (g *remoteMongoQueueGroup) Prune(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	colls, err := g.getExistingCollections(ctx, g.opts.Queue.DB.Client, g.opts.Queue.DB.DB, g.opts.Prefix)
	if err != nil {
		return errors.Wrap(err, "getting collections")
	}
	collsToCheck := []string{}
	for _, coll := range colls {
		// This is an optimization. If we've added to the queue recently enough, there's no
		// need to query its contents, since it cannot be old enough to prune.
		if t, ok := g.ttlMap[g.idFromCollection(coll)]; !ok || ok && time.Since(t) > g.opts.TTL {
			collsToCheck = append(collsToCheck, coll)
		}
	}
	catcher := grip.NewBasicCatcher()
	wg := &sync.WaitGroup{}
	collsDeleteChan := make(chan string, len(collsToCheck))
	collsDropChan := make(chan string, len(collsToCheck))

	for _, coll := range collsToCheck {
		collsDropChan <- coll
	}
	close(collsDropChan)

	wg = &sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func(ch chan string) {
			defer recovery.LogStackTraceAndContinue("panic in pruning collections")
			defer wg.Done()
			for nextColl := range collsDropChan {
				c := g.opts.Queue.DB.Client.Database(g.opts.Queue.DB.DB).Collection(nextColl)
				count, err := c.CountDocuments(ctx, bson.M{
					"status.completed": true,
					"status.in_prog":   false,
					"$or": []bson.M{
						{"status.mod_ts": bson.M{"$gte": time.Now().Add(-g.opts.TTL)}},
						{
							"retry_info.retryable":   true,
							"retry_info.needs_retry": true,
						},
					},
				})
				if err != nil {
					catcher.Add(err)
					return
				}
				if count > 0 {
					return
				}
				count, err = c.CountDocuments(ctx, bson.M{"status.completed": false})
				if err != nil {
					catcher.Add(err)
					return
				}
				if count > 0 {
					return
				}
				if queue, ok := g.queues[g.idFromCollection(nextColl)]; ok {
					queue.Close(ctx)
					select {
					case <-ctx.Done():
						return
					case ch <- g.idFromCollection(nextColl):
						// pass
					}
				}
				if err := c.Drop(ctx); err != nil {
					catcher.Add(err)
				}
			}
		}(collsDeleteChan)
	}
	wg.Wait()
	close(collsDeleteChan)
	for id := range collsDeleteChan {
		delete(g.queues, id)
		delete(g.ttlMap, id)
	}

	return catcher.Resolve()
}

// Close the queues.
func (g *remoteMongoQueueGroup) Close(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.canceler()
	waitCh := make(chan struct{})
	wg := &sync.WaitGroup{}
	go func() {
		defer recovery.LogStackTraceAndContinue("panic in remote queue group closer")
		for _, queue := range g.queues {
			wg.Add(1)
			go func(queue amboy.Queue) {
				defer recovery.LogStackTraceAndContinue("panic in remote queue group closer")
				defer wg.Done()
				queue.Close(ctx)
			}(queue)
		}
		wg.Wait()
		close(waitCh)
	}()
	select {
	case <-waitCh:
		return nil
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	}
}

func (g *remoteMongoQueueGroup) collectionFromID(id string) string {
	return addJobsSuffix(g.opts.Prefix + id)
}

func (g *remoteMongoQueueGroup) idFromCollection(collection string) string {
	return trimJobsSuffix(strings.TrimPrefix(collection, g.opts.Prefix))
}
