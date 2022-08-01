package queue

import (
	"context"
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
)

// remoteMongoQueueGroup is a group of queues backed by MongoDB where each queue
// belongs to its own collection for namespace isolation.
type remoteMongoQueueGroup struct {
	collPrefix string
	canceler   context.CancelFunc
	mu         sync.RWMutex
	opts       MongoDBQueueGroupOptions
	queues     map[string]amboy.Queue
	ttlMap     map[string]time.Time
}

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
	// precedence over the DefaultQueue and PerQueueRegexp options.
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

type listCollectionsOutput struct {
	Name string `bson:"name"`
}

// NewMongoDBQueueGroup constructs a new remote queue group with the given
// prefix for its collection names. If the TTL is 0, the queues will not be
// TTLed except when the client explicitly calls Prune.
//
// The MongoDB remote queue group creates a new collection for every queue. This
// is probably most viable for lower volume workloads; however, the caching
// mechanism may be more responsive in some situations.
func NewMongoDBQueueGroup(ctx context.Context, collPrefix string, opts MongoDBQueueGroupOptions) (amboy.QueueGroup, error) {
	if opts.DefaultQueue.DB == nil {
		return nil, errors.New("must provide DB options")
	}

	// Because of the way this queue is implemented, the driver's collection
	// name has group information, but the jobs within the collection don't have
	// any group information. Therefore, we have to set UseGroups to false so
	// that the driver will treat the jobs as if they're in their own queue.
	opts.DefaultQueue.DB.UseGroups = false
	opts.DefaultQueue.DB.GroupName = ""

	// Collection must be provided for queue options, but the collection's name
	// is not yet known here; the collection's name is determine when the queue
	// is generated dynamically in Get. Therefore, the Collection set here is
	// not actually important and is only necessary to pass validation; the
	// actual collection name will be validated when the queue is created.
	originalCollName := opts.DefaultQueue.DB.Collection
	opts.DefaultQueue.DB.Collection = "placeholder"

	if err := opts.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid remote queue options")
	}

	opts.DefaultQueue.DB.Collection = originalCollName

	ctx, cancel := context.WithCancel(ctx)
	g := &remoteMongoQueueGroup{
		collPrefix: collPrefix,
		canceler:   cancel,
		opts:       opts,
		queues:     map[string]amboy.Queue{},
		ttlMap:     map[string]time.Time{},
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
					grip.Error(message.WrapError(g.Prune(ctx), "pruning remote queue group"))
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

	colls, err := g.getExistingCollections(ctx)
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

	out, _ := g.getExistingCollections(ctx)

	return out
}

func (g *remoteMongoQueueGroup) startProcessingRemoteQueue(ctx context.Context, coll string, opts ...amboy.QueueOptions) (amboy.Queue, error) {
	id := g.idFromCollection(coll)
	// The driver already adds the jobs suffix implicitly to the collection
	// name.
	coll = trimJobsSuffix(coll)
	mdbOpts, err := getMongoDBQueueOptions(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "getting queue options")
	}

	queueOpts := g.opts.getQueueOptsWithPrecedence(id, mdbOpts...)

	// The collection name has to be set to ensure the queue uses its
	// collection-level namespace.
	queueOpts.DB.Collection = coll
	// These settings must apply to all queues in the queue group because if the
	// queue-specific options differ from the defaults, it will affect the queue
	// group's ability to manage jobs properly.
	queueOpts.DB.UseGroups = g.opts.DefaultQueue.DB.UseGroups
	queueOpts.DB.GroupName = g.opts.DefaultQueue.DB.GroupName

	q, err := queueOpts.buildQueue(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "constructing queue")
	}

	if err := q.Start(ctx); err != nil {
		return nil, errors.Wrap(err, "starting queue")
	}
	return q, nil
}

func (g *remoteMongoQueueGroup) getExistingCollections(ctx context.Context) ([]string, error) {
	c, err := g.opts.DefaultQueue.DB.Client.Database(g.opts.DefaultQueue.DB.DB).ListCollections(ctx, bson.M{"name": bson.M{"$regex": fmt.Sprintf("^%s.*", g.collPrefix)}})
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

	queue, err := g.startProcessingRemoteQueue(ctx, g.collectionFromID(id), opts...)
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

	colls, err := g.getExistingCollections(ctx)
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
				c := g.opts.DefaultQueue.DB.Client.Database(g.opts.DefaultQueue.DB.DB).Collection(nextColl)
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
	return addJobsSuffix(g.collPrefix + id)
}

func (g *remoteMongoQueueGroup) idFromCollection(collection string) string {
	return trimJobsSuffix(strings.TrimPrefix(collection, g.collPrefix))
}
