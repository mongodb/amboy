package queue

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// QueueConstructor is a function passed by the client which makes a new queue for a QueueGroup.
type QueueConstructor func(ctx context.Context) (amboy.Queue, error)

// RemoteConstructor is a function passed by a client which makes a new remote queue for a QueueGroup.
type RemoteConstructor func(ctx context.Context) (Remote, error)

// localQueueGroup is a group of in-memory queues.
type localQueueGroup struct {
	mu          sync.RWMutex
	canceler    context.CancelFunc
	queues      map[string]amboy.Queue
	constructor QueueConstructor
	ttlMap      map[string]time.Time
	ttl         time.Duration
}

type LocalQueueGroupOptions struct {
	Constructor QueueConstructor
	TTL         time.Duration
}

// NewLocalQueueGroup constructs a new local queue group. If ttl is 0, the queues will not be
// TTLed except when the client explicitly calls Prune.
func NewLocalQueueGroup(ctx context.Context, opts LocalQueueGroupOptions) (amboy.QueueGroup, error) {
	if opts.Constructor == nil {
		return nil, errors.New("must pass a constructor")
	}
	if opts.TTL < 0 {
		return nil, errors.New("ttl must be greater than or equal to 0")
	}
	if opts.TTL > 0 && opts.TTL < time.Second {
		return nil, errors.New("ttl cannot be less than 1 second, unless it is 0")
	}
	ctx, cancel := context.WithCancel(ctx)
	g := &localQueueGroup{
		canceler:    cancel,
		queues:      map[string]amboy.Queue{},
		constructor: opts.Constructor,
		ttlMap:      map[string]time.Time{},
		ttl:         opts.TTL,
	}
	if opts.TTL > 0 {
		go func() {
			defer recovery.LogStackTraceAndContinue("panic in local queue group ticker")
			ticker := time.NewTicker(opts.TTL)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					g.Prune(ctx)
				}
			}
		}()
	}
	return g, nil
}

// Get a queue with the given index. Get sets the last accessed time to now. Note that this means
// that the caller must add a job to the queue within the TTL, or else it may have attempted to add
// a job to a closed queue.
func (g *localQueueGroup) Get(ctx context.Context, id string) (amboy.Queue, error) {
	g.mu.RLock()
	if queue, ok := g.queues[id]; ok {
		g.ttlMap[id] = time.Now()
		g.mu.RUnlock()
		return queue, nil
	}
	g.mu.RUnlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	// Check again in case the map was modified after we released the read lock.
	if queue, ok := g.queues[id]; ok {
		g.ttlMap[id] = time.Now()
		return queue, nil
	}

	queue, err := g.constructor(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "problem starting queue")
	}
	if err := queue.Start(ctx); err != nil {
		return nil, errors.Wrap(err, "problem starting queue")
	}
	g.queues[id] = queue
	g.ttlMap[id] = time.Now()
	return queue, nil
}

// Put a queue at the given index.
func (g *localQueueGroup) Put(ctx context.Context, id string, queue amboy.Queue) error {
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

// Prune old queues.
func (g *localQueueGroup) Prune(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	queues := make([]amboy.Queue, 0, len(g.ttlMap))
	for queueID, t := range g.ttlMap {
		if time.Since(t) > g.ttl {
			if q, ok := g.queues[queueID]; ok {
				delete(g.queues, queueID)
				queues = append(queues, q)
			}
			delete(g.ttlMap, queueID)
		}
	}
	wg := &sync.WaitGroup{}
	for _, queue := range queues {
		wg.Add(1)
		go func(queue amboy.Queue) {
			queue.Runner().Close()
			wg.Done()
		}(queue)
	}
	wg.Wait()
	return nil
}

// Close the queues.
func (g *localQueueGroup) Close(ctx context.Context) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.canceler()
	waitCh := make(chan struct{})
	wg := &sync.WaitGroup{}
	go func() {
		for _, queue := range g.queues {
			wg.Add(1)
			go func(queue amboy.Queue) {
				defer recovery.LogStackTraceAndContinue("panic in local queue group closer")
				defer wg.Done()
				queue.Runner().Close()
			}(queue)
		}
		wg.Wait()
		close(waitCh)
	}()
	select {
	case <-waitCh:
		return
	case <-ctx.Done():
		return
	}
}

// remoteQueueGroup is a group of database-backed queues.
type remoteQueueGroup struct {
	canceler       context.CancelFunc
	client         *mongo.Client
	constructor    RemoteConstructor
	mongooptions   MongoDBOptions
	mu             sync.RWMutex
	prefix         string
	pruneFrequency time.Duration
	queues         map[string]amboy.Queue
	ttl            time.Duration
	ttlMap         map[string]time.Time
}

type RemoteQueueGroupOptions struct {
	// Client is a connection to a database.
	Client *mongo.Client

	// Constructor is a function passed by the client to construct a remote queue.
	Constructor RemoteConstructor

	// MongoOptions are connection options for the database.
	MongoOptions MongoDBOptions

	// Prefix is a string prepended to the queue collections.
	Prefix string

	// PruneFrequency is how often Prune runs.
	PruneFrequency time.Duration

	// TTL is how old the oldest task in the queue must be for the collection to be pruned.
	TTL time.Duration
}

type listCollectionsOutput struct {
	Name string `bson:"name"`
}

// NewRemoteQueueGroup constructs a new remote queue group. If ttl is 0, the queues will not be
// TTLed except when the client explicitly calls Prune.
func NewRemoteQueueGroup(ctx context.Context, opts RemoteQueueGroupOptions) (amboy.QueueGroup, error) {
	ctx, cancel := context.WithCancel(ctx)

	if err := opts.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid remote queue options")
	}

	colls, err := getExistingCollections(ctx, opts.Client, opts.MongoOptions.DB, opts.Prefix)
	if err != nil {
		return nil, errors.Wrap(err, "problem getting existing collections")
	}

	g := &remoteQueueGroup{
		canceler:       cancel,
		client:         opts.Client,
		constructor:    opts.Constructor,
		mongooptions:   opts.MongoOptions,
		prefix:         opts.Prefix,
		pruneFrequency: opts.PruneFrequency,
		ttl:            opts.TTL,
	}

	g.queues = map[string]amboy.Queue{}
	g.ttlMap = map[string]time.Time{}
	catcher := grip.NewBasicCatcher()
	for _, coll := range colls {
		q, err := g.startProcessingRemoteQueue(ctx, coll)
		if err != nil {
			catcher.Add(errors.Wrap(err, "problem starting queue"))
		} else {
			g.queues[idFromCollection(g.prefix, coll)] = q
			g.ttlMap[idFromCollection(g.prefix, coll)] = time.Now()
		}
	}
	if catcher.HasErrors() {
		return nil, catcher.Resolve()
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
					g.Prune(ctx)
				}
			}
		}()
	}
	return g, nil
}

func (g *remoteQueueGroup) startProcessingRemoteQueue(ctx context.Context, coll string) (Remote, error) {
	q, err := g.constructor(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "problem starting queue")
	}
	d, err := OpenNewMongoDriver(ctx, coll, g.mongooptions, g.client)
	if err != nil {
		return nil, errors.Wrap(err, "problem opening driver")
	}
	if err := q.SetDriver(d); err != nil {
		return nil, errors.Wrap(err, "problem setting driver")
	}
	if err := q.Start(ctx); err != nil {
		return nil, errors.Wrap(err, "problem starting queue")
	}
	return q, nil
}

func (opts RemoteQueueGroupOptions) validate() error {
	if opts.Client == nil {
		return errors.New("must pass a client")
	}
	if opts.Constructor == nil {
		return errors.New("must pass a constructor")
	}
	if opts.TTL < 0 {
		return errors.New("ttl must be greater than or equal to 0")
	}
	if opts.TTL > 0 && opts.TTL < time.Second {
		return errors.New("ttl cannot be less than 1 second, unless it is 0")
	}
	if opts.PruneFrequency < 0 {
		return errors.New("prune frequency must be greater than or equal to 0")
	}
	if opts.PruneFrequency > 0 && opts.TTL < time.Second {
		return errors.New("prune frequency cannot be less than 1 second, unless it is 0")
	}
	if (opts.TTL == 0 && opts.PruneFrequency != 0) || (opts.TTL != 0 && opts.PruneFrequency == 0) {
		return errors.New("ttl and prune frequency must both be 0 or both be not 0")
	}
	if opts.MongoOptions.DB == "" {
		return errors.New("db must be set")
	}
	if opts.Prefix == "" {
		return errors.New("prefix must be set")
	}
	if opts.MongoOptions.URI == "" {
		return errors.New("uri must be set")
	}
	return nil
}

func getExistingCollections(ctx context.Context, client *mongo.Client, db, prefix string) ([]string, error) {
	c, err := client.Database(db).ListCollections(ctx, bson.M{"name": bson.M{"$regex": fmt.Sprintf("^%s.*", prefix)}})
	if err != nil {
		return nil, errors.Wrap(err, "problem calling listCollections")
	}
	defer c.Close(ctx)
	var collections []listCollectionsOutput
	for c.Next(ctx) {
		elem := &listCollectionsOutput{}
		if err := c.Decode(elem); err != nil {
			return nil, errors.Wrap(err, "problem parsing listCollections output")
		}
		collections = append(collections, *elem)
	}
	collNames := []string{}
	for _, coll := range collections {
		collNames = append(collNames, coll.Name)
	}
	return collNames, nil
}

// Get a queue with the given index. Get sets the last accessed time to now. Note that this means
// that the caller must add a job to the queue within the TTL, or else it may have attempted to add
// a job to a closed queue.
func (g *remoteQueueGroup) Get(ctx context.Context, id string) (amboy.Queue, error) {
	g.mu.RLock()
	if queue, ok := g.queues[id]; ok {
		g.ttlMap[id] = time.Now()
		g.mu.RUnlock()
		return queue, nil
	}
	g.mu.RUnlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	// Check again in case the map was modified after we released the read lock.
	if queue, ok := g.queues[id]; ok {
		g.ttlMap[id] = time.Now()
		return queue, nil
	}

	queue, err := g.startProcessingRemoteQueue(ctx, collectionFromID(g.prefix, id))
	if err != nil {
		return nil, errors.Wrap(err, "problem starting queue")
	}
	g.queues[id] = queue
	g.ttlMap[id] = time.Now()
	return queue, nil
}

// Put a queue at the given index.
func (g *remoteQueueGroup) Put(ctx context.Context, id string, queue amboy.Queue) error {
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

	queue, err := g.startProcessingRemoteQueue(ctx, collectionFromID(g.prefix, id))
	if err != nil {
		return errors.Wrap(err, "problem starting queue")
	}
	g.queues[id] = queue
	g.ttlMap[id] = time.Now()
	return nil
}

// Prune queues that have no pending work, and have completed work older than the TTL.
func (g *remoteQueueGroup) Prune(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	colls, err := getExistingCollections(ctx, g.client, g.mongooptions.DB, g.prefix)
	if err != nil {
		return errors.Wrap(err, "problem getting collections")
	}
	for i, coll := range colls {
		// This is an optimization. If we've added to the queue recently enough, there's no
		// need to query its contents, since it cannot be old enough to prune.
		if t, ok := g.ttlMap[coll]; ok && time.Since(t) < g.ttl {
			remove(colls, i)
		}
	}
	catcher := grip.NewBasicCatcher()
	wg := &sync.WaitGroup{}
	collsDeleteChan := make(chan string, len(colls))
	for _, coll := range colls {
		wg.Add(1)
		go func(collection string, ch chan string) {
			defer wg.Done()
			c := g.client.Database(g.mongooptions.DB).Collection(collection)
			one := c.FindOne(ctx, bson.M{"status.mod_ts": bson.M{"$gte": time.Now().Add(-g.ttl)}})
			if err := one.Err(); err != nil {
				catcher.Add(err)
				return
			}
			if err := one.Decode(struct{}{}); err != nil {
				if err != mongo.ErrNoDocuments {
					catcher.Add(err)
					return
				}
			} else {
				return
			}
			one = c.FindOne(ctx, bson.M{"status.completed": false})
			if err := one.Err(); err != nil {
				catcher.Add(err)
				return
			}
			if err := one.Decode(struct{}{}); err != nil {
				if err != mongo.ErrNoDocuments {
					catcher.Add(err)
					return
				}
			} else {
				return
			}
			if queue, ok := g.queues[idFromCollection(g.prefix, collection)]; ok {
				queue.Runner().Close()
				ch <- idFromCollection(g.prefix, collection)
			}
			if err := c.Drop(ctx); err != nil {
				catcher.Add(err)
			}
		}(coll, collsDeleteChan)
	}
	wg.Wait()
	close(collsDeleteChan)
	for id := range collsDeleteChan {
		delete(g.queues, id)
		delete(g.ttlMap, id)
	}

	// Another prune may have gotten to the collection first, so we should close the queue.
	queuesDeleteChan := make(chan string, len(g.queues))
	wg = &sync.WaitGroup{}
outer:
	for id, q := range g.queues {
		for _, coll := range colls {
			if id == idFromCollection(g.prefix, coll) {
				continue outer
			}
		}
		wg.Add(1)
		go func(collection string, ch chan string) {
			defer wg.Done()
			q.Runner().Close()
			ch <- id
		}(id, queuesDeleteChan)
	}
	wg.Wait()
	close(queuesDeleteChan)
	for id := range queuesDeleteChan {
		delete(g.queues, id)
		delete(g.ttlMap, id)
	}
	return catcher.Resolve()
}

// Close the queues.
func (g *remoteQueueGroup) Close(ctx context.Context) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.canceler()
	waitCh := make(chan struct{})
	wg := &sync.WaitGroup{}
	go func() {
		for _, queue := range g.queues {
			wg.Add(1)
			go func(queue amboy.Queue) {
				defer recovery.LogStackTraceAndContinue("panic in remote queue group closer")
				defer wg.Done()
				queue.Runner().Close()
			}(queue)
		}
		wg.Wait()
		close(waitCh)
	}()
	select {
	case <-waitCh:
		return
	case <-ctx.Done():
		return
	}
}

func collectionFromID(prefix, id string) string {
	return prefix + id
}

func idFromCollection(prefix, collection string) string {
	return strings.TrimPrefix(collection, prefix)
}

// remove efficiently from a slice if order doesn't matter https://stackoverflow.com/a/37335777.
func remove(s []string, i int) []string {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}
