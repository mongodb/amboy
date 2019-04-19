package queue

import (
	"context"
	"sync"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
)

type remoteMongoQueueGroupSingle struct {
	canceler       context.CancelFunc
	client         *mongo.Client
	constructor    RemoteConstructor
	mu             sync.RWMutex
	mongooptions   MongoDBOptions
	prefix         string
	pruneFrequency time.Duration
	queues         map[string]amboy.Queue
}

// NewMongoRemoteSingleQueueGroup constructs a new remote queue group. If ttl is 0, the queues will not be
// TTLed except when the client explicitly calls Prune.
func NewMongoRemoteSingleQueueGroup(ctx context.Context, opts RemoteQueueGroupOptions, client *mongo.Client, mdbopts MongoDBOptions) (amboy.QueueGroup, error) {
	if err := opts.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid remote queue options")
	}

	if mdbopts.DB == "" {
		return nil, errors.New("no database name specified")
	}

	if mdbopts.URI == "" {
		return nil, errors.New("no mongodb uri specified")
	}

	ctx, cancel := context.WithCancel(ctx)
	g := &remoteMongoQueueGroup{
		canceler:       cancel,
		client:         client,
		mongooptions:   mdbopts,
		constructor:    opts.Constructor,
		prefix:         opts.Prefix,
		pruneFrequency: opts.PruneFrequency,
		queues:         map[string]amboy.Queue{},
	}

	if opts.PruneFrequency > 0 {
		go func() {
			pruneCtx, pruneCancel := context.WithCancel(context.Background())
			defer pruneCancel()
			defer recovery.LogStackTraceAndContinue("panic in remote queue group ticker")
			ticker := time.NewTicker(opts.PruneFrequency)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					grip.Error(message.WrapError(g.Prune(pruneCtx), "problem pruning remote queue group database"))
				}
			}
		}()
	}

	return g, nil
}

func (g *remoteMongoQueueGroupSingle) Get(ctx context.Context, name string) (amboy.Queue, error) {
	g.mu.RLock()
	if queue, ok := g.queues[id]; ok {
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

	driver, err := OpenNewMongoGroupDriver(ctx, g.prefix, name, g.mongooptions, g.client)
	if err != nil {
		return nil, errors.Wrap(err, "problem opening driver for queue")
	}

	queue, err := g.constructor(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "problem opening driver for queue")
	}

	queue.SetDriver(driver)
	g.queues[name] = queue

	return queue, nil
}

func (g *remoteMongoQueueGroupSingle) Put(ctx context.Context, name string, queue amboy.Queue) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.queues[name]; ok {
		return errors.New("cannot put a queue into group with existing name")
	}

	g.queues[name] = queue
	return nil
}

func (g *remoteMongoQueueGroupSingle) Prune(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	for name, queue := range g.queues {
		if queue.Stats().IsComplete() {
			queue.Runner().Close(ctx)
			delete(name, queue)
		}
	}
	return nil
}

func (g *remoteQueueGroupConstructor) Close(ctx context.Context) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.canceler()
	for name, queue := range g.queues {
		queue.Runner().Close(ctx)
		delete(name, queue)
	}
}
