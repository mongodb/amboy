package queue

import (
	"context"
	"sync"
	"time"

	"github.com/mongodb/amboy"
	"github.com/pkg/errors"
)

// NewLocalQueueGroup constructs a new local queue group. If ttl is 0, the collection will not be
// TTLed except when the client explicitly calls Prune.
func NewLocalQueueGroup(ctx context.Context, constructor QueueConstructor, ttl time.Duration) (amboy.QueueGroup, error) {
	if constructor == nil {
		return nil, errors.New("must pass a constructor")
	}
	if ttl < 0 {
		return nil, errors.New("ttl must be greater than or equal to 0")
	}
	ctx, cancel := context.WithCancel(ctx)
	g := &localQueueGroup{
		canceler:    cancel,
		queues:      map[string]amboy.Queue{},
		constructor: constructor,
		ttlMap:      map[string]time.Time{},
		ttl:         ttl,
	}
	if ttl > 0 {
		go func(ctx context.Context) {
			ticker := time.NewTicker(ttl)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					g.Prune()
				}
			}
		}(ctx)
	}
	return g, nil
}

// QueueConstructor is a function passed by the client which makes a new queue.
type QueueConstructor func(ctx context.Context) (amboy.Queue, error)

// localQueueGroup is a group of in-memory queues.
type localQueueGroup struct {
	mu          sync.RWMutex
	canceler    context.CancelFunc
	queues      map[string]amboy.Queue
	constructor func(context.Context) (amboy.Queue, error)
	ttlMap      map[string]time.Time
	ttl         time.Duration
}

// Get a queue with the given index.
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
		return nil, errors.Wrap(err, "problem constructing queue")
	}
	g.queues[id] = queue
	g.ttlMap[id] = time.Now()
	return queue, nil
}

// Put a queue at the given index.
func (g *localQueueGroup) Put(id string, queue amboy.Queue) error {
	g.mu.RLock()
	if _, ok := g.queues[id]; ok {
		g.mu.RUnlock()
		return errors.WithStack(errors.New("a queue already exists at this index"))
	}
	g.mu.RUnlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	// Check again in case the map was modified after we released the read lock.
	if _, ok := g.queues[id]; ok {
		return errors.WithStack(errors.New("a queue already exists at this index"))
	}

	g.queues[id] = queue
	g.ttlMap[id] = time.Now()
	return nil
}

// Prune old queues.
func (g *localQueueGroup) Prune() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	for queueID, t := range g.ttlMap {
		if time.Since(t) > g.ttl {
			if _, ok := g.queues[queueID]; ok {
				delete(g.queues, queueID)
			}
			delete(g.ttlMap, queueID)
		}
	}
	return nil
}

// Close the queues.
func (g *localQueueGroup) Close(ctx context.Context) {
	g.canceler()
	wg := &sync.WaitGroup{}
	for _, queue := range g.queues {
		wg.Add(1)
		go func(queue amboy.Queue) {
			queue.Runner().Close()
			wg.Done()
		}(queue)
	}
	wg.Wait()
}
