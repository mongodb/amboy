package queue

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// remoteMgoQueueGroup is a group of database-backed queues.
type remoteMgoQueueGroup struct {
	canceler context.CancelFunc
	session  *mgo.Session
	opts     RemoteQueueGroupOptions
	dbOpts   MongoDBOptions
	cache    GroupCache
}

// NewMgoRemoteQueueGroup constructs a new remote queue group. If ttl is 0, the queues will not be
// TTLed except when the client explicitly calls Prune.
func NewMgoRemoteQueueGroup(ctx context.Context, opts RemoteQueueGroupOptions, session *mgo.Session, mdbopts MongoDBOptions) (amboy.QueueGroup, error) {
	if err := opts.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid remote queue options")
	}

	if mdbopts.DB == "" {
		return nil, errors.New("no database name specified")
	}

	if mdbopts.URI == "" {
		return nil, errors.New("no mongodb uri specified")
	}

	session = session.Clone()
	ctx, cancel := context.WithCancel(ctx)
	g := &remoteMgoQueueGroup{
		canceler: cancel,
		session:  session,
		dbOpts:   mdbopts,
		opts:     opts,
		cache:    NewGroupCache(opts.TTL),
	}

	if opts.PruneFrequency > 0 && opts.TTL > 0 {
		if err := g.Prune(ctx); err != nil {
			return nil, errors.Wrap(err, "problem pruning queue")
		}

		go func() {
			defer recovery.LogStackTraceAndContinue("panic in remote queue group ticker")
			ticker := time.NewTicker(opts.PruneFrequency)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					grip.Error(message.WrapError(g.Prune(ctx), "problem pruning remote queue group database"))
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
					grip.Error(message.WrapError(g.startQueues(ctx), "problem starting queues"))
				}
			}
		}()
	}

	if err := g.startQueues(ctx); err != nil {
		return nil, errors.WithStack(err)
	}

	return g, nil
}

func (g *remoteMgoQueueGroup) Len() int { return g.cache.Len() }

func (g *remoteMgoQueueGroup) Queues(ctx context.Context) []string {
	out, _ := g.getExistingCollections(ctx, g.session, g.dbOpts.DB, g.opts.Prefix)

	return out
}

func (g *remoteMgoQueueGroup) startQueues(ctx context.Context) error {
	colls, err := g.getExistingCollections(ctx, g.session, g.dbOpts.DB, g.opts.Prefix)
	if err != nil {
		return errors.Wrap(err, "problem getting existing collections")
	}

	catcher := grip.NewBasicCatcher()
	for _, coll := range colls {
		q, err := g.startProcessingRemoteQueue(ctx, coll)
		if err != nil {
			catcher.Wrap(err, "problem starting queue")
		} else {
			catcher.Add(g.cache.Set(g.idFromCollection(coll), q, 0))
		}
	}

	return catcher.Resolve()
}

func (g *remoteMgoQueueGroup) startProcessingRemoteQueue(ctx context.Context, coll string) (Remote, error) {
	q, err := g.buildRemoteQueue(ctx, coll)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if err := q.Start(ctx); err != nil {
		return nil, errors.Wrap(err, "problem starting queue")
	}

	return q, nil
}

func (g *remoteMgoQueueGroup) buildRemoteQueue(ctx context.Context, coll string) (Remote, error) {
	coll = trimJobsSuffix(coll)
	q := g.opts.constructor(ctx, coll)

	d, err := OpenNewMgoDriver(ctx, coll, g.dbOpts, g.session.Clone())
	if err != nil {
		return nil, errors.Wrap(err, "problem opening driver")
	}
	if err := q.SetDriver(d); err != nil {
		return nil, errors.Wrap(err, "problem setting driver")
	}
	return q, nil
}

func (g *remoteMgoQueueGroup) getExistingCollections(ctx context.Context, session *mgo.Session, db, prefix string) ([]string, error) {
	colls, err := session.DB(db).CollectionNames()
	if err != nil {
		return nil, errors.Wrap(err, "problem calling listCollections")
	}
	var collections []string
	for _, c := range colls {
		if strings.HasPrefix(c, prefix) {
			collections = append(collections, c)
		}
	}

	return collections, nil
}

// Get a queue with the given index. Get sets the last accessed time to now. Note that this means
// that the caller must add a job to the queue within the TTL, or else it may have attempted to add
// a job to a closed queue.
func (g *remoteMgoQueueGroup) Get(ctx context.Context, id string) (amboy.Queue, error) {
	q := g.cache.Get(id)
	if q != nil {
		return q, nil
	}

	queue, err := g.buildRemoteQueue(ctx, g.collectionFromID(id))
	if err != nil {
		return nil, errors.Wrap(err, "problem starting queue")
	}

	err = g.cache.Set(id, queue, g.opts.TTL)
	if err != nil {
		return g.cache.Get(id), nil
	}

	if err := queue.Start(ctx); err != nil {
		return nil, errors.Wrap(err, "problem starting queue")
	}

	return queue, nil
}

// Put a queue at the given index. The caller is responsible for starting thq queue.
func (g *remoteMgoQueueGroup) Put(ctx context.Context, id string, queue amboy.Queue) error {
	return g.cache.Set(id, queue, g.opts.TTL)
}

// Prune queues that have no pending work, and have completed work older than the TTL.
func (g *remoteMgoQueueGroup) Prune(ctx context.Context) error {
	colls, err := g.getExistingCollections(ctx, g.session, g.dbOpts.DB, g.opts.Prefix)
	if err != nil {
		return errors.Wrap(err, "problem getting collections")
	}

	work := make(chan string, len(colls))
	for _, coll := range colls {
		work <- coll
	}
	close(work)

	catcher := grip.NewBasicCatcher()

	wg := &sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer recovery.LogStackTraceAndContinue("panic in pruning collections")
			defer wg.Done()
			session := g.session.Clone()
			defer session.Close()
			for nextColl := range work {
				c := session.DB(g.dbOpts.DB).C(nextColl)
				count, err := c.Find(bson.M{
					"status.completed": true,
					"status.in_prog":   false,
					"status.mod_ts":    bson.M{"$gte": time.Now().Add(-g.opts.TTL)},
				}).Count()
				if err != nil {
					catcher.Add(err)
					return
				}
				if count > 0 {
					continue
				}
				count, err = c.Find(bson.M{"status.completed": false}).Count()
				if err != nil {
					catcher.Add(err)
					return
				}
				if count > 0 {
					continue
				}

				catcher.Add(c.DropCollection())
				catcher.Add(g.cache.Remove(ctx, nextColl))
			}
		}()
	}
	wg.Wait()

	return catcher.Resolve()
}

// Close the queues.
func (g *remoteMgoQueueGroup) Close(ctx context.Context) error { return g.cache.Close(ctx) }

func (g *remoteMgoQueueGroup) collectionFromID(id string) string {
	return addJobsSuffix(g.opts.Prefix + id)
}

func (g *remoteMgoQueueGroup) idFromCollection(collection string) string {
	return trimJobsSuffix(strings.TrimPrefix(collection, g.opts.Prefix))
}
