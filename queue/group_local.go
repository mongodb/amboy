package queue

import (
	"context"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
)

// localQueueGroup is a group of in-memory queues.
type localQueueGroup struct {
	canceler context.CancelFunc
	opts     LocalQueueGroupOptions
	cache    GroupCache
}

// LocalQueueGroupOptions describe options passed to NewLocalQueueGroup.
type LocalQueueGroupOptions struct {
	DefaultQueue LocalQueueOptions
	TTL          time.Duration
}

func (o *LocalQueueGroupOptions) Validate() error {
	catcher := grip.NewBasicCatcher()
	catcher.NewWhen(o.TTL < 0, "TTL cannot be negative")
	catcher.NewWhen(o.TTL > 0 && o.TTL < time.Second, "TTL cannot be less than 1 second, unless it is 0")
	catcher.Wrap(o.DefaultQueue.Validate(), "invalid queue options")
	return catcher.Resolve()
}

// LocalQueueOptions represent options to construct a local queue.
type LocalQueueOptions struct {
	Constructor func(ctx context.Context) (amboy.Queue, error)
}

func (o *LocalQueueOptions) BuildQueue(ctx context.Context) (amboy.Queue, error) {
	return o.Constructor(ctx)
}

func (o *LocalQueueOptions) Validate() error {
	if o.Constructor == nil {
		return errors.New("must specify a queue constructor")
	}
	return nil
}

func getLocalQueueOptions(opts ...amboy.QueueOptions) ([]LocalQueueOptions, error) {
	var localOpts []LocalQueueOptions

	for _, o := range opts {
		switch opt := o.(type) {
		case *LocalQueueOptions:
			if opt != nil {
				localOpts = append(localOpts, *opt)
			}
		default:
			return nil, errors.Errorf("found queue options of type '%T', but they must be local queue options", opt)
		}
	}

	return localOpts, nil
}

// mergeLocalQueueOptions merges all the given LocalQueueOptions into a single
// set of options. Options are applied in the order they're specified and
// conflicting options are overwritten.
func mergeLocalQueueOptions(opts ...LocalQueueOptions) LocalQueueOptions {
	var merged LocalQueueOptions
	for _, o := range opts {
		if o.Constructor != nil {
			merged.Constructor = o.Constructor
		}
	}
	return merged
}

// NewLocalQueueGroup constructs a new local queue group. If ttl is 0, the queues will not be
// TTLed except when the client explicitly calls Prune.
func NewLocalQueueGroup(ctx context.Context, opts LocalQueueGroupOptions) (amboy.QueueGroup, error) {
	if err := opts.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid options")
	}
	g := &localQueueGroup{
		opts:  opts,
		cache: NewGroupCache(opts.TTL),
	}
	ctx, g.canceler = context.WithCancel(ctx)

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
					grip.Error(message.WrapError(g.Prune(ctx),
						message.Fields{
							"group": "local queue group background pruning",
							"ttl":   opts.TTL,
						}))
				}
			}
		}()
	}
	return g, nil
}

func (g *localQueueGroup) Len() int { return g.cache.Len() }

func (g *localQueueGroup) Queues(_ context.Context) []string {
	return g.cache.Names()
}

// Get a queue with the given id. Get sets the last accessed time to now. Note
// that this means that the time between when the queue is retrieved and when
// the caller actually performs an operation on the queue (e.g. add a job) must
// be within the TTL; otherwise, the queue might be closed before the operation
// is done.
func (g *localQueueGroup) Get(ctx context.Context, id string, opts ...amboy.QueueOptions) (amboy.Queue, error) {
	q := g.cache.Get(id)
	if q != nil {
		return q, nil
	}

	localQueueOpts, err := getLocalQueueOptions(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "invalid queue options")
	}
	queueOpts := mergeLocalQueueOptions(append([]LocalQueueOptions{g.opts.DefaultQueue}, localQueueOpts...)...)
	if err := queueOpts.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid queue options")
	}
	queue, err := queueOpts.BuildQueue(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "starting queue")
	}

	if err = g.cache.Set(id, queue, g.opts.TTL); err != nil {
		// It should be safe to throw away the queue here because another thread
		// already created it and we haven't started the workers.
		if q := g.cache.Get(id); q != nil {
			return q, nil
		}

		return nil, errors.Wrap(err, "caching queue")
	}

	if err = queue.Start(ctx); err != nil {
		return nil, errors.WithStack(err)
	}

	return queue, nil
}

// Put a queue at the given index.
func (g *localQueueGroup) Put(ctx context.Context, id string, queue amboy.Queue) error {
	return errors.WithStack(g.cache.Set(id, queue, g.opts.TTL))
}

// Prune old queues.
func (g *localQueueGroup) Prune(ctx context.Context) error { return g.cache.Prune(ctx) }

// Close the queues.
func (g *localQueueGroup) Close(ctx context.Context) error { return g.cache.Close(ctx) }
