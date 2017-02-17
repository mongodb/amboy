package queue

import (
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/dependency"
	"github.com/mongodb/amboy/pool"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/message"
	"golang.org/x/net/context"
)

// SimpleRemoteOrdered queue implements the amboy.Queue interface and
// uses a driver backend, like the RemoteUnordered queue. However,
// this implementation evaluates and respects dependencies, unlike the
// RemoteUnordered implementation which executes all tasks.
//
// The term simple differentiates from a queue that schedules tasks in
// order based on reported edges, which may be more efficient with
// more complex dependency graphs. Internally SimpleRemoteOrdered and
// RemoteUnordred share an implementation *except* for the Next method,
// which differs in task dispatching strategies.
type SimpleRemoteOrdered struct {
	*remoteBase
}

// NewSimpleRemoteOrdered returns a queue with a configured local
// runner with the specified number of workers.
func NewSimpleRemoteOrdered(size int) *SimpleRemoteOrdered {
	q := &SimpleRemoteOrdered{remoteBase: newRemoteBase()}
	grip.CatchError(q.SetRunner(pool.NewLocalWorkers(size, q)))
	grip.Infof("creating new remote job queue with %d workers", size)

	return q
}

// Next contains the unique implementation details of the
// SimpleRemoteOrdered queue. It fetches a job from the backend,
// skipping all jobs that are: locked (in progress elsewhere,) marked
// as "Passed" (all work complete,) and Unresolvable (e.g. stuck). For
// jobs that are Ready to run, it dispatches them immediately.
//
// For job that are Blocked, Next also skips these jobs *but* in hopes
// that the next time this job is dispatched its dependencies will be
// ready. if there is only one Edge reported, blocked will attempt to
// dispatch the dependent job.
func (q *SimpleRemoteOrdered) Next(ctx context.Context) amboy.Job {
	start := time.Now()
	count := 1
	for {
		select {
		case <-ctx.Done():
			return nil
		case job := <-q.channel:
			lock, err := q.driver.GetLock(ctx, job)
			if err != nil {
				grip.Warning(err)
				continue
			}

			if lock.IsLocked(ctx) {
				grip.Infof("task '%s' is locked, skipping", job.ID())
				continue
			}

			lock.Lock(ctx)
			q.driver.Reload(job)

			if job.Status().Completed {
				grip.Infof("task '%s' is completed, skipping (but may be misfiled)", job.ID())
				lock.Unlock(ctx)
				continue
			}

			dep := job.Dependency()

			// The final statement ensures that jobs are dispatched in "order," or at
			// least that jobs are only dispatched when "ready" and that jobs that are
			// blocked, should wait for their dependencies to complete.
			//
			// The local version of this queue reads all jobs in and builds a DAG, which
			// it then sorts and executes in order. This takes a more rudimentary approach.
			switch dep.State() {
			case dependency.Ready:
				grip.Debugf("returning job from remote source, count = %d; duration = %s",
					count, time.Since(start))

				count++
				return job
			case dependency.Passed:
				q.addBlocked(job.ID())
				lock.Unlock(ctx)
				continue
			case dependency.Unresolved:
				grip.Warning(message.MakeFieldsMessage("detected a dependency error",
					message.Fields{
						"job":   job.ID(),
						"edges": dep.Edges(),
						"dep":   dep.Type(),
					}))
				lock.Unlock(ctx)
				grip.Infof("job %s is unresolved", job.ID())
				continue
			case dependency.Blocked:
				// this is just an optimization; if there's one dependency its easy
				// to move that job *up* in the queue by submitting it here. there's a
				// chance, however, that it's already in progress and we'll end up
				// running it twice.
				lock.Unlock(ctx)

				edges := dep.Edges()
				grip.Infof("job %s is blocked. eep! [%v]", job.ID(), edges)
				if len(edges) == 1 {
					dj, ok := q.Get(edges[0])
					if ok {
						// might need to make this non-blocking.
						q.channel <- dj
					}
				} else if len(edges) == 0 {
					grip.Criticalf("blocked task %s has no edges", job.ID())
				} else {
					grip.Infof("job '%s' has %d dependencies, passing for now",
						job.ID(), len(edges))
				}
				continue
			default:
				lock.Unlock(ctx)
				grip.Warning(message.MakeFieldsMessage("detected invalid dependency",
					message.Fields{
						"job":   job.ID(),
						"edges": dep.Edges(),
						"dep":   dep.Type(),
						"state": message.Fields{
							"value":  dep.State(),
							"valid":  dependency.IsValidState(dep.State()),
							"string": dep.State().String(),
						},
					}))
			}
		}
	}

}
