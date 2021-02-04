package pool

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
)

const (
	nilJobWaitIntervalMax = time.Second
	baseJobInterval       = time.Millisecond
)

func jitterNilJobWait() time.Duration {
	return time.Duration(rand.Int63n(int64(nilJobWaitIntervalMax)))

}

func executeJob(ctx context.Context, id string, job amboy.Job, q amboy.Queue) {
	var jobCtx context.Context
	if maxTime := job.TimeInfo().MaxTime; maxTime > 0 {
		var jobCancel context.CancelFunc
		jobCtx, jobCancel = context.WithTimeout(ctx, maxTime)
		defer jobCancel()
	} else {
		jobCtx = ctx
	}
	job.Run(jobCtx)
	q.Complete(ctx, job)
	ti := job.TimeInfo()
	r := message.Fields{
		"job":           job.ID(),
		"job_type":      job.Type().Name,
		"duration_secs": ti.Duration().Seconds(),
		"dispatch_secs": ti.Start.Sub(ti.Created).Seconds(),
		"pending_secs":  ti.End.Sub(ti.Created).Seconds(),
		"queue_type":    fmt.Sprintf("%T", q),
		"stat":          job.Status(),
		"pool":          id,
		"max_time_secs": ti.MaxTime.Seconds(),
	}
	err := job.Error()
	if err != nil {
		r["error"] = err.Error()
	}

	if err != nil {
		grip.Error(r)
	} else {
		grip.Debug(r)
	}
}

func worker(bctx context.Context, id string, q amboy.Queue, wg *sync.WaitGroup) {
	var (
		err    error
		job    amboy.Job
		cancel context.CancelFunc
		ctx    context.Context
	)

	// kim: NOTE: if `go worker()` happens and the thread is added to the
	// WaitGroup while Wait() is trying to handle a 0 count (e.g. the thread
	// count hits 0, then goes back up to 1). We can fix this potentially by
	// checking for context error either at the beginning of worker() _or_
	// before every call to `go worker()`.
	// if bctx.Err() != nil {
	//     return
	// }

	wg.Add(1)
	defer wg.Done()
	defer func() {
		// if we hit a panic we want to add an error to the job;
		// kim: NOTE: let's say that executeJob panics due to the context
		// cancellation (for whatever reason) during (*localWorkers).Close().
		// Then, it will attempt to recover() by starting a new worker
		// goroutine, which, while concurrently marking threads done, may
		// temporarily hit count 0 but then the restarted worker will add 1 to
		// the count and panic wg.Wait().
		// For example, let's say there are 2 workers left (count=2). Worker 1 panics,
		// hits this recovery handler, then starts a new goroutine (count=1).
		// Then, worker 2 exits normally (count=0), which triggers wg.Wait() to
		// check for doneness. However, the new goroutine adds one more thread
		// (count=1) while it's still counting, causing the wg.Wait() to panic.
		// Source: https://stackoverflow.com/questions/39800700/waitgroup-is-reused-before-previous-wait-has-returned
		err = recovery.HandlePanicWithError(recover(), nil, "worker process encountered error")
		if err != nil {
			if job != nil {
				job.AddError(err)
				q.Complete(bctx, job)
			}
			// start a replacement worker.
			go worker(bctx, id, q, wg)
		}

		if cancel != nil {
			cancel()
		}
	}()

	timer := time.NewTimer(baseJobInterval)
	defer timer.Stop()
	for {
		select {
		case <-bctx.Done():
			return
		case <-timer.C:
			job := q.Next(bctx)
			if job == nil {
				timer.Reset(jitterNilJobWait())
				continue
			}

			ctx, cancel = context.WithCancel(bctx)
			executeJob(ctx, id, job, q)
			cancel()
			timer.Reset(baseJobInterval)
		}
	}
}
