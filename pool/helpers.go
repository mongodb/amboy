package pool

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
)

type workUnit struct {
	job    amboy.Job
	cancel context.CancelFunc
}

func executeJob(ctx context.Context, id string, job amboy.Job, q amboy.Queue) {
	runJob(ctx, job, q, time.Now())

	r := message.Fields{
		"job":           job.ID(),
		"job_type":      job.Type().Name,
		"duration_secs": job.TimeInfo().Duration().Seconds(),
		"queue_type":    fmt.Sprintf("%T", q),
		"stat":          job.Status(),
		"pool":          id,
	}
	if err := job.Error(); err != nil {
		r["error"] = err.Error()
		grip.Error(r)
	} else {
		grip.Debug(r)
	}

}

func runJob(ctx context.Context, job amboy.Job, q amboy.Queue, startAt time.Time) {
	ti := amboy.JobTimeInfo{
		Start: time.Now(),
	}
	job.UpdateTimeInfo(ti)

	maxTime := job.TimeInfo().MaxTime
	if maxTime > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, maxTime)
		defer cancel()
	}

	if err := job.Lock(q.ID()); err != nil {
		job.AddError(errors.Wrap(err, "problem locking job"))
		return
	}
	if err := q.Save(ctx, job); err != nil {
		job.AddError(errors.Wrap(err, "problem saving job state"))
		return
	}

	pingerCtx, stopPing := context.WithCancel(ctx)
	go func() {
		iters := 0
		timer := time.NewTimer(amboy.LockTimeout / 2)
		defer timer.Stop()
		for {
			select {
			case <-pingerCtx.Done():
				return
			case <-timer.C:
				if err := job.Lock(q.ID()); err != nil {
					job.AddError(errors.Wrapf(err, "problem pinging job lock on cycle #%d", iters))
					return
				}
				if err := q.Save(ctx, job); err != nil {
					job.AddError(errors.Wrapf(err, "problem saving job for lock ping on cycle #%d", iters))
					return
				}

				timer.Reset(amboy.LockTimeout / 2)
			}
			iters++
		}
	}()

	// TODO: start lock pinging thread

	job.Run(ctx)
	// we want the final end time to include
	// marking complete, but setting it twice is
	// necessary for some queues
	ti.End = time.Now()
	job.UpdateTimeInfo(ti)
	job.Unlock(q.ID())

	stopPing()

	q.Complete(ctx, job)
	ti.End = time.Now()
	job.UpdateTimeInfo(ti)
}

func worker(ctx context.Context, id string, jobs <-chan workUnit, q amboy.Queue, wg *sync.WaitGroup) {
	var (
		err    error
		job    amboy.Job
		cancel context.CancelFunc
	)

	wg.Add(1)
	defer wg.Done()
	defer func() {
		// if we hit a panic we want to add an error to the job;
		err = recovery.HandlePanicWithError(recover(), nil, "worker process encountered error")
		if err != nil {
			if job != nil {
				job.AddError(err)
				q.Complete(ctx, job)
			}
			// start a replacement worker.
			go worker(ctx, id, jobs, q, wg)
		}

		if cancel != nil {
			cancel()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case wu := <-jobs:
			if wu.job == nil {
				continue
			}

			job = wu.job
			cancel = wu.cancel
			executeJob(ctx, id, job, q)
			cancel()
		}
	}
}

func startWorkerServer(ctx context.Context, q amboy.Queue, wg *sync.WaitGroup) <-chan workUnit {
	var nctx context.Context

	output := make(chan workUnit)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				wu := workUnit{}
				nctx, wu.cancel = context.WithCancel(ctx)

				job := q.Next(nctx)
				if job == nil {
					continue
				}

				if job.Status().Completed {
					grip.Debugf("job '%s' was dispatched from the queue but was completed",
						job.ID())
					continue
				}
				wu.job = job
				output <- wu
			}
		}
	}()

	return output
}
