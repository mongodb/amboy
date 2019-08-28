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

func executeJob(ctx context.Context, id string, job amboy.Job, q amboy.Queue) {
	didRun := runJob(ctx, job, q, time.Now())

	r := message.Fields{
		"job":           job.ID(),
		"job_type":      job.Type().Name,
		"duration_secs": job.TimeInfo().Duration().Seconds(),
		"queue_type":    fmt.Sprintf("%T", q),
		"stat":          job.Status(),
		"pool":          id,
		"executed":      didRun,
	}
	err := job.Error()
	if err != nil {
		r["error"] = err.Error()
	}

	if didRun && err != nil {
		grip.Error(r)
	} else {
		grip.Debug(r)
	}
}

func runJob(ctx context.Context, job amboy.Job, q amboy.Queue, startAt time.Time) bool {
	ti := amboy.JobTimeInfo{
		Start: time.Now(),
	}
	job.UpdateTimeInfo(ti)
	defer func() {
		ti.End = time.Now()
		job.UpdateTimeInfo(ti)
	}()

	maxTime := job.TimeInfo().MaxTime
	if maxTime > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, maxTime)
		defer cancel()
	}

	if err := job.Lock(q.ID()); err != nil {
		job.AddError(errors.Wrap(err, "problem locking job"))
		return false
	}
	if err := q.Save(ctx, job); err != nil {
		job.AddError(errors.Wrap(err, "problem saving job state"))
		return false
	}

	pingerCtx, stopPing := context.WithCancel(ctx)
	defer stopPing()
	go func() {
		defer recovery.LogStackTraceAndContinue("background lock ping", job.ID())
		iters := 0
		ticker := time.NewTicker(amboy.LockTimeout / 2)
		defer ticker.Stop()
		for {
			select {
			case <-pingerCtx.Done():
				return
			case <-ticker.C:
				if err := job.Lock(q.ID()); err != nil {
					job.AddError(errors.Wrapf(err, "problem pinging job lock on cycle #%d", iters))
					return
				}
				if err := q.Save(ctx, job); err != nil {
					job.AddError(errors.Wrapf(err, "problem saving job for lock ping on cycle #%d", iters))
					return
				}
			}
			iters++
		}
	}()

	job.Run(ctx)

	// we want the final end time to include
	// marking complete, but setting it twice is
	// necessary for some queues
	ti.End = time.Now()
	job.UpdateTimeInfo(ti)

	stopPing()

	q.Complete(ctx, job)

	return true
}

func worker(bctx context.Context, id string, jobs <-chan amboy.Job, q amboy.Queue, wg *sync.WaitGroup) {
	var (
		err    error
		job    amboy.Job
		cancel context.CancelFunc
		ctx    context.Context
	)

	wg.Add(1)
	defer wg.Done()
	defer func() {
		// if we hit a panic we want to add an error to the job;
		err = recovery.HandlePanicWithError(recover(), nil, "worker process encountered error")
		if err != nil {
			if job != nil {
				job.AddError(err)
				q.Complete(bctx, job)
			}
			// start a replacement worker.
			go worker(bctx, id, jobs, q, wg)
		}

		if cancel != nil {
			cancel()
		}
	}()

	for {
		select {
		case <-bctx.Done():
			return
		case job = <-jobs:
			if job == nil {
				continue
			}

			ctx, cancel = context.WithCancel(bctx)
			executeJob(ctx, id, job, q)
			cancel()
		}
	}
}

func startWorkerServer(ctx context.Context, q amboy.Queue, wg *sync.WaitGroup) <-chan amboy.Job {
	output := make(chan amboy.Job)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				job := q.Next(ctx)
				if job == nil {
					continue
				}

				if job.Status().Completed {
					grip.Debug(message.Fields{
						"message":    "completed job dispatched from the queue",
						"job":        job.ID(),
						"queue_type": fmt.Sprintf("%T", q),
						"stat":       job.Status(),
					})
					continue
				}
				output <- job
			}
		}
	}()

	return output
}
