package pool

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/evergreen-ci/utility"
	"github.com/mongodb/amboy"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/recovery"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	nilJobWaitIntervalMax = time.Second
	baseJobInterval       = time.Millisecond

	libraryName                     = "github.com/mongodb/amboy"
	jobIDAttribute                  = "amboy.job.id"
	jobNameAttribute                = "amboy.job.type.name"
	jobDispatchMSAttribute          = "amboy.job.dispatch_ms"
	jobRetryRetryableAttribute      = "amboy.retry.retryable"
	jobRetryBaseIDAttribute         = "amboy.retry.base_id"
	jobRetryCurrentAttemptAttribute = "amboy.retry.current_attempt"
	jobRetryMaxAttemptsAttribute    = "amboy.retry.max_attempts"
)

func jitterNilJobWait() time.Duration {
	return time.Duration(rand.Int63n(int64(nilJobWaitIntervalMax)))

}

func executeJob(ctx context.Context, id string, j amboy.Job, q amboy.Queue) {
	ctx, span := jobSpan(ctx, j)
	defer span.End()

	var jobCtx context.Context
	if maxTime := j.TimeInfo().MaxTime; maxTime > 0 {
		var jobCancel context.CancelFunc
		jobCtx, jobCancel = context.WithTimeout(ctx, maxTime)
		defer jobCancel()
	} else {
		jobCtx = ctx
	}
	j.Run(jobCtx)
	if err := q.Complete(ctx, j); err != nil {
		grip.Warning(message.WrapError(err, message.Fields{
			"message":  "could not mark job complete",
			"job_id":   j.ID(),
			"queue_id": q.ID(),
		}))
		// If the job cannot not marked as complete in the queue, set the end
		// time so that the calculated job execution statistics are valid.
		j.UpdateTimeInfo(amboy.JobTimeInfo{
			End: time.Now(),
		})
	}

	amboy.WithRetryableQueue(q, func(rq amboy.RetryableQueue) {
		if !j.RetryInfo().ShouldRetry() {
			return
		}

		rh := rq.RetryHandler()
		if rh == nil {
			grip.Error(message.Fields{
				"message":  "cannot retry a job in a queue that does not support retrying",
				"job_id":   j.ID(),
				"queue_id": rq.ID(),
			})
			return
		}

		if err := rh.Put(ctx, j); err != nil {
			grip.Error(message.WrapError(err, message.Fields{
				"message":  "could not prepare job for retry",
				"job_id":   j.ID(),
				"queue_id": rq.ID(),
			}))
		}
	})

	ti := j.TimeInfo()
	msg := message.Fields{
		"job_id":          j.ID(),
		"job_type":        j.Type().Name,
		"duration_secs":   ti.Duration().Seconds(),
		"dispatch_secs":   ti.Start.Sub(ti.Created).Seconds(),
		"turnaround_secs": ti.End.Sub(ti.Created).Seconds(),
		"queue_type":      fmt.Sprintf("%T", q),
		"queue_id":        q.ID(),
		"stat":            j.Status(),
		"pool":            id,
		"max_time_secs":   ti.MaxTime.Seconds(),
	}

	if err := j.Error(); err != nil {
		grip.Error(message.WrapError(err, msg))
		span.SetStatus(codes.Error, "job encountered error")
		span.RecordError(err, trace.WithStackTrace(true))
	} else {
		grip.Info(msg)
	}
}

func jobSpan(ctx context.Context, j amboy.Job) (context.Context, trace.Span) {
	ti := j.TimeInfo()
	attributes := []attribute.KeyValue{
		attribute.String(jobIDAttribute, j.ID()),
		attribute.String(jobNameAttribute, j.Type().Name),
		attribute.Int64(jobDispatchMSAttribute, ti.Start.Sub(ti.Created).Milliseconds()),
		attribute.Bool(jobRetryRetryableAttribute, j.RetryInfo().Retryable),
	}
	if j.RetryInfo().Retryable {
		attributes = append(attributes,
			attribute.String(jobRetryBaseIDAttribute, j.RetryInfo().BaseJobID),
			attribute.Int(jobRetryCurrentAttemptAttribute, j.RetryInfo().CurrentAttempt),
			attribute.Int(jobRetryMaxAttemptsAttribute, j.RetryInfo().MaxAttempts))
	}
	ctx = utility.ContextWithAttributes(ctx, attributes)

	return otel.GetTracerProvider().Tracer(libraryName).Start(ctx, j.Type().Name)
}

func worker(bctx context.Context, id string, q amboy.Queue, wg *sync.WaitGroup, mu sync.Locker) {
	var (
		err    error
		job    amboy.Job
		cancel context.CancelFunc
		ctx    context.Context
	)

	mu.Lock()
	wg.Add(1)
	mu.Unlock()

	defer wg.Done()
	defer func() {
		// if we hit a panic we want to add an error to the job;
		err = recovery.HandlePanicWithError(recover(), nil, "worker process encountered error")
		if err != nil {
			if job != nil {
				job.AddError(err)
				if err := q.Complete(ctx, job); err != nil {
					grip.Warning(message.WrapError(err, message.Fields{
						"message":     "could not mark job complete",
						"job_id":      job.ID(),
						"queue_id":    q.ID(),
						"panic_error": err,
					}))
					job.AddError(err)
				}
			}
			// start a replacement worker.
			go worker(bctx, id, q, wg, mu)
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
