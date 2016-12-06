/*
Waiting for Jobs to Complete

The amboy package proves a number of generic methods that, using the
Queue.Stats() method, block until all jobs are complete. They provide
different semantics, which may be useful in different
circumstances. All of these functions wait until the total number of
jobs submitted to the queue is equal to the number of completed jobs,
and as a result these methods don't prevent other threads from adding
jobs to the queue after beginning to wait.
*/
package amboy

import (
	"time"

	"golang.org/x/net/context"
)

// Wait takes a queue and blocks until all tasks are completed. This
// operation runs in a tight-loop, which means that the Wait will
// return *as soon* as possible all tasks or complete. Conversely,
// it's also possible that frequent repeated calls to Stats() may
// contend with resources needed for dispatching jobs or marking them
// complete.
func Wait(q Queue) {
	for {
		if q.Stats().isComplete() {
			break
		}
	}
}

// WaitCtx make it possible to cancel, either directly or using a
// deadline or timeout, a Wait operation using a context object. The
// return value is true if all tasks are complete, and false if the
// operation returns early because it was canceled.
func WaitCtx(ctx context.Context, q Queue) bool {
	for {
		if ctx.Err() != nil {
			return false
		}

		if q.Stats().isComplete() {
			return true
		}
	}
}

// WaitInterval adds a sleep between stats calls, as a way of
// throttling the impact of repeated Stats calls to the queue.
func WaitInterval(q Queue, interval time.Duration) {
	for {
		if q.Stats().isComplete() {
			break
		}

		time.Sleep(interval)
	}
}

// WaitCtxInterval provides the Wait operation and accepts a context
// for cancellation while also waiting for an interval between stats
// calls. The return value reports if the operation was canceled or if
// all tasks are complete.
func WaitCtxInterval(ctx context.Context, q Queue, interval time.Duration) bool {
	timer := time.NewTimer(0)

	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			if q.Stats().isComplete() {
				return true
			}

			timer.Reset(interval)
		}
	}
}
