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
)

// mockDispatcher provides a mock implementation of a Dispatcher whose behavior
// can be more easily configured for testing.
type mockDispatcher struct {
	queue              amboy.Queue
	mu                 sync.Mutex
	dispatched         map[string]dispatcherInfo
	shouldFailDispatch func(amboy.Job) bool
	initialLock        func(amboy.Job) error
	lockPing           func(context.Context, amboy.Job)
}

func newMockDispatcher(q amboy.Queue) *mockDispatcher {
	return &mockDispatcher{
		queue:      q,
		dispatched: map[string]dispatcherInfo{},
	}
}

func (d *mockDispatcher) Dispatch(ctx context.Context, j amboy.Job) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.shouldFailDispatch != nil && d.shouldFailDispatch(j) {
		return errors.Errorf("fail dispatch for job '%s'", j.ID())
	}

	if _, ok := d.dispatched[j.ID()]; ok {
		return errors.Errorf("cannot dispatch a job more than once")
	}

	ti := amboy.JobTimeInfo{
		Start: time.Now(),
	}
	j.UpdateTimeInfo(ti)

	if d.initialLock != nil {
		if err := d.initialLock(j); err != nil {
			return errors.Wrap(err, "taking initial lock on job")
		}
	} else {
		if err := j.Lock(d.queue.ID(), d.queue.Info().LockTimeout); err != nil {
			return errors.Wrap(err, "locking job")
		}
		if err := d.queue.Save(ctx, j); err != nil {
			return errors.Wrap(err, "saving job lock")
		}
	}

	pingCtx, stopPing := context.WithCancel(ctx)
	if d.lockPing != nil {
		go func() {
			defer recovery.LogStackTraceAndContinue("mock background job lock ping", j.ID())
			d.lockPing(ctx, j)
		}()
	} else {
		go func() {
			defer recovery.LogStackTraceAndContinue("mock background job lock ping", j.ID())
			pingJobLock(ctx, pingCtx, d.queue, j, func() {})
		}()
	}

	d.dispatched[j.ID()] = dispatcherInfo{
		job:      j,
		stopPing: stopPing,
	}

	return nil
}

func (d *mockDispatcher) Release(ctx context.Context, j amboy.Job) {
	d.mu.Lock()
	defer d.mu.Unlock()

	grip.Debug(message.WrapError(d.release(j), message.Fields{
		"service":  "mock dispatcher",
		"queue_id": d.queue.ID(),
		"job_id":   j.ID(),
	}))
}

func (d *mockDispatcher) release(j amboy.Job) error {
	info, ok := d.dispatched[j.ID()]
	if !ok {
		return errors.New("attempting to release an unowned job")
	}

	info.stopPing()
	delete(d.dispatched, j.ID())

	return nil
}

func (d *mockDispatcher) Complete(ctx context.Context, j amboy.Job) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.release(j); err != nil {
		grip.Debug(message.WrapError(err, message.Fields{
			"service":  "mock dispatcher",
			"queue_id": d.queue.ID(),
			"job_id":   j.ID(),
		}))
		return
	}

	ti := j.TimeInfo()
	ti.End = time.Now()
	j.UpdateTimeInfo(ti)
}
