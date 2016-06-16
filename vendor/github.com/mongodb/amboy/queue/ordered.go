// +build cgo,!gccgo

/*
Local Ordered Queue

The ordered queue evaluates the dependency information provided by the
tasks and then dispatches tasks to workers to ensure that all
dependencies have run before attempting to run a task.  If there are
cycles in the dependency graph, the queue will not run any tasks. This
implementation is local, in the sense that there is no persistence or
shared state between queue implementations.

By deafult, LocalOrdered uses the amboy/pool.Workers implementation of
amboy.Runner interface.
*/

package queue

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/gonum/graph"
	"github.com/gonum/graph/simple"
	"github.com/gonum/graph/topo"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/dependency"
	"github.com/mongodb/amboy/pool"
	"github.com/tychoish/grip"
)

// LocalOrdered implements a dependency aware local queue. The queue
// will execute tasks ordered by the topological sort of the
// dependency graph derived from the Edges() output of each job's
// Dependency object. If no task edges are specified, task ordering
// should be roughly equivalent to other non-ordered queues. If there
// are cycles in the dependency graph, the queue will error before
// starting.
type LocalOrdered struct {
	started    bool
	closed     bool
	numStarted int
	channel    chan amboy.Job
	tasks      struct {
		m         map[string]amboy.Job
		ids       map[string]int
		nodes     map[int]amboy.Job
		completed map[string]bool
		graph     *simple.DirectedGraph
	}

	// Composed functionality:
	runner amboy.Runner
	grip   grip.Journaler
	*sync.RWMutex
}

// NewLocalOrdered constructs an LocalOrdered object. The "workers"
// argument is passed to a default pool.SimplePool object.
func NewLocalOrdered(workers int) *LocalOrdered {
	q := &LocalOrdered{
		channel: make(chan amboy.Job, 100),
	}
	q.tasks.m = make(map[string]amboy.Job)
	q.tasks.ids = make(map[string]int)
	q.tasks.nodes = make(map[int]amboy.Job)
	q.tasks.completed = make(map[string]bool)
	q.tasks.graph = simple.NewDirectedGraph(1, 0)
	q.RWMutex = &sync.RWMutex{}

	q.grip = grip.NewJournaler(fmt.Sprintf("amboy.queue.ordered"))
	q.grip.CloneSender(grip.Sender())

	r := pool.NewLocalWorkers(workers, q)
	q.runner = r

	return q
}

// Put adds a job to the queue. If the queue has started dispatching
// jobs you cannot add new jobs to the queue. Additionally all jobs
// must have unique names. (i.e. job.ID() values.)
func (q *LocalOrdered) Put(j amboy.Job) error {
	name := j.ID()

	q.Lock()
	defer q.Unlock()

	if q.started {
		return fmt.Errorf("cannot add %s because ordered task dispatching has begun", name)
	}

	if q.closed {
		return fmt.Errorf("cannot add %s because queue is closed", name)
	}

	if _, ok := q.tasks.m[name]; ok {
		return fmt.Errorf("cannot add %s, because a job exists with that name", name)
	}

	id := q.tasks.graph.NewNodeID()
	node := simple.Node(id)

	q.tasks.m[name] = j
	q.tasks.ids[name] = id
	q.tasks.nodes[id] = j
	q.tasks.graph.AddNode(node)

	return nil
}

// Runner returns the embedded task runner.
func (q *LocalOrdered) Runner() amboy.Runner {
	return q.runner
}

// SetRunner allows users to substitute alternate Runner
// implementations at run time. This method fails if the runner has
// started.
func (q *LocalOrdered) SetRunner(r amboy.Runner) error {
	if q.runner.Started() {
		return errors.New("cannot change runners after starting")
	}

	q.runner = r
	return nil
}

// Started returns true when the Queue has begun dispatching tasks to
// runners.
func (q *LocalOrdered) Started() bool {
	return q.started
}

// Next returns a job from the Queue. This call is non-blocking. If
// there are no pending jobs at the moment, then Next returns an
// error. If the queue is closed and all jobs are complete, then Next
// also returns an error.
func (q *LocalOrdered) Next() (amboy.Job, error) {
	select {
	case job, ok := <-q.channel:
		if !ok {
			return nil, errors.New("all jobs complete")
		}

		return job, nil
	default:
		return nil, errors.New("no pending jobs")
	}
}

// Results provides an iterator of all "result objects," or completed
// amboy.Job objects. Does not wait for all results to be complete, and is
// closed when all results have been exhausted, even if there are more
// results pending. Other implementations may have different semantics
// for this method.
func (q *LocalOrdered) Results() <-chan amboy.Job {
	q.RLock()
	output := make(chan amboy.Job, len(q.tasks.completed))
	q.RUnlock()

	go func() {
		q.RLock()
		defer q.RUnlock()
		for _, job := range q.tasks.m {
			if job.Completed() {
				output <- job
			}
		}
		close(output)
	}()

	return output
}

// Get takes a name and returns a completed job.
func (q *LocalOrdered) Get(name string) (amboy.Job, bool) {
	q.RLock()
	defer q.RUnlock()

	j, ok := q.tasks.m[name]

	return j, ok
}

// Stats returns a statistics object with data about the total number
// of jobs tracked by the queue.
func (q *LocalOrdered) Stats() *amboy.QueueStats {
	s := &amboy.QueueStats{}

	q.RLock()
	defer q.RUnlock()

	s.Completed = len(q.tasks.completed)
	s.Total = len(q.tasks.m)
	s.Pending = s.Total - s.Completed
	s.Running = q.numStarted - s.Completed

	return s
}

func (q *LocalOrdered) buildGraph() error {
	q.RLock()
	defer q.RUnlock()

	for name, job := range q.tasks.m {
		id, ok := q.tasks.ids[name]
		if !ok {
			return fmt.Errorf("problem building a graph for job %s", name)
		}

		edges := job.Dependency().Edges()

		if len(edges) == 0 {
			// this won't block because this method is
			// only, called in Start() after the runner
			// has started, so these jobs are processed
			// asap.
			q.channel <- job
			continue
		}

		for _, dep := range edges {
			edgeID, ok := q.tasks.ids[dep]
			if !ok {
				return fmt.Errorf("for job %s, the %s dependency is not resolvable [%s]",
					name, dep, strings.Join(edges, ", "))
			}
			edge := simple.Edge{
				F: simple.Node(id),
				T: simple.Node(edgeID),
				W: 2,
			}
			q.tasks.graph.SetEdge(edge)
		}
	}

	return nil
}

// Start starts the runner worker processes organizes the graph and
// begins dispatching jobs to the workers.
func (q *LocalOrdered) Start() error {
	if q.closed {
		return fmt.Errorf("cannot start a closed queue")
	}

	if q.started {
		return nil
	}

	err := q.runner.Start()
	if err != nil {
		return err
	}

	q.started = true

	err = q.buildGraph()
	if err != nil {
		return err
	}

	ordered, err := topo.Sort(q.tasks.graph)
	if err != nil {
		return err
	}

	go q.jobDispatch(ordered)
	return nil
}

// Job dispatching that takes an ordering of graph.Nodsand waits for
// dependencies to be resolved before adding them to the queue.
func (q *LocalOrdered) jobDispatch(orderedJobs []graph.Node) {
	// we need to make sure that dependencies don't just get
	// dispatched before their dependents but that they're
	// finished. We iterate through the sorted list in reverse
	// order:
	for i := len(orderedJobs) - 1; i >= 0; i-- {
		graphItem := orderedJobs[i]

		q.Lock()
		job := q.tasks.nodes[graphItem.ID()]
		q.numStarted++
		q.Unlock()

		if job.Dependency().State() == dependency.Passed {
			q.Complete(job)
			continue
		}
		if job.Dependency().State() == dependency.Ready {
			q.channel <- job
			continue
		}

		deps := job.Dependency().Edges()
		completedDeps := make(map[string]bool)
		for {
			for _, dep := range deps {
				if completedDeps[dep] {
					// if this is true, then we've
					// seen this task before and
					// we're not waiting for it
					continue
				}

				if q.tasks.completed[dep] || q.tasks.m[dep].Completed() {
					// we've not seen this task
					// before, but we're not
					// waiting for it. We'll do a
					// less expensive check in the
					// future.
					completedDeps[dep] = true
				}
				// if neither of the above cases are
				// true, then we're still waiting for
				// a job. might make sense to put a
				// timeout here. On the other hand, if
				// there are cycles in the graph, the
				// topo.Sort should fail, and we'd
				// never get here, so assuming client
				// jobs aren't buggy it's safe enough
				// to wait here.
			}
			if len(deps) == len(completedDeps) {
				// all dependencies have passed, we can try to dispatch the job.

				if job.Dependency().State() == dependency.Passed {
					q.Complete(job)
				} else if job.Dependency().State() == dependency.Ready {
					q.channel <- job
				}

				// when the job is dispatched, we can
				// move on to the next item in the ordered queue.
				break
			}
		}
	}
}

// Complete marks a job as complete in the context of this queue instance.
func (q *LocalOrdered) Complete(j amboy.Job) {
	q.grip.Debugf("marking job (%s) as complete", j.ID())

	q.Lock()
	defer q.Unlock()

	q.tasks.completed[j.ID()] = true
}

// Wait blocks until all pending jobs in the queue are complete.
func (q *LocalOrdered) Wait() {
	for {
		stats := q.Stats()
		q.grip.Debugf("waiting for %d pending jobs (total=%d)", stats.Pending, stats.Total)
		if stats.Pending == 0 {
			break
		}
	}
}

// Close closes the queue, waiting for all jobs to complete and the
// runner to complete.
func (q *LocalOrdered) Close() {
	q.Wait()

	if !q.Closed() {
		close(q.channel)

		q.Lock()
		q.closed = true
		q.Unlock()
	}

	q.runner.Wait()
}

// Closed is true when the queue has successfully exited and false
// otherwise.
func (q *LocalOrdered) Closed() bool {
	q.RLock()
	defer q.RUnlock()

	return q.closed
}
