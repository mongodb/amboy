package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/job"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/grip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueSmoke(t *testing.T) {
	bctx := context.Background()
	for _, test := range []struct {
		Name        string
		Constructor func(context.Context, int) (amboy.Queue, error)
	}{
		{
			Name:        "Local",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, error) { return NewLocalUnordered(size), nil },
		},
		{
			Name: "LocalSingleRunner",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, error) {
				q := NewLocalUnordered(size)
				runner := pool.NewSingle()
				if err := runner.SetQueue(q); err != nil {
					return nil, err
				}
				if err := q.SetRunner(runner); err != nil {
					return nil, err
				}
				return q, nil
			},
		},
		{
			Name: "RateLimitedSimple",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, error) {
				q := NewLocalUnordered(size)

				runner, err := pool.NewSimpleRateLimitedWorkers(size, 10*time.Millisecond, q)
				if err != nil {
					return nil, err
				}

				if err := q.SetRunner(runner); err != nil {
					return nil, err

				}

				return q, nil
			},
		},
		{
			Name: "RateLimitedAverage",
			Constructor: func(ctx context.Context, size int) (amboy.Queue, error) {
				q := NewLocalUnordered(size)

				runner, err := pool.NewMovingAverageRateLimitedWorkers(size, 100, time.Second, q)
				if err != nil {
					return nil, err
				}

				if err := q.SetRunner(runner); err != nil {
					return nil, err

				}

				return q, nil
			},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			for _, size := range []struct {
				Name string
				Size int
			}{
				{Name: "Single", Size: 1},
				{Name: "Double", Size: 2},
				{Name: "Quad", Size: 4},
				{Name: "Octo", Size: 8},
				{Name: "DoubleOcto", Size: 16},
				{Name: "QuadDoubleOcto", Size: 32},
				{Name: "OctoOcto", Size: 64},
			} {
				t.Run(size.Name, func(t *testing.T) {
					// t.Run("UnorderedWorkload", func(t *testing.T) {
					ctx, cancel := context.WithCancel(bctx)
					defer cancel()

					q, err := test.Constructor(ctx, size.Size)
					require.NoError(t, err)

					require.NoError(t, q.Start(ctx))

					testNames := []string{"test", "second", "workers", "forty-two", "true", "false", ""}
					numJobs := size.Size * len(testNames)

					wg := &sync.WaitGroup{}

					for i := 0; i < size.Size; i++ {
						wg.Add(1)
						go func(num int) {
							for _, name := range testNames {
								cmd := fmt.Sprintf("echo %s.%d", name, num)
								j := job.NewShellJob(cmd, "")
								assert.NoError(t, q.Put(j),
									fmt.Sprintf("with %d workers", num))
								_, ok := q.Get(j.ID())
								assert.True(t, ok)
							}
							wg.Done()
						}(i)
					}
					wg.Wait()
					time.Sleep(100 * time.Millisecond)

					amboy.WaitCtxInterval(ctx, q, 100*time.Millisecond)

					assert.Equal(t, numJobs, q.Stats().Total, fmt.Sprintf("with %d workers", size.Size))

					amboy.WaitCtxInterval(ctx, q, 100*time.Millisecond)

					grip.Infof("workers complete for %d worker smoke test", size.Size)
					assert.Equal(t, numJobs, q.Stats().Completed, fmt.Sprintf("%+v", q.Stats()))
					for result := range q.Results(ctx) {
						assert.True(t, result.Status().Completed, fmt.Sprintf("with %d workers", size.Size))

						// assert that we had valid time info persisted
						ti := result.TimeInfo()
						assert.NotZero(t, ti.Start)
						assert.NotZero(t, ti.End)
					}

					statCounter := 0
					for stat := range q.JobStats(ctx) {
						statCounter++
						assert.True(t, stat.ID != "")
					}
					assert.Equal(t, numJobs, statCounter, fmt.Sprintf("want jobStats for every job"))

					grip.Infof("completed results check for %d worker smoke test", size.Size)
					// })
				})
			}
		})
	}
}
