/*
Package pool provides specific implementations of the amboy.Runner interface
that serve as the worker pools for jobs in work queues.

Intentionally, most of the important logic about job execution and dispatching
happens in the Queue implementation, and the Runner implementations are
simplistic.

Rate Limiting Pools Amboy includes a rate limiting pool to control the flow of
jobs processed by the queue. The averaged pool uses an exponential weighted
moving average and a targeted number of jobs to complete over an interval to
achieve a reasonable flow of jobs through the runner.
*/
package pool

// this file is intentional documentation only.
