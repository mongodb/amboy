package amboy

// WithRetryableJob is a convenience function to perform an operation if the Job
// is a RetryableJob; otherwise, it is a no-op. Returns whether or not the job
// was a RetryableJob.
func WithRetryableJob(j Job, op func(RetryableJob)) bool {
	rj, ok := j.(RetryableJob)
	if !ok {
		return false
	}

	op(rj)

	return true
}

// WithRetryableQueue is a convenience function to perform an operation if the
// Job is a RetryableJob; otherwise, it is a no-op. Returns whether ot not the
// queue was a RetryableQueue.
func WithRetryableQueue(q Queue, op func(RetryableQueue)) bool {
	rq, ok := q.(RetryableQueue)
	if !ok {
		return false
	}

	op(rq)

	return true
}
