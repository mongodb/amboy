package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/pool"
	"github.com/mongodb/amboy/registry"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

type sqsFIFOQueue struct {
	sqsClient  *sqs.SQS
	sqsURL     string
	started    bool
	numRunning int
	tasks      struct { // map jobID to job information
		completed map[string]bool
		all       map[string]amboy.Job
	}
	runner amboy.Runner
	mutex  sync.RWMutex
}

func NewSQSFifoQueue(queueName string, workers int) amboy.Queue {
	q := &sqsFIFOQueue{}
	q.tasks.completed = make(map[string]bool)
	q.tasks.all = make(map[string]amboy.Job)
	q.runner = pool.NewLocalWorkers(workers, q)
	q.started = false
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-west-2"),
	}))
	q.sqsClient = sqs.New(sess)
	result, err := q.sqsClient.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(fmt.Sprintf("%s.fifo", queueName)),
		Attributes: map[string]*string{
			"FifoQueue":              aws.String("true"),
			"DelaySeconds":           aws.String("60"),
			"MessageRetentionPeriod": aws.String("86400"),
		},
	})
	if err != nil {
		grip.Errorf("Error creating queue: %s", err)
		return nil
	}
	q.sqsURL = *result.QueueUrl
	return q
}

func (q *sqsFIFOQueue) Put(j amboy.Job) error {
	name := j.ID()
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if !q.Started() {
		return errors.Errorf("cannot put job %s; queue not started", name)
	}

	if _, ok := q.tasks.all[name]; ok {
		return errors.Errorf("cannot add %s because duplicate job already exists", name)
	}

	j.UpdateTimeInfo(amboy.JobTimeInfo{
		Created: time.Now(),
	})
	jobItem, err := registry.MakeJobInterchange(j, amboy.JSON)
	job, err := json.Marshal(jobItem)
	if err != nil { // is this the right way to handle this?
		return err
	}
	groupID := RandomString(16)
	dedupID := j.ID()
	_, err = q.sqsClient.SendMessage(&sqs.SendMessageInput{
		MessageBody:            aws.String(string(job)),
		QueueUrl:               &q.sqsURL,
		MessageGroupId:         &groupID,
		MessageDeduplicationId: &dedupID,
	})

	if err != nil {
		return errors.Errorf("Error sending message: %s", err)
	}
	return nil
}

// Returns the next job in the queue. These calls are
// blocking, but may be interrupted with a canceled context.
func (q *sqsFIFOQueue) Next(ctx context.Context) amboy.Job {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if ctx.Err() != nil {
		return nil
	}
	messageOutput, err := q.sqsClient.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: &q.sqsURL,
	})
	if err != nil || len(messageOutput.Messages) == 0 {
		grip.Debug("No messages received")
		return nil
	}
	message := messageOutput.Messages[0]
	q.sqsClient.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &q.sqsURL,
		ReceiptHandle: message.ReceiptHandle,
	})

	var jobItem *registry.JobInterchange
	err = json.Unmarshal([]byte(*message.Body), &jobItem)
	if err != nil {
		return nil
	}
	job, err := jobItem.Resolve(amboy.JSON)
	if err != nil {
		return nil
	}
	q.numRunning++
	return job
}

func (q *sqsFIFOQueue) Get(name string) (amboy.Job, bool) {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	j, ok := q.tasks.all[name]
	return j, ok
}

// true if queue has started dispatching jobs
func (q *sqsFIFOQueue) Started() bool {
	return q.started
}

// Used to mark a Job complete and remove it from the pending
// work of the queue.
func (q *sqsFIFOQueue) Complete(ctx context.Context, job amboy.Job) {
	name := job.ID()
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if ctx.Err() != nil {
		grip.Noticef("did not complete %s job, because operation "+
			"was canceled.", name)
		return
	}
	q.tasks.completed[name] = true
	if q.tasks.all[name] != nil {
		q.tasks.all[name].SetStatus(job.Status())
		q.tasks.all[name].UpdateTimeInfo(job.TimeInfo())
	}

}

// Returns a channel that produces completed Job objects.
func (q *sqsFIFOQueue) Results(ctx context.Context) <-chan amboy.Job {
	results := make(chan amboy.Job)

	go func() {
		q.mutex.Lock()
		defer q.mutex.Unlock()
		defer close(results)
		for name, job := range q.tasks.all {
			if ctx.Err() != nil {
				return
			}
			if _, ok := q.tasks.completed[name]; ok {
				results <- job
			}
		}
	}()
	return results
}

// Returns a channel that produces the status objects for all
// jobs in the queue, completed and otherwise.
func (q *sqsFIFOQueue) JobStats(ctx context.Context) <-chan amboy.JobStatusInfo {
	allInfo := make(chan amboy.JobStatusInfo)

	go func() {
		q.mutex.Lock()
		defer q.mutex.Unlock()
		defer close(allInfo)
		for name, job := range q.tasks.all {
			if ctx.Err() != nil {
				return
			}
			stat := job.Status()
			stat.ID = name
			allInfo <- stat
		}
	}()
	return allInfo
}

// Returns an object that contains statistics about the
// current state of the Queue.
func (q *sqsFIFOQueue) Stats() amboy.QueueStats {
	q.mutex.RLock()
	defer q.mutex.RUnlock()
	s := amboy.QueueStats{
		Completed: len(q.tasks.completed),
	}
	s.Running = q.numRunning - s.Completed

	var numMsgs, numMsgsInFlight int
	output, err := q.sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{aws.String("ApproximateNumberOfMessages"),
			aws.String("ApproximateNumberOfMessagesNotVisible")},
		QueueUrl: &q.sqsURL,
	})
	if err != nil {
		return s
	}

	numMsgs, _ = strconv.Atoi(*output.Attributes["ApproximateNumberOfMessages"])
	numMsgsInFlight, _ = strconv.Atoi(*output.Attributes["ApproximateNumberOfMessagesNotVisible"])

	s.Pending = numMsgs + numMsgsInFlight
	s.Total = s.Pending + s.Completed

	return s
}

// Getter for the Runner implementation embedded in the Queue
// instance.
func (q *sqsFIFOQueue) Runner() amboy.Runner {
	return q.runner
}

func (q *sqsFIFOQueue) SetRunner(r amboy.Runner) error {
	if q.Started() {
		return errors.New("cannot change runners after starting")
	}

	q.runner = r
	return nil
}

// Begins the execution of the job Queue, using the embedded
// Runner.
func (q *sqsFIFOQueue) Start(ctx context.Context) error {
	if q.Started() {
		return nil
	}

	q.started = true
	err := q.runner.Start(ctx)
	if err != nil {
		return errors.Wrap(err, "problem starting runner")
	}
	return nil
}
