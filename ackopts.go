package gopq

import "time"

// AckOpts represents the queue-level settings for how acknowledgement
// of messages is handled.

const (
	InfiniteRetries = -1
)

type AckOpts struct {
	AckTimeout   time.Duration
	MaxRetries   int
	RetryBackoff time.Duration

	// Has a default behaviour of dropping the message
	BehaviourOnFailure func(msg Msg) error
	FailureCallbacks   []func(msg Msg) error
}

// RegisterOnFailureCallback adds a callback to the queue that is called when
// a message fails to acknowledge.
func (q *AcknowledgeableQueue) RegisterOnFailureCallback(fn func(msg Msg) error) {
	q.FailureCallbacks = append(q.FailureCallbacks, fn)
}

// RegisterDeadLetterQueue sets the dead letter queue for this AcknowledgeableQueue.
// This is shorthand for RegisterFailureCallback -> dlq.Enqueue.
func (q *AcknowledgeableQueue) RegisterDeadLetterQueue(dlq Enqueuer) {
	q.RegisterOnFailureCallback(func(msg Msg) error {
		return dlq.Enqueue(msg.Item)
	})
}
