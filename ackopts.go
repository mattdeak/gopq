package gopq

import "time"

// AckOpts represents the queue-level settings for how acknowledgement
// of messages is handled.

const (
	InfiniteRetries = -1
)

type BehaviourOnFailure int

type AckOpts struct {
	AckTimeout      time.Duration
	MaxRetries      int
	RetryBackoff      time.Duration

	// Has a default behaviour of dropping the message
	BehaviourOnFailure func (msg Msg) error
	FailureCallbacks []func (msg Msg) error
}


func (opts *AckOpts) RegisterFailureCallback(fn func(msg Msg) error) {
	opts.FailureCallbacks = append(opts.FailureCallbacks, fn)
}

// RegisterDeadLetterQueue sets the dead letter queue for this AcknowledgeableQueue.
// This is shorthand for RegisterFailureCallback -> Enqueue.
func (opts *AckOpts) RegisterDeadLetterQueue(dlq Enqueuer) {
	opts.RegisterFailureCallback(func(msg Msg) error {
		return dlq.Enqueue(msg.Item)
	})
}