package gopq

import "time"

// AckOpts represents the queue-level settings for how acknowledgement
// of messages is handled.

const (
	InfiniteRetries = -1
)

type AckOpts struct {
	AckTimeout      time.Duration
	MaxRetries      int
	RetryBackoff    time.Duration
	DeadLetterQueue Enqueuer
}
