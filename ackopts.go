package godq

import "time"

// AckOpts represents the queue-level settings for how acknowledgement
// of messages is handled.
type AckOpts struct {
	AckTimeout time.Duration
	MaxRetries int
	DeadLetterQueue Queue
	RetryBackoff    time.Duration
}