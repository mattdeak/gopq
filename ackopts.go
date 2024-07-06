package godq

import "time"

// AckOpts represents the queue-level settings for how acknowledgement
// of messages is handled.
type AckOpts struct {
	// AckTimeout is the timeout for acknowledging a message.
	// A value of 0 means no timeout. A value of -1 means infinite timeout.
	AckTimeout time.Duration
}