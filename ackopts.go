package gopq

import "time"

type AckOpts struct {
	AckTimeout time.Duration
	// MaxRetries is the maximum number of times to re-queue an item before giving up.
	// A value of 0 means no retries. A value of -1 means infinite retries.
	MaxRetries int
}