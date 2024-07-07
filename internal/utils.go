package internal

import (
	"fmt"
	"sync/atomic"
)

var queueCounter uint64

func GetUniqueTableName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, atomic.AddUint64(&queueCounter, 1))
}

// MakeNotifyChan creates a new channel with a buffer of 1.
// This is used to notify the queue that an item has been enqueued.
// I'm not sure if this is the best way to do this, but it works for now.
func MakeNotifyChan() chan struct{} {
	return make(chan struct{}, 1)
}
