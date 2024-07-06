package godq

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type TryDequeuer interface {
	TryDequeueCtx(ctx context.Context) (Msg, error)
}


// dequeueBlocking blocks until an item is available to dequeue, or the context is cancelled.
func dequeueBlocking(ctx context.Context, dequeuer TryDequeuer, pollInterval time.Duration, notifyChan chan struct{}) (Msg, error) {
	for {
		item, err := dequeuer.TryDequeueCtx(ctx)
        if err == nil {
            return item, nil
        }
        if err != sql.ErrNoRows {
            return Msg{}, err
        }


		select {
			case <-ctx.Done():
				return Msg{}, fmt.Errorf("context done: %w", ctx.Err())
			case <-time.After(pollInterval): // Continue
			case <-notifyChan: // Continue
		}
	}
}