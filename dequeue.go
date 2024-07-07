package gopq

import (
	"context"
	"database/sql"
	"log"
	"reflect"
	"time"
)

type TryDequeuer interface {
	TryDequeueCtx(ctx context.Context) (Msg, error)
}

type ErrNoItemsWaiting struct{}

func (e *ErrNoItemsWaiting) Error() string {
	return "no items waiting"
}

type ErrDBLocked struct{}

func (e *ErrDBLocked) Error() string {
	return "database locked"
}

type ErrContextDone struct{}

func (e *ErrContextDone) Error() string {
	return "context done"
}

// A helper function to handle common dequeue errors.
func handleDequeueResult(id int64, item []byte, err error) (Msg, error) {
	if err == sql.ErrNoRows {
		return Msg{}, &ErrNoItemsWaiting{}
	}

	if err == context.Canceled {
		return Msg{}, &ErrContextDone{}
	}

	if err != nil {
		log.Println("Error dequeueing item:", err)
		log.Println("Error type:", reflect.TypeOf(err))
		return Msg{}, err
	}

	return Msg{
		ID:   id,
		Item: item,
	}, nil
}

// dequeueBlocking blocks until an item is available to dequeue, or the context is cancelled.
func dequeueBlocking(ctx context.Context, dequeuer TryDequeuer, pollInterval time.Duration, notifyChan chan struct{}) (Msg, error) {
	for {
		item, err := dequeuer.TryDequeueCtx(ctx)
		if err == nil {
			return item, nil
		}

		_, ok := err.(*ErrNoItemsWaiting)
		if !ok {
			return Msg{}, err
		}

		select {
		case <-ctx.Done():
			return Msg{}, &ErrContextDone{}

		case <-time.After(pollInterval): // Continue
		case <-notifyChan: // Continue

		}
	}
}
