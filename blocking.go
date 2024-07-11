package gopq

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

type tryDequeuer interface {
	TryDequeueCtx(ctx context.Context) (Msg, error)
}

type ErrNoItemsWaiting struct{}

func (e *ErrNoItemsWaiting) Error() string {
	return "no items waiting"
}

type ErrDBLocked struct{}

func (e *ErrDBLocked) Error() string {
	return "database table is locked"
}

// A helper function to handle common dequeue errors.
func handleDequeueResult(id int64, item []byte, err error) (Msg, error) {
	if err == sql.ErrNoRows {
		return Msg{}, &ErrNoItemsWaiting{}
	}

	// On error on cancelled context
	if err != nil {
		return Msg{}, err
	}

	return Msg{
		ID:   id,
		Item: item,
	}, nil
}

// dequeueBlocking blocks until an item is available to dequeue, or the context is cancelled.
func dequeueBlocking(ctx context.Context, dequeuer tryDequeuer, pollInterval time.Duration, notifyChan chan struct{}) (Msg, error) {
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
			return Msg{}, context.Canceled

		case <-time.After(pollInterval): // Continue
		case <-notifyChan: // Continue

		}
	}
}

type tryEnqueuer interface {
	TryEnqueueCtx(ctx context.Context, item []byte) error
}

func handleEnqueueResult(err error) error {
	if err == sql.ErrNoRows {
		return &ErrNoItemsWaiting{}
	}

	if strings.Contains(err.Error(), "database table is locked") {
		return &ErrDBLocked{}
	}

	if err != nil {
		return err
	}

	return nil
}

func enqueueBlocking(ctx context.Context, enqueuer tryEnqueuer, item []byte, pollInterval time.Duration) error {
	for {
		err := enqueuer.TryEnqueueCtx(ctx, item)
		if err == nil {
			return nil
		}

		if !errors.Is(err, &ErrDBLocked{}) {
			return err
		}

		select {
		case <-ctx.Done():
			return context.Canceled
		case <-time.After(pollInterval):
			// Continue to next attempt
		}
	}
}

type tryAcker interface {
	TryAckCtx(ctx context.Context, id int64) error
	TryNackCtx(ctx context.Context, id int64) error
}

func ackBlocking(ctx context.Context, acker tryAcker, id int64, pollInterval time.Duration) error {
	for {
		err := acker.TryAckCtx(ctx, id)

		if err == nil {
			return nil
		}

		// We return on all errors except a locked DB
		// which we just wait on by trying again.
		if !strings.Contains(err.Error(), "database table is locked") {
			return err
		}

		select {
		case <-ctx.Done():
			return context.Canceled
		case <-time.After(pollInterval):
			// Continue to next attempt
		}
	}
}

func nackBlocking(ctx context.Context, acker tryAcker, id int64, pollInterval time.Duration) error {
	for {
		err := acker.TryNackCtx(ctx, id)
		if err == nil {
			return nil
		}

		// We return on all errors except a locked DB
		// which we just wait on by trying again.
		if !strings.Contains(err.Error(), "database table is locked") {
			return err
		}

		select {
		case <-ctx.Done():
			return context.Canceled
		case <-time.After(pollInterval):
			// Continue to next attempt
		}
	}
}
