package gopq

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
    defaultAckTimeout = 30 * time.Second
    defaultPollInterval = 100 * time.Millisecond
)


type Enqueuer interface {
    // Enqueue adds an item to the queue.
    Enqueue(item []byte) error
}


type Dequeuer interface {
    // Dequeue removes and returns the oldest item from the queue. This is blocking.
    Dequeue() (Msg, error)
    // DequeueCtx removes and returns the oldest item from the queue. This is blocking.
    DequeueCtx(ctx context.Context) (Msg, error)
    // TryDequeue attempts to dequeue an item from the queue without blocking.
    // It returns sql.ErrNoRows if there are no items available to dequeue.
    TryDequeue() (Msg, error)
    // TryDequeueCtx attempts to dequeue an item from the queue without blocking.
    // It returns sql.ErrNoRows if there are no items available to dequeue.
    TryDequeueCtx(ctx context.Context) (Msg, error)
}

// EnqueuerDequeuer is a generic queue interface. All queues implement this interface.
type EnqueuerDequeuer interface {
    // Enqueue adds an item to the queue.
    Enqueue(item []byte) error
    // Dequeue removes and returns the oldest item from the queue. This is blocking.
    Dequeue() (Msg, error)
    // DequeueCtx removes and returns the oldest item from the queue. This is blocking.
    DequeueCtx(ctx context.Context) (Msg, error)
    // TryDequeue attempts to dequeue an item from the queue without blocking.
    // It returns sql.ErrNoRows if there are no items available to dequeue.
    TryDequeue() (Msg, error)
    // TryDequeueCtx attempts to dequeue an item from the queue without blocking.
    // It returns sql.ErrNoRows if there are no items available to dequeue.
    TryDequeueCtx(ctx context.Context) (Msg, error)
}

// AckableEnqueuerDequeuer is a generic queue interface for queues that support acknowledgement of messages.
// AckQueue and UniqueAckQueue implement this interface.
type AckableEnqueuerDequeuer interface {
    EnqueuerDequeuer
    // Ack marks an item as processed.
    Ack(id int64) error
    // Nack marks an item as not processed.
    Nack(id int64) error
}

// Msg is a message in the queue.
type Msg struct {
    ID int64
    Item []byte
}

type baseQueue struct {
    db *sql.DB
    name string
    pollInterval time.Duration
    notifyChan chan struct{}
}