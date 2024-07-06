package godq

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


// Queue represents a durable queue interface.
// It provides methods for enqueueing and dequeueing items,
// with both blocking and non-blocking operations.
type Queue interface {
    // Enqueue adds an item to the queue.
    // It returns an error if the operation fails.
    Enqueue(item []byte) error

    // Dequeue removes and returns the next item from the queue.
    // It blocks if the queue is empty until an item becomes available.
    // Returns an error if the operation fails.
    Dequeue() (Msg, error)

    // DequeueCtx removes and returns the next item from the queue.
    // It blocks if the queue is empty until an item becomes available or the context is cancelled.
    // Returns an error if the operation fails or the context is cancelled.
    DequeueCtx(ctx context.Context) (Msg, error)

    // TryDequeue attempts to remove and return the next item from the queue.
    // It returns immediately, even if the queue is empty.
    // Returns an error if the operation fails or the queue is empty.
    TryDequeue() (Msg, error)

    // TryDequeueCtx attempts to remove and return the next item from the queue.
    // It returns immediately if an item is available, or waits until the context is cancelled.
    // Returns an error if the operation fails, the queue is empty, or the context is cancelled.
    TryDequeueCtx(ctx context.Context) (Msg, error)

    // Close closes the queue and releases any resources.
    // It returns an error if the operation fails.
    Close() error
}

// AckableQueue extends the DQueue interface with acknowledgement capabilities.
// It allows for explicit acknowledgement or negative acknowledgement of processed items.
type AckableQueue interface {
    Queue

    // Ack acknowledges that an item has been successfully processed.
    // It takes the ID of the message to acknowledge.
    // Returns an error if the operation fails or the message doesn't exist.
    Ack(id int64) error

    // Nack indicates that an item processing has failed and should be requeued.
    // It takes the ID of the message to negative acknowledge.
    // Returns an error if the operation fails or the message doesn't exist.
    Nack(id int64) error
}

// Msg represents a message in the queue.
// It contains the message ID and the actual data.
type Msg struct {
    // ID is a unique identifier for the message within the queue.
    ID int64

    // Item contains the actual message data.
    Item []byte
}

type baseQueue struct {
    db *sql.DB
    name string
    pollInterval time.Duration
    notifyChan chan struct{}
}

func (q *baseQueue) Close() error {
    return q.db.Close()
}