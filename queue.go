package gopq

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	defaultAckTimeout   = 30 * time.Second
	defaultPollInterval = 10 * time.Millisecond
)

// Enqueuer provides methods for enqueueing items to the queue.
type Enqueuer interface {
	// Enqueue adds an item to the queue.
	// It returns an error if the operation fails.
	Enqueue(item []byte) error
}

// Dequeuer provides methods for dequeueing items from the queue.
type Dequeuer interface {
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
}

// Queue represents a durable queue interface.
// It provides methods for enqueueing and dequeueing items,
// with both blocking and non-blocking operations.
type Queuer interface {
	Enqueuer
	Dequeuer
	Close() error
}

// AckableQueue extends the DQueue interface with acknowledgement capabilities.
// It allows for explicit acknowledgement or negative acknowledgement of processed items.
type AckableQueue interface {
	Queuer

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

// Queue represents the basic queue structure.
// It contains the database connection, queue name, and other necessary fields for queue operations.
type Queue struct {
	db           *sql.DB
	name         string
	pollInterval time.Duration
	notifyChan   chan struct{}
	queries      baseQueries
}

type AcknowledgeableQueue struct {
	Queue
	ackOpts    AckOpts
	ackQueries ackQueries
}

type baseQueries struct {
	createTable string
	enqueue     string
	tryDequeue  string
	len         string
}

type ackQueries struct {
	ack string
}

// Close closes the database connection associated with the queue.
// It should be called when the queue is no longer needed to free up resources.
func (q *Queue) Close() error {
	return q.db.Close()
}

// Enqueue adds an item to the queue.
// It returns an error if the operation fails.
func (q *Queue) Enqueue(item []byte) error {
	_, err := q.db.Exec(q.queries.enqueue, item)
	if err != nil {
		return err
	}

	// Send a notification to the channel to wake up the dequeueing goroutine.
	select {
	case q.notifyChan <- struct{}{}:
	default:
	}
	return nil
}

// Dequeue blocks until an item is available. Uses background context.
func (q *Queue) Dequeue() (Msg, error) {
	return q.DequeueCtx(context.Background())
}

// Dequeue blocks until an item is available or the context is canceled.
// If the context is canceled, it returns an empty Msg and an error.
func (q *Queue) DequeueCtx(ctx context.Context) (Msg, error) {
	return dequeueBlocking(ctx, q, q.pollInterval, q.notifyChan)
}

func (q *Queue) TryDequeue() (Msg, error) {
	return q.TryDequeueCtx(context.Background())
}

func (q *Queue) TryDequeueCtx(ctx context.Context) (Msg, error) {
	row := q.db.QueryRow(q.queries.tryDequeue)
	var id int64
	var item []byte
	err := row.Scan(&id, &item)
	return handleDequeueResult(id, item, err)
}

// Len returns the number of items in the queue.
// It returns the count and any error encountered during the operation.
func (q *Queue) Len() (int, error) {
	row := q.db.QueryRow(q.queries.len)
	var count int
	err := row.Scan(&count)
	return count, err
}

// SetDeadLetterQueue sets the dead letter queue for this AcknowledgeableQueue.
// Items that exceed the maximum retry count will be moved to this queue.
func (q *AcknowledgeableQueue) SetDeadLetterQueue(dlq Enqueuer) {
	q.ackOpts.DeadLetterQueue = dlq
}

// Ack acknowledges that an item has been successfully processed.
// It takes the ID of the message to acknowledge and returns an error if the operation fails.
func (q *AcknowledgeableQueue) Ack(id int64) error {
	_, err := q.db.Exec(q.ackQueries.ack, id)
	return err
}

// Dequeue removes and returns the next item from the queue.
// It blocks if the queue is empty until an item becomes available.
// It uses a background context internally.
func (q *AcknowledgeableQueue) Dequeue() (Msg, error) {
	return q.DequeueCtx(context.Background())
}

// DequeueCtx removes and returns the next item from the queue.
// It blocks if the queue is empty until an item becomes available or the context is cancelled.
func (q *AcknowledgeableQueue) DequeueCtx(ctx context.Context) (Msg, error) {
	return dequeueBlocking(ctx, q, q.pollInterval, q.notifyChan)
}

// TryDequeue attempts to remove and return the next item from the queue.
// It returns immediately, even if the queue is empty.
// It uses a background context internally.
func (q *AcknowledgeableQueue) TryDequeue() (Msg, error) {
	return q.TryDequeueCtx(context.Background())
}

// TryDequeueCtx attempts to remove and return the next item from the queue.
// It returns immediately if an item is available, or waits until the context is cancelled.
func (q *AcknowledgeableQueue) TryDequeueCtx(ctx context.Context) (Msg, error) {
	newAckDeadline := time.Now().Add(q.ackOpts.AckTimeout)
	row := q.db.QueryRowContext(ctx, q.queries.tryDequeue, newAckDeadline)
	var id int64
	var item []byte
	err := row.Scan(&id, &item)
	return handleDequeueResult(id, item, err)
}

// Nack indicates that an item processing has failed and should be requeued.
// It takes the ID of the message to negative acknowledge.
// Returns an error if the operation fails or the message doesn't exist.
func (q *AcknowledgeableQueue) Nack(id int64) error {
	return nackImpl(q.db, q.name, id, q.ackOpts, q.ackOpts.DeadLetterQueue)
}

// ExpireAck expires the acknowledgement deadline for an item,
// which requeues it to the front of the queue.
// It takes the ID of the message to expire the acknowledgement deadline for.
// Returns an error if the operation fails or the message doesn't exist.
func (q *AcknowledgeableQueue) ExpireAck(id int64) error {
	return expireAckDeadline(q.db, q.name, id)
}
