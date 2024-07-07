package godq

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mattdeak/godq/internal"
)

const (
	ackCreateTableQuery = `
        CREATE TABLE IF NOT EXISTS %[1]s (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item BLOB NOT NULL,
            enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP,
            ack_deadline TIMESTAMP,
            retry_count INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_processed ON %[1]s(processed_at);
        CREATE INDEX IF NOT EXISTS idx_ack_deadline ON %[1]s(ack_deadline);
    `
	ackEnqueueQuery = `
        INSERT INTO %s (item) VALUES (?)
    `
	ackTryDequeueQuery = `
		WITH oldest AS (
			SELECT id, item
			FROM %[1]s
			WHERE processed_at IS NULL AND (ack_deadline < CURRENT_TIMESTAMP OR ack_deadline IS NULL)
			ORDER BY enqueued_at ASC
			LIMIT 1
		)
		UPDATE %[1]s 
		SET ack_deadline = ?
		WHERE id = (SELECT id FROM oldest)
		RETURNING id, item
    `
	ackAckQuery = `
		UPDATE %s 
		SET processed_at = CURRENT_TIMESTAMP 
		WHERE id = ? AND ack_deadline >= CURRENT_TIMESTAMP
	`
	ackLenQuery = `
        SELECT COUNT(*) FROM %s WHERE processed_at IS NULL AND (ack_deadline < CURRENT_TIMESTAMP OR ack_deadline IS NULL)
    `
)

// AckQueue is a queue that provides the ability to acknowledge messages.
type AckQueue struct {
	baseQueue
	opts            AckOpts
	deadLetterQueue Enqueuer
}

// NewAckQueue creates a new ack queue.
// If filePath is empty, the queue will be created in memory.
func NewAckQueue(filePath string, opts AckOpts) (*AckQueue, error) {
	db, err := internal.InitializeDB(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create ack queue: %w", err)
	}

	tableName := internal.GetUniqueTableName("ack_queue")
	err = internal.PrepareDB(db, tableName, ackCreateTableQuery, ackEnqueueQuery, ackTryDequeueQuery, ackAckQuery, ackLenQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to create ack queue: %w", err)
	}

	q, err := setupAckQueue(db, tableName, defaultPollInterval, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create ack queue: %w", err)
	}
	return q, nil
}

func setupAckQueue(db *sql.DB, name string, pollInterval time.Duration, opts AckOpts) (*AckQueue, error) {
	_, err := db.Exec(fmt.Sprintf(ackCreateTableQuery, name))
	if err != nil {
		return nil, err
	}

	notifyChan := make(chan struct{}, 1)
	return &AckQueue{
		baseQueue: baseQueue{db: db, name: name, pollInterval: pollInterval, notifyChan: notifyChan},
		opts:      opts,
	}, nil
}

// Enqueue adds an item to the queue.
func (pq *AckQueue) Enqueue(item []byte) error {
	_, err := pq.db.Exec(fmt.Sprintf(ackEnqueueQuery, pq.name), item)
	if err != nil {
		return err
	}
	go func() {
		pq.notifyChan <- struct{}{}
	}()
	return nil
}

// Dequeue blocks until an item is available. It uses a background context.
func (pq *AckQueue) Dequeue() (Msg, error) {
	return pq.DequeueCtx(context.Background())
}

// DequeueCtx blocks until an item is available or the context is canceled.
func (pq *AckQueue) DequeueCtx(ctx context.Context) (Msg, error) {
	return dequeueBlocking(ctx, pq, pq.pollInterval, pq.notifyChan)
}

// TryDequeue attempts to dequeue an item from the queue without blocking.
func (pq *AckQueue) TryDequeue() (Msg, error) {
	return pq.TryDequeueCtx(context.Background())
}

// TryDequeueCtx attempts to dequeue an item from the queue.
// It returns the item and its ID, or an error if the item could not be dequeued.
func (pq *AckQueue) TryDequeueCtx(ctx context.Context) (Msg, error) {
	row := pq.db.QueryRowContext(ctx, fmt.Sprintf(ackTryDequeueQuery, pq.name, pq.name), time.Now().Add(pq.opts.AckTimeout))
	var id int64
	var item []byte
	err := row.Scan(&id, &item)
	return handleDequeueResult(id, item, err)
}

// Ack marks an item as processed.
func (pq *AckQueue) Ack(id int64) error {
	res, err := pq.db.Exec(fmt.Sprintf(ackAckQuery, pq.name), id)

	if err != nil {
		return fmt.Errorf("failed to ack: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to ack: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("failed to ack: no rows affected - possible ack expiration")
	}

	return nil
}

// If present, failed messages will be added to the dead letter queue.
// Otherwise, they will be discarded.
func (pq *AckQueue) SetDeadLetterQueue(queue Enqueuer) {
	pq.deadLetterQueue = queue
}

// Nack marks an item as not processed and handles based on AckOpts
func (pq *AckQueue) Nack(id int64) error {
	return nackImpl(pq.db, pq.name, id, pq.opts, pq.deadLetterQueue)
}

func (pq *AckQueue) ExpireAck(id int64) error {
	return expireAckDeadline(pq.db, pq.name, id)
}

// Len returns the number of items in the queue.
func (pq *AckQueue) Len() (int, error) {
	row := pq.db.QueryRow(fmt.Sprintf(ackLenQuery, pq.name))
	var count int
	err := row.Scan(&count)
	return count, err
}
