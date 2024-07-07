package godq

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mattdeak/godq/internal"
)

// UniqueAckQueue is a acknowledgeable queue that ensures that each item is only processed once.
type UniqueAckQueue struct {
	baseQueue
	opts AckOpts
	deadLetterQueue Enqueuer
}

// NewUniqueAckQueue creates a new unique ack queue.
func NewUniqueAckQueue(filePath string, opts AckOpts) (*UniqueAckQueue, error) {
	db, err := internal.InitializeDB(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create unique ack queue: %w", err)
	}

	queue, err := setupUniqueAckQueue(db, "unique_ack_queue", defaultPollInterval, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create unique ack queue: %w", err)
	}
	return queue, nil
}

func setupUniqueAckQueue(db *sql.DB, name string, pollInterval time.Duration, opts AckOpts) (*UniqueAckQueue, error) {
	_, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			item BLOB NOT NULL,
			enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			processed_at TIMESTAMP,
			ack_deadline TIMESTAMP,
			retry_count INTEGER DEFAULT 0,
			UNIQUE(item) ON CONFLICT IGNORE
		);
		CREATE INDEX IF NOT EXISTS idx_processed ON %s(processed_at);
		CREATE INDEX IF NOT EXISTS idx_ack_deadline ON %s(ack_deadline);
	`, name, name, name))
	if err != nil {
		return nil, err
	}

	notifyChan := make(chan struct{}, 1)
	return &UniqueAckQueue{
		baseQueue:  baseQueue{db: db, name: name, pollInterval: pollInterval, notifyChan: notifyChan},
		opts: opts,
		deadLetterQueue: nil,
	}, nil
}

// Enqueue adds an item to the queue.
func (uaq *UniqueAckQueue) Enqueue(item []byte) error {
	_, err := uaq.db.Exec(fmt.Sprintf(`
		INSERT INTO %s (item) VALUES (?)
	`, uaq.name), item)
	if err != nil {
		return err
	}
	go func() {
		uaq.notifyChan <- struct{}{}
	}()
	return nil
}

// Dequeue blocks until an item is available. Uses background context.
func (uaq *UniqueAckQueue) Dequeue() (Msg, error) {
	return uaq.DequeueCtx(context.Background())
}

// DequeueCtx attempts to dequeue an item without blocking using a context.
// If no item is available, it returns an empty Msg and an error.
func (uaq *UniqueAckQueue) DequeueCtx(ctx context.Context) (Msg, error) {
	return dequeueBlocking(ctx, uaq, uaq.pollInterval, uaq.notifyChan)
}


// TryDequeue attempts to dequeue an item without blocking.
// If no item is available, it returns an empty Msg and an error.
func (uaq *UniqueAckQueue) TryDequeue() (Msg, error) {
	return uaq.TryDequeueCtx(context.Background())
}

// TryDequeueCtx attempts to dequeue an item without blocking using a context.
// If no item is available, it returns an empty Msg and an error.
func (uaq *UniqueAckQueue) TryDequeueCtx(ctx context.Context) (Msg, error) {
	row := uaq.db.QueryRowContext(ctx, fmt.Sprintf(`
		WITH oldest AS (
			SELECT id, item
			FROM %s
			WHERE ack_deadline IS NULL OR ack_deadline < CURRENT_TIMESTAMP
			ORDER BY enqueued_at ASC
			LIMIT 1
		)
		UPDATE %s SET ack_deadline = ? WHERE id = (SELECT id FROM oldest)
		RETURNING id, item
	`, uaq.name, uaq.name), time.Now().Add(uaq.opts.AckTimeout))
	var id int64
	var item []byte

	err := row.Scan(&id, &item)
	if err != nil {
		return Msg{}, fmt.Errorf("failed to dequeue: %w", err)
	}

	return Msg{
		ID:   id,
		Item: item,
	}, nil
}

// Ack marks an item as processed and removes it from the queue.
func (uaq *UniqueAckQueue) Ack(id int64) error {
	res, err := uaq.db.Exec(fmt.Sprintf(`
		DELETE FROM %s 
		WHERE id = ? AND ack_deadline >= CURRENT_TIMESTAMP
	`, uaq.name), id)

	if err != nil {
		return fmt.Errorf("failed to ack: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to ack: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("failed to ack: no rows affected")
	}

	return nil
}


// SetDeadLetterQueue sets the dead letter queue. Only requires the queuer to be an Enqueuer.
// If present, failed messages will be added to the dead letter queue.
func (uaq *UniqueAckQueue) SetDeadLetterQueue(dlq Enqueuer) {
	uaq.deadLetterQueue = dlq
}

// Nack marks an item as not processed and handles based on AckOpts
func (uaq *UniqueAckQueue) Nack(id int64) error {
	return nackImpl(uaq.db, uaq.name, id, uaq.opts, uaq.deadLetterQueue)
}


func (uaq *UniqueAckQueue) ExpireAck(id int64) error {
	return expireAckDeadline(uaq.db, uaq.name, id)
}

// Len returns the number of items available in the queue.
func (uaq *UniqueAckQueue) Len() (int, error) {
	row := uaq.db.QueryRow(fmt.Sprintf(`
		SELECT COUNT(*) FROM %s
		WHERE processed_at IS NULL
		AND (ack_deadline IS NULL OR ack_deadline < CURRENT_TIMESTAMP)
	`, uaq.name))
	var count int
	err := row.Scan(&count)
	return count, err
}
