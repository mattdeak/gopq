package gopq

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mattdeak/gopq/internal"
)


type AckQueue struct {
	baseQueue
	ackTimeout time.Duration
}

func NewAckQueue(filePath string, opts AckOpts) (*AckQueue, error) {
	db, err := internal.InitializeDB(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create ack queue: %w", err)
	}

	queue, err := setupAckQueue(db, "ack_queue", defaultPollInterval, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create ack queue: %w", err)
	}
	return queue, nil
}

func setupAckQueue(db *sql.DB, name string, pollInterval time.Duration, opts AckOpts) (*AckQueue, error) {
    _, err := db.Exec(fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item BLOB NOT NULL,
            enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP,
            ack_deadline TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_processed ON %s(processed_at);
        CREATE INDEX IF NOT EXISTS idx_ack_deadline ON %s(ack_deadline);
    `, name, name, name))
    if err != nil {
        return nil, err
    }

	notifyChan := make(chan struct{}, 1)
    return &AckQueue{
        baseQueue: baseQueue{db: db, name: name, pollInterval: pollInterval, notifyChan: notifyChan},
		ackTimeout: opts.AckTimeout,
    }, nil
}

func (pq *AckQueue) Enqueue(item []byte) error {
    _, err := pq.db.Exec(fmt.Sprintf(`
        INSERT INTO %s (item) VALUES (?)
    `, pq.name), item)
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
    row := pq.db.QueryRowContext(ctx, fmt.Sprintf(`
		WITH oldest AS (
			SELECT id, item
			FROM %s
			WHERE processed_at IS NULL AND (ack_deadline < CURRENT_TIMESTAMP OR ack_deadline IS NULL)
			ORDER BY enqueued_at ASC
			LIMIT 1
		)
		UPDATE %s 
		SET ack_deadline = ?
		WHERE id = (SELECT id FROM oldest)
		RETURNING id, item
    `, pq.name, pq.name), time.Now().Add(pq.ackTimeout))
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

func (pq *AckQueue) Ack(id int64) error {
	res, err := pq.db.Exec(fmt.Sprintf(`
		UPDATE %s 
		SET processed_at = CURRENT_TIMESTAMP 
		WHERE id = ? AND ack_deadline >= CURRENT_TIMESTAMP
	`, pq.name), id)

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

// Nack marks an item as not processed and re-queues it.
func (pq *AckQueue) Nack(id int64) error {
	res, err := pq.db.Exec(fmt.Sprintf(`
		UPDATE %s 
		SET ack_deadline = NULL, enqueued_at = CURRENT_TIMESTAMP 
		WHERE id = ?
	`, pq.name), id)

	if err != nil {
		return fmt.Errorf("failed to nack: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to nack: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("failed to nack: no rows affected")
	}

	return nil
}

func (pq *AckQueue) Len() (int, error) {
    row := pq.db.QueryRow(fmt.Sprintf(`
        SELECT COUNT(*) FROM %s WHERE processed_at IS NULL AND (ack_deadline < CURRENT_TIMESTAMP OR ack_deadline IS NULL)
    `, pq.name))
    var count int
    err := row.Scan(&count)
    return count, err
}
