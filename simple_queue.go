package godq

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type SimpleQueue struct {
	baseQueue
}

// NewSimpleQueue creates a new simple queue.
func NewSimpleQueue(filePath string) (*SimpleQueue, error) {
	dbPath := fmt.Sprintf("file:%s?_journal_mode=WAL", filePath)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	return setupSimpleQueue(db, "simple_queue", defaultPollInterval)
}

func setupSimpleQueue(db *sql.DB, name string, pollInterval time.Duration) (*SimpleQueue, error) {
	_, err := db.Exec(fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item BLOB NOT NULL,
            enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_processed ON %s(processed_at);
    `, name, name))
	if err != nil {
		return nil, err
	}

	notifyChan := make(chan struct{}, 1)
	return &SimpleQueue{
		baseQueue: baseQueue{db: db, name: name, pollInterval: pollInterval, notifyChan: notifyChan},
	}, nil
}

// Enqueue adds an item to the queue.
func (pq *SimpleQueue) Enqueue(item []byte) error {
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

// Dequeue blocks until an item is available. Uses background context.
func (pq *SimpleQueue) Dequeue() (Msg, error) {
	return pq.DequeueCtx(context.Background())
}

// Dequeue blocks until an item is available or the context is canceled.
// If the context is canceled, it returns an empty Msg and an error.
func (pq *SimpleQueue) DequeueCtx(ctx context.Context) (Msg, error) {
	return dequeueBlocking(ctx, pq, pq.pollInterval, pq.notifyChan)
}

// TryDequeue attempts to dequeue an item without blocking.
// If no item is available, it returns an empty Msg and an error.
func (pq *SimpleQueue) TryDequeue() (Msg, error) {
	return pq.TryDequeueCtx(context.Background())
}

// TryDequeueCtx attempts to dequeue an item without blocking using a context.
// If no item is available, it returns an empty Msg and an error.
func (pq *SimpleQueue) TryDequeueCtx(ctx context.Context) (Msg, error) {
	row := pq.db.QueryRow(fmt.Sprintf(`
		WITH oldest AS (
			SELECT id, item
			FROM %s
			WHERE processed_at IS NULL
			ORDER BY enqueued_at ASC
			LIMIT 1
		)
		UPDATE %s
		SET processed_at = CURRENT_TIMESTAMP
		WHERE id = (SELECT id FROM oldest)
		RETURNING id, item
    `, pq.name, pq.name))
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

// Len returns the number of items in the queue.
func (pq *SimpleQueue) Len() (int, error) {
	row := pq.db.QueryRow(fmt.Sprintf(`
        SELECT COUNT(*) FROM %s WHERE processed_at IS NULL
    `, pq.name))
	var count int
	err := row.Scan(&count)
	return count, err
}
