package godq

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mattdeak/godq/internal"
)

const (
	simpleCreateTableQuery = `
        CREATE TABLE IF NOT EXISTS %[1]s (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item BLOB NOT NULL,
            enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_processed ON %[1]s(processed_at);
    `
	simpleEnqueueQuery = `
        INSERT INTO %s (item) VALUES (?)
    `
	simpleTryDequeueQuery = `
		WITH oldest AS (
			SELECT id, item
			FROM %[1]s
			WHERE processed_at IS NULL
			ORDER BY enqueued_at ASC
			LIMIT 1
		)
		UPDATE %[1]s
		SET processed_at = CURRENT_TIMESTAMP
		WHERE id = (SELECT id FROM oldest)
		RETURNING id, item
    `
	simpleLenQuery = `
        SELECT COUNT(*) FROM %s WHERE processed_at IS NULL
    `
)

type SimpleQueue struct {
	baseQueue
}

// NewSimpleQueue creates a new simple queue.
// If filePath is empty, the queue will be created in memory.
func NewSimpleQueue(filePath string) (*SimpleQueue, error) {
	db, err := internal.InitializeDB(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	tableName := internal.GetUniqueTableName("simple_queue")
	err = internal.PrepareDB(db, tableName, simpleCreateTableQuery, simpleEnqueueQuery, simpleTryDequeueQuery, simpleLenQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare database: %w", err)
	}
	return setupSimpleQueue(db, tableName, defaultPollInterval)
}

func setupSimpleQueue(db *sql.DB, name string, pollInterval time.Duration) (*SimpleQueue, error) {
	_, err := db.Exec(fmt.Sprintf(simpleCreateTableQuery, name, name))
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
	_, err := pq.db.Exec(fmt.Sprintf(simpleEnqueueQuery, pq.name), item)
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
	row := pq.db.QueryRow(fmt.Sprintf(simpleTryDequeueQuery, pq.name, pq.name))
	var id int64
	var item []byte
	err := row.Scan(&id, &item)
	return handleDequeueResult(id, item, err)
}

// Len returns the number of items in the queue.
func (pq *SimpleQueue) Len() (int, error) {
	row := pq.db.QueryRow(fmt.Sprintf(simpleLenQuery, pq.name))
	var count int
	err := row.Scan(&count)
	return count, err
}