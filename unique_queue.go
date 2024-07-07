package godq

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mattdeak/godq/internal"
)

const (
	uniqueCreateTableQuery = `
        CREATE TABLE IF NOT EXISTS %s (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_hash TEXT NOT NULL,
            item BLOB NOT NULL,
            enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(item_hash) ON CONFLICT IGNORE
        );
    `
	uniqueEnqueueQuery = `
        INSERT INTO %s (item) VALUES (?)
    `
	uniqueTryDequeueQuery = `
		WITH oldest AS (
			SELECT id, item
			FROM %[1]s
			ORDER BY enqueued_at ASC
			LIMIT 1
		)
		DELETE FROM %[1]s
		WHERE id = (SELECT id FROM oldest)
		RETURNING id, item
    `
	uniqueLenQuery = `
        SELECT COUNT(*) FROM %s
    `
)

// UniqueQueue is a queue that ensures that each item is only processed once.
type UniqueQueue struct {
	baseQueue
}

// NewUniqueQueue creates a new unique queue.
func NewUniqueQueue(filePath string) (*UniqueQueue, error) {
	db, err := internal.InitializeDB(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create unique queue: %w", err)
	}

	tableName := internal.GetUniqueTableName("unique_queue")
	err = internal.PrepareDB(db, tableName, uniqueCreateTableQuery, uniqueEnqueueQuery, uniqueTryDequeueQuery, uniqueLenQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to create unique queue: %w", err)
	}
	return setupUniqueQueue(db, tableName, defaultPollInterval)
}

func setupUniqueQueue(db *sql.DB, name string, pollInterval time.Duration) (*UniqueQueue, error) {
	_, err := db.Exec(fmt.Sprintf(uniqueCreateTableQuery, name))
	if err != nil {
		return nil, err
	}

	notifyChan := make(chan struct{}, 1)
	return &UniqueQueue{
		baseQueue: baseQueue{db: db, name: name, pollInterval: pollInterval, notifyChan: notifyChan},
	}, nil
}

// Enqueue adds an item to the queue.
func (pq *UniqueQueue) Enqueue(item []byte) error {
	_, err := pq.db.Exec(fmt.Sprintf(uniqueEnqueueQuery, pq.name), item)
	if err != nil {
		return err
	}
	go func() {
		pq.notifyChan <- struct{}{}
	}()
	return nil
}

// Dequeue blocks until an item is available. Uses background context.
func (pq *UniqueQueue) Dequeue() (Msg, error) {
	return pq.DequeueCtx(context.Background())
}

// Dequeue blocks until an item is available or the context is canceled.
func (pq *UniqueQueue) DequeueCtx(ctx context.Context) (Msg, error) {
	return dequeueBlocking(ctx, pq, pq.pollInterval, pq.notifyChan)
}

// TryDequeue attempts to dequeue an item without blocking.
// If no item is available, it returns an empty Msg and an error.
func (pq *UniqueQueue) TryDequeue() (Msg, error) {
	return pq.TryDequeueCtx(context.Background())
}

// TryDequeueCtx attempts to dequeue an item without blocking using a context.
// If no item is available, it returns an empty Msg and an error.
func (pq *UniqueQueue) TryDequeueCtx(ctx context.Context) (Msg, error) {
	row := pq.db.QueryRowContext(ctx, fmt.Sprintf(uniqueTryDequeueQuery, pq.name))
	var id int64
	var item []byte
	err := row.Scan(&id, &item)
	return handleDequeueResult(id, item, err)
}

// Len returns the number of items in the queue.
func (pq *UniqueQueue) Len() (int, error) {
	row := pq.db.QueryRow(fmt.Sprintf(uniqueLenQuery, pq.name))
	var count int
	err := row.Scan(&count)
	return count, err
}