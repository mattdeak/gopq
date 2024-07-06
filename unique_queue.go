package godq

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mattdeak/godq/internal"
)

type UniqueQueue struct {
    baseQueue
}

func NewUniqueQueue(filePath string) (*UniqueQueue, error) {
	db, err := internal.InitializeDB(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create unique queue: %w", err)
	}
    return setupUniqueQueue(db, "unique_queue", defaultPollInterval)
}

func setupUniqueQueue(db *sql.DB, name string, pollInterval time.Duration) (*UniqueQueue, error) {
    _, err := db.Exec(fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item BLOB NOT NULL,
            enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(item) ON CONFLICT IGNORE
        );
    `, name))
    if err != nil {
        return nil, err
    }

	notifyChan := make(chan struct{}, 1)
    return &UniqueQueue{
        baseQueue: baseQueue{db: db, name: name, pollInterval: pollInterval, notifyChan: notifyChan},
    }, nil
}

func (pq *UniqueQueue) Enqueue(item []byte) error {
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

func (pq *UniqueQueue) Dequeue() (Msg, error) {
	return pq.DequeueCtx(context.Background())
}


// Dequeue blocks until an item is available or the context is canceled.
func (pq *UniqueQueue) DequeueCtx(ctx context.Context) (Msg, error) {
	return dequeueBlocking(ctx, pq, pq.pollInterval, pq.notifyChan)
}

func (pq *UniqueQueue) TryDequeue() (Msg, error) {
	return pq.TryDequeueCtx(context.Background())
}

func (pq *UniqueQueue) TryDequeueCtx(ctx context.Context) (Msg, error) {
    row := pq.db.QueryRowContext(ctx, fmt.Sprintf(`
		WITH oldest AS (
			SELECT id, item
			FROM %s
			ORDER BY enqueued_at ASC
			LIMIT 1
		)
		DELETE FROM %s
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

func (pq *UniqueQueue) Len() (int, error) {
    row := pq.db.QueryRow(fmt.Sprintf(`
        SELECT COUNT(*) FROM %s
    `, pq.name))
    var count int
    err := row.Scan(&count)
    return count, err
}
