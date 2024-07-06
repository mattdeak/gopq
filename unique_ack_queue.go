package godq

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mattdeak/godq/internal"
)

type UniqueAckQueue struct {
	baseQueue
	ackTimeout time.Duration
}

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
		ackTimeout: opts.AckTimeout,
	}, nil
}

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

func (uaq *UniqueAckQueue) Dequeue() (Msg, error) {
	return uaq.DequeueCtx(context.Background())
}

func (uaq *UniqueAckQueue) DequeueCtx(ctx context.Context) (Msg, error) {
	return dequeueBlocking(ctx, uaq, uaq.pollInterval, uaq.notifyChan)
}

func (uaq *UniqueAckQueue) TryDequeue() (Msg, error) {
	return uaq.TryDequeueCtx(context.Background())
}

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
	`, uaq.name, uaq.name), time.Now().Add(uaq.ackTimeout))
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

func (uaq *UniqueAckQueue) Nack(id int64) error {
	res, err := uaq.db.Exec(fmt.Sprintf(`
		UPDATE %s SET ack_deadline = NULL WHERE id = ?
	`, uaq.name), id)

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

func (uaq *UniqueAckQueue) Len() (int, error) {
	row := uaq.db.QueryRow(fmt.Sprintf(`
		SELECT COUNT(*) FROM %s
	`, uaq.name))
	var count int
	err := row.Scan(&count)
	return count, err
}