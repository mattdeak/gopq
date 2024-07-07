package godq

import (
	"fmt"

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

// NewAckQueue creates a new ack queue.
// If filePath is empty, the queue will be created in memory.
func NewAckQueue(filePath string, opts AckOpts) (*AcknowledgeableQueue, error) {
	db, err := internal.InitializeDB(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create ack queue: %w", err)
	}

	tableName := internal.GetUniqueTableName("ack_queue")

	formattedCreateTableQuery := fmt.Sprintf(ackCreateTableQuery, tableName)
	formattedEnqueueQuery := fmt.Sprintf(ackEnqueueQuery, tableName)
	formattedTryDequeueQuery := fmt.Sprintf(ackTryDequeueQuery, tableName)
	formattedAckQuery := fmt.Sprintf(ackAckQuery, tableName)
	formattedLenQuery := fmt.Sprintf(ackLenQuery, tableName)

	err = internal.PrepareDB(db, formattedCreateTableQuery, formattedEnqueueQuery, formattedTryDequeueQuery, formattedAckQuery, formattedLenQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to create ack queue: %w", err)
	}

	return &AcknowledgeableQueue{
		Queue: Queue{
			db:           db,
			name:         tableName,
			pollInterval: defaultPollInterval,
			notifyChan:   internal.MakeNotifyChan(),
			queries: baseQueries{
				createTable: formattedCreateTableQuery,
				enqueue:     formattedEnqueueQuery,
				tryDequeue:  formattedTryDequeueQuery,
				len:         formattedLenQuery,
			},
		},
		ackOpts: opts,
		ackQueries: ackQueries{
			ack: formattedAckQuery,
		},
	}, nil
}
