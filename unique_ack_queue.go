package gopq

import (
	"fmt"

	"github.com/mattdeak/gopq/internal"
)

const (
	uniqueAckCreateTableQuery = `
		CREATE TABLE IF NOT EXISTS %[1]s (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			item BLOB NOT NULL,
			enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			ack_deadline INTEGER,
			retry_count INTEGER DEFAULT 0,
			UNIQUE(item) ON CONFLICT IGNORE
		);
		CREATE INDEX IF NOT EXISTS idx_ack_deadline ON %[1]s(ack_deadline);
	`
	uniqueAckEnqueueQuery = `
		INSERT INTO %s (item) VALUES (?)
	`
	uniqueAckTryDequeueQuery = `
		WITH oldest AS (
			SELECT id, item
			FROM %[1]s
			WHERE (ack_deadline < ? OR ack_deadline IS NULL)
			ORDER BY enqueued_at ASC
			LIMIT 1
		)
		UPDATE %[1]s SET ack_deadline = ? WHERE id = (SELECT id FROM oldest)
		RETURNING id, item
	`
	uniqueAckAckQuery = `
		DELETE FROM %s 
		WHERE id = ? AND ack_deadline >= ?
	`
	uniqueAckLenQuery = `
		SELECT COUNT(*) FROM %s
		WHERE ack_deadline IS NULL OR ack_deadline < ?
	`
)

// NewUniqueAckQueue creates a new unique ack queue.
func NewUniqueAckQueue(filePath string, opts AckOpts) (*AcknowledgeableQueue, error) {
	db, err := internal.InitializeDB(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create unique ack queue: %w", err)
	}
	tableName := internal.DetermineTableName("unique_ack_queue", filePath)

	formattedCreateTableQuery := fmt.Sprintf(uniqueAckCreateTableQuery, tableName)
	formattedEnqueueQuery := fmt.Sprintf(uniqueAckEnqueueQuery, tableName)
	formattedTryDequeueQuery := fmt.Sprintf(uniqueAckTryDequeueQuery, tableName)
	formattedAckQuery := fmt.Sprintf(uniqueAckAckQuery, tableName)
	formattedLenQuery := fmt.Sprintf(uniqueAckLenQuery, tableName)

	err = internal.PrepareDB(db, formattedCreateTableQuery, formattedEnqueueQuery, formattedTryDequeueQuery, formattedAckQuery, formattedLenQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to create unique ack queue: %w", err)
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
		AckOpts: opts,
		ackQueries: ackQueries{
			ack: formattedAckQuery,
		},
	}, nil
}
