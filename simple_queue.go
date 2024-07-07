package godq

import (
	"fmt"

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

// NewSimpleQueue creates a new simple queue.
// If filePath is empty, the queue will be created in memory.
func NewSimpleQueue(filePath string) (*Queue, error) {
	db, err := internal.InitializeDB(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	tableName := internal.GetUniqueTableName("simple_queue")

	formattedCreateTableQuery := fmt.Sprintf(simpleCreateTableQuery, tableName)
	formattedEnqueueQuery := fmt.Sprintf(simpleEnqueueQuery, tableName)
	formattedTryDequeueQuery := fmt.Sprintf(simpleTryDequeueQuery, tableName)
	formattedLenQuery := fmt.Sprintf(simpleLenQuery, tableName)

	err = internal.PrepareDB(db, formattedCreateTableQuery, formattedEnqueueQuery, formattedTryDequeueQuery, formattedLenQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare database: %w", err)
	}

	return &Queue{
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
	}, nil
}
