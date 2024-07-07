package godq

import (
	"fmt"

	"github.com/mattdeak/godq/internal"
)

const (
	uniqueCreateTableQuery = `
        CREATE TABLE IF NOT EXISTS %s (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item BLOB NOT NULL,
            enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(item) ON CONFLICT IGNORE
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

// NewUniqueQueue creates a new unique queue.
func NewUniqueQueue(filePath string) (*Queue, error) {
	db, err := internal.InitializeDB(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create unique queue: %w", err)
	}

	tableName := internal.GetUniqueTableName("unique_queue")

	formattedCreateTableQuery := fmt.Sprintf(uniqueCreateTableQuery, tableName)
	formattedEnqueueQuery := fmt.Sprintf(uniqueEnqueueQuery, tableName)
	formattedTryDequeueQuery := fmt.Sprintf(uniqueTryDequeueQuery, tableName)
	formattedLenQuery := fmt.Sprintf(uniqueLenQuery, tableName)



	err = internal.PrepareDB(db, formattedCreateTableQuery, formattedEnqueueQuery, formattedTryDequeueQuery, formattedLenQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to create unique queue: %w", err)
	}

	notifyChan := make(chan struct{}, 1)
	return &Queue{
		db:           db,
		name:         tableName,
		pollInterval: defaultPollInterval,
		notifyChan:   notifyChan,
		queries: baseQueries{
			createTable: formattedCreateTableQuery,
			enqueue:     formattedEnqueueQuery,
			tryDequeue:  formattedTryDequeueQuery,
			len:         formattedLenQuery,
		},
	}, nil

}
