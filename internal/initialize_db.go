package internal

import (
	"database/sql"
	"fmt"
	"log/slog"
)

func InitializeDB(fileName string) (*sql.DB, error) {
	var dbPath string
	if fileName == "" {
		slog.Info("Using in-memory database")
		dbPath = "file::memory:?cache=shared"
	} else {
		slog.Info("Using SQLite database", "path", fileName)
		dbPath = fmt.Sprintf("file:%s?_journal_mode=WAL", fileName)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}


	db.SetMaxOpenConns(1)

	return db, nil

}

func PrepareDB(db *sql.DB, createTableQuery string, queries ...string) error {
	_, err := db.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	for _, query := range queries {
		_, err := db.Prepare(query)
		if err != nil {
			return fmt.Errorf("failed to prepare query: %w", err)
		}
	}

	return nil
}
