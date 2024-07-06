package internal

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
)


func InitializeDB(fileName string) (*sql.DB, error) {
	if _, err := os.Stat(fileName); err == nil {
		slog.Info("Database file already exists. Re-using existing database.", "file", fileName)
	}

	dbPath := fmt.Sprintf("file:%s?_journal_mode=WAL", fileName)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return db, nil

}