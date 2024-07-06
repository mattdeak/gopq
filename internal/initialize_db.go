package internal

import (
	"database/sql"
	"fmt"
)


func InitializeDB(fileName string) (*sql.DB, error) {
	dbPath := fmt.Sprintf("file:%s?_journal_mode=WAL", fileName)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return db, nil

}