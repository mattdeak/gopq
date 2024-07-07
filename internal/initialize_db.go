package internal

import (
	"database/sql"
	"fmt"
)

func InitializeDB(fileName string) (*sql.DB, error) {
	var dbPath string
	if fileName == "" {
		dbPath = "file::memory:?cache=shared"
	} else {
		dbPath = fmt.Sprintf("file:%s?_journal_mode=WAL", fileName)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// not sure if this is the best way to do this
	// but it does stop lock contention
	db.SetMaxOpenConns(1)

	return db, nil

}


func PrepareDB(db *sql.DB, tableName string, createTableQuery string, queries ...string) error {
	_, err := db.Exec(fmt.Sprintf(createTableQuery, tableName))
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	for _, query := range queries {
		_, err := db.Prepare(fmt.Sprintf(query, tableName))
		if err != nil {
			return fmt.Errorf("failed to prepare query: %w", err)
		}
	}

	return nil
}
