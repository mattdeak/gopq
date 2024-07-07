package godq

import (
	"database/sql"
	"fmt"
	"time"
)

const (
	selectItemDetailsQuery  = "SELECT retry_count, ack_deadline FROM %s WHERE id = ?"
	selectItemForDLQQuery   = "SELECT item FROM %s WHERE id = ?"
	deleteItemQuery         = "DELETE FROM %s WHERE id = ?"
	updateItemForRetryQuery = `
		UPDATE %s 
		SET ack_deadline = ?, retry_count = retry_count + 1
		WHERE id = ?
	`
	expireAckDeadlineQuery = `
		UPDATE %s 
		SET ack_deadline = datetime('now', '-1 second')
		WHERE id = ?
	`
)

func nackImpl(db *sql.DB, tableName string, id int64, opts AckOpts, deadLetterQueue Enqueuer) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback() // will fail if committed, but that's fine
	}()

	var retryCount int
	var currentAckDeadline time.Time
	err = tx.QueryRow(fmt.Sprintf(selectItemDetailsQuery, tableName), id).Scan(&retryCount, &currentAckDeadline)
	if err != nil {
		return fmt.Errorf("failed to get item details: %w", err)
	}

	// Check if the ack deadline has expired
	if currentAckDeadline.Before(time.Now()) {
		return fmt.Errorf("ack deadline has expired, cannot nack")
	}

	if retryCount >= opts.MaxRetries && opts.MaxRetries != InfiniteRetries {
		if deadLetterQueue != nil {
			var item []byte
			err = tx.QueryRow(fmt.Sprintf(selectItemForDLQQuery, tableName), id).Scan(&item)
			if err != nil {
				return fmt.Errorf("failed to get item for dead letter queue: %w", err)
			}
			err = deadLetterQueue.Enqueue(item)
			if err != nil {
				return fmt.Errorf("failed to enqueue to dead letter queue: %w", err)
			}
		}
		_, err = tx.Exec(fmt.Sprintf(deleteItemQuery, tableName), id)
		if err != nil {
			return fmt.Errorf("failed to delete item: %w", err)
		}
	} else {
		// Use the maximum of retryBackoff and ackTimeout
		newDeadline := time.Now().Add(max(opts.RetryBackoff, opts.AckTimeout))
		_, err = tx.Exec(fmt.Sprintf(updateItemForRetryQuery, tableName), newDeadline, id)
		if err != nil {
			return fmt.Errorf("failed to update item for retry: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// max returns the maximum of two time.Duration values
func max(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func expireAckDeadline(db *sql.DB, name string, id int64) error {
	_, err := db.Exec(fmt.Sprintf(expireAckDeadlineQuery, name), id)
	return err
}
