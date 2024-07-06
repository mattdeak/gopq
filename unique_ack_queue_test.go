package godq_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/mattdeak/godq"
	_ "github.com/mattn/go-sqlite3"
)

func TestNewUniqueAckQueue(t *testing.T) {
	tempFile := tempFilePath(t)
	defer os.Remove(tempFile)

	q, err := godq.NewUniqueAckQueue(tempFile, godq.AckOpts{AckTimeout: time.Second})
	if err != nil {
		t.Fatalf("NewUniqueAckQueue() error = %v", err)
	}
	if q == nil {
		t.Fatal("NewUniqueAckQueue() returned nil")
	}
}

func TestUniqueAckQueue_Enqueue(t *testing.T) {
	q := setupTestUniqueAckQueue(t)

	err := q.Enqueue([]byte("test item"))
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	count, err := q.Len()
	if err != nil {
		t.Fatalf("Len() error = %v", err)
	}
	if count != 1 {
		t.Errorf("Expected queue length 1, got %d", count)
	}

	// Test uniqueness
	err = q.Enqueue([]byte("test item"))
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	count, err = q.Len()
	if err != nil {
		t.Fatalf("Len() error = %v", err)
	}
	if count != 1 {
		t.Errorf("Expected queue length 1 after duplicate enqueue, got %d", count)
	}
}

func TestUniqueAckQueue_TryDequeue(t *testing.T) {
	q := setupTestUniqueAckQueue(t)

	// Enqueue an item
	err := q.Enqueue([]byte("test item"))
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	// Dequeue the item
	msg, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue() error = %v", err)
	}
	if string(msg.Item) != "test item" {
		t.Errorf("Expected 'test item', got '%s'", string(msg.Item))
	}

	// Try to dequeue from empty queue
	_, err = q.TryDequeue()
	if err == nil {
		t.Error("Expected error when dequeuing from empty queue, got nil")
	}
}

func TestUniqueAckQueue_DequeueCtx(t *testing.T) {
	q := setupTestUniqueAckQueue(t)

	// Test dequeue with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := q.DequeueCtx(ctx)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// Enqueue and dequeue
	err = q.Enqueue([]byte("test item"))
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	msg, err := q.DequeueCtx(context.Background())
	if err != nil {
		t.Fatalf("DequeueCtx() error = %v", err)
	}
	if string(msg.Item) != "test item" {
		t.Errorf("Expected 'test item', got '%s'", string(msg.Item))
	}
}

func TestUniqueAckQueue_Ack(t *testing.T) {
	q := setupTestUniqueAckQueue(t)

	err := q.Enqueue([]byte("test item"))
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	msg, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue() error = %v", err)
	}

	err = q.Ack(msg.ID)
	if err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	// Verify item is removed after Ack
	count, err := q.Len()
	if err != nil {
		t.Fatalf("Len() error = %v", err)
	}
	if count != 0 {
		t.Errorf("Expected queue length 0 after Ack, got %d", count)
	}
}

func TestUniqueAckQueue_Nack(t *testing.T) {
	q := setupTestUniqueAckQueue(t)

	err := q.Enqueue([]byte("test item"))
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	msg, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue() error = %v", err)
	}

	err = q.Nack(msg.ID)
	if err != nil {
		t.Fatalf("Nack() error = %v", err)
	}

	// Verify item is still in queue after Nack
	count, err := q.Len()
	if err != nil {
		t.Fatalf("Len() error = %v", err)
	}
	if count != 1 {
		t.Errorf("Expected queue length 1 after Nack, got %d", count)
	}
}

func TestUniqueAckQueue_Len(t *testing.T) {
	q := setupTestUniqueAckQueue(t)

	count, err := q.Len()
	if err != nil {
		t.Fatalf("Len() error = %v", err)
	}
	if count != 0 {
		t.Errorf("Expected empty queue, got length %d", count)
	}

	err = q.Enqueue([]byte("item1"))
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	err = q.Enqueue([]byte("item2"))
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	count, err = q.Len()
	if err != nil {
		t.Fatalf("Len() error = %v", err)
	}
	if count != 2 {
		t.Errorf("Expected queue length 2, got %d", count)
	}
}

func setupTestUniqueAckQueue(t *testing.T) *godq.UniqueAckQueue {
	tempFile := tempFilePath(t)
	t.Cleanup(func() { os.Remove(tempFile) })

	// Set ack timeout to a very long time in the future
	// so should never break unless we force it to
	q, err := godq.NewUniqueAckQueue(tempFile, godq.AckOpts{AckTimeout: time.Hour * 24 * 365 * 100}) // 100 years
	if err != nil {
		t.Fatalf("Failed to create test queue: %v", err)
	}

	return q
}