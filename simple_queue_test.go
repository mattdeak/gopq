package godq_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/mattdeak/godq"
	_ "github.com/mattn/go-sqlite3"
)

func TestNewSimpleQueue(t *testing.T) {
	tempFile := tempFilePath(t)
	defer os.Remove(tempFile)

	q, err := godq.NewSimpleQueue(tempFile)
	if err != nil {
		t.Fatalf("NewSimpleQueue() error = %v", err)
	}
	if q == nil {
		t.Fatal("NewSimpleQueue() returned nil")
	}
}

func TestSimpleQueue_Enqueue(t *testing.T) {
	q := setupTestQueue(t)

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
}

func TestSimpleQueue_TryDequeue(t *testing.T) {
	q := setupTestQueue(t)

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
		t.Errorf("Expected error, got nil")
	}
}

func TestSimpleQueue_DequeueCtx(t *testing.T) {
	q := setupTestQueue(t)

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

func TestSimpleQueue_Len(t *testing.T) {
	q := setupTestQueue(t)

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

func setupTestQueue(t *testing.T) *godq.Queue {
	tempFile := tempFilePath(t)
	t.Cleanup(func() { os.Remove(tempFile) })

	q, err := godq.NewSimpleQueue(tempFile)
	if err != nil {
		t.Fatalf("Failed to create test queue: %v", err)
	}
	return q
}
