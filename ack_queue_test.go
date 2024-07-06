package godq_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/mattdeak/godq"
	_ "github.com/mattn/go-sqlite3"
)

func TestNewAckQueue(t *testing.T) {
	tempFile := tempFilePath(t)
	defer os.Remove(tempFile)

	q, err := godq.NewAckQueue(tempFile, godq.AckOpts{AckTimeout: time.Second})
	if err != nil {
		t.Fatalf("NewAckQueue() error = %v", err)
	}
	if q == nil {
		t.Fatal("NewAckQueue() returned nil")
	}
}

func TestAckQueue_Enqueue(t *testing.T) {
	q := setupTestAckQueue(t)

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

func TestAckQueue_TryDequeue(t *testing.T) {
	q := setupTestAckQueue(t)

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
	res, err := q.TryDequeue()
	if err == nil {
		t.Error("Expected error when dequeuing from empty queue, got nil")
	}

	if res.Item != nil {
		t.Errorf("Expected nil, got %v", res)
	}
}

func TestAckQueue_DequeueCtx(t *testing.T) {
	q := setupTestAckQueue(t)

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

func TestAckQueue_Ack(t *testing.T) {
	q := setupTestAckQueue(t)

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

func TestAckQueue_Nack(t *testing.T) {
	q := setupTestAckQueue(t)

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

func TestAckQueue_Len(t *testing.T) {
	q := setupTestAckQueue(t)

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

func setupTestAckQueue(t *testing.T) *godq.AckQueue {
	tempFile := tempFilePath(t)
	t.Cleanup(func() { os.Remove(tempFile) })

	q, err := godq.NewAckQueue(tempFile, godq.AckOpts{AckTimeout: time.Hour * 999})
	if err != nil {
		t.Fatalf("Failed to create test queue: %v", err)
	}
	return q
}
