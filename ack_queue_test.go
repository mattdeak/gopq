package godq_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	q := setupDefaultTestAckQueue(t)

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
	q := setupDefaultTestAckQueue(t)

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
	q := setupDefaultTestAckQueue(t)

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
	q := setupDefaultTestAckQueue(t)

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

func TestAckQueue_Len(t *testing.T) {
	q := setupDefaultTestAckQueue(t)

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

func TestAckQueue_Nack(t *testing.T) {
	tests := []struct {
		name            string
		maxRetries      int
		deadLetterQueue bool
		operations      func(*testing.T, *godq.AckQueue, *godq.AckQueue)
		expectedResult  func(*testing.T, *godq.AckQueue, *godq.AckQueue)
	}{
		{
			name:            "No retries, no dead letter queue",
			maxRetries:      0,
			deadLetterQueue: false,
			operations: func(t *testing.T, q, dlq *godq.AckQueue) {
				err := q.Enqueue([]byte("test"))
				require.NoError(t, err)
				msg, err := q.TryDequeue()
				require.NoError(t, err)
				err = q.Nack(msg.ID)
				require.NoError(t, err)
				err = q.ExpireAck(msg.ID)
				require.NoError(t, err)
			},
			expectedResult: func(t *testing.T, q, dlq *godq.AckQueue) {
				_, err := q.TryDequeue()
				assert.Error(t, err) // Item should be removed
				assert.Equal(t, 0, queueLength(t, q))
			},
		},
		{
			name:            "With retries, no dead letter queue",
			maxRetries:      2,
			deadLetterQueue: false,
			operations: func(t *testing.T, q, dlq *godq.AckQueue) {
				err := q.Enqueue([]byte("test"))
				require.NoError(t, err)
				for i := 0; i < 3; i++ {
					msg, err := q.TryDequeue()
					require.NoError(t, err)
					err = q.Nack(msg.ID)
					require.NoError(t, err)
					err = q.ExpireAck(msg.ID)
					require.NoError(t, err)
				}
			},
			expectedResult: func(t *testing.T, q, dlq *godq.AckQueue) {
				_, err := q.TryDequeue()
				assert.Error(t, err) // Item should be removed after max retries
				assert.Equal(t, 0, queueLength(t, q))
			},
		},
		{
			name:            "With retries and dead letter queue",
			maxRetries:      1,
			deadLetterQueue: true,
			operations: func(t *testing.T, q, dlq *godq.AckQueue) {
				err := q.Enqueue([]byte("test"))
				require.NoError(t, err)
				for i := 0; i < 2; i++ {
					msg, err := q.TryDequeue()
					require.NoError(t, err)
					err = q.Nack(msg.ID)
					require.NoError(t, err)
					err = q.ExpireAck(msg.ID)
					require.NoError(t, err)
				}
			},
			expectedResult: func(t *testing.T, q, dlq *godq.AckQueue) {
				assert.Equal(t, 0, queueLength(t, q))
				assert.Equal(t, 1, queueLength(t, dlq))
			},
		},
		{
			name:            "Infinite retries",
			maxRetries:      -1,
			deadLetterQueue: false,
			operations: func(t *testing.T, q, dlq *godq.AckQueue) {
				err := q.Enqueue([]byte("test"))
				require.NoError(t, err)
				for i := 0; i < 10; i++ {
					msg, err := q.TryDequeue()
					require.NoError(t, err)
					err = q.Nack(msg.ID)
					require.NoError(t, err)
					err = q.ExpireAck(msg.ID)
					require.NoError(t, err)
				}
			},
			expectedResult: func(t *testing.T, q, dlq *godq.AckQueue) {
				assert.Equal(t, 1, queueLength(t, q))
			},
		},
		{
			name:            "Nack with expired ack deadline",
			maxRetries:      1,
			deadLetterQueue: false,
			operations: func(t *testing.T, q, dlq *godq.AckQueue) {
				err := q.Enqueue([]byte("test"))
				require.NoError(t, err)
				msg, err := q.TryDequeue()
				require.NoError(t, err)
				err = q.ExpireAck(msg.ID)
				require.NoError(t, err)
				err = q.Nack(msg.ID)
				assert.Error(t, err) // Expect an error for expired ack deadline
			},
			expectedResult: func(t *testing.T, q, dlq *godq.AckQueue) {
				// If the ack expires, the item was effectively never dequeued.
				assert.Equal(t, 1, queueLength(t, q))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := setupTestAckQueue(t, godq.AckOpts{
				AckTimeout:   time.Hour, // long timeout, we expire manually
				MaxRetries:   tt.maxRetries,
				RetryBackoff: time.Millisecond, // This doesn't matter as we're using ExpireAck
			})

			var dlq *godq.AckQueue
			if tt.deadLetterQueue {
				dlq = setupDefaultTestAckQueue(t)
				q.SetDeadLetterQueue(dlq)
			}

			tt.operations(t, q, dlq)
			tt.expectedResult(t, q, dlq)
		})
	}
}

func queueLength(t *testing.T, q *godq.AckQueue) int {
	length, err := q.Len()
	require.NoError(t, err)
	return length
}

func setupDefaultTestAckQueue(t *testing.T) *godq.AckQueue {
	return setupTestAckQueue(t, godq.AckOpts{
		AckTimeout:   time.Hour * 999,
		MaxRetries:   0,
		RetryBackoff: time.Second,
	})
}

func setupTestAckQueue(t *testing.T, opts godq.AckOpts) *godq.AckQueue {
	tempFile := tempFilePath(t)
	t.Cleanup(func() { os.Remove(tempFile) })

	q, err := godq.NewAckQueue(tempFile, opts)
	if err != nil {
		t.Fatalf("Failed to create test queue: %v", err)
	}
	return q
}