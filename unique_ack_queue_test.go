package gopq_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/mattdeak/gopq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewUniqueAckQueue(t *testing.T) {
	tempFile := tempFilePath(t)
	defer os.Remove(tempFile)

	q, err := gopq.NewUniqueAckQueue(tempFile, gopq.AckOpts{AckTimeout: time.Second})
	if err != nil {
		t.Fatalf("NewUniqueAckQueue() error = %v", err)
	}
	if q == nil {
		t.Fatal("NewUniqueAckQueue() returned nil")
	}
}

func TestUniqueAckQueue_Enqueue(t *testing.T) {
	q := setupDefaultTestUniqueAckQueue(t)

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
	q := setupDefaultTestUniqueAckQueue(t)

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
	q := setupDefaultTestUniqueAckQueue(t)

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
	q := setupDefaultTestUniqueAckQueue(t)

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

func TestUniqueAckQueue_Len(t *testing.T) {
	q := setupDefaultTestUniqueAckQueue(t)

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

func TestUniqueAckQueue_Nack(t *testing.T) {
	tests := []struct {
		name            string
		maxRetries      int
		deadLetterQueue bool
		operations      func(*testing.T, *gopq.AcknowledgeableQueue, *gopq.AcknowledgeableQueue)
		expectedResult  func(*testing.T, *gopq.AcknowledgeableQueue, *gopq.AcknowledgeableQueue)
	}{
		{
			name:            "No retries, no dead letter queue",
			maxRetries:      0,
			deadLetterQueue: false,
			operations: func(t *testing.T, q, dlq *gopq.AcknowledgeableQueue) {
				err := q.Enqueue([]byte("test"))
				require.NoError(t, err)
				msg, err := q.TryDequeue()
				require.NoError(t, err)
				err = q.Nack(msg.ID)
				require.NoError(t, err)
				err = q.ExpireAck(msg.ID)
				require.NoError(t, err)
			},
			expectedResult: func(t *testing.T, q, dlq *gopq.AcknowledgeableQueue) {
				_, err := q.TryDequeue()
				assert.Error(t, err) // Item should be removed
				assert.Equal(t, 0, uniqueQueueLength(t, q))
			},
		},
		{
			name:            "With retries, no dead letter queue",
			maxRetries:      2,
			deadLetterQueue: false,
			operations: func(t *testing.T, q, dlq *gopq.AcknowledgeableQueue) {
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
			expectedResult: func(t *testing.T, q, dlq *gopq.AcknowledgeableQueue) {
				_, err := q.TryDequeue()
				assert.Error(t, err) // Item should be removed after max retries
				assert.Equal(t, 0, uniqueQueueLength(t, q))
			},
		},
		{
			name:            "With retries and dead letter queue",
			maxRetries:      1,
			deadLetterQueue: true,
			operations: func(t *testing.T, q, dlq *gopq.AcknowledgeableQueue) {
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
			expectedResult: func(t *testing.T, q, dlq *gopq.AcknowledgeableQueue) {
				assert.Equal(t, 0, uniqueQueueLength(t, q))
				assert.Equal(t, 1, uniqueQueueLength(t, dlq))
			},
		},
		{
			name:            "Infinite retries",
			maxRetries:      -1,
			deadLetterQueue: false,
			operations: func(t *testing.T, q, dlq *gopq.AcknowledgeableQueue) {
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
			expectedResult: func(t *testing.T, q, dlq *gopq.AcknowledgeableQueue) {
				assert.Equal(t, 1, uniqueQueueLength(t, q))
			},
		},
		{
			name:            "Nack with expired ack deadline",
			maxRetries:      1,
			deadLetterQueue: false,
			operations: func(t *testing.T, q, dlq *gopq.AcknowledgeableQueue) {
				err := q.Enqueue([]byte("test"))
				require.NoError(t, err)
				msg, err := q.TryDequeue()
				require.NoError(t, err)
				err = q.ExpireAck(msg.ID)
				require.NoError(t, err)
				err = q.Nack(msg.ID)
				assert.Error(t, err) // Expect an error for expired ack deadline
			},
			expectedResult: func(t *testing.T, q, dlq *gopq.AcknowledgeableQueue) {
				// If the ack expires, the item was effectively never dequeued.
				assert.Equal(t, 1, uniqueQueueLength(t, q))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := setupTestUniqueAckQueue(t, gopq.AckOpts{
				AckTimeout:   time.Hour, // long timeout, we expire manually
				MaxRetries:   tt.maxRetries,
				RetryBackoff: time.Millisecond, // This doesn't matter as we're using ExpireAck
			})

			var dlq *gopq.AcknowledgeableQueue
			if tt.deadLetterQueue {
				dlq = setupDefaultTestUniqueAckQueue(t)
				q.SetDeadLetterQueue(dlq)
			}

			tt.operations(t, q, dlq)
			tt.expectedResult(t, q, dlq)
		})
	}
}

func uniqueQueueLength(t *testing.T, q *gopq.AcknowledgeableQueue) int {
	length, err := q.Len()
	require.NoError(t, err)
	return length
}

func setupDefaultTestUniqueAckQueue(t *testing.T) *gopq.AcknowledgeableQueue {
	return setupTestUniqueAckQueue(t, gopq.AckOpts{
		AckTimeout:   time.Hour * 999,
		MaxRetries:   0,
		RetryBackoff: time.Second,
	})
}

func setupTestUniqueAckQueue(t *testing.T, opts gopq.AckOpts) *gopq.AcknowledgeableQueue {
	tempFile := tempFilePath(t)
	t.Cleanup(func() { os.Remove(tempFile) })

	q, err := gopq.NewUniqueAckQueue(tempFile, opts)
	if err != nil {
		t.Fatalf("Failed to create test queue: %v", err)
	}
	return q
}
