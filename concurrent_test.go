//go:build long
// +build long

package gopq_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mattdeak/gopq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrentOperations(t *testing.T) {
	t.Log("starting TestConcurrentOperations")
	q, err := gopq.NewUniqueAckQueue("test.db", gopq.AckOpts{
		AckTimeout: 5 * time.Second,
		MaxRetries: 3,
	})
	require.NoError(t, err)
	defer q.Close()

	numProducers := 10
	numConsumers := 5
	itemsPerProducer := 100

	var producerWg sync.WaitGroup
	var consumerWg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Log("Starting producers")
	// Start producers
	for i := 0; i < numProducers; i++ {
		producerWg.Add(1)
		go func(id int) {
			defer producerWg.Done()
			for j := 0; j < itemsPerProducer; j++ {
				item := fmt.Sprintf("Producer %d - Item %d", id, j)
				err := q.Enqueue([]byte(item))
				assert.NoError(t, err)
				if j%10 == 0 {
					t.Logf("Producer %d enqueued item %d", id, j)
				}
			}
			t.Logf("Producer %d finished", id)
		}(i)
	}

	t.Log("Starting consumers")
	// Start consumers
	consumedItems := make(chan string, numProducers*itemsPerProducer)
	for i := 0; i < numConsumers; i++ {
		consumerWg.Add(1)
		go func(id int) {
			defer consumerWg.Done()
			itemsConsumed := 0
			for {
				select {
				case <-ctx.Done():
					t.Logf("Consumer %d stopping, consumed %d items", id, itemsConsumed)
					return
				default:
					msg, err := q.DequeueCtx(ctx)
					if err != nil {
						if _, ok := err.(*gopq.ErrContextDone); ok {
							t.Logf("Consumer %d context done, consumed %d items", id, itemsConsumed)
							return
						}
						t.Errorf("Consumer %d error: %v", id, err)
						return
					}
					consumedItems <- string(msg.Item)
					itemsConsumed++
					if itemsConsumed%10 == 0 {
						t.Logf("Consumer %d dequeued %d items", id, itemsConsumed)
					}
					time.Sleep(time.Millisecond) // Simulate processing time
					err = q.Ack(msg.ID)
					assert.NoError(t, err)
				}
			}
		}(i)
	}

	t.Log("Waiting for producers to finish")
	producerWg.Wait()
	t.Log("All producers finished")

	t.Log("Waiting for all items to be consumed")
	// Wait for all items to be consumed
	timeout := time.After(10 * time.Second)
	itemsConsumed := 0
	for itemsConsumed < numProducers*itemsPerProducer {
		select {
		case <-consumedItems:
			itemsConsumed++
			if itemsConsumed%100 == 0 {
				t.Logf("Total items consumed: %d", itemsConsumed)
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for all items to be consumed. Consumed %d out of %d", itemsConsumed, numProducers*itemsPerProducer)
		}
	}

	t.Log("All items consumed, stopping consumers")
	// Stop consumers
	cancel()

	// Give consumers time to stop
	time.Sleep(100 * time.Millisecond)

	t.Log("Checking final queue length")
	// Check queue length
	length, err := q.Len()
	assert.NoError(t, err)
	assert.Equal(t, 0, length, "Queue should be empty after all items are consumed")
	t.Logf("Final queue length: %d", length)
}

func BenchmarkConcurrentOperations(b *testing.B) {
	q, err := gopq.NewUniqueAckQueue("", gopq.AckOpts{
		AckTimeout: 5 * time.Second,
		MaxRetries: 3,
	})
	require.NoError(b, err)
	defer q.Close()

	numProducers := 5
	numConsumers := 3

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		ctx, cancel := context.WithCancel(context.Background())

		// Start producers
		for j := 0; j < numProducers; j++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				item := fmt.Sprintf("Producer %d - Item %d", id, i)
				err := q.Enqueue([]byte(item))
				if err != nil {
					b.Error(err)
				}
			}(j)
		}

		// Start consumers
		for j := 0; j < numConsumers; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				msg, err := q.DequeueCtx(ctx)
				if err != nil {
					return
				}
				time.Sleep(time.Millisecond) // Simulate processing time
				err = q.Ack(msg.ID)
				if err != nil {
					b.Error(err)
				}
			}()
		}

		wg.Wait()
		cancel()
	}
}