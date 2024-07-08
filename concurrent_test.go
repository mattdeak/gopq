package gopq_test

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattdeak/gopq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrentOperations(t *testing.T) {
	t.Log("starting TestConcurrentOperations")
	q, err := gopq.NewUniqueAckQueue("", gopq.AckOpts{
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
						if err == context.Canceled {
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

func BenchmarkConcurrentOperationsInMemory(b *testing.B) {
	q, err := gopq.NewUniqueAckQueue("", gopq.AckOpts{
		AckTimeout: 5 * time.Second,
		MaxRetries: 3,
	})
	require.NoError(b, err)
	defer q.Close()

	numProducers := runtime.GOMAXPROCS(0) / 2
	numConsumers := runtime.GOMAXPROCS(0) / 2
	itemsPerProducer := 1000

	b.ResetTimer()
	benchmarkConcurrentOperations(b, q, numProducers, numConsumers, itemsPerProducer)
}

func BenchmarkConcurrentOperationsOnDisk(b *testing.B) {
    q, err := gopq.NewUniqueAckQueue("test.db", gopq.AckOpts{
        AckTimeout: 5 * time.Second,
        MaxRetries: 3,
    })
    require.NoError(b, err)
    defer q.Close()
	defer os.Remove("test.db")

    // Set up the benchmark parameters
    numProducers := runtime.GOMAXPROCS(0) / 2
    numConsumers := runtime.GOMAXPROCS(0) / 2
    itemsPerProducer := 1000 // Increased from 5 to 1000 for a more substantial workload

    b.ResetTimer()

    // Run the actual benchmark
    benchmarkConcurrentOperations(b, q, numProducers, numConsumers, itemsPerProducer)

}

func benchmarkConcurrentOperations(b *testing.B, q *gopq.AcknowledgeableQueue, numProducers, numConsumers, itemsPerProducer int) {
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            var wg sync.WaitGroup
            ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
            defer cancel()

            producedCount := int64(0)
            consumedCount := int64(0)

            // Start producers
            for j := 0; j < numProducers; j++ {
                wg.Add(1)
                go func(id int) {
                    defer wg.Done()
                    for k := 0; k < itemsPerProducer; k++ {
                        select {
                        case <-ctx.Done():
                            return
                        default:
                            item := fmt.Sprintf("Producer %d - Item %d", id, k)
                            err := q.Enqueue([]byte(item))
                            if err != nil {
                                b.Error(err)
                            }
                            atomic.AddInt64(&producedCount, 1)
                        }
                    }
                }(j)
            }

            // Start consumers
            for j := 0; j < numConsumers; j++ {
                wg.Add(1)
                go func(id int) {
                    defer wg.Done()
                    for {
                        select {
                        case <-ctx.Done():
                            return
                        default:
                            msg, err := q.DequeueCtx(ctx)
                            if err != nil {
                                if err == context.Canceled || err == context.DeadlineExceeded {
                                    return
                                }
                                b.Error(err)
                                return
                            }
                            err = q.Ack(msg.ID)
                            if err != nil {
                                b.Error(err)
                            }
                            atomic.AddInt64(&consumedCount, 1)
                        }
                    }
                }(j)
            }

            // Wait for completion or timeout
            wg.Wait()

            // Ensure all items are consumed
            for atomic.LoadInt64(&consumedCount) < atomic.LoadInt64(&producedCount) {
                select {
                case <-ctx.Done():
                    b.Error("Benchmark timed out")
                    return
                default:
                    time.Sleep(10 * time.Millisecond)
                }
            }

            // Ensure queue is empty
            length, err := q.Len()
            if err != nil {
                b.Error(err)
            }
            if length != 0 {
                b.Errorf("Queue not empty at end of benchmark. Length: %d", length)
            }
        }
    })
}