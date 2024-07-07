package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/mattdeak/gopq"
)

func main() {
	// Create a new queue in memory
	queue, err := gopq.NewUniqueAckQueue("", gopq.AckOpts{
		AckTimeout: 15 * time.Second,
		MaxRetries: 3,
	})
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}
	defer queue.Close()

	// Number of concurrent producers and consumers
	numProducers := 5
	numConsumers := 3

	var wg sync.WaitGroup

	// Start producers
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < numProducers; i++ {
				item := fmt.Sprintf("Producer %d - Item %d", id, i)
				err := queue.Enqueue([]byte(item))
				if err != nil {
					log.Printf("Producer %d failed to enqueue: %v", id, err)
				} else {
					fmt.Printf("Producer %d enqueued: %s\n", id, item)
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(i)
	}

	// Cancel the context after 10 seconds to stop the consumers
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Second)
		fmt.Println("Ending Test - Stopping consumers")
		cancel()
	}()

	// Some simple utilities to collect stats
	processedCount := 0
	ackFailures := 0
	processAttempts := map[string]int{}

	type result struct {
		msg     gopq.Msg
		nack    bool
		success bool
	}

	statsCh := make(chan result, 100)

	go func() {
		for res := range statsCh {
			processedCount++
			if _, ok := processAttempts[string(res.msg.Item)]; !ok {
				processAttempts[string(res.msg.Item)] = 0
			}
			processAttempts[string(res.msg.Item)]++
			if !res.success {
				ackFailures++
			}
		}
	}()

	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				// This will block until an item is available. We're assuming the producers
				// should run forever. If you know your producers will stop, you may want to use
				// TryDequeue instead to avoid blocking.
				msg, err := queue.DequeueCtx(ctx)
				if _, ok := err.(*gopq.ErrContextDone); ok {
					return
				}
				if err != nil {
					log.Printf("Consumer %d failed to dequeue: %v", id, err)
					continue
				}
				fmt.Printf("Consumer %d dequeued: %s\n", id, string(msg.Item))

				// Simulate processing
				time.Sleep(200 * time.Millisecond)

				// Randomly ack or nack
				willNack := rand.Intn(3) == 0
				if willNack {
					err = queue.Nack(msg.ID)
					fmt.Printf("Consumer %d nacked: %s\n", id, string(msg.Item))
				} else {
					err = queue.Ack(msg.ID)
					fmt.Printf("Consumer %d acked: %s\n", id, string(msg.Item))
				}
				statsCh <- result{msg: msg, nack: willNack, success: err == nil}
			}
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Final queue length
	length, _ := queue.Len()
	fmt.Printf("We simulated stopping in 10 seconds, but DequeueCtx() would have blocked indefinitely if not cancelled.\n")
	fmt.Printf("This is better for processes that need to exit cleanly, rather than being killed by SIGKILL.\n")
	fmt.Printf("Final queue length: %d\n", length)
	fmt.Printf("Total processed: %d\n", processedCount)
	fmt.Printf("Total ack failures: %d\n", ackFailures)
	fmt.Printf("Total process attempts: %d\n", len(processAttempts))
}
