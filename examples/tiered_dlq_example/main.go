package main

import (
	"fmt"
	"log"
	"time"

	"github.com/mattdeak/godq"
)

func main() {
	// Create queues
	mainQueue, err := godq.NewAckQueue("", godq.AckOpts{
		AckTimeout: 2 * time.Second,
		MaxRetries: 2,
	})
	if err != nil {
		log.Fatalf("Failed to create main queue: %v", err)
	}
	defer mainQueue.Close()

	dlq1, err := godq.NewAckQueue("", godq.AckOpts{
		AckTimeout: 5 * time.Second,
		MaxRetries: 1,
	})
	if err != nil {
		log.Fatalf("Failed to create DLQ1: %v", err)
	}
	defer dlq1.Close()

	finalDLQ, err := godq.NewSimpleQueue("")
	if err != nil {
		log.Fatalf("Failed to create final DLQ: %v", err)
	}
	defer finalDLQ.Close()

	// Set up the chain
	mainQueue.SetDeadLetterQueue(dlq1)
	dlq1.SetDeadLetterQueue(finalDLQ)

	// Enqueue an item
	err = mainQueue.Enqueue([]byte("Problematic item"))
	if err != nil {
		log.Fatalf("Failed to enqueue item: %v", err)
	}
	fmt.Println("Enqueued: Problematic item")

	// Function to process queue
	processQueue := func(q godq.AckableQueue, name string, attempts int) {
		for i := 0; i < attempts; i++ {
			msg, err := q.Dequeue()
			if err != nil {
				log.Printf("Failed to dequeue from %s: %v", name, err)
				return
			}
			fmt.Printf("%s - Attempt %d: Dequeued: %s\n", name, i+1, string(msg.Item))

			// Simulate processing failure
			time.Sleep(1 * time.Second)
			err = q.Nack(msg.ID)
			if err != nil {
				log.Printf("Failed to nack item in %s: %v", name, err)
			} else {
				fmt.Printf("%s - Attempt %d: Item nacked\n", name, i+1)
			}

			// Wait for item to be available again
			time.Sleep(2 * time.Second)
		}
	}

	// Process through the queue chain
	processQueue(mainQueue, "Main Queue", 3)
	processQueue(dlq1, "DLQ1", 2)

	// Check queue lengths
	mainLen, _ := mainQueue.Len()
	dlq1Len, _ := dlq1.Len()
	finalLen, _ := finalDLQ.Len()

	fmt.Printf("Main queue length: %d\n", mainLen)
	fmt.Printf("DLQ1 length: %d\n", dlq1Len)
	fmt.Printf("Final DLQ length: %d\n", finalLen)

	// Dequeue from final DLQ
	finalMsg, err := finalDLQ.Dequeue()
	if err != nil {
		log.Fatalf("Failed to dequeue from final DLQ: %v", err)
	}
	fmt.Printf("Dequeued from final DLQ: %s\n", string(finalMsg.Item))
}