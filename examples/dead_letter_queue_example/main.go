package main

import (
	"fmt"
	"log"
	"time"

	"github.com/mattdeak/gopq"
)

func main() {
	// Create main queue
	mainQueue, err := gopq.NewAckQueue("", gopq.AckOpts{
		AckTimeout: 2 * time.Second,
		MaxRetries: 2,
	})
	if err != nil {
		log.Fatalf("Failed to create main queue: %v", err)
	}
	defer mainQueue.Close()

	// Create dead letter queue
	dlq, err := gopq.NewSimpleQueue("")
	if err != nil {
		log.Fatalf("Failed to create dead letter queue: %v", err)
	}
	defer dlq.Close()

	// Set dead letter queue
	mainQueue.RegisterDeadLetterQueue(dlq)

	// Enqueue an item
	err = mainQueue.Enqueue([]byte("Problematic item"))
	if err != nil {
		log.Fatalf("Failed to enqueue item: %v", err)
	}
	fmt.Println("Enqueued: Problematic item")

	// Simulate processing and failing multiple times
	for i := 0; i < 3; i++ {
		msg, err := mainQueue.Dequeue()
		if err != nil {
			log.Fatalf("Failed to dequeue item: %v", err)
		}
		fmt.Printf("Attempt %d: Dequeued: %s\n", i+1, string(msg.Item))

		// Simulate processing failure
		time.Sleep(1 * time.Second)
		err = mainQueue.Nack(msg.ID)
		if err != nil {
			log.Printf("Failed to nack item: %v", err)
		} else {
			fmt.Printf("Attempt %d: Item nacked\n", i+1)
		}

		// Wait for item to be available again
		time.Sleep(2 * time.Second)
	}

	// Check main queue (should be empty)
	mainLen, _ := mainQueue.Len()
	fmt.Printf("Main queue length: %d\n", mainLen)

	// Check dead letter queue
	dlqLen, _ := dlq.Len()
	fmt.Printf("Dead letter queue length: %d\n", dlqLen)

	// Dequeue from dead letter queue
	dlqMsg, err := dlq.Dequeue()
	if err != nil {
		log.Fatalf("Failed to dequeue from DLQ: %v", err)
	}
	fmt.Printf("Dequeued from DLQ: %s\n", string(dlqMsg.Item))
}
