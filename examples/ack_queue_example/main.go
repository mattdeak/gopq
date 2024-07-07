package main

import (
	"fmt"
	"log"
	"time"

	"github.com/mattdeak/godq"
)

func main() {
	// Create a new acknowledged queue with custom options
	queue, err := godq.NewAckQueue("", godq.AckOpts{
		AckTimeout:   5 * time.Second,
		MaxRetries:   3,
		RetryBackoff: 1 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}
	defer queue.Close()

	// Enqueue an item
	err = queue.Enqueue([]byte("Process me"))
	if err != nil {
		log.Fatalf("Failed to enqueue item: %v", err)
	}
	fmt.Println("Enqueued: Process me")

	// Dequeue and process the item
	msg, err := queue.Dequeue()
	if err != nil {
		log.Fatalf("Failed to dequeue item: %v", err)
	}
	fmt.Printf("Dequeued: %s\n", string(msg.Item))

	// Simulate processing
	time.Sleep(2 * time.Second)

	// Acknowledge the item
	err = queue.Ack(msg.ID)
	if err != nil {
		log.Printf("Failed to acknowledge item: %v", err)
	} else {
		fmt.Println("Item acknowledged")
	}

	// Enqueue another item for Nack demonstration
	err = queue.Enqueue([]byte("Fail me"))
	if err != nil {
		log.Fatalf("Failed to enqueue item: %v", err)
	}
	fmt.Println("Enqueued: Fail me")

	// Dequeue and process the item
	msg, err = queue.Dequeue()
	if err != nil {
		log.Fatalf("Failed to dequeue item: %v", err)
	}
	fmt.Printf("Dequeued: %s\n", string(msg.Item))

	// Simulate failed processing
	time.Sleep(1 * time.Second)

	// Negative acknowledge the item
	err = queue.Nack(msg.ID)
	if err != nil {
		log.Printf("Failed to negative acknowledge item: %v", err)
	} else {
		fmt.Println("Item negative acknowledged")
	}

	// Wait for retry
	time.Sleep(2 * time.Second)

	// Dequeue the retried item
	msg, err = queue.Dequeue()
	if err != nil {
		log.Fatalf("Failed to dequeue retried item: %v", err)
	}
	fmt.Printf("Dequeued retried item: %s\n", string(msg.Item))
}