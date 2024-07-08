package main

import (
	"fmt"
	"log"

	"github.com/mattdeak/gopq"
)

func main() {
	// Create a new unique queue
	queue, err := gopq.NewUniqueQueue("unique_queue.db")
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}
	defer queue.Close()

	// Enqueue items (including duplicates)
	items := []string{"item1", "item2", "item1", "item3", "item2"}
	for _, item := range items {
		err := queue.Enqueue([]byte(item))
		if err != nil {
			log.Printf("Failed to enqueue item: %v", err)
		} else {
			fmt.Printf("Attempted to enqueue: %s\n", item)
		}
	}

	// Check queue length
	length, err := queue.Len()
	if err != nil {
		log.Fatalf("Failed to get queue length: %v", err)
	}
	fmt.Printf("Queue length: %d\n", length)

	// Dequeue and print all items
	for {
		msg, err := queue.TryDequeue()
		if err != nil {
			log.Printf("Dequeue failed: %v", err)
			break
		}
		fmt.Printf("Dequeued: %s\n", string(msg.Item))
	}

	// Demonstrate UniqueAckQueue
	ackQueue, err := gopq.NewUniqueAckQueue("unique_ack_queue.db", gopq.AckOpts{
		MaxRetries: 2,
	})
	if err != nil {
		log.Fatalf("Failed to create unique ack queue: %v", err)
	}
	defer ackQueue.Close()

	// Enqueue items to UniqueAckQueue
	ackItems := []string{"ack1", "ack2", "ack1", "ack3"}
	for _, item := range ackItems {
		err := ackQueue.Enqueue([]byte(item))
		if err != nil {
			log.Printf("Failed to enqueue item to ack queue: %v", err)
		} else {
			fmt.Printf("Attempted to enqueue to ack queue: %s\n", item)
		}
	}

	// Dequeue and acknowledge items
	for i := 0; i < 3; i++ {
		msg, err := ackQueue.TryDequeue()
		if err != nil {
			log.Printf("Dequeue from ack queue failed: %v", err)
			continue
		}
		fmt.Printf("Dequeued from ack queue: %s\n", string(msg.Item))
		err = ackQueue.Ack(msg.ID)
		if err != nil {
			log.Printf("Failed to acknowledge item: %v", err)
		} else {
			fmt.Printf("Acknowledged: %s\n", string(msg.Item))
		}
	}
}
