package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mattdeak/godq"
)

func main() {
	// Create a new simple queue
	queue, err := godq.NewSimpleQueue("")
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}
	defer queue.Close()

	// Enqueue items
	items := []string{"item1", "item2", "item3"}
	for _, item := range items {
		err := queue.Enqueue([]byte(item))
		if err != nil {
			log.Printf("Failed to enqueue item: %v", err)
		} else {
			fmt.Printf("Enqueued: %s\n", item)
		}
	}

	// Dequeue using TryDequeue (non-blocking)
	msg, err := queue.TryDequeue()
	if err != nil {
		log.Printf("TryDequeue failed: %v", err)
	} else {
		fmt.Printf("TryDequeue: %s\n", string(msg.Item))
	}

	// Dequeue using DequeueCtx with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msg, err = queue.DequeueCtx(ctx)
	if err != nil {
		log.Printf("DequeueCtx failed: %v", err)
	} else {
		fmt.Printf("DequeueCtx: %s\n", string(msg.Item))
	}

	// Dequeue remaining items
	for {
		msg, err := queue.TryDequeue()
		if err != nil {
			log.Printf("Dequeue failed: %v", err)
			break
		}
		fmt.Printf("Dequeued: %s\n", string(msg.Item))
	}
}