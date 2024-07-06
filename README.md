# GoPQ: Go Persistent Queue

GoPQ is a lightweight, persistent queue implementation in Go, using SQLite as the underlying storage mechanism. It provides various queue types to suit different use cases, including simple queues, acknowledged queues, and unique item queues.

## Features

- Persistent storage using SQLite
- Multiple queue types:
  - Simple Queue
  - Acknowledged Queue
  - Unique Queue
  - Unique Acknowledged Queue
- Blocking and non-blocking dequeue operations
- Context support for cancellation and timeouts
- Thread-safe operations

## Installation

To use GoPQ in your Go project, run:

```
go get github.com/mattdeak/gopq
```

Make sure you have SQLite installed on your system.

## Usage

Here are some examples of how to use different queue types:

### Simple Queue

```go
import "github.com/mattdeak/gopq"

// Create a new simple queue
queue, err := gopq.NewSimpleQueue("queue.db")
if err != nil {
    // Handle error
}

// Enqueue an item
err = queue.Enqueue([]byte("Hello, World!"))
if err != nil {
    // Handle error
}

// Dequeue an item
msg, err := queue.Dequeue()
if err != nil {
    // Handle error
}
fmt.Println(string(msg.Item))
```

### Acknowledged Queue

```go
import "github.com/mattdeak/gopq"

// Create a new acknowledged queue with a 30-second ack timeout
queue, err := gopq.NewAckQueue("ack_queue.db", gopq.AckOpts{AckTimeout: 30 * time.Second})
if err != nil {
    // Handle error
}

// Enqueue an item
err = queue.Enqueue([]byte("Process me"))
if err != nil {
    // Handle error
}

// Dequeue an item
msg, err := queue.Dequeue()
if err != nil {
    // Handle error
}

// Process the item...

// Acknowledge the item
err = queue.Ack(msg.ID)
if err != nil {
    // Handle error
}
```

### Unique Queue

```go
import "github.com/mattdeak/gopq"

// Create a new unique queue
queue, err := gopq.NewUniqueQueue("unique_queue.db")
if err != nil {
    // Handle error
}

// Enqueue items (duplicates will be ignored)
queue.Enqueue([]byte("unique_item_1"))
queue.Enqueue([]byte("unique_item_1")) // This will be ignored
queue.Enqueue([]byte("unique_item_2"))

// Dequeue items
msg1, _ := queue.Dequeue()
msg2, _ := queue.Dequeue()
msg3, err := queue.TryDequeue() // This will return an error (queue is empty)
```

## Queue Types

1. **SimpleQueue**: Basic FIFO queue with no additional features.
2. **AckQueue**: Queue with acknowledgment support. Items must be acknowledged after processing, or they will be requeued after a timeout.
3. **UniqueQueue**: Queue that only allows unique items. Duplicate enqueue attempts are silently ignored.
4. **UniqueAckQueue**: Combination of UniqueQueue and AckQueue. Ensures unique items with acknowledgment support.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.


### Future Work
- More Queue Configurability
- Various Efficiency Improvements
