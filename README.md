# godq: Go Disk Queue

A lightweight, sqlite-backed persistent queue implementation in Go.

[![Go](https://github.com/mattdeak/godq/actions/workflows/go.yml/badge.svg)](https://github.com/mattdeak/godq/actions/workflows/go.yml)
[![GoDoc](https://godoc.org/github.com/mattdeak/godq?status.svg)](https://godoc.org/github.com/mattdeak/godq)

godq is a lightweight, persistent queue implementation in Go, using SQLite as the underlying storage mechanism. It provides various queue types to suit different use cases, including simple queues, acknowledged queues, and unique item queues.

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

To use godq in your Go project, run:

```
go get github.com/mattdeak/godq
```

Make sure you have SQLite installed on your system.

## Getting Started
Here's a minimal example to get you started with godq:
```go
package main

import (
    "fmt"
    "github.com/mattdeak/godq"
)

func main() {
    // Create a new simple queue
    queue, err := godq.NewSimpleQueue("myqueue.db")
    if err != nil {
        panic(err)
    }
    defer queue.Close()

    // Enqueue an item
    err = queue.Enqueue([]byte("Hello, godq!"))
    if err != nil {
        panic(err)
    }

    // Dequeue an item
    msg, err := queue.Dequeue()
    if err != nil {
        panic(err)
    }

    fmt.Println(string(msg.Item)) // Output: Hello, godq!
}
```

## Usage

Here are some examples of how to use different queue types:

### Simple Queue

```go
import "github.com/mattdeak/godq"

// Create a new simple queue
queue, err := godq.NewSimpleQueue("queue.db")
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
import "github.com/mattdeak/godq"
// Create a new acknowledged queue with custom options
queue, err := godq.NewAckQueue("ack_queue.db", godq.AckOpts{
AckTimeout: 30 time.Second,
MaxRetries: 3,
RetryBackoff: 5 time.Second,
})
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
// Acknowledge the item if processing was successful
err = queue.Ack(msg.ID)
if err != nil {
// Handle error
}
// Or, if processing failed, negative acknowledge the item
err = queue.Nack(msg.ID)
if err != nil {
// Handle error
}// Acknowledge the item if processing was successful
```

### Unique Queue

```go
import "github.com/mattdeak/godq"

// Create a new unique queue
queue, err := godq.NewUniqueQueue("unique_queue.db")
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

1. **SimpleQueue**: Basic FIFO queue with no additional features. Ideal for simple task queues or message passing where order matters but acknowledgment isn't necessary.
2. **AckQueue**: Queue with acknowledgment support. Items must be acknowledged after processing, or they will be requeued after a timeout. Suitable for ensuring task completion in distributed systems or when processing reliability is crucial.
3. **UniqueQueue**: Queue that only allows unique items. Duplicate enqueue attempts are silently ignored. Useful for de-duplication scenarios or when you need to ensure only one instance of a task is queued.
4. **UniqueAckQueue**: Combination of UniqueQueue and AckQueue. Ensures unique items with acknowledgment support. Ideal for scenarios requiring both de-duplication and reliable processing.


## Advanced Features
### Dead Letter Queue
For AckQueue and UniqueAckQueue, you can set up a dead letter queue to handle messages that exceed the maximum retry count automatically:
```go
mainQueue, := godq.NewAckQueue("main_queue.db", godq.AckOpts{
MaxRetries: 3,
})
deadLetterQueue, := godq.NewSimpleQueue("dead_letter.db") // Can be any Enqueuer (supports Enqueue([]byte))
mainQueue.SetDeadLetterQueue(deadLetterQueue)
```

Your dead letter queues can be any queue type or anything that supports the Enqueuer interface.
For example, you could make a simple logging dead letter queue.

```go
type LogEnqueuer struct {}

func (l *LogEnqueuer) Enqueue(item []byte) error {
    log.Println(string(item))
    return nil
}
```

And elsewhere:
```go

queue.SetDeadLetterQueue(LogEnqueuer {})

```

Since all queues can be used as dead letter queues, you could, if you wanted too, put dead letter queues on your dead letter queues.
See the examples directory for more.


### Configurable Retry Mechanism

AckQueue and UniqueAckQueue support configurable retry mechanisms:

```go
queue, := godq.NewAckQueue("queue.db", godq.AckOpts{
AckTimeout: 1 time.Minute, // Any message that takes longer than 1 minute to ack will be requeued.
MaxRetries: 5 // 0 for no retries, -1 for infinite retries
RetryBackoff: 10 time.Second, // Sets a new ack deadline to be the max of (current deadline, now + retry backoff)
})
```

## Examples

There are several, more detailed examples demonstrating various features of godq. These examples are located in the `examples` directory at the root of the project. To run an example, navigate to its directory and use `go run main.go`. For instance:

Available examples:
- Simple Queue Usage: `examples/simple_queue_example`
- Acknowledged Queue: `examples/ack_queue_example`
- Unique Queue: `examples/unique_queue_example`
- Dead Letter Queue: `examples/dead_letter_queue_example`
- Multiple Tiered Dead Letter Queues: `examples/tiered_dlq_example`
- Concurrent Queue Usage: `examples/concurrent_queue_example`

Each example demonstrates different features and use cases of godq. We encourage you to explore these examples to better understand how to use the library in your projects.

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change. Ensure to update tests as appropriate.


## License
This project is licensed under the MIT License.


### Future Work
- More Queue Configurability (LIFO, Priority Queues, etc.)
- Batch Enqueue/Dequeue operations
- Various Efficiency Improvements