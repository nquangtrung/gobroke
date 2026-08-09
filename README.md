# GoBroke

A lightweight pub/sub message broker library written in Go.

## Overview

GoBroke is a simple yet powerful message broker that enables publish-subscribe communication patterns in Go applications. It provides a clean API for creating publishers, subscribers, and managing topics.

## Features

- **Simple API** - Easy-to-use publish-subscribe interface
- **Topic Management** - Automatic topic creation and management
- **Flexible Subscribers** - Subscribe to topics with custom handler functions
- **Context Support** - Built-in context handling for graceful shutdown
- **Thread-Safe** - Safe concurrent access to the broker
- **Drop Strategies** - Control message handling when buffers fill (DropOldest, DropNewest)
- **Worker Strategies** - Process messages with simple or multiple concurrent workers
- **Customizable Buffering** - Configure buffer sizes and behavior per subscription

## Installation

```bash
go get trontria.com/gobroke
```

## Quick Start

### Creating a Broker

```go
package main

import (
	"trontria.com/gobroke"
)

func main() {
	// Create a new broker
	broker := gobroke.NewBroker()
	defer broker.Stop()

	// Subscribe to a topic
	subscriber := broker.Subscribe("events", func(data any) {
		println("Received:", data)
	})

	// Create a publisher for a topic
	publisher := broker.CreatePublisher("events")

	// Publish a message
	broker.Publish("events", "Hello, World!")
}
```

## Usage

### Subscribing

Subscribe to a topic and handle incoming messages:

```go
subscriber := broker.Subscribe("my-topic", func(data any) {
	// Handle the message
	fmt.Println("Message received:", data)
})

// Unsubscribe when needed
subscriber.Unsubscribe()
```

### Named Subscriptions

Create multiple named subscribers to the same topic:

```go
broker.NamedSubscribe("fruits", "subscriber1", gobroke.SubscribeParams{
	Handler: func(data any) {
		fmt.Println("Subscriber1 received:", data)
	},
})

broker.NamedSubscribe("fruits", "subscriber2", gobroke.SubscribeParams{
	Handler: func(data any) {
		fmt.Println("Subscriber2 received:", data)
	},
})
```

### Subscriber Strategies

GoBroke provides flexible strategies to handle subscriber behavior:

#### Drop Strategies

Control what happens when a subscriber's buffer is full:

**Drop Newest (default)** - New incoming messages are discarded:

```go
broker.NamedSubscribe("events", "handler1", gobroke.SubscribeParams{
	Handler: func(data any) {
		fmt.Println("Received:", data)
	},
	Strategy: gobroke.NewStrategy(gobroke.SingleBuffered, 4).
		WithDrop(gobroke.DropNewest),
})
```

**Drop Oldest** - Oldest buffered messages are discarded to make room for new ones:

```go
broker.NamedSubscribe("events", "handler2", gobroke.SubscribeParams{
	Handler: func(data any) {
		fmt.Println("Received:", data)
	},
	Strategy: gobroke.NewStrategy(gobroke.SingleBuffered, 4).
		WithDrop(gobroke.DropOldest),
})
```

#### Worker Strategies

Control how messages are processed:

**Simple Worker (default)** - Messages are processed sequentially:

```go
broker.NamedSubscribe("tasks", "worker1", gobroke.SubscribeParams{
	Handler: func(data any) {
		fmt.Println("Processing:", data)
	},
	Strategy: gobroke.NewStrategy(gobroke.SingleBuffered, 10).
		WithSimpleWorker(),
})
```

**Multiple Workers** - Process multiple messages concurrently with a limit:

```go
broker.NamedSubscribe("tasks", "worker2", gobroke.SubscribeParams{
	Handler: func(data any) {
		time.Sleep(time.Second) // Simulate work
		fmt.Println("Processed:", data)
	},
	Strategy: gobroke.NewStrategy(gobroke.SingleBuffered, 10).
		WithMutipleWorker(3), // Max 3 concurrent workers
})
```

#### Combining Strategies

You can combine drop and worker strategies:

```go
broker.NamedSubscribe("events", "complex-handler", gobroke.SubscribeParams{
	Handler: func(data any) {
		time.Sleep(time.Millisecond * 500)
		fmt.Println("Processed:", data)
	},
	Strategy: gobroke.NewStrategy(gobroke.SingleBuffered, 5).
		WithDrop(gobroke.DropOldest).
		WithMutipleWorker(2),
})
```

### Publishing

Create a publisher for a topic or publish directly:

```go
// Method 1: Using a Publisher
publisher := broker.CreatePublisher("my-topic")
publisher.Publish("message data")

// Method 2: Direct publish to broker
broker.Publish("my-topic", "message data")
```

### Managing Topics

Set up topics with custom parameters:

```go
broker.SetupTopic(gobroke.TopicSetupParams{
	Name: "my-topic",
})

topic := broker.GetTopic("my-topic")
// Use topic information as needed
```

## API

### Broker Interface

- `Start()` - Start the broker
- `Stop()` - Stop the broker
- `Subscribe(topic string, handler func(data any)) Subscriber` - Subscribe to a topic
- `NamedSubscribe(topic, name string, params SubscribeParams) Subscriber` - Create a named subscription with advanced options
- `CreatePublisher(topic string) Publisher` - Create a publisher for a topic
- `Publish(topic string, data any)` - Publish data to a topic
- `SetupTopic(params TopicSetupParams)` - Configure a topic
- `GetTopic(topic string) Topic` - Get a topic

### Strategy Interface

- `Receive(data any)` - Receive a message
- `Consume() chan any` - Get the message channel
- `Stop()` - Stop the strategy
- `WithDrop(strategyType DropStrategyType) Strategy` - Set the drop strategy (DropOldest, DropNewest)
- `WithSimpleWorker() Strategy` - Use simple sequential worker
- `WithMutipleWorker(maxWorker int) Strategy` - Use multiple concurrent workers

### Drop Strategies

- `DropNewest` - Discard new messages when buffer is full (default)
- `DropOldest` - Discard oldest messages to make room for new ones

### Worker Strategies

- `Simple` - Process messages sequentially
- `MultipleWorker` - Process multiple messages concurrently

### Subscriber Interface

- `Unsubscribe()` - Unsubscribe from the topic

### Publisher Interface

- `Publish(data any)` - Publish data

## Examples

### Complete Publisher-Subscriber Example

```go
package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"trontria.com/gobroke"
)

func main() {
	// Create and defer stop
	broker := gobroke.NewBroker()
	defer broker.Stop()

	topic := "fruits"

	// Setup topic
	broker.SetupTopic(gobroke.TopicSetupParams{
		Name: topic,
	})

	// Create first subscriber
	received1 := []string{}
	broker.NamedSubscribe(topic, "s1", gobroke.SubscribeParams{
		Handler: func(data any) {
			received1 = append(received1, data.(string))
			log.Printf("[s1] received: %v", data)
		},
	})

	// Create subscriber that unsubscribes after 2 messages
	received2 := []string{}
	var subscriber2 gobroke.Subscriber
	subscriber2 = broker.NamedSubscribe(topic, "s2", gobroke.SubscribeParams{
		Handler: func(data any) {
			received2 = append(received2, data.(string))
			log.Printf("[s2] received: %v", data)
			if len(received2) == 2 {
				subscriber2.Unsubscribe()
			}
		},
	})

	// Create publisher and publish messages
	publisher := broker.CreatePublisher(topic)
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				message := fmt.Sprintf("message-%d-%d", idx, j)
				log.Printf("Publishing: %s", message)
				publisher.Publish(message)
				time.Sleep(100 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	log.Printf("s1 received: %v", received1)
	log.Printf("s2 received: %v", received2)
}
```

### Handling Slow Subscribers

When a subscriber's handler is slow, use buffering strategies:

```go
// Drop oldest messages when buffer fills up
broker.NamedSubscribe("events", "busy-handler", gobroke.SubscribeParams{
	Handler: func(data any) {
		time.Sleep(time.Second) // Simulate slow processing
		fmt.Println("Processed:", data)
	},
	Strategy: gobroke.NewStrategy(gobroke.SingleBuffered, 4).
		WithDrop(gobroke.DropOldest),
})
```

### Concurrent Message Processing

Process messages concurrently with multiple workers:

```go
broker.NamedSubscribe("batch-jobs", "parallel-worker", gobroke.SubscribeParams{
	Handler: func(data any) {
		// Multiple instances of this handler run concurrently
		log.Printf("Starting job: %v", data)
		time.Sleep(time.Second * 2) // Simulate work
		log.Printf("Completed job: %v", data)
	},
	Strategy: gobroke.NewStrategy(gobroke.SingleBuffered, 20).
		WithMutipleWorker(5), // Process up to 5 messages concurrently
})
```

### Advanced Strategy Example

Combine multiple strategies for fine-grained control:

```go
broker.NamedSubscribe("priority-queue", "advanced", gobroke.SubscribeParams{
	Handler: func(data any) {
		log.Printf("Processing: %v", data)
		time.Sleep(time.Millisecond * 500)
	},
	Strategy: gobroke.NewStrategy(gobroke.SingleBuffered, 15).
		WithDrop(gobroke.DropOldest).      // Keep newest messages
		WithMutipleWorker(3),               // Max 3 concurrent workers
})
```

## License

MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Feel free to open issues or pull requests.
