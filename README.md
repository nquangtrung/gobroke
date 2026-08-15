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

Coming soon

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

### Publishing

Create a publisher for a  topic or publish directly:

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

## License

MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Feel free to open issues or pull requests.
