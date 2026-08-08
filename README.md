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

Get information about topics:

```go
topic := broker.GetTopic("my-topic")
// Use topic information as needed
```

## API

### Broker Interface

- `Start()` - Start the broker
- `Stop()` - Stop the broker
- `Subscribe(topic string, handler func(data any)) Subscriber` - Subscribe to a topic
- `CreatePublisher(topic string) Publisher` - Create a publisher for a topic
- `Publish(topic string, data any)` - Publish data to a topic
- `GetTopic(topic string) Topic` - Get a topic

### Subscriber Interface

- `Unsubscribe()` - Unsubscribe from the topic

### Publisher Interface

- `Publish(data any)` - Publish data

## License

MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Feel free to open issues or pull requests.
