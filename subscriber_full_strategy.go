package gobroke

import "log"

type SubscriberFullStrategyType int

const (
	DropOldest SubscriberFullStrategyType = iota
	DropNewest
)

type SubscriberFullStrategy interface {
	HandleFull(subscriber Subscriber, data any)
}

type DropOldestStrategy struct{}

func (s *DropOldestStrategy) HandleFull(subscriber Subscriber, data any) {
	// drop the oldest data
	discarded := <-subscriber.Receive()
	log.Printf("[%s] discarded %v", subscriber.Name(), discarded)
	// push the new data to the subscriber channel
	subscriber.Receive() <- data
}

type DropNewestStrategy struct{}

func (s *DropNewestStrategy) HandleFull(subscriber Subscriber, data any) {
	// drop the newest data, leaving the subscriber channel full
	// simply discard the data
	// no-op
}

func newSubscriberFullStrategy(strategyType SubscriberFullStrategyType) SubscriberFullStrategy {
	switch strategyType {
	case DropOldest:
		return &DropOldestStrategy{}
	case DropNewest:
		return &DropNewestStrategy{}
	default:
		return &DropNewestStrategy{}
	}
}
