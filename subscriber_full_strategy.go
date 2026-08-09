package gobroke

import "log"

type SubscriberFullStrategyType int

const (
	DropOldest SubscriberFullStrategyType = iota
	DropNewest
)

type FullStrategy interface {
	HandleFull(strategy Strategy, data any)
}

type DropOldestStrategy struct{}

func (s *DropOldestStrategy) HandleFull(strategy Strategy, data any) {
	// drop the oldest data
	discarded := <-strategy.Consume()
	log.Printf("discarded %v", discarded)
	// push the new data to the subscriber channel
	strategy.Consume() <- data
}

type DropNewestStrategy struct{}

func (s *DropNewestStrategy) HandleFull(strategy Strategy, data any) {
	// drop the newest data, leaving the subscriber channel full
	// simply discard the data
	// no-op
}

func newFullStrategy(strategyType SubscriberFullStrategyType) FullStrategy {
	switch strategyType {
	case DropOldest:
		return &DropOldestStrategy{}
	case DropNewest:
		return &DropNewestStrategy{}
	default:
		return &DropNewestStrategy{}
	}
}
