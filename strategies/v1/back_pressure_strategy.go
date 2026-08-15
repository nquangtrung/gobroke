package v1

import (
	"trontria.com/gobroke/strategies"
)

type BackPressureStrategyType int

const (
	DropOldest BackPressureStrategyType = iota
	DropNewest
)

type BackPressureStrategy interface {
	Drop(strategy strategies.Strategy, data any)
}

type DropOldestStrategy struct{}

func (s *DropOldestStrategy) Drop(strategy strategies.Strategy, data any) {
	// drop the oldest data
	// discarded := <-strategy.Consume()
	// log.Printf("discarded %v", discarded)
	// push the new data to the subscriber channel
	// strategy.Consume() <- data
}

type DropNewestStrategy struct{}

func (s *DropNewestStrategy) Drop(strategy strategies.Strategy, data any) {
	// drop the newest data, leaving the subscriber channel full
	// simply discard the data
	// no-op
}

func NewBackPressureStrategy(strategyType BackPressureStrategyType) BackPressureStrategy {
	switch strategyType {
	case DropOldest:
		return &DropOldestStrategy{}
	case DropNewest:
		return &DropNewestStrategy{}
	default:
		return &DropNewestStrategy{}
	}
}
