package v1

import (
	"trontria.com/gobroke/strategies"
)

type BackPressureStrategyType int

const (
	DropOldest BackPressureStrategyType = iota
	DropNewest
)

type BackPressureStrategy[T any] interface {
	Drop(strategy strategies.Strategy[T], data T)
}

type DropOldestStrategy[T any] struct{}

func (s *DropOldestStrategy[T]) Drop(strategy strategies.Strategy[T], data T) {
	// drop the oldest data
	// discarded := <-strategy.Consume()
	// log.Printf("discarded %v", discarded)
	// push the new data to the subscriber channel
	// strategy.Consume() <- data
}

type DropNewestStrategy[T any] struct{}

func (s *DropNewestStrategy[T]) Drop(strategy strategies.Strategy[T], data T) {
	// drop the newest data, leaving the subscriber channel full
	// simply discard the data
	// no-op
}

func NewBackPressureStrategy[T any](strategyType BackPressureStrategyType) BackPressureStrategy[T] {
	switch strategyType {
	case DropOldest:
		return &DropOldestStrategy[T]{}
	case DropNewest:
		return &DropNewestStrategy[T]{}
	default:
		return &DropNewestStrategy[T]{}
	}
}
