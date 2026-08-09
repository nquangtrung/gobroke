package gobroke

import "log"

type SubscriberFullStrategyType int

const (
	DropOldest SubscriberFullStrategyType = iota
	DropNewest
)

type SubscriberStrategyType int

const (
	SingleBuffered SubscriberStrategyType = iota
	// DoubleBuffered
)

type SubscribeParams struct {
	Handler  func(data any)
	Strategy SubscriberStrategy
}

type SubscriberStrategy interface {
	Receive(data any)
	Consume() chan any
	WithFullStrategy(strategyType SubscriberFullStrategyType) SubscriberStrategy
	Stop()
}

type SingleBufferedConsumerStrategy struct {
	channel      chan any
	fullStrategy SubscriberFullStrategy
}

func (s SingleBufferedConsumerStrategy) Receive(data any) {
	select {
	case s.channel <- data:
		return
	default:
		s.fullStrategy.HandleFull(s, data)
	}
}

func (s SingleBufferedConsumerStrategy) Consume() chan any {
	return s.channel
}

func (s SingleBufferedConsumerStrategy) WithFullStrategy(strategyType SubscriberFullStrategyType) SubscriberStrategy {
	s.fullStrategy = newSubscriberFullStrategy(strategyType)
	return s
}

func (s SingleBufferedConsumerStrategy) Stop() {
	close(s.channel)
	s.channel = nil
}

type SubscriberFullStrategy interface {
	HandleFull(strategy SubscriberStrategy, data any)
}

type DropOldestStrategy struct{}

func (s *DropOldestStrategy) HandleFull(strategy SubscriberStrategy, data any) {
	// drop the oldest data
	discarded := <-strategy.Consume()
	log.Printf("discarded %v", discarded)
	// push the new data to the subscriber channel
	strategy.Consume() <- data
}

type DropNewestStrategy struct{}

func (s *DropNewestStrategy) HandleFull(strategy SubscriberStrategy, data any) {
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

func NewSubscriberStrategy(strategyType SubscriberStrategyType, bufferSize ...int) SubscriberStrategy {
	resolvedBufferSize := 10
	if len(bufferSize) > 0 {
		resolvedBufferSize = bufferSize[0]
	}
	channel := make(chan any, resolvedBufferSize)
	switch strategyType {
	case SingleBuffered:
		return SingleBufferedConsumerStrategy{
			channel: channel,
		}.WithFullStrategy(DropNewest)
	default:
		return SingleBufferedConsumerStrategy{
			channel: channel,
		}.WithFullStrategy(DropNewest)
	}
}
