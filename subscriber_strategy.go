package gobroke

type SubscriberFullStrategyType int

const (
	DropOldest SubscriberFullStrategyType = iota
	DropNewest
)

type SubscriberStrategyType int

const (
	Direct SubscriberStrategyType = iota
	Buffered
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

type DirectConsumerStrategy struct {
	channel      chan any
	fullStrategy SubscriberFullStrategy
}

func (s DirectConsumerStrategy) Receive(data any) {
	select {
	case s.channel <- data:
		return
	default:
		s.fullStrategy.HandleFull(data)
	}
}

func (s DirectConsumerStrategy) Consume() chan any {
	return s.channel
}

func (s DirectConsumerStrategy) WithFullStrategy(strategyType SubscriberFullStrategyType) SubscriberStrategy {
	s.fullStrategy = newSubscriberFullStrategy(strategyType)
	return s
}

func (s DirectConsumerStrategy) Stop() {
	close(s.channel)
	s.channel = nil
}

type SubscriberFullStrategy interface {
	HandleFull(data any)
}

// type DropOldestStrategy struct{}

// func (s *DropOldestStrategy) HandleFull(data any) {
// 	// drop the oldest data
// 	discarded := <-subscriber.Receive()
// 	log.Printf("[%s] discarded %v", subscriber.Name(), discarded)
// 	// push the new data to the subscriber channel
// 	subscriber.Receive() <- data
// }

type DropNewestStrategy struct{}

func (s *DropNewestStrategy) HandleFull(data any) {
	// drop the newest data, leaving the subscriber channel full
	// simply discard the data
	// no-op
}

func newSubscriberFullStrategy(strategyType SubscriberFullStrategyType) SubscriberFullStrategy {
	switch strategyType {
	case DropOldest:
		return &DropNewestStrategy{}
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
	case Direct:
		return DirectConsumerStrategy{
			channel: channel,
		}.WithFullStrategy(DropNewest)
	default:
		return DirectConsumerStrategy{
			channel: channel,
		}.WithFullStrategy(DropNewest)
	}
}
