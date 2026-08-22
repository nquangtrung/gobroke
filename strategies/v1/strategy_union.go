package v1

import "trontria.com/gobroke/strategies"

type SubscriberStrategyType int

const (
	SingleBuffered SubscriberStrategyType = iota
)

type StrategyUnion[T any] interface {
	strategies.Strategy[T]
	GetBackPressureStrategy() BackPressureStrategy[T]
	GetDeadLetterQueueStrategy() DeadLetterQueueStrategy[T]
	GetWorkerStrategy() WorkerStrategy[T]

	WithDeadLetterQueue(strategy DeadLetterQueueStrategy[T]) StrategyUnion[T]
	WithBackPressure(strategy BackPressureStrategy[T]) StrategyUnion[T]
	WithWorker(strategy WorkerStrategy[T]) StrategyUnion[T]
}

type SingleBufferedConsumerStrategy[T any] struct {
	channel      chan T
	backpressure BackPressureStrategy[T]
	worker       WorkerStrategy[T]
	dlq          DeadLetterQueueStrategy[T]
}

func (s SingleBufferedConsumerStrategy[T]) Receive(data T) {
	select {
	case s.channel <- data:
		return
	default:
	}
	if s.dlq.Deliver(s, data) {
		return
	}
	s.backpressure.Drop(s, data)
}

func (s SingleBufferedConsumerStrategy[T]) Consume() chan T {
	return s.channel
}

func (s SingleBufferedConsumerStrategy[T]) Drop(data T) {
}

func (s SingleBufferedConsumerStrategy[T]) Execute(handler func(data T), data T) {
	s.GetWorkerStrategy().Execute(handler, data)
}

func (s SingleBufferedConsumerStrategy[T]) GetWorkerStrategy() WorkerStrategy[T] {
	return s.worker
}

func (s SingleBufferedConsumerStrategy[T]) GetBackPressureStrategy() BackPressureStrategy[T] {
	return s.backpressure
}
func (s SingleBufferedConsumerStrategy[T]) WithBackPressure(strategy BackPressureStrategy[T]) StrategyUnion[T] {
	s.backpressure = strategy
	return s
}

func (s SingleBufferedConsumerStrategy[T]) WithWorker(strategy WorkerStrategy[T]) StrategyUnion[T] {
	s.worker = strategy
	return s
}

func (s SingleBufferedConsumerStrategy[T]) GetDeadLetterQueueStrategy() DeadLetterQueueStrategy[T] {
	return s.dlq
}
func (s SingleBufferedConsumerStrategy[T]) WithDeadLetterQueue(strategy DeadLetterQueueStrategy[T]) StrategyUnion[T] {
	s.dlq = strategy
	return s
}

func (s SingleBufferedConsumerStrategy[T]) Stop() {
	close(s.channel)
	s.channel = nil
}

func NewStrategyUnion[T any](strategyType SubscriberStrategyType, bufferSize ...int) StrategyUnion[T] {
	resolvedBufferSize := 10
	if len(bufferSize) > 0 {
		resolvedBufferSize = bufferSize[0]
	}
	channel := make(chan T, resolvedBufferSize)

	switch strategyType {
	case SingleBuffered:
		return SingleBufferedConsumerStrategy[T]{
			channel: channel,
		}.
			WithBackPressure(NewBackPressureStrategy[T](DropNewest)).
			WithDeadLetterQueue(NewNoDeadLetterQueueStrategy[T]()).
			WithWorker(NewSimpleWorker[T]())
	default:
		return SingleBufferedConsumerStrategy[T]{
			channel: channel,
		}.
			WithBackPressure(NewBackPressureStrategy[T](DropNewest)).
			WithDeadLetterQueue(NewNoDeadLetterQueueStrategy[T]()).
			WithWorker(NewSimpleWorker[T]())
	}
}
