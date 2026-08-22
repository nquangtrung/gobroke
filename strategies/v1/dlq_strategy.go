package v1

import "trontria.com/gobroke/strategies"

type DeadLetterQueueStrategy[T any] interface {
	Deliver(strategy strategies.Strategy[T], data T) bool
}

type ExternalChannelDeadLetterQueueStrategy[T any] struct {
	channel chan T
}

func (e *ExternalChannelDeadLetterQueueStrategy[T]) Deliver(strategy strategies.Strategy[T], data T) bool {
	select {
	case e.channel <- data:
		return true
	default:
		return false
	}
}

type NoDeadLetterQueueStrategy[T any] struct {
}

func (e *NoDeadLetterQueueStrategy[T]) Deliver(strategy strategies.Strategy[T], data T) bool {
	return false
}

func NewExternalChannelDeadLetterQueueStrategy[T any](channel chan T) DeadLetterQueueStrategy[T] {
	return &ExternalChannelDeadLetterQueueStrategy[T]{
		channel: channel,
	}
}

func NewNoDeadLetterQueueStrategy[T any]() DeadLetterQueueStrategy[T] {
	return &NoDeadLetterQueueStrategy[T]{}
}
