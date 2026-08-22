package v1

import "log"

type WorkerStrategy[T any] interface {
	Execute(handler func(data T), data T)
}

type WorkerStrategyType int

const (
	Simple WorkerStrategyType = iota
	MultipleWorker
)

type SimpleWorkerStrategy[T any] struct{}

func (s SimpleWorkerStrategy[T]) Execute(handler func(data T), data T) {
	handler(data)
}

type MultipleWorkerStrategy[T any] struct {
	maxRoutine int
	guard      chan any
}

func (s MultipleWorkerStrategy[T]) Execute(handler func(data T), data T) {
	if s.guard == nil {
		s.guard = make(chan any, s.maxRoutine)
	}

	log.Printf("waiting for guard to release, for data: %s / len: %d", data, len(s.guard))
	s.guard <- data

	go func() {
		log.Printf("starting up routine, for data: %s / len: %d", data, len(s.guard))
		handler(data)
		<-s.guard
	}()
}

type WorkerStrategyBuilder[T any] interface {
	Strategy(t WorkerStrategyType) WorkerStrategyBuilder[T]
	MaxRoutine(l int) WorkerStrategyBuilder[T]
	Build() WorkerStrategy[T]
}

type WorkerStrategyBuilderImpl[T any] struct {
	strategy   WorkerStrategyType
	maxRoutine int
}

func (w WorkerStrategyBuilderImpl[T]) Strategy(t WorkerStrategyType) WorkerStrategyBuilder[T] {
	w.strategy = t
	return w
}

func (w WorkerStrategyBuilderImpl[T]) MaxRoutine(l int) WorkerStrategyBuilder[T] {
	w.maxRoutine = l
	return w
}

func (w WorkerStrategyBuilderImpl[T]) Build() WorkerStrategy[T] {
	switch w.strategy {
	case Simple:
		return &SimpleWorkerStrategy[T]{}
	case MultipleWorker:
		return &MultipleWorkerStrategy[T]{w.maxRoutine, make(chan any, w.maxRoutine)}
	default:
		return &SimpleWorkerStrategy[T]{}
	}
}

func NewSimpleWorker[T any]() WorkerStrategy[T] {
	builder := WorkerStrategyBuilderImpl[T]{}.
		Strategy(Simple)
	return builder.Build()
}

func NewMultipleWorker[T any](maxWorker int) WorkerStrategy[T] {
	builder := WorkerStrategyBuilderImpl[T]{}.
		Strategy(MultipleWorker).
		MaxRoutine(maxWorker)
	return builder.Build()
}
