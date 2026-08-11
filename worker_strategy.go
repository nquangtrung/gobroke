package gobroke

import "log"

type WorkerStrategy interface {
	Execute(handler func(data any), data any)
}

type WorkerStrategyType int

const (
	Simple WorkerStrategyType = iota
	MultipleWorker
)

type SimpleWorkerStrategy struct{}

func (s SimpleWorkerStrategy) Execute(handler func(data any), data any) {
	handler(data)
}

type MultipleWorkerStrategy struct {
	maxRoutine int
	guard      chan any
}

func (s MultipleWorkerStrategy) Execute(handler func(data any), data any) {
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

type WorkerStrategyBuilder interface {
	Strategy(t WorkerStrategyType) WorkerStrategyBuilder
	MaxRoutine(l int) WorkerStrategyBuilder
	Build() WorkerStrategy
}

type WorkerStrategyBuilderImpl struct {
	strategy   WorkerStrategyType
	maxRoutine int
}

func (w WorkerStrategyBuilderImpl) Strategy(t WorkerStrategyType) WorkerStrategyBuilder {
	w.strategy = t
	return w
}

func (w WorkerStrategyBuilderImpl) MaxRoutine(l int) WorkerStrategyBuilder {
	w.maxRoutine = l
	return w
}

func (w WorkerStrategyBuilderImpl) Build() WorkerStrategy {
	switch w.strategy {
	case Simple:
		return &SimpleWorkerStrategy{}
	case MultipleWorker:
		return &MultipleWorkerStrategy{w.maxRoutine, make(chan any, w.maxRoutine)}
	default:
		return &SimpleWorkerStrategy{}
	}
}

func NewSimpleWorker() WorkerStrategy {
	builder := WorkerStrategyBuilderImpl{}.
		Strategy(Simple)
	return builder.Build()
}

func NewMultipleWorker(maxWorker int) WorkerStrategy {
	builder := WorkerStrategyBuilderImpl{}.
		Strategy(MultipleWorker).
		MaxRoutine(maxWorker)
	return builder.Build()
}
