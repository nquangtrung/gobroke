package gobroke

type SubscriberStrategyType int

const (
	SingleBuffered SubscriberStrategyType = iota
	// DoubleBuffered
)

type SubscribeParams struct {
	Handler  func(data any)
	Strategy Strategy
}

type Strategy interface {
	Receive(data any)
	Consume() chan any
	Stop()

	GetFullStrategy() FullStrategy
	GetWorkerStrategy() WorkerStrategy

	WithFullStrategy(strategyType SubscriberFullStrategyType) Strategy

	WithSimpleWorkerStrategy() Strategy
	WithMutipleWorkerStrategy(maxWorker int) Strategy
}

type SingleBufferedConsumerStrategy struct {
	channel chan any
	full    FullStrategy
	worker  WorkerStrategy
}

func (s SingleBufferedConsumerStrategy) Receive(data any) {
	select {
	case s.channel <- data:
		return
	default:
		s.full.HandleFull(s, data)
	}
}

func (s SingleBufferedConsumerStrategy) Consume() chan any {
	return s.channel
}

func (s SingleBufferedConsumerStrategy) GetWorkerStrategy() WorkerStrategy {
	return s.worker
}

func (s SingleBufferedConsumerStrategy) GetFullStrategy() FullStrategy {
	return s.full
}

func (s SingleBufferedConsumerStrategy) WithFullStrategy(strategyType SubscriberFullStrategyType) Strategy {
	s.full = newFullStrategy(strategyType)
	return s
}

func (s SingleBufferedConsumerStrategy) WithSimpleWorkerStrategy() Strategy {
	builder := WorkerStrategyBuilderImpl{}.Strategy(Simple)
	s.worker = builder.Build()
	return s
}

func (s SingleBufferedConsumerStrategy) WithMutipleWorkerStrategy(maxWorker int) Strategy {
	builder := WorkerStrategyBuilderImpl{}.Strategy(MultipleWorker).MaxRoutine(maxWorker)
	s.worker = builder.Build()
	return s
}

func (s SingleBufferedConsumerStrategy) Stop() {
	close(s.channel)
	s.channel = nil
}

func NewStrategy(strategyType SubscriberStrategyType, bufferSize ...int) Strategy {
	resolvedBufferSize := 10
	if len(bufferSize) > 0 {
		resolvedBufferSize = bufferSize[0]
	}
	channel := make(chan any, resolvedBufferSize)
	switch strategyType {
	case SingleBuffered:
		return SingleBufferedConsumerStrategy{
			channel: channel,
		}.WithFullStrategy(DropNewest).WithSimpleWorkerStrategy()
	default:
		return SingleBufferedConsumerStrategy{
			channel: channel,
		}.WithFullStrategy(DropNewest).WithSimpleWorkerStrategy()
	}
}
