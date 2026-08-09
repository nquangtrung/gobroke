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

	GetFullStrategy() DropStrategy
	GetWorkerStrategy() WorkerStrategy

	WithDrop(strategyType SubscriberFullStrategyType) Strategy

	WithSimpleWorker() Strategy
	WithMutipleWorker(maxWorker int) Strategy
}

type SingleBufferedConsumerStrategy struct {
	channel chan any
	full    DropStrategy
	worker  WorkerStrategy
}

func (s SingleBufferedConsumerStrategy) Receive(data any) {
	select {
	case s.channel <- data:
		return
	default:
		s.full.Drop(s, data)
	}
}

func (s SingleBufferedConsumerStrategy) Consume() chan any {
	return s.channel
}

func (s SingleBufferedConsumerStrategy) GetWorkerStrategy() WorkerStrategy {
	return s.worker
}

func (s SingleBufferedConsumerStrategy) GetFullStrategy() DropStrategy {
	return s.full
}

func (s SingleBufferedConsumerStrategy) WithDrop(strategyType SubscriberFullStrategyType) Strategy {
	s.full = newFullStrategy(strategyType)
	return s
}

func (s SingleBufferedConsumerStrategy) WithSimpleWorker() Strategy {
	builder := WorkerStrategyBuilderImpl{}.Strategy(Simple)
	s.worker = builder.Build()
	return s
}

func (s SingleBufferedConsumerStrategy) WithMutipleWorker(maxWorker int) Strategy {
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
		}.WithDrop(DropNewest).WithSimpleWorker()
	default:
		return SingleBufferedConsumerStrategy{
			channel: channel,
		}.WithDrop(DropNewest).WithSimpleWorker()
	}
}
