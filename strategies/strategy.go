package strategies

type SubscriberStrategyType int

const (
	SingleBuffered SubscriberStrategyType = iota
	// DoubleBuffered
)


type Strategy interface {
	Receive(data any)
	Consume() chan any
	Stop()

	GetBackPressureStrategy() BackPressureStrategy
	GetWorkerStrategy() WorkerStrategy

	WithBackPressure(strategy BackPressureStrategy) Strategy
	WithWorker(strategy WorkerStrategy) Strategy
}

type SingleBufferedConsumerStrategy struct {
	channel      chan any
	backpressure BackPressureStrategy
	worker       WorkerStrategy
}

func (s SingleBufferedConsumerStrategy) Receive(data any) {
	select {
	case s.channel <- data:
		return
	default:
		s.backpressure.Drop(s, data)
	}
}

func (s SingleBufferedConsumerStrategy) Consume() chan any {
	return s.channel
}

func (s SingleBufferedConsumerStrategy) GetWorkerStrategy() WorkerStrategy {
	return s.worker
}

func (s SingleBufferedConsumerStrategy) GetBackPressureStrategy() BackPressureStrategy {
	return s.backpressure
}

func (s SingleBufferedConsumerStrategy) WithBackPressure(strategy BackPressureStrategy) Strategy {
	s.backpressure = strategy
	return s
}

func (s SingleBufferedConsumerStrategy) WithWorker(strategy WorkerStrategy) Strategy {
	s.worker = strategy
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
		}.
			WithBackPressure(NewBackPressureStrategy(DropNewest)).
			WithWorker(NewSimpleWorker())
	default:
		return SingleBufferedConsumerStrategy{
			channel: channel,
		}.
			WithBackPressure(NewBackPressureStrategy(DropNewest)).
			WithWorker(NewSimpleWorker())
	}
}
