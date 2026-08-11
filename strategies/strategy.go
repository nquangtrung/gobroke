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
}

type StrategyUnion interface {
	Strategy
	GetBackPressureStrategy() BackPressureStrategy
	GetDeadLetterQueueStrategy() DeadLetterQueueStrategy
	GetWorkerStrategy() WorkerStrategy

	WithDeadLetterQueue(strategy DeadLetterQueueStrategy) StrategyUnion
	WithBackPressure(strategy BackPressureStrategy) StrategyUnion
	WithWorker(strategy WorkerStrategy) StrategyUnion
}

type SingleBufferedConsumerStrategy struct {
	channel      chan any
	backpressure BackPressureStrategy
	worker       WorkerStrategy
	dlq          DeadLetterQueueStrategy
}

func (s SingleBufferedConsumerStrategy) Receive(data any) {
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

func (s SingleBufferedConsumerStrategy) Consume() chan any {
	return s.channel
}

func (s SingleBufferedConsumerStrategy) GetWorkerStrategy() WorkerStrategy {
	return s.worker
}

func (s SingleBufferedConsumerStrategy) GetBackPressureStrategy() BackPressureStrategy {
	return s.backpressure
}
func (s SingleBufferedConsumerStrategy) WithBackPressure(strategy BackPressureStrategy) StrategyUnion {
	s.backpressure = strategy
	return s
}

func (s SingleBufferedConsumerStrategy) WithWorker(strategy WorkerStrategy) StrategyUnion {
	s.worker = strategy
	return s
}

func (s SingleBufferedConsumerStrategy) GetDeadLetterQueueStrategy() DeadLetterQueueStrategy {
	return s.dlq
}
func (s SingleBufferedConsumerStrategy) WithDeadLetterQueue(strategy DeadLetterQueueStrategy) StrategyUnion {
	s.dlq = strategy
	return s
}

func (s SingleBufferedConsumerStrategy) Stop() {
	close(s.channel)
	s.channel = nil
}

func NewStrategy(strategyType SubscriberStrategyType, bufferSize ...int) StrategyUnion {
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
			WithDeadLetterQueue(NewNoDeadLetterQueueStrategy()).
			WithWorker(NewSimpleWorker())
	default:
		return SingleBufferedConsumerStrategy{
			channel: channel,
		}.
			WithBackPressure(NewBackPressureStrategy(DropNewest)).
			WithDeadLetterQueue(NewNoDeadLetterQueueStrategy()).
			WithWorker(NewSimpleWorker())
	}
}
