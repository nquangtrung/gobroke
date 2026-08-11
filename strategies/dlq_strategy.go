package strategies

type DeadLetterQueueStrategy interface {
	Deliver(strategy Strategy, data any) bool
}

type ExternalChannelDeadLetterQueueStrategy struct {
	channel chan any
}

func (e *ExternalChannelDeadLetterQueueStrategy) Deliver(strategy Strategy, data any) bool {
	select {
	case e.channel <- data:
		return true
	default:
		return false
	}
}

type NoDeadLetterQueueStrategy struct {
}

func (e *NoDeadLetterQueueStrategy) Deliver(strategy Strategy, data any) bool {
	return false
}

func NewExternalChannelDeadLetterQueueStrategy(channel chan any) DeadLetterQueueStrategy {
	return &ExternalChannelDeadLetterQueueStrategy{
		channel: channel,
	}
}

func NewNoDeadLetterQueueStrategy() DeadLetterQueueStrategy {
	return &NoDeadLetterQueueStrategy{}
}
