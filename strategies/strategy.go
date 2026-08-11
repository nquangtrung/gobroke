package strategies

type Strategy interface {
	Receive(data any)
	Consume() chan any
	Stop()
	Execute(handler func(data any), data any)
}
