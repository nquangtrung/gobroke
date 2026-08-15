package strategies

type Strategy interface {
	Receive(data any)
	Stop()
}
