package strategies

type Strategy[T any] interface {
	Receive(data T)
	Stop()
	Drop(data T)
}
