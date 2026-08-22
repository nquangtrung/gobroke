package gobroke

import "sync"

type Publishers[T any] struct {
	all []Publisher[T]
	mu  sync.Mutex
}

type Publisher[T any] struct {
	topic  string
	broker *Broker[T]
}

func (p *Publisher[T]) Publish(data T) {
	p.broker.Publish(p.topic, data)
}

func (p *Publisher[T]) Topic() *Topic[T] {
	return p.broker.GetTopic(p.topic)
}
