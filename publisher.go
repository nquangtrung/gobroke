package gobroke

import "sync"

type Publishers struct {
	all []Publisher
	mu  sync.Mutex
}

type Publisher struct {
	topic  string
	broker *Broker
}

func (p *Publisher) Publish(data any) {
	p.broker.Publish(p.topic, data)
}

func (p *Publisher) Topic() *Topic {
	return p.broker.GetTopic(p.topic)
}
