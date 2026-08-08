package gobroke

import "sync"

type Publisher interface {
	TopicChild
	Publish(data any)
}

type Publishers struct {
	all []Publisher
	mu  sync.Mutex
}

type PublisherImpl struct {
	topic  string
	broker Broker
}

func (p *PublisherImpl) Publish(data any) {
	p.broker.Publish(p.topic, data)
}

func (p *PublisherImpl) Topic() Topic {
	return p.broker.GetTopic(p.topic)
}
