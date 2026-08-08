package gobroke

import (
	"context"
	"sync"
)

type Broker interface {
	Start()
	Stop()

	Subscribe(topic string, handler func(data any)) Subscriber
	NamedSubscribe(topic string, name string, handler func(data any)) Subscriber
	CreatePublisher(topic string) Publisher
	Publish(topic string, data any)

	GetTopic(topic string) Topic

	SetupTopic(topic string) Topic
}

type BrokerImpl struct {
	topics *Topics
	ctx    context.Context
	cancel context.CancelFunc
}

func (b *BrokerImpl) Start() {
	var once sync.Once
	once.Do(func() {
		b.topics = &Topics{
			all: make(map[string]Topic),
		}

		b.ctx, b.cancel = context.WithCancel(context.Background())
		b.topics.mu.Lock()
		defer b.topics.mu.Unlock()

		b.topics.all = make(map[string]Topic)
	})
}

func (b *BrokerImpl) Stop() {
	b.cancel()
}

func (b *BrokerImpl) Publish(topicName string, data any) {
	b.topics.mu.Lock()
	defer b.topics.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}

	topic.Publish(data)
}

func (b *BrokerImpl) GetTopic(topicName string) Topic {
	return b.topics.all[topicName]
}

func (b *BrokerImpl) SetupTopic(topicName string) Topic {
	b.topics.mu.Lock()
	defer b.topics.mu.Unlock()

	topic := b.topics.all[topicName]
	if topic == nil {
		topic = NewTopic(topicName, b)
		b.topics.all[topicName] = topic
	}
	topic.Start(b.ctx)

	return b.topics.all[topicName]
}

func (b *BrokerImpl) Subscribe(topicName string, handler func(data any)) Subscriber {
	b.topics.mu.Lock()
	defer b.topics.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}
	subscriber := topic.Subscribe(handler)

	return subscriber
}

func (b *BrokerImpl) NamedSubscribe(topicName string, name string, handler func(data any)) Subscriber {
	b.topics.mu.Lock()
	defer b.topics.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}
	subscriber := topic.NamedSubscribe(name, handler)

	return subscriber
}

func (b *BrokerImpl) Unsubscribe(subscriber Subscriber) {
	subscriber.Unsubscribe()
}

func (b *BrokerImpl) CreatePublisher(topicName string) Publisher {
	b.topics.mu.Lock()
	defer b.topics.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}
	publisher := topic.CreatePublisher(b)

	return publisher
}
