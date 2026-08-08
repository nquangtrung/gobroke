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

	SetupTopic(params TopicSetupParams) Topic
}

type BrokerImpl struct {
	topics map[string]Topic
	mu     sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
}

func (b *BrokerImpl) Start() {
	var once sync.Once
	once.Do(func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		b.topics = make(map[string]Topic)
		b.ctx, b.cancel = context.WithCancel(context.Background())
	})
}

func (b *BrokerImpl) Stop() {
	b.cancel()
}

func (b *BrokerImpl) Publish(topicName string, data any) {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}

	topic.Publish(data)
}

func (b *BrokerImpl) GetTopic(topicName string) Topic {
	return b.topics[topicName]
}

func (b *BrokerImpl) SetupTopic(params TopicSetupParams) Topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	topicName := params.Name

	topic := b.topics[topicName]
	if topic == nil {
		topic = newTopic(b, params)
		b.topics[topicName] = topic
	}
	topic.Start(b.ctx)

	return b.topics[topicName]
}

func (b *BrokerImpl) Subscribe(topicName string, handler func(data any)) Subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}
	subscriber := topic.Subscribe(handler)

	return subscriber
}

func (b *BrokerImpl) NamedSubscribe(topicName string, name string, handler func(data any)) Subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()

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
	b.mu.Lock()
	defer b.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}
	publisher := topic.CreatePublisher(b)

	return publisher
}
