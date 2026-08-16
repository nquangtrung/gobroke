package gobroke

import (
	"context"
	"log"
	"sync"

	"trontria.com/gobroke/strategies"
)

type SubscribeParams struct {
	Handler  func(data any)
	Strategy strategies.Strategy
}

type Broker struct {
	topics map[string]*Topic
	mu     sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	wg *sync.WaitGroup
}

func (b *Broker) Done() <-chan struct{} {
	return b.ctx.Done()
}

func (b *Broker) Start() {
	var once sync.Once
	once.Do(func() {
		b.topics = make(map[string]*Topic)
		b.ctx, b.cancel = context.WithCancel(context.Background())
	})
}

func (b *Broker) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, topic := range b.topics {
		topic.Stop()
	}

	b.wg.Wait()
}

func (b *Broker) Publish(topicName string, data any) {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}

	topic.Publish(data)
}

func (b *Broker) GetTopic(topicName string) *Topic {
	return b.topics[topicName]
}

func (b *Broker) SetupTopic(params TopicSetupParams) *Topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	topicName := params.Name

	topic := b.topics[topicName]
	if topic == nil {
		topic = newTopic(b, params)
		b.topics[topicName] = topic
	}
	b.wg.Add(1)

	return b.topics[topicName]
}

func (b *Broker) ReleaseTopic(topic *Topic) {
	log.Printf("releasing topic %s", topic.GetName())
	b.wg.Done()
}

func (b *Broker) Subscribe(topicName string, params SubscribeParams) Subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}
	subscriber := topic.Subscribe(params)

	return subscriber
}

func (b *Broker) NamedSubscribe(topicName string, name string, params SubscribeParams) Subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}
	subscriber := topic.NamedSubscribe(name, params)

	return subscriber
}

func (b *Broker) Unsubscribe(subscriber *Subscriber) {
	subscriber.Unsubscribe()
}

func (b *Broker) CreatePublisher(topicName string) *Publisher {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}
	publisher := topic.CreatePublisher(b)

	return publisher
}

func NewBroker() *Broker {
	var wg sync.WaitGroup
	broker := &Broker{
		wg: &wg,
	}
	broker.Start()
	return broker
}
