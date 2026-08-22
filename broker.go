package gobroke

import (
	"context"
	"log"
	"sync"

	"trontria.com/gobroke/strategies"
)

type SubscribeParams[T any] struct {
	Handler  func(data T)
	Strategy strategies.Strategy[T]
}

type Broker[T any] struct {
	topics map[string]*Topic[T]
	mu     sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	wg *sync.WaitGroup
}

func (b *Broker[T]) Done() <-chan struct{} {
	return b.ctx.Done()
}

func (b *Broker[T]) Start() {
	var once sync.Once
	once.Do(func() {
		b.topics = make(map[string]*Topic[T])
		b.ctx, b.cancel = context.WithCancel(context.Background())
	})
}

func (b *Broker[T]) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, topic := range b.topics {
		topic.Stop()
	}

	b.wg.Wait()
}

func (b *Broker[T]) Publish(topicName string, data T) {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}

	topic.Publish(data)
}

func (b *Broker[T]) GetTopic(topicName string) *Topic[T] {
	return b.topics[topicName]
}

func (b *Broker[T]) SetupTopic(params TopicSetupParams) *Topic[T] {
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

func (b *Broker[T]) ReleaseTopic(topic *Topic[T]) {
	log.Printf("releasing topic %s", topic.GetName())
	b.wg.Done()
}

func (b *Broker[T]) Subscribe(topicName string, params SubscribeParams[T]) Subscriber[T] {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}
	subscriber := topic.Subscribe(params)

	return subscriber
}

func (b *Broker[T]) NamedSubscribe(topicName string, name string, params SubscribeParams[T]) Subscriber[T] {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}
	subscriber := topic.NamedSubscribe(name, params)

	return subscriber
}

func (b *Broker[T]) Unsubscribe(subscriber *Subscriber[T]) {
	subscriber.Unsubscribe()
}

func (b *Broker[T]) CreatePublisher(topicName string) *Publisher[T] {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}
	publisher := topic.CreatePublisher(b)

	return publisher
}

func NewBroker[T any]() *Broker[T] {
	var wg sync.WaitGroup
	broker := &Broker[T]{
		wg: &wg,
	}
	broker.Start()
	return broker
}
