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

type Broker interface {
	Start()
	Stop()

	Done() <-chan struct{}

	Subscribe(topic string, params SubscribeParams) Subscriber
	NamedSubscribe(topic string, name string, params SubscribeParams) Subscriber

	CreatePublisher(topic string) Publisher
	Publish(topic string, data any)

	GetTopic(topic string) Topic

	SetupTopic(params TopicSetupParams) Topic
	ReleaseTopic(topic Topic)
}

type BrokerImpl struct {
	topics map[string]Topic
	mu     sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	wg *sync.WaitGroup
}

func (b *BrokerImpl) Done() <-chan struct{} {
	return b.ctx.Done()
}

func (b *BrokerImpl) Start() {
	var once sync.Once
	once.Do(func() {
		b.topics = make(map[string]Topic)
		b.ctx, b.cancel = context.WithCancel(context.Background())
	})
}

func (b *BrokerImpl) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, topic := range b.topics {
		topic.Stop()
	}

	b.wg.Wait()
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
	topic.Start()
	b.wg.Add(1)

	return b.topics[topicName]
}

func (b *BrokerImpl) ReleaseTopic(topic Topic) {
	log.Printf("releasing topic %s", topic.GetName())
	b.wg.Done()
}

func (b *BrokerImpl) Subscribe(topicName string, params SubscribeParams) Subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}
	subscriber := topic.Subscribe(params)

	return subscriber
}

func (b *BrokerImpl) NamedSubscribe(topicName string, name string, params SubscribeParams) Subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic := b.GetTopic(topicName)
	if topic == nil {
		panic("topic " + topicName + " not found")
	}
	subscriber := topic.NamedSubscribe(name, params)

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
