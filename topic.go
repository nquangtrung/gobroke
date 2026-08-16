package gobroke

import (
	"crypto/rand"
	"log"
	"sync"
	"time"

	"trontria.com/gobroke/strategies"
	"trontria.com/gobroke/strategies/chain"
	"trontria.com/gobroke/worker"
)

type TopicSetupParams struct {
	Name       string
	BufferSize int
}

type Topic struct {
	name        string
	subscribers *Subscribers
	publishers  *Publishers
	broker      *Broker

	bufferSize int
	params     TopicSetupParams

	receiveChannel chan any
	mu             sync.Mutex
}

func (t *Topic) GetParams() TopicSetupParams {
	return t.params
}

func (t *Topic) SendToAllSubscribers(data any) {
	t.subscribers.mu.Lock()
	defer t.subscribers.mu.Unlock()

	subs := t.subscribers.all

	for _, subscriber := range subs {
		log.Printf("[%s] [%s] publishing data to subscriber: %v", t.name, subscriber.Name(), data)
		go func() { subscriber.Receive() <- data }()
	}
}

func (t *Topic) GetName() string {
	return t.name
}

func (t *Topic) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.subscribers.mu.Lock()
	defer t.subscribers.mu.Unlock()
	for _, sub := range t.subscribers.all {
		sub.Stop()
	}
	t.subscribers.all = nil

	t.broker.ReleaseTopic(t)

	log.Printf("[%s] topic shutdown", t.name)
}

func (t *Topic) Subscribe(params SubscribeParams) Subscriber {
	return t.NamedSubscribe(rand.Text(), params)
}

func resolveSubscriberStrategy(params SubscribeParams) strategies.Strategy {
	if params.Strategy != nil {
		return params.Strategy
	}

	return chain.NewFromSlice([]chain.ChainNode{
		chain.NewConsumeNode(chain.NewConsumeNodeParams{
			Name: "consume",
			Runner: worker.NewMultipleWorkerPool(worker.MultipleWorkerPoolParams{
				MaxWorker:  1,
				BufferSize: 19,
				Handler:    params.Handler,
			}),
			TimeOut: time.Millisecond * 500,
		}),
		chain.NewDropNode(),
	})
}

func (t *Topic) NamedSubscribe(name string, params SubscribeParams) Subscriber {
	log.Printf("[%s] named subscribe requested", t.name)
	t.mu.Lock()
	log.Printf("[%s] lock acquired for named subscribe", t.name)
	defer t.mu.Unlock()

	subscriber := NewSubscriber(t.broker, t.name, name, resolveSubscriberStrategy(params))
	go subscriber.Start()

	t.subscribers.all = append(t.subscribers.all, *subscriber)

	return *subscriber
}

func (t *Topic) Publish(data any) {
	t.SendToAllSubscribers(data)
}

func (t *Topic) CreatePublisher(b *Broker) *Publisher {
	t.mu.Lock()
	log.Printf("[%s] lock acquired for create publisher", t.name)
	defer t.mu.Unlock()

	publisher := &Publisher{
		topic:  t.GetName(),
		broker: b,
	}
	t.publishers.all = append(t.publishers.all, *publisher)
	return publisher
}

func (t *Topic) Unsubscribe(subscriber Subscriber) {
	go func() {
		t.subscribers.mu.Lock()
		defer t.subscribers.mu.Unlock()

		for i, s := range t.subscribers.all {
			if s.Name() == subscriber.Name() {
				t.subscribers.all = append(t.subscribers.all[:i], t.subscribers.all[i+1:]...)
				s.Stop()
				log.Printf("[%s] [%s] unsubscribed\n", t.name, s.Name())
				return
			}
		}
	}()
}

func resolveBufferSize(params TopicSetupParams) int {
	if params.BufferSize == 0 {
		return 10
	}
	return params.BufferSize
}

func newTopic(b *Broker, params TopicSetupParams) *Topic {
	bufferSize := resolveBufferSize(params)

	return &Topic{
		name:        params.Name,
		subscribers: &Subscribers{},
		publishers:  &Publishers{},
		broker:      b,
		bufferSize:  bufferSize,
		params:      params,
	}
}
