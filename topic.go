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

type Topic[T any] struct {
	name        string
	subscribers *Subscribers[T]
	publishers  *Publishers[T]
	broker      *Broker[T]

	bufferSize int
	params     TopicSetupParams

	receiveChannel chan any
	mu             sync.Mutex
}

func (t *Topic[T]) GetParams() TopicSetupParams {
	return t.params
}

func (t *Topic[T]) SendToAllSubscribers(data T) {
	t.subscribers.mu.Lock()
	defer t.subscribers.mu.Unlock()

	subs := t.subscribers.all

	for _, subscriber := range subs {
		log.Printf("[%s] [%s] publishing data to subscriber: %v", t.name, subscriber.Name(), data)
		go func() { subscriber.Receive() <- data }()
	}
}

func (t *Topic[T]) GetName() string {
	return t.name
}

func (t *Topic[T]) Stop() {
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

func (t *Topic[T]) Subscribe(params SubscribeParams[T]) Subscriber[T] {
	return t.NamedSubscribe(rand.Text(), params)
}

func (t *Topic[T]) NamedSubscribe(name string, params SubscribeParams[T]) Subscriber[T] {
	log.Printf("[%s] named subscribe requested", t.name)
	t.mu.Lock()
	log.Printf("[%s] lock acquired for named subscribe", t.name)
	defer t.mu.Unlock()

	subscriber := NewSubscriber(t.broker, t.name, name, resolveSubscriberStrategy(params))
	go subscriber.Start()

	t.subscribers.all = append(t.subscribers.all, *subscriber)

	return *subscriber
}

func (t *Topic[T]) Publish(data T) {
	t.SendToAllSubscribers(data)
}

func (t *Topic[T]) CreatePublisher(b *Broker[T]) *Publisher[T] {
	t.mu.Lock()
	log.Printf("[%s] lock acquired for create publisher", t.name)
	defer t.mu.Unlock()

	publisher := &Publisher[T]{
		topic:  t.GetName(),
		broker: b,
	}
	t.publishers.all = append(t.publishers.all, *publisher)
	return publisher
}

func (t *Topic[T]) Unsubscribe(subscriber Subscriber[T]) {
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

func newTopic[T any](b *Broker[T], params TopicSetupParams) *Topic[T] {
	bufferSize := resolveBufferSize(params)

	return &Topic[T]{
		name:        params.Name,
		subscribers: &Subscribers[T]{},
		publishers:  &Publishers[T]{},
		broker:      b,
		bufferSize:  bufferSize,
		params:      params,
	}
}

func resolveSubscriberStrategy[T any](params SubscribeParams[T]) strategies.Strategy[T] {
	if params.Strategy != nil {
		return params.Strategy
	}

	return chain.NewFromSlice([]chain.ChainNode[T]{
		chain.NewConsumeNode(chain.NewConsumeNodeParams[T]{
			Name: "consume",
			Runner: worker.NewMultipleWorkerPool(worker.MultipleWorkerPoolParams[T]{
				MaxWorker:  1,
				BufferSize: 19,
				Handler:    params.Handler,
			}),
			TimeOut: time.Millisecond * 500,
		}),
		chain.NewDropNode[T](),
	})
}
