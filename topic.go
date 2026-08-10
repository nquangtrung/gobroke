package gobroke

import (
	"context"
	"crypto/rand"
	"log"
	"sync"
)

type TopicChild interface {
	Topic() Topic
}

type Topic interface {
	GetName() string

	Start(ctx context.Context)
	Stop()

	Publish(data any)
	CreatePublisher(b Broker) Publisher

	Subscribe(params SubscribeParams) Subscriber
	NamedSubscribe(name string, params SubscribeParams) Subscriber
	Unsubscribe(subscriber Subscriber)

	GetParams() TopicSetupParams
}

type TopicSetupParams struct {
	Name       string
	BufferSize int
}

type TopicImpl struct {
	name        string
	subscribers *Subscribers
	publishers  *Publishers
	broker      Broker

	bufferSize int
	params     TopicSetupParams

	receiveChannel chan any
	stopChannel    chan bool
	mu             sync.Mutex
	ctx            context.Context
	cancel         context.CancelFunc
}

func (t *TopicImpl) GetParams() TopicSetupParams {
	return t.params
}

func (t *TopicImpl) Loop(ctx context.Context) {
	for {
		select {
		case <-t.stopChannel:
			log.Printf("[%s] topic stopped", t.name)
			return
		case <-ctx.Done():
			log.Printf("[%s] topic context done, start graceful shutdown", t.name)
			t.Stop()
		case data := <-t.receiveChannel:
			t.subscribers.mu.Lock()

			subs := t.subscribers.all

			log.Printf("[%s] received data: %v", t.name, data)
			for _, subscriber := range subs {
				subscriber.Receive(data)
			}
			// Can not use defer here, defer will wait until the end of the function,
			// which is after for
			t.subscribers.mu.Unlock()
		}
	}
}
func (t *TopicImpl) Start(ctx context.Context) {
	t.mu.Lock()

	if t.stopChannel != nil {
		return
	}

	t.stopChannel = make(chan bool)
	t.ctx, t.cancel = context.WithCancel(ctx)

	go func() {
		t.mu.Unlock()

		log.Printf("[%s] topic started", t.name)
		t.Loop(ctx)
	}()
}

func (t *TopicImpl) GetName() string {
	return t.name
}

func (t *TopicImpl) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.stopChannel <- true

	close(t.receiveChannel)
	close(t.stopChannel)

	t.receiveChannel = nil
	t.stopChannel = nil

	t.subscribers.mu.Lock()
	defer t.subscribers.mu.Unlock()

	t.subscribers.all = nil

	log.Printf("[%s] topic stopped", t.name)
}

func (t *TopicImpl) Subscribe(params SubscribeParams) Subscriber {
	return t.NamedSubscribe(rand.Text(), params)
}

func resolveSubscriberStrategy(params SubscribeParams) Strategy {
	if params.Strategy != nil {
		return params.Strategy
	}

	return NewStrategy(SingleBuffered)
}

func (t *TopicImpl) NamedSubscribe(name string, params SubscribeParams) Subscriber {
	t.mu.Lock()
	defer t.mu.Unlock()

	subscriber := &SubscriberImpl{
		topic:    t.name,
		handler:  params.Handler,
		broker:   t.broker,
		name:     name,
		strategy: resolveSubscriberStrategy(params),
	}
	subscriber.Start(t.ctx)

	t.subscribers.all = append(t.subscribers.all, subscriber)

	return subscriber
}

func (t *TopicImpl) Publish(data any) {
	t.receiveChannel <- data
}

func (t *TopicImpl) CreatePublisher(b Broker) Publisher {
	t.mu.Lock()
	defer t.mu.Unlock()

	publisher := &PublisherImpl{
		topic:  t.GetName(),
		broker: b,
	}
	t.publishers.all = append(t.publishers.all, publisher)
	return publisher
}

func (t *TopicImpl) Unsubscribe(subscriber Subscriber) {
	t.mu.Lock()
	defer t.mu.Unlock()
	log.Printf("[%s] [%s] unsubscribe requested, before: {%d}\n", t.name, subscriber.Name(), len(t.subscribers.all))
	defer log.Printf("[%s] [%s] unsubscribe finished, after: {%d}\n", t.name, subscriber.Name(), len(t.subscribers.all))

	for i, s := range t.subscribers.all {
		if s.Name() == subscriber.Name() {
			t.subscribers.all = append(t.subscribers.all[:i], t.subscribers.all[i+1:]...)
			s.Stop()
			log.Printf("[%s] [%s] unsubscribed\n", t.name, s.Name())
			return
		}
	}

}

func resolveBufferSize(params TopicSetupParams) int {
	if params.BufferSize == 0 {
		return 10
	}
	return params.BufferSize
}

func newTopic(b Broker, params TopicSetupParams) Topic {
	bufferSize := resolveBufferSize(params)

	return &TopicImpl{
		name: params.Name,

		subscribers: &Subscribers{},
		publishers:  &Publishers{},

		broker:         b,
		receiveChannel: make(chan any, bufferSize),

		bufferSize: bufferSize,
		params:     params,
	}
}
