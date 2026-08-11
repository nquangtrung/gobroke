package gobroke

import (
	"crypto/rand"
	"log"
	"sync"
)

type TopicChild interface {
	Topic() Topic
}

type Topic interface {
	GetName() string

	Start()
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

type TopicStatus int

const (
	TopicInitialized TopicStatus = iota
	TopicRunning
	TopicDraining
	TopicStopped
)

type TopicImpl struct {
	name        string
	subscribers *Subscribers
	publishers  *Publishers
	broker      Broker
	status      TopicStatus

	bufferSize int
	params     TopicSetupParams

	receiveChannel chan any
	stopChannel    chan bool
	mu             sync.Mutex
}

func (t *TopicImpl) GetParams() TopicSetupParams {
	return t.params
}

func (t *TopicImpl) SendToAllSubscribers(data any) {
	t.subscribers.mu.Lock()
	defer t.subscribers.mu.Unlock()

	subs := t.subscribers.all

	log.Printf("[%s] loop received data: %v", t.name, data)
	for _, subscriber := range subs {
		subscriber.Receive(data)
	}
}
func (t *TopicImpl) Loop() {
	for {
		select {
		case <-t.stopChannel:
			log.Printf("[%s] topic received stop signal", t.name)
			t.Shutdown()
			return
		case <-t.broker.Done():
			log.Printf("[%s] topic received canceled signal", t.name)
			t.Shutdown()
			return
		case data := <-t.receiveChannel:
			t.SendToAllSubscribers(data)
		}
	}
}
func (t *TopicImpl) Start() {
	t.mu.Lock()

	log.Printf("[%s] lock acquired for start", t.name)
	if t.stopChannel != nil {
		return
	}

	t.stopChannel = make(chan bool, 1)

	go func() {
		t.status = TopicRunning
		t.mu.Unlock()

		log.Printf("[%s] topic started", t.name)
		t.Loop()
	}()
}

func (t *TopicImpl) GetName() string {
	return t.name
}

func (t *TopicImpl) Drain() {
	t.status = TopicDraining
	log.Printf("[%s] start draining, messages left: %d", t.name, len(t.receiveChannel))

	// Now do not receive anything, just keep draining the stopChannel
	t.subscribers.mu.Lock()
	defer t.subscribers.mu.Unlock()

	for {
		select {
		case data := <-t.receiveChannel:

			subs := t.subscribers.all
			log.Printf("[%s] draining data: %v", t.name, data)
			for _, subscriber := range subs {
				subscriber.Receive(data)
			}
		default:
			log.Printf("[%s] draining complete, messages left: %d", t.name, len(t.receiveChannel))
			return
		}
	}
}

func (t *TopicImpl) Stop() {
	t.stopChannel <- true
}

func (t *TopicImpl) Shutdown() {
	log.Printf("[%s] shutdown requested", t.name)
	t.mu.Lock()
	log.Printf("[%s] lock acquired for shutdown", t.name)
	defer t.mu.Unlock()

	t.Drain()

	close(t.receiveChannel)
	t.receiveChannel = nil

	t.subscribers.mu.Lock()
	defer t.subscribers.mu.Unlock()
	for _, sub := range t.subscribers.all {
		sub.Stop()
	}
	t.subscribers.all = nil

	t.status = TopicStopped
	t.broker.ReleaseTopic(t)

	log.Printf("[%s] topic shutdown", t.name)
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
	log.Printf("[%s] lock acquired for named subscribe", t.name)
	defer t.mu.Unlock()

	if t.status != TopicRunning {
		return nil
	}

	subscriber := &SubscriberImpl{
		topic:    t.name,
		handler:  params.Handler,
		broker:   t.broker,
		name:     name,
		strategy: resolveSubscriberStrategy(params),
	}
	subscriber.Start()

	t.subscribers.all = append(t.subscribers.all, subscriber)

	return subscriber
}

func (t *TopicImpl) Publish(data any) {
	if t.status != TopicRunning {
		return
	}
	t.receiveChannel <- data
}

func (t *TopicImpl) CreatePublisher(b Broker) Publisher {
	t.mu.Lock()
	log.Printf("[%s] lock acquired for create publisher", t.name)
	defer t.mu.Unlock()

	publisher := &PublisherImpl{
		topic:  t.GetName(),
		broker: b,
	}
	t.publishers.all = append(t.publishers.all, publisher)
	return publisher
}

func (t *TopicImpl) Unsubscribe(subscriber Subscriber) {
	go func() {
		log.Printf("[%s] [%s] unsubscribe requested, before: {%d}\n", t.name, subscriber.Name(), len(t.subscribers.all))
		t.subscribers.mu.Lock()
		defer log.Printf("[%s] [%s] unsubscribe finished, after: {%d}\n", t.name, subscriber.Name(), len(t.subscribers.all))
		defer t.subscribers.mu.Unlock()

		for i, s := range t.subscribers.all {
			if s.Name() == subscriber.Name() {
				log.Printf("[%s] [%s] %d %d\n", t.name, s.Name(), len(t.subscribers.all[:i]), len(t.subscribers.all[i+1:]))
				t.subscribers.all = append(t.subscribers.all[:i], t.subscribers.all[i+1:]...)
				log.Printf("[%s] [%s] sssunsubscribe finished, after: {%d}\n", t.name, subscriber.Name(), len(t.subscribers.all))
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

func newTopic(b Broker, params TopicSetupParams) Topic {
	bufferSize := resolveBufferSize(params)

	return &TopicImpl{
		name:   params.Name,
		status: TopicInitialized,

		subscribers: &Subscribers{},
		publishers:  &Publishers{},

		broker:         b,
		receiveChannel: make(chan any, bufferSize),

		bufferSize: bufferSize,
		params:     params,
	}
}
