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

	Subscribe(handler func(data any)) Subscriber
	NamedSubscribe(name string, handler func(data any)) Subscriber
	Unsubscribe(subscriber Subscriber)
}

type TopicImpl struct {
	name        string
	subscribers *Subscribers
	publishers  *Publishers
	broker      Broker

	receiveChannel chan any
	stopChannel    chan bool
	mu             sync.Mutex
	ctx            context.Context
	cancel         context.CancelFunc
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
		for {
			log.Printf("[%s] awaiting published data, ctx.Done(), or stopChannel signal", t.name)
			select {
			case <-t.stopChannel:
				log.Printf("[%s] topic stopped", t.name)
				return
			case <-ctx.Done():
				log.Printf("[%s] topic context done, start graceful shutdown", t.name)
				t.Stop()
			case data := <-t.receiveChannel:
				log.Printf("[%s] received data: %v", t.name, data)
				t.subscribers.mu.Lock()
				subs := t.subscribers.all

				for _, subscriber := range subs {
					select {
					case subscriber.Receive() <- data:
						log.Printf("[%s] -> [%s]: %v", t.name, subscriber.Name(), data)
						continue
					default:
						// subscriber channel is full, discard the current data
						log.Printf("[%s] -> [%s] subscriber channel is full, discarding data: %v", t.name, subscriber.Name(), data)
						continue
					}
				}
				t.subscribers.mu.Unlock()
			}

		}
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

func (t *TopicImpl) Subscribe(handler func(data any)) Subscriber {
	return t.NamedSubscribe(rand.Text(), handler)
}

func (t *TopicImpl) NamedSubscribe(name string, handler func(data any)) Subscriber {
	t.mu.Lock()
	defer t.mu.Unlock()

	subscriber := &SubscriberImpl{
		topic:   t.name,
		handler: handler,
		broker:  t.broker,
		name:    name,
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
	log.Printf("[%s] [%s] unsubscribe requested\n", t.name, subscriber.Name())
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, s := range t.subscribers.all {
		if s.Name() == subscriber.Name() {
			s.Stop()
			t.subscribers.all = append(t.subscribers.all[:i], t.subscribers.all[i+1:]...)
			log.Printf("[%s] [%s] unsubscribed\n", t.name, s.Name())
			return
		}
	}
}

type Topics struct {
	all map[string]Topic
	mu  sync.Mutex
}

func NewTopic(topicName string, b Broker) Topic {
	return &TopicImpl{
		name:           topicName,
		subscribers:    &Subscribers{},
		publishers:     &Publishers{},
		broker:         b,
		receiveChannel: make(chan any),
	}
}
