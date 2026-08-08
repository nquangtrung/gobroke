package gobroke

import (
	"context"
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
	defer t.mu.Unlock()

	if t.stopChannel != nil {
		return
	}

	log.Printf("[%s] topic started", t.name)
	t.stopChannel = make(chan bool)
	t.ctx, t.cancel = context.WithCancel(ctx)
	go func() {
		for {
			t.subscribers.mu.Lock()
			subs := t.subscribers.all
			t.subscribers.mu.Unlock()

			select {
			case <-t.stopChannel:
				log.Printf("[%s] topic stopped", t.name)
				return
			case <-ctx.Done():
				log.Printf("[%s] topic context done, start graceful shutdown", t.name)
				t.Stop()
			case data := <-t.receiveChannel:
				for _, subscriber := range subs {
					select {
					case subscriber.Receive() <- data:
					default:
						// subscriber channel is full, discard the current data
						continue
					}
				}
			default:
				continue
			}
		}
	}()
}

func (t *TopicImpl) GetName() string {
	return t.name
}

func (t *TopicImpl) Stop() {
	defer close(t.receiveChannel)
	defer close(t.stopChannel)

	t.stopChannel <- true
	t.cancel()
	t.subscribers.mu.Lock()
	defer t.subscribers.mu.Unlock()

	for _, subscriber := range t.subscribers.all {
		subscriber.Stop()
	}
	t.subscribers.all = nil

	log.Printf("[%s] topic stopped", t.name)
}

func (t *TopicImpl) Subscribe(handler func(data any)) Subscriber {
	t.subscribers.mu.Lock()
	defer t.subscribers.mu.Unlock()

	subscriber := &SubscriberImpl{
		topic:   t.name,
		handler: handler,
		broker:  t.broker,
	}
	subscriber.Start(t.ctx)

	t.subscribers.all = append(t.subscribers.all, subscriber)

	return subscriber
}

func (t *TopicImpl) Publish(data any) {
	t.receiveChannel <- data
}

func (t *TopicImpl) CreatePublisher(b Broker) Publisher {
	t.publishers.mu.Lock()
	defer t.publishers.mu.Unlock()

	publisher := &PublisherImpl{
		topic:  t.GetName(),
		broker: b,
	}
	t.publishers.all = append(t.publishers.all, publisher)
	return publisher
}

func (t *TopicImpl) Unsubscribe(subscriber Subscriber) {
	t.subscribers.mu.Lock()
	defer t.subscribers.mu.Unlock()

	for i, s := range t.subscribers.all {
		if s == subscriber {
			t.subscribers.all = append(t.subscribers.all[:i], t.subscribers.all[i+1:]...)
			s.Stop()
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
