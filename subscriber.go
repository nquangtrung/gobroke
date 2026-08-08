package gobroke

import (
	"context"
	"log"
	"sync"
)

type Subscriber interface {
	TopicChild
	Unsubscribe()
	Receive() chan any

	Start(ctx context.Context)
	Stop()
}

type SubscriberImpl struct {
	topic   string
	handler func(data any)
	broker  Broker

	receivechannel chan any
	stopChannel    chan bool
	mu             sync.Mutex
}

type Subscribers struct {
	all []Subscriber
	mu  sync.Mutex
}

func (s *SubscriberImpl) Topic() Topic {
	return s.broker.GetTopic(s.topic)
}

func (s *SubscriberImpl) Unsubscribe() {
	topic := s.Topic()
	topic.Unsubscribe(s)
}

func (s *SubscriberImpl) Receive() chan any {
	return s.receivechannel
}

func (s *SubscriberImpl) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopChannel != nil {
		return
	}

	s.receivechannel = make(chan any, 10)
	s.stopChannel = make(chan bool)

	go func() {
		log.Printf("[%s] subscriber started", s.topic)
		for {
			select {
			case <-s.stopChannel:
				log.Printf("[%s] subscriber stopped", s.topic)
				return
			case <-ctx.Done():
				log.Printf("[%s] subscriber context done, start graceful shutdown", s.topic)
				s.Stop()
			case data := <-s.receivechannel:
				log.Printf("[%s] subscriber received data: %v", s.topic, data)
				s.handler(data)
			default:
				continue
			}
		}
	}()
}

func (s *SubscriberImpl) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopChannel == nil {
		return
	}

	s.stopChannel <- true
	defer close(s.stopChannel)
	defer close(s.receivechannel)

	log.Printf("[%s] subscriber stopped", s.topic)
}
