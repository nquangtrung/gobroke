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

	Name() string

	Start(ctx context.Context)
	Stop()
}

type SubscriberImpl struct {
	topic   string
	handler func(data any)
	broker  Broker

	name string

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

func (s *SubscriberImpl) Name() string {
	return s.name
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

	if s.stopChannel != nil {
		return
	}

	s.receivechannel = make(chan any, 10)
	s.stopChannel = make(chan bool)

	go func() {
		s.mu.Unlock()
		log.Printf("[%s] [%s] subscriber started", s.topic, s.name)
		for {
			select {
			case <-s.stopChannel:
				log.Printf("[%s] [%s] subscriber stopped", s.topic, s.name)
				return
			case <-ctx.Done():
				log.Printf("[%s] [%s] subscriber context done, start graceful shutdown", s.topic, s.name)
				s.Stop()
			case data := <-s.receivechannel:
				log.Printf("[%s] [%s] subscriber received data: %v", s.topic, s.name, data)
				s.handler(data)
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

	close(s.stopChannel)
	close(s.receivechannel)

	s.stopChannel = nil
	s.receivechannel = nil

	log.Printf("[%s] [%s] subscriber stopped", s.topic, s.Name())
}
