package gobroke

import (
	"context"
	"log"
	"sync"
)

type Subscriber interface {
	TopicChild
	Unsubscribe()

	Name() string

	Start(ctx context.Context)
	Stop()

	Receive(data any)
}

type SubscriberImpl struct {
	topic   string
	handler func(data any)
	broker  Broker

	name string

	stopChannel chan bool
	mu          sync.Mutex

	strategy Strategy
}

type Subscribers struct {
	all []Subscriber
	mu  sync.Mutex
}

func (s *SubscriberImpl) Receive(data any) {
	s.strategy.Receive(data)
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

func (s *SubscriberImpl) Loop(ctx context.Context) {
	log.Printf("[%s] [%s] subscriber started", s.topic, s.name)
	for {
		select {
		case <-s.stopChannel:
			log.Printf("[%s] [%s] subscriber stopped", s.topic, s.name)
			return
		case <-ctx.Done():
			log.Printf("[%s] [%s] subscriber context done, start graceful shutdown", s.topic, s.name)
			s.Stop()
		case data := <-s.strategy.Consume():
			log.Printf("[%s] [%s] received data (%s)", s.topic, s.name, data)
			s.strategy.GetWorkerStrategy().Execute(s.handler, data)
		}
	}
}
func (s *SubscriberImpl) Start(ctx context.Context) {
	s.mu.Lock()

	if s.stopChannel != nil {
		return
	}

	s.stopChannel = make(chan bool)

	go func() {
		s.mu.Unlock()
		s.Loop(ctx)
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
	s.stopChannel = nil

	s.strategy.Stop()

	log.Printf("[%s] [%s] subscriber stopped", s.topic, s.Name())
}
