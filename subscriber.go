package gobroke

import (
	"log"
	"sync"

	"trontria.com/gobroke/strategies"
)

type Subscriber interface {
	TopicChild
	Unsubscribe()

	Name() string

	Start()
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

	strategy strategies.Strategy
}

type Subscribers struct {
	all []Subscriber
	mu  sync.Mutex
}

func (s *SubscriberImpl) Receive(data any) {
	log.Printf("[%s] [%s] subscriber passing to strategy %s", s.topic, s.name, data)
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

func (s *SubscriberImpl) Loop() {
	log.Printf("[%s] [%s] subscriber started", s.topic, s.name)
	for {
		select {
		case <-s.stopChannel:
			log.Printf("[%s] [%s] subscriber stopped", s.topic, s.name)
			s.Shutdown()
			return
		case data := <-s.strategy.Consume():
			log.Printf("[%s] [%s] subscriber received data (%s)", s.topic, s.name, data)
			s.strategy.Execute(s.handler, data)
		}
	}
}
func (s *SubscriberImpl) Start() {
	s.mu.Lock()

	if s.stopChannel != nil {
		return
	}

	s.stopChannel = make(chan bool)

	go func() {
		s.mu.Unlock()
		s.Loop()
	}()
}

func (s *SubscriberImpl) Stop() {
	s.stopChannel <- true
}
func (s *SubscriberImpl) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.strategy.Stop()

	log.Printf("[%s] [%s] subscriber stopped", s.topic, s.Name())
}
