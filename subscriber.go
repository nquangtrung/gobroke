package gobroke

import (
	"log"
	"sync"

	"trontria.com/gobroke/strategies"
	"trontria.com/gobroke/utils"
)

// type Subscriber interface {
// 	Unsubscribe()

// 	Name() string

// 	TopicChild
// 	utils.Runner
// }

type Subscribers[T any] struct {
	all []Subscriber[T]
	mu  sync.Mutex
}

type Subscriber[T any] struct {
	broker *Broker[T]
	topic  string
	name   string

	utils.BaseRunner[T]
}

func (s Subscriber[T]) Topic() *Topic[T] {
	return s.broker.GetTopic(s.topic)
}

func (s Subscriber[T]) Name() string {
	return s.name
}

func (s Subscriber[T]) Unsubscribe() {
	topic := s.Topic()
	topic.Unsubscribe(s)
}

type SubscriberProcessor[T any] struct {
	strategy strategies.Strategy[T]
	topic    string
	name     string
}

func (s *SubscriberProcessor[T]) Process(data T) {
	log.Printf("[%s] [%s] subscriber passing to strategy %v", s.topic, s.name, data)
	s.strategy.Receive(data)
}

func (s *SubscriberProcessor[T]) CleanUp(channel chan T) {
	s.strategy.Stop()
	log.Printf("[%s] [%s] subscriber stopped", s.topic, s.name)
}

func NewSubscriber[T any](broker *Broker[T], topic string, name string, strategy strategies.Strategy[T]) *Subscriber[T] {
	runner := utils.NewBaseRunner[T](10, &SubscriberProcessor[T]{
		topic:    topic,
		name:     name,
		strategy: strategy,
	})
	return &Subscriber[T]{
		topic:      topic,
		broker:     broker,
		name:       name,
		BaseRunner: *runner,
	}
}
