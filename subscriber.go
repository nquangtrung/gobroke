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

type Subscribers struct {
	all []Subscriber
	mu  sync.Mutex
}

type Subscriber struct {
	broker *Broker
	topic  string
	name   string

	utils.BaseRunner
}

func (s Subscriber) Topic() *Topic {
	return s.broker.GetTopic(s.topic)
}

func (s Subscriber) Name() string {
	return s.name
}

func (s Subscriber) Unsubscribe() {
	topic := s.Topic()
	topic.Unsubscribe(s)
}

type SubscriberProcessor struct {
	strategy strategies.Strategy
	topic    string
	name     string
}

func (s *SubscriberProcessor) Process(data any) {
	log.Printf("[%s] [%s] subscriber passing to strategy %s", s.topic, s.name, data)
	s.strategy.Receive(data)
}

func (s *SubscriberProcessor) CleanUp(channel chan any) {
	s.strategy.Stop()
	log.Printf("[%s] [%s] subscriber stopped", s.topic, s.name)
}

func NewSubscriber(broker *Broker, topic string, name string, strategy strategies.Strategy) *Subscriber {
	runner := utils.NewBaseRunner(10, &SubscriberProcessor{
		topic:    topic,
		name:     name,
		strategy: strategy,
	})
	return &Subscriber{
		topic:      topic,
		broker:     broker,
		name:       name,
		BaseRunner: *runner,
	}
}
