package main

import (
	"log"
	"sync"

	"trontria.com/gobroke"
)

func main() {
	broker := gobroke.NewBroker()
	defer func() {
		// Allow time for all messages to be processed before stopping
		broker.Stop()
	}()

	topic := "fruits"

	received1 := []string{}
	broker.Subscribe(topic, func(data any) {
		// log.Printf("[subscriber 1] %s is incredible", data.(string))
		received1 = append(received1, data.(string))
		log.Printf("[subscriber 1] received %v", received1)
	})
	received2 := []string{}
	broker.Subscribe(topic, func(data any) {
		// log.Printf("[subscriber 2] %s is incredible", data.(string))
		received2 = append(received2, data.(string))
		log.Printf("[subscriber 2] received %v", received2)
	})

	var wg sync.WaitGroup
	wg.Add(3)

	genericPublisher(broker, topic, &wg, "apple")
	genericPublisher(broker, topic, &wg, "orange")
	genericPublisher(broker, topic, &wg, "peach")

	wg.Wait()
}

func genericPublisher(broker gobroke.Broker, topic string, wg *sync.WaitGroup, value any) {
	publisher := broker.CreatePublisher(topic)

	go func() {
		for i := range 3 {
			log.Printf("[%s] publishing #%d", value, i)
			publisher.Publish(value)
		}
		wg.Done()
	}()
}
