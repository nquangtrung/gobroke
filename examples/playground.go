package main

import (
	"log"
	"sync"
	"time"

	"trontria.com/gobroke"
)

func main() {
	broker := gobroke.NewBroker()
	defer func() {
		broker.Stop()
	}()

	topic := "fruits"
	broker.SetupTopic(topic)

	received1 := []string{}
	broker.NamedSubscribe(topic, "s1", func(data any) {
		received1 = append(received1, data.(string))
		log.Printf("[s1] received %v", data)
	})
	received2 := []string{}
	var subscriber2 gobroke.Subscriber
	subscriber2 = broker.NamedSubscribe(topic, "s2", func(data any) {
		received2 = append(received2, data.(string))
		log.Printf("[s2] received %v", received2)
		if len(received2) == 2 {
			subscriber2.Unsubscribe()
		}
	})

	time.Sleep(time.Millisecond * 100)

	var wg sync.WaitGroup

	fruits := []string{
		"apple",
		"orange",
		"peach",
	}
	for _, fruit := range fruits {
		wg.Add(1)
		looper(broker, topic, &wg, fruit)
	}

	wg.Wait()

	log.Println("Published all available fruits")
	log.Printf("[received1] %v", received1)
	log.Printf("[received2] %v", received2)
}

func looper(broker gobroke.Broker, topic string, wg *sync.WaitGroup, value any) {
	publisher := broker.CreatePublisher(topic)

	go func() {
		for i := range 3 {
			log.Printf("[%s] publishing #%d", value, i)
			publisher.Publish(value)
			time.Sleep(time.Second)
		}
		wg.Done()
	}()
}
