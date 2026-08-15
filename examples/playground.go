package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"trontria.com/gobroke"
	"trontria.com/gobroke/strategies"
)

func main() {
	broker := gobroke.NewBroker()

	topic := "fruits"
	broker.SetupTopic(gobroke.TopicSetupParams{
		Name: topic,
	})

	received1 := []string{}
	// broker.NamedSubscribe(topic, "s1", gobroke.SubscribeParams{
	// 	Handler: func(data any) {
	// 		received1 = append(received1, data.(string))
	// 		log.Printf("[s1] received %v", data)
	// 	},
	// })

	received2 := []string{}
	// var subscriber2 gobroke.Subscriber
	// subscriber2 = broker.NamedSubscribe(topic, "s2", gobroke.SubscribeParams{
	// 	Handler: func(data any) {
	// 		received2 = append(received2, data.(string))
	// 		log.Printf("[s2] received %v", received2)
	// 		if len(received2) == 2 {
	// 			subscriber2.Unsubscribe()
	// 		}
	// 	},
	// })

	received3 := []string{}
	// broker.NamedSubscribe(topic, "s-hang-oldest", gobroke.SubscribeParams{
	// 	Handler: func(data any) {
	// 		log.Println("[s-hang-oldest] before hanging, received", data)
	// 		time.Sleep(time.Millisecond * 1000)
	// 		log.Println("[s-hang-oldest] finished hanging, received", data)
	// 		received3 = append(received3, data.(string))
	// 	},
	// 	Strategy: strategies.NewStrategyUnion(strategies.SingleBuffered).
	// 		WithBackPressure(strategies.NewBackPressureStrategy(strategies.DropOldest)).
	// 		WithWorker(strategies.NewMultipleWorker(3)),
	// })

	received4 := []string{}
	// broker.NamedSubscribe(topic, "s-hang-newest", gobroke.SubscribeParams{
	// 	Handler: func(data any) {
	// 		log.Println("[s-hang-newest] before hanging, received", data)
	//
	// 		log.Println("[s-hang-newest] finished hanging, received", data)
	// 		received4 = append(received4, data.(string))
	// 	},
	// 	Strategy: strategies.NewStrategyUnion(strategies.SingleBuffered, 4),
	// })

	received5 := []string{}
	dlq := []string{}
	chain := strategies.FromSlice([]strategies.ChainNode{
		strategies.NewConsumeNode(strategies.NewMultipleWorkerPool(strategies.MultipleWorkerPoolParams{
			MaxWorker:  3,
			BufferSize: 10,
			Handler: func(data any) {
				log.Printf("s-strategy-v2 received %s", data)
				time.Sleep(time.Millisecond * 1000)
				received5 = append(received5, data.(string))
			}})),
		strategies.NewConsumeNode(strategies.NewMultipleWorkerPool(strategies.MultipleWorkerPoolParams{
			MaxWorker:  1,
			BufferSize: 1,
			Handler: func(data any) {
				log.Printf("dlq received %s", data)
				dlq = append(dlq, data.(string))
			}})),
	})
	broker.NamedSubscribe(topic, "s-strategy-v2", gobroke.SubscribeParams{
		Strategy: strategies.NewStrategyChain(chain),
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

	defer func() {
		broker.Stop()
		log.Printf("[received1] %v", received1)
		log.Printf("[received2] %v", received2)
		log.Printf("[received3] %v", received3)
		log.Printf("[received4] %v", received4)
		log.Printf("[received5] %v", received5)
		log.Printf("[dlq] %v", dlq)
	}()
}

func looper(broker gobroke.Broker, topic string, wg *sync.WaitGroup, value string) {
	publisher := broker.CreatePublisher(topic)

	go func() {
		for i := range 5 {
			valueToPublish := fmt.Sprintf("%s-%d", value, i)
			log.Printf("[%s] publishing %s", value, valueToPublish)
			publisher.Publish(valueToPublish)
			time.Sleep(time.Millisecond * 500)
		}
		wg.Done()
	}()
}
