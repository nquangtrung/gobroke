package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"trontria.com/gobroke"
	"trontria.com/gobroke/strategies/chain"
	"trontria.com/gobroke/worker"
)

func main() {
	broker := gobroke.NewBroker()

	topic := "fruits"
	broker.SetupTopic(gobroke.TopicSetupParams{
		Name: topic,
	})

	received1 := []string{}
	broker.NamedSubscribe(topic, "s1", gobroke.SubscribeParams{
		Handler: func(data any) {
			received1 = append(received1, data.(string))
			log.Printf("[s1] received %v", data)
		},
	})

	received2 := []string{}
	dlq := []string{}
	strategy := chain.NewFromSlice([]chain.ChainNode{
		chain.NewConsumeNode(chain.NewConsumeNodeParams{
			TimeOut: time.Millisecond * 500,
			Drop:    chain.DropFirst,
			Name:    "consume",
			Runner: worker.NewMultipleWorkerPool(worker.MultipleWorkerPoolParams{
				MaxWorker:  3,
				BufferSize: 1,
				Handler: func(data any) {
					log.Printf("s-strategy-v2 received %s", data)
					time.Sleep(time.Millisecond * 3000)
					received2 = append(received2, data.(string))
				}}),
		}),
		chain.NewConsumeNode(chain.NewConsumeNodeParams{
			TimeOut: time.Millisecond * 500,
			Name:    "dlq",
			Runner: worker.NewMultipleWorkerPool(worker.MultipleWorkerPoolParams{
				MaxWorker:  1,
				BufferSize: 1,
				Handler: func(data any) {
					log.Printf("dlq received %s", data)
					time.Sleep(time.Millisecond * 3000)
					dlq = append(dlq, data.(string))
				}}),
		}),
		chain.NewDropNode(),
	})
	broker.NamedSubscribe(topic, "s-strategy-v2", gobroke.SubscribeParams{
		Strategy: strategy,
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
		log.Printf("[dlq] %v", dlq)
	}()
}

func looper(broker gobroke.Broker, topic string, wg *sync.WaitGroup, value string) {
	publisher := broker.CreatePublisher(topic)

	go func() {
		for i := range 15 {
			valueToPublish := fmt.Sprintf("%s-%d", value, i)
			log.Printf("[%s] publishing %s", value, valueToPublish)
			publisher.Publish(valueToPublish)
			time.Sleep(time.Millisecond * 500)
		}
		wg.Done()
	}()
}
