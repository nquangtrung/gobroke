package chain

import (
	"log"
	"time"

	"trontria.com/gobroke/strategies"
	"trontria.com/gobroke/utils"
)

type DropType int

const (
	DropFirst DropType = iota
	DropLast
)

type ConsumeNode[T any] struct {
	timeOut time.Duration
	name    string
	runner  *utils.BaseRunner[T]
	drop    DropType
	Nextable[T]
}

func (c *ConsumeNode[T]) Stop(strategy strategies.Strategy[T]) {
	c.runner.Stop()
	c.Nextable.Stop(strategy)
}
func (c *ConsumeNode[T]) Consume(strategy strategies.Strategy[T], data T) {
	timeOut := time.After(c.timeOut)
	select {
	case c.runner.Receive() <- data:
		log.Printf("[%s] consume node consuming %v", c.name, data)
		return
	case <-timeOut:
		log.Printf("[%s] consume node busy, passing it to the next node", c.name)
		c.Next(strategy, data)
	}
}
func (c *ConsumeNode[T]) Drop(data T) {
	if c.drop == DropFirst {
		dropped := <-c.runner.Receive()
		c.runner.Receive() <- data
		log.Printf("[%s] dropped first %v", c.name, dropped)
	} else {
		// Do nothing
		log.Printf("[%s] dropped last %v", c.name, data)
	}
}

type NewConsumeNodeParams[T any] struct {
	TimeOut time.Duration
	Runner  *utils.BaseRunner[T]
	Drop    DropType
	Name    string
}

func NewConsumeNode[T any](params NewConsumeNodeParams[T]) *ConsumeNode[T] {
	go params.Runner.Start()
	return &ConsumeNode[T]{
		runner:  params.Runner,
		timeOut: params.TimeOut,
		drop:    params.Drop,
		name:    params.Name,
	}
}
