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

type ConsumeNode struct {
	timeOut time.Duration
	name    string
	runner  *utils.BaseRunner
	drop    DropType
	Nextable
}

func (c *ConsumeNode) Stop(strategy strategies.Strategy) {
	c.runner.Stop()
	c.Nextable.Stop(strategy)
}
func (c *ConsumeNode) Consume(strategy strategies.Strategy, data any) {
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
func (c *ConsumeNode) Drop(data any) {
	if c.drop == DropFirst {
		dropped := <-c.runner.Receive()
		c.runner.Receive() <- data
		log.Printf("[%s] dropped first %v", c.name, dropped)
	} else {
		// Do nothing
		log.Printf("[%s] dropped last %v", c.name, data)
	}
}

type NewConsumeNodeParams struct {
	TimeOut time.Duration
	Runner  *utils.BaseRunner
	Drop    DropType
	Name    string
}

func NewConsumeNode(params NewConsumeNodeParams) ChainNode {
	go params.Runner.Start()
	return &ConsumeNode{
		runner:  params.Runner,
		timeOut: params.TimeOut,
		drop:    params.Drop,
		name:    params.Name,
	}
}
