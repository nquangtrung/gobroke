package chain

import (
	"log"
	"time"

	"trontria.com/gobroke/strategies"
	"trontria.com/gobroke/utils"
)

type ConsumeNode struct {
	timeOut time.Duration
	runner  *utils.BaseRunner
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
		log.Printf("consume node consuming %v", data)
		return
	case <-timeOut:
		log.Printf("consume node busy, passing it to the next node")
		c.Next(strategy, data)
	}
}

type NewConsumeNodeParams struct {
	TimeOut time.Duration
	Runner  *utils.BaseRunner
}

func NewConsumeNode(params NewConsumeNodeParams) ChainNode {
	go params.Runner.Start()
	return &ConsumeNode{
		runner:  params.Runner,
		timeOut: params.TimeOut,
	}
}
