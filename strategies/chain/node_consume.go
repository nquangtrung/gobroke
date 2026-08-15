package chain

import (
	"log"

	"trontria.com/gobroke/strategies"
	"trontria.com/gobroke/strategies/worker"
)

type ConsumeNode struct {
	pool worker.WorkerPool
	Nextable
}

func (c *ConsumeNode) Stop(strategy strategies.Strategy) {
	c.pool.Stop()
	c.Nextable.Stop(strategy)
}
func (c *ConsumeNode) Consume(strategy strategies.Strategy, data any) {
	select {
	case c.pool.Receive() <- data:
		log.Printf("consume node consuming %v", data)
		return
	default:
		log.Printf("consume node busy, passing it to the next node")
		c.Next(strategy, data)
	}
}
