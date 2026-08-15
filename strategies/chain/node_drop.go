package chain

import (
	"log"

	"trontria.com/gobroke/strategies"
)

type DropNode struct{}

func (d *DropNode) Consume(strategy strategies.Strategy, data any) {
	log.Printf("last node, dropping %v", data)
	strategy.Drop(data)
}
func (d *DropNode) Then(node ChainNode) ChainNode {
	return d
}
func (d *DropNode) Stop(strategy strategies.Strategy) {}
func (d *DropNode) Drop(data any)                     {}

func NewDropNode() ChainNode {
	return &DropNode{}
}
