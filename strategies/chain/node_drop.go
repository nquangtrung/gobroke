package chain

import (
	"log"

	"trontria.com/gobroke/strategies"
)

type DropNode struct {
	Terminal
}

func (d *DropNode) Consume(strategy strategies.Strategy, data any) {
	log.Printf("last node, dropping %v", data)
	strategy.Drop(data)
}

func NewDropNode() *DropNode {
	return &DropNode{}
}
