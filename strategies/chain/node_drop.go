package chain

import "trontria.com/gobroke/strategies"

type DropNodeType = int

const (
	DropLatest DropNodeType = iota
	DropFirst
)

type DropNode struct {
	dropType DropNodeType
}

func (d *DropNode) Consume(strategy strategies.Strategy, data any) {
}
