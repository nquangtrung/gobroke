package chain

import (
	"trontria.com/gobroke/strategies"
)

type ByPassNode struct {
	Nextable
}

func (b *ByPassNode) Consume(strategy strategies.Strategy, data any) {
	b.Next(strategy, data)
}
