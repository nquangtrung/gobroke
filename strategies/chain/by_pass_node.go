package chain

import (
	"trontria.com/gobroke/strategies"
)

type ByPassNode[T any] struct {
	Nextable[T]
}

func (b *ByPassNode[T]) Consume(strategy strategies.Strategy[T], data T) {
	b.Next(strategy, data)
}
