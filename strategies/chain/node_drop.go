package chain

import (
	"log"

	"trontria.com/gobroke/strategies"
)

type DropNode[T any] struct {
	Terminal[T]
}

func (d *DropNode[T]) Consume(strategy strategies.Strategy[T], data T) {
	log.Printf("last node, dropping %v", data)
	strategy.Drop(data)
}

func NewDropNode[T any]() *DropNode[T] {
	return &DropNode[T]{}
}
