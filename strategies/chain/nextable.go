package chain

import (
	"trontria.com/gobroke/strategies"
)

type Nextable[T any] struct {
	next ChainNode[T]
}

func (n *Nextable[T]) Then(node ChainNode[T]) {
	n.next = node
}
func (n *Nextable[T]) Next(strategy strategies.Strategy[T], data T) {
	if n.next != nil {
		n.next.Consume(strategy, data)
	} else {
	}
}
func (n *Nextable[T]) Stop(strategy strategies.Strategy[T]) {
	if n.next != nil {
		n.next.Stop(strategy)
	}
}
