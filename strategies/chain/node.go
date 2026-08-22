package chain

import "trontria.com/gobroke/strategies"

type ChainNode[T any] interface {
	Consume(strategy strategies.Strategy[T], data T)
	Then(node ChainNode[T])
	Stop(strategy strategies.Strategy[T])
	Drop(data T)
}

func fromSlice[T any](nodes []ChainNode[T]) ChainNode[T] {
	head := nodes[0]
	current := head
	for i := 1; i < len(nodes); i++ {
		current.Then(nodes[i])
		current = nodes[i]
	}
	return head
}
