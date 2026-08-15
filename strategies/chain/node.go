package chain

import "trontria.com/gobroke/strategies"

type ChainNode interface {
	Consume(strategy strategies.Strategy, data any)
	Then(node ChainNode) ChainNode
	Stop(strategy strategies.Strategy)
}

func fromSlice(nodes []ChainNode) ChainNode {
	head := nodes[0]
	current := head
	for i := 1; i < len(nodes); i++ {
		current.Then(nodes[i])
		current = nodes[i]
	}
	return head
}
