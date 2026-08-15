package chain

import (
	"log"

	"trontria.com/gobroke/strategies"
)

type Nextable struct {
	next ChainNode
}

func (n *Nextable) Then(node ChainNode) ChainNode {
	n.next = node
	return node
}
func (n *Nextable) Next(strategy strategies.Strategy, data any) {
	if n.next != nil {
		log.Printf("Executing next node")
		n.next.Consume(strategy, data)
	} else {
		log.Printf("This is the last node, end")
	}
}
func (n *Nextable) Stop(strategy strategies.Strategy) {
	if n.next != nil {
		n.next.Stop(strategy)
	}
}
