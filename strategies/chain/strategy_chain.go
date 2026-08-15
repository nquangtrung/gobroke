package chain

import (
	"log"

	"trontria.com/gobroke/strategies"
)

type StrategyChain interface {
	strategies.Strategy
}

type StrategyChainWithNode struct {
	head ChainNode
}

func (s *StrategyChainWithNode) Receive(data any) {
	log.Printf("chain strategy received %s", data)
	s.head.Consume(s, data)
}

func (s *StrategyChainWithNode) Stop() {
	s.head.Stop(s)
}
func (s *StrategyChainWithNode) Drop(data any) {
	go s.head.Drop(data)
}
func NewStrategyChain(chain ChainNode) StrategyChain {
	strategy := &StrategyChainWithNode{
		head: chain,
	}

	return strategy
}

func NewStrategyFromSlice(nodes []ChainNode) StrategyChain {
	head := fromSlice(nodes)
	return NewStrategyChain(head)
}
