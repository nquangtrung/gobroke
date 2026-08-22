package chain

import (
	"log"

	"trontria.com/gobroke/strategies"
)

type StrategyChain[T any] interface {
	strategies.Strategy[T]
}

type StrategyChainWithNode[T any] struct {
	head ChainNode[T]
}

func (s *StrategyChainWithNode[T]) Receive(data T) {
	log.Printf("chain strategy received %v", data)
	s.head.Consume(s, data)
}

func (s *StrategyChainWithNode[T]) Stop() {
	s.head.Stop(s)
}
func (s *StrategyChainWithNode[T]) Drop(data T) {
	go s.head.Drop(data)
}
func New[T any](chain ChainNode[T]) StrategyChain[T] {
	strategy := &StrategyChainWithNode[T]{
		head: chain,
	}

	return strategy
}

func NewFromSlice[T any](nodes []ChainNode[T]) StrategyChain[T] {
	head := fromSlice(nodes)
	return New(head)
}
