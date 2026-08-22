package chain

import "trontria.com/gobroke/strategies"

type Terminal[T any] struct{}

func (d *Terminal[T]) Then(node ChainNode[T])               {}
func (d *Terminal[T]) Stop(strategy strategies.Strategy[T]) {}
func (d *Terminal[T]) Drop(data T)                          {}
