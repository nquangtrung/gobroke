package chain

import "trontria.com/gobroke/strategies"

type Terminal struct{}

func (d *Terminal) Then(node ChainNode)               {}
func (d *Terminal) Stop(strategy strategies.Strategy) {}
func (d *Terminal) Drop(data any)                     {}
