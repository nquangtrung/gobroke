package gobroke

import "sync"

func NewBroker() Broker {
	var wg sync.WaitGroup
	broker := &BrokerImpl{
		wg: &wg,
	}
	broker.Start()
	return broker
}
