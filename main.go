package gobroke

func NewBroker() Broker {
	broker := &BrokerImpl{}
	broker.Start()
	return broker
}
