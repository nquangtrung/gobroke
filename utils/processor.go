package utils

import "log"

type Processor interface {
	Process(data any)
	CleanUp(channel chan any)
}

type Runner interface {
	Start()
	Stop()
	Receive() chan any
}

type BaseRunner struct {
	channel     chan any
	stopChannel chan any
	processor   Processor
}

func NewBaseRunner(buffer int, processor Processor) *BaseRunner {
	return &BaseRunner{
		channel:     make(chan any, buffer),
		stopChannel: make(chan any),
		processor:   processor,
	}
}
func (r BaseRunner) Receive() chan any {
	return r.channel
}

func (r *BaseRunner) Start() {
	for {
		select {
		case data := <-r.channel:
			r.processor.Process(data)
		case <-r.stopChannel:
			r.Drain()
			close(r.channel)
			close(r.stopChannel)
			r.processor.CleanUp(r.channel)
			return
		}
	}
}

func (r *BaseRunner) Drain() {
	log.Printf("draining the rest of the channel: %d", len(r.channel))
	for len(r.channel) > 0 {
		data := <-r.channel
		r.processor.Process(data)
	}
}

func (r *BaseRunner) Stop() {
	r.stopChannel <- true
}
