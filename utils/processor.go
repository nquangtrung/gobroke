package utils

type Processor interface {
	Process(data any)
	CleanUp()
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
			close(r.channel)
			r.processor.CleanUp()
			return
		}
	}
}

func (r *BaseRunner) Stop() {
	r.stopChannel <- true
}
