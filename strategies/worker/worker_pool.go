package worker

import "log"

type WorkerPool interface {
	Receive() chan any
	Start()
	Stop()
}

type ExecutionWorkerPool interface {
	Execute(data any)
}

type SingleWorkerPool struct {
	channel     chan any
	stopChannel chan any
	handler     func(data any)
}

func (s *SingleWorkerPool) Execute(data any) {
	s.handler(data)
}

func (s *SingleWorkerPool) Start() {
	for {
		select {
		case data := <-s.channel:
			s.Execute(data)
		case <-s.stopChannel:
			log.Println("worker receive stop signal, draining channel")
			s.Drain()
			defer close(s.stopChannel)
			defer close(s.channel)
			return
		}
	}
}

func (s *SingleWorkerPool) Drain() {
	for len(s.channel) > 0 {
		log.Printf("worker drains %d data left", len(s.channel))
		data := <-s.channel
		s.Execute(data)
	}
	log.Println("worker drained all data")
}

func (s *SingleWorkerPool) Stop() {
	s.stopChannel <- true
}

func (s *SingleWorkerPool) Receive() chan any {
	return s.channel
}

type MultiWorkerPool struct {
	SingleWorkerPool
	guard chan any
}

func (m *MultiWorkerPool) Execute(data any) {
	log.Printf("executing %v", data)
	m.guard <- data
	go func() {
		m.handler(data)
		<-m.guard
	}()
}

func NewSingleWorkerPool(bufferSize int, handler func(data any)) WorkerPool {
	return &SingleWorkerPool{
		handler:     handler,
		channel:     make(chan any, bufferSize),
		stopChannel: make(chan any),
	}
}

type MultipleWorkerPoolParams struct {
	MaxWorker  int
	BufferSize int
	Handler    func(data any)
}

func NewMultipleWorkerPool(params MultipleWorkerPoolParams) WorkerPool {
	return &MultiWorkerPool{
		guard: make(chan any, params.MaxWorker),
		SingleWorkerPool: SingleWorkerPool{
			handler:     params.Handler,
			channel:     make(chan any, params.BufferSize),
			stopChannel: make(chan any),
		},
	}
}
