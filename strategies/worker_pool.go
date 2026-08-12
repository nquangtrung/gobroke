package strategies

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
			defer close(s.stopChannel)
			defer close(s.channel)
			return
		}
	}
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
	m.guard <- data
	go func() {
		m.handler(data)
		<-m.guard
	}()
}

func NewSingleWorkerPool(handler func(data any)) WorkerPool {
	return &SingleWorkerPool{
		handler: handler,
	}
}

func NewMultipleWorkerPool(maxWorker int, handler func(data any)) WorkerPool {
	return &MultiWorkerPool{
		guard: make(chan any, maxWorker),
		SingleWorkerPool: SingleWorkerPool{
			handler: handler,
		},
	}
}
