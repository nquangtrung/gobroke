package worker

import (
	"log"

	"trontria.com/gobroke/utils"
)

type MultiWorkerPoolProcessor struct {
	guard   chan any
	handler func(data any)
}

func (m MultiWorkerPoolProcessor) Process(data any) {
	log.Printf("executing %v, spawned %d/%d", data, len(m.guard), cap(m.guard))
	m.guard <- data
	go func() {
		m.handler(data)
		<-m.guard
	}()
}

func (m MultiWorkerPoolProcessor) CleanUp(channel chan any) {
	close(m.guard)
}

type MultipleWorkerPoolParams struct {
	MaxWorker  int
	BufferSize int
	Handler    func(data any)
}

func NewMultipleWorkerPool(params MultipleWorkerPoolParams) *utils.BaseRunner {
	return utils.NewBaseRunner(params.BufferSize, MultiWorkerPoolProcessor{
		handler: params.Handler,
		guard:   make(chan any, params.MaxWorker),
	})
}
