package worker

import (
	"log"

	"trontria.com/gobroke/utils"
)

type MultiWorkerPoolProcessor[T any] struct {
	guard   chan any
	handler func(data T)
}

func (m MultiWorkerPoolProcessor[T]) Process(data T) {
	log.Printf("executing %v, spawned %d/%d", data, len(m.guard), cap(m.guard))
	m.guard <- data
	go func() {
		m.handler(data)
		<-m.guard
	}()
}

func (m MultiWorkerPoolProcessor[T]) CleanUp(channel chan T) {
	close(m.guard)
}

type MultipleWorkerPoolParams[T any] struct {
	MaxWorker  int
	BufferSize int
	Handler    func(data T)
}

func NewMultipleWorkerPool[T any](params MultipleWorkerPoolParams[T]) *utils.BaseRunner[T] {
	return utils.NewBaseRunner[T](params.BufferSize, MultiWorkerPoolProcessor[T]{
		handler: params.Handler,
		guard:   make(chan any, params.MaxWorker),
	})
}
