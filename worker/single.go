package worker

import (
	"log"

	"trontria.com/gobroke/utils"
)

type SingleWorkerPoolProcessor[T any] struct {
	handler func(data T)
}

func (s SingleWorkerPoolProcessor[T]) Process(data T) {
	log.Printf("executing %v on 1 worker", data)
	s.handler(data)
}

func (s SingleWorkerPoolProcessor[T]) CleanUp(channel chan T) {
}

func NewSingleWorkerPool[T any](bufferSize int, handler func(data T)) *utils.BaseRunner[T] {
	return utils.NewBaseRunner(bufferSize, SingleWorkerPoolProcessor[T]{
		handler: handler,
	})
}
