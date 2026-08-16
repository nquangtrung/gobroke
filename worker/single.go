package worker

import (
	"log"

	"trontria.com/gobroke/utils"
)

type SingleWorkerPoolProcessor struct {
	handler func(data any)
}

func (s SingleWorkerPoolProcessor) Process(data any) {
	log.Printf("executing %v on 1 worker", data)
	s.handler(data)
}

func (s SingleWorkerPoolProcessor) CleanUp(channel chan any) {
}

func NewSingleWorkerPool(bufferSize int, handler func(data any)) *utils.BaseRunner {
	return utils.NewBaseRunner(bufferSize, SingleWorkerPoolProcessor{
		handler: handler,
	})
}
