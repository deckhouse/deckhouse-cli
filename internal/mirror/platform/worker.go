package platform

import (
	"sync"
)

type ConcurrentWorker[T, R any] struct {
	workersCount int
}

func NewConcurrentWorker[T, R any](workersCount int) *ConcurrentWorker[T, R] {
	return &ConcurrentWorker[T, R]{
		workersCount: workersCount,
	}
}

func (cw *ConcurrentWorker[T, R]) Do(inputs []T, handler func(input T) (R, error), resultHandler func(item R) error) error {
	wg := new(sync.WaitGroup)
	semaphore := make(chan struct{}, cw.workersCount)
	stop := make(chan struct{})
	resultChannel := make(chan R, 10)
	errChannel := make(chan error)

	go func() {
		for result := range resultChannel {
			if resultHandler == nil {
				continue
			}

			if err := resultHandler(result); err != nil {
				errChannel <- err

				return
			}
		}

		stop <- struct{}{}
	}()

	for _, input := range inputs {
		semaphore <- struct{}{}
		wg.Add(1)

		go func(input T) {
			defer func() { <-semaphore }()
			defer wg.Done()

			result, err := handler(input)
			if err != nil {
				errChannel <- err

				return
			}

			resultChannel <- result
		}(input)
	}

	select {
	case err := <-errChannel:
		return err
	case <-stop:
		close(resultChannel)
		return nil
	}
}
