/*
Copyright 2025 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
