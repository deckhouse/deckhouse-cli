/*
Copyright 2024 Flant JSC

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

package usecase

import (
	"context"
	"fmt"

	"github.com/deckhouse/deckhouse-cli/internal/system/domain"
)

// QueueListUseCase handles queue listing
type QueueListUseCase struct {
	queueService QueueService
	logger       Logger
}

// NewQueueListUseCase creates a new QueueListUseCase
func NewQueueListUseCase(queueService QueueService, logger Logger) *QueueListUseCase {
	return &QueueListUseCase{
		queueService: queueService,
		logger:       logger,
	}
}

// Execute lists all queues
func (uc *QueueListUseCase) Execute(ctx context.Context) ([]domain.Queue, error) {
	queues, err := uc.queueService.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list queues: %w", err)
	}
	return queues, nil
}

// QueueMainUseCase handles main queue retrieval
type QueueMainUseCase struct {
	queueService QueueService
	logger       Logger
}

// NewQueueMainUseCase creates a new QueueMainUseCase
func NewQueueMainUseCase(queueService QueueService, logger Logger) *QueueMainUseCase {
	return &QueueMainUseCase{
		queueService: queueService,
		logger:       logger,
	}
}

// Execute gets main queue info
func (uc *QueueMainUseCase) Execute(ctx context.Context) (*domain.Queue, error) {
	queue, err := uc.queueService.GetMainQueue(ctx)
	if err != nil {
		return nil, fmt.Errorf("get main queue: %w", err)
	}
	return queue, nil
}

