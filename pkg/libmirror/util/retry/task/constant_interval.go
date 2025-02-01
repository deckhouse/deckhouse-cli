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

package task

import (
	"context"
	"time"
)

type ConstantRetryIntervalTask struct {
	maxRetries   uint
	waitInterval time.Duration
	payload      func(context.Context) error
}

func WithConstantRetries(maxRetries uint, waitInterval time.Duration, payload func(ctx context.Context) error) *ConstantRetryIntervalTask {
	task := &ConstantRetryIntervalTask{
		maxRetries:   maxRetries,
		waitInterval: waitInterval,
		payload:      payload,
	}

	if task.maxRetries == 0 {
		task.maxRetries = 1
	}
	if task.waitInterval <= 0 {
		task.waitInterval = time.Second
	}

	return task
}

func (s *ConstantRetryIntervalTask) Do(ctx context.Context, _ uint) error {
	return s.payload(ctx)
}

func (s *ConstantRetryIntervalTask) Interval(_ uint) time.Duration {
	return s.waitInterval
}

func (s *ConstantRetryIntervalTask) MaxRetries() uint {
	return s.maxRetries
}
