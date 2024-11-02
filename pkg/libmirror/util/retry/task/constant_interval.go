package task

import (
	"context"
	"time"
)

type ConstantRetryIntervalTask struct {
	maxRetries   uint
	waitInterval time.Duration
	payload      func(ctx context.Context) error
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
