package task

import "time"

type ConstantRetryIntervalTask struct {
	maxRetries   uint
	waitInterval time.Duration
	payload      func() error
}

func WithConstantRetries(maxRetries uint, waitInterval time.Duration, payload func() error) *ConstantRetryIntervalTask {
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

func (s *ConstantRetryIntervalTask) Do(_ uint) error {
	return s.payload()
}

func (s *ConstantRetryIntervalTask) Interval(_ uint) time.Duration {
	return s.waitInterval
}

func (s *ConstantRetryIntervalTask) MaxRetries() uint {
	return s.maxRetries
}
