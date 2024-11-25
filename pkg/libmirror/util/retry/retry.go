package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
)

type Task interface {
	Do(ctx context.Context, retryCount uint) error
	Interval(retryCount uint) time.Duration
	MaxRetries() uint
}

func RunTask(logger contexts.Logger, name string, task Task) error {
	return RunTaskWithContext(context.Background(), logger, name, task)
}

func RunTaskWithContext(ctx context.Context, logger contexts.Logger, name string, task Task) error {
	restarts := uint(0)
	var lastErr error
	for restarts < task.MaxRetries() {
		if restarts > 0 {
			interval := task.Interval(restarts)
			logger.InfoF("%s failed, next retry in %v", name, interval)
			select {
			case <-time.After(interval):
				// Pause completed, proceed with next attempt
			case <-ctx.Done():
				return fmt.Errorf("%q: task cancelled during retry wait: %w", name, ctx.Err())
			}
		}

		logger.InfoLn(name)
		lastErr = task.Do(ctx, restarts)
		if lastErr == nil {
			return nil
		}

		restarts += 1
	}

	return fmt.Errorf("%q: task failed to many times, last error: %w", name, lastErr)
}
