package retry

import (
	"fmt"
	"time"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
)

type Task interface {
	Do(retryCount uint) error
	Interval(retryCount uint) time.Duration
	MaxRetries() uint
}

func RunTask(logger contexts.Logger, name string, task Task) error {
	restarts := uint(0)
	var lastErr error
	for restarts < task.MaxRetries() {
		if restarts > 0 {
			interval := task.Interval(restarts)
			logger.InfoF("%s failed, next retry in %v", name, interval)
			time.Sleep(interval)
		}

		logger.InfoLn(name)
		lastErr = task.Do(restarts)
		if lastErr == nil {
			return nil
		}

		restarts += 1
	}

	return fmt.Errorf("%q: task failed to many times, last error: %w", name, lastErr)
}
