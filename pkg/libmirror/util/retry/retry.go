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

package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
)

type Task interface {
	Do(ctx context.Context, retryCount uint) error
	Interval(retryCount uint) time.Duration
	MaxRetries() uint
}

func RunTask(logger params.Logger, name string, task Task) error {
	return RunTaskWithContext(context.Background(), logger, name, task)
}

func RunTaskWithContext(ctx context.Context, logger params.Logger, name string, task Task) error {
	restarts := uint(0)
	var lastErr error
	for restarts < task.MaxRetries() {
		if restarts > 0 {
			interval := task.Interval(restarts)
			logger.Infof("%s failed, next retry in %v", name, interval)
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

		restarts++
	}

	return fmt.Errorf("%q: task failed to many times, last error: %w", name, lastErr)
}
