package retry

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

var testLogger = log.NewSLogger(slog.LevelDebug)

func TestRunSuccessfulTask(t *testing.T) {
	task := &successfulTask{}

	require.NoErrorf(t, RunTask(context.TODO(), testLogger, "TestRunSuccessfulTask", task), "Task should run without errors")
	require.Equalf(t, uint(1), task.runCount, "Task should only be called once")
}

type successfulTask struct {
	runCount uint
}

func (s *successfulTask) Do(ctx context.Context, _ uint) error {
	s.runCount += 1
	return nil
}

func (s *successfulTask) Interval(_ uint) time.Duration {
	return time.Second
}

func (s *successfulTask) MaxRetries() uint {
	return 2
}

func TestRunFailingTask(t *testing.T) {
	task := &failingTask{}
	require.ErrorContainsf(t, RunTask(context.TODO(), testLogger, "TestRunFailingTask", task), "failing task", "Task should fail with error")
	require.Equalf(t, uint(5), task.runCount, "Task should run 5 times")
	require.Equalf(t, uint(4), task.reportedRetryCount, "Task should be retried 4 times")
}

type failingTask struct {
	runCount           uint
	reportedRetryCount uint
}

func (s *failingTask) Do(ctx context.Context, retryCount uint) error {
	s.runCount += 1
	s.reportedRetryCount = retryCount
	return errors.New("failing task")
}

func (s *failingTask) Interval(_ uint) time.Duration {
	return 50 * time.Millisecond
}

func (s *failingTask) MaxRetries() uint {
	return 5
}

func TestRunEventuallySuccessfulTask(t *testing.T) {
	task := &eventualSuccessTask{}
	require.NoErrorf(t, RunTask(context.TODO(), testLogger, "TestRunEventuallySuccessfulTask", task), "Task should not fail")
	require.Equalf(t, uint(2), task.runCount, "Task should run 2 times")
}

type eventualSuccessTask struct {
	runCount uint
}

func (s *eventualSuccessTask) Do(ctx context.Context, _ uint) error {
	s.runCount += 1
	if s.runCount > 0 && s.runCount%2 == 0 {
		return nil
	}
	return errors.New("failing task")
}

func (s *eventualSuccessTask) Interval(_ uint) time.Duration {
	return 50 * time.Millisecond
}

func (s *eventualSuccessTask) MaxRetries() uint {
	return 4
}
