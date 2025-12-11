package util //nolint: revive

import (
	"log/slog"
	"os"
)

func SetupLogger() *slog.Logger {
	logLevel := slog.LevelInfo

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))

	return logger
}
