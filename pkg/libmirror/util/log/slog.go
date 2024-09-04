package log

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"gitlab.com/greyxor/slogor"
)

const processPrefix = ">"

type SLogger struct {
	delegate     *slog.Logger
	processDepth int
}

func NewSLogger(logLevel slog.Level) *SLogger {
	return &SLogger{
		delegate: slog.New(slogor.NewHandler(os.Stdout, slogor.Options{
			TimeFormat: time.StampMilli,
			Level:      logLevel,
		})),
	}
}

func (s *SLogger) DebugF(format string, a ...any) {
	s.delegate.Debug(s.formatRecord(format, a...))
}

func (s *SLogger) DebugLn(a ...any) {
	s.delegate.Debug(s.formatRecord("", a...))
}

func (s *SLogger) InfoF(format string, a ...any) {
	s.delegate.Info(s.formatRecord(format, a...))
}

func (s *SLogger) InfoLn(a ...any) {
	s.delegate.Info(s.formatRecord("", a...))
}

func (s *SLogger) WarnF(format string, a ...any) {
	s.delegate.Warn(s.formatRecord(format, a...))
}

func (s *SLogger) WarnLn(a ...any) {
	s.delegate.Warn(s.formatRecord("", a...))
}

func (s *SLogger) Process(topic string, run func() error) error {
	s.delegate.Info(topic)
	s.processDepth += 1
	defer func() { s.processDepth -= 1 }()
	if err := run(); err != nil {
		s.delegate.Error(
			topic+" failed",
			"error", err)
		return err
	}
	s.delegate.Info(topic + " succeeded")
	return nil
}

func (s *SLogger) formatRecord(template string, args ...any) string {
	prefix := strings.Repeat(processPrefix, s.processDepth)

	if template == "" {
		msg := &strings.Builder{}
		msg.WriteString(prefix)
		for _, arg := range args {
			msg.WriteString(fmt.Sprintf(" %v", arg))
		}

		return msg.String()
	}

	return fmt.Sprintf(prefix+" "+template, args...)
}
