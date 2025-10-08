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

package log

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"gitlab.com/greyxor/slogor"
)

const processPrefix = "║"

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
	start := time.Now()
	s.delegate.Info(strings.Repeat("║", s.processDepth) + "╔ " + topic)
	s.processDepth += 1
	defer func() { s.processDepth -= 1 }()
	if err := run(); err != nil {
		s.delegate.Error(
			strings.Repeat("║", s.processDepth-1)+topic+" failed",
			"error", err)
		return err
	}
	s.delegate.Info(strings.Repeat("║", s.processDepth-1) + "╚ " + topic + " succeeded in " + time.Since(start).String())
	return nil
}

func (s *SLogger) formatRecord(template string, args ...any) string {
	prefix := strings.Repeat(processPrefix, s.processDepth)

	if template == "" {
		msg := &strings.Builder{}
		msg.WriteString(prefix)
		for _, arg := range args {
			fmt.Fprintf(msg, " %v", arg)
		}

		return msg.String()
	}

	return fmt.Sprintf(prefix+" "+template, args...)
}
