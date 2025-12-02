/*
Copyright 2025 Flant JSC

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

package adapters

import (
	"github.com/deckhouse/deckhouse-cli/internal/mirror/usecase"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

// Compile-time interface check
var _ usecase.Logger = (*LoggerAdapter)(nil)

// LoggerAdapter adapts log.SLogger to usecase.Logger
type LoggerAdapter struct {
	logger *log.SLogger
}

// NewLoggerAdapter creates a new logger adapter
func NewLoggerAdapter(logger *log.SLogger) *LoggerAdapter {
	return &LoggerAdapter{logger: logger}
}

func (a *LoggerAdapter) Info(msg string) {
	a.logger.InfoLn(msg)
}

func (a *LoggerAdapter) Infof(format string, args ...interface{}) {
	a.logger.Infof(format, args...)
}

func (a *LoggerAdapter) Warn(msg string) {
	a.logger.WarnLn(msg)
}

func (a *LoggerAdapter) Warnf(format string, args ...interface{}) {
	a.logger.Warnf(format, args...)
}

func (a *LoggerAdapter) Debug(msg string) {
	a.logger.DebugLn(msg)
}

func (a *LoggerAdapter) Debugf(format string, args ...interface{}) {
	a.logger.Debugf(format, args...)
}

func (a *LoggerAdapter) Process(name string, fn func() error) error {
	return a.logger.Process(name, fn)
}

// Underlying returns the underlying SLogger for cases where direct access is needed
func (a *LoggerAdapter) Underlying() *log.SLogger {
	return a.logger
}

