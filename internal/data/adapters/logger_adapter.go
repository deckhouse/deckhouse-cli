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

package adapters

import (
	"log/slog"

	"github.com/deckhouse/deckhouse-cli/internal/data/usecase"
)

// Compile-time check that SlogAdapter implements usecase.Logger
var _ usecase.Logger = (*SlogAdapter)(nil)

// SlogAdapter adapts *slog.Logger to usecase.Logger interface
type SlogAdapter struct {
	log *slog.Logger
}

// NewSlogAdapter creates a new SlogAdapter
func NewSlogAdapter(log *slog.Logger) *SlogAdapter {
	return &SlogAdapter{log: log}
}

func (a *SlogAdapter) Info(msg string, args ...any) {
	a.log.Info(msg, args...)
}

func (a *SlogAdapter) Warn(msg string, args ...any) {
	a.log.Warn(msg, args...)
}

func (a *SlogAdapter) Error(msg string, args ...any) {
	a.log.Error(msg, args...)
}

func (a *SlogAdapter) Debug(msg string, args ...any) {
	a.log.Debug(msg, args...)
}

