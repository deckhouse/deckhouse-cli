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
	"log"

	"github.com/deckhouse/deckhouse-cli/internal/backup/usecase"
)

// Compile-time check
var _ usecase.Logger = (*SlogLogger)(nil)

// SlogLogger adapts standard log to usecase.Logger
type SlogLogger struct{}

// NewSlogLogger creates a new SlogLogger
func NewSlogLogger() *SlogLogger {
	return &SlogLogger{}
}

func (l *SlogLogger) Info(msg string, args ...any) {
	log.Println(append([]any{"INFO:", msg}, args...)...)
}

func (l *SlogLogger) Warn(msg string, args ...any) {
	log.Println(append([]any{"WARN:", msg}, args...)...)
}

func (l *SlogLogger) Error(msg string, args ...any) {
	log.Println(append([]any{"ERROR:", msg}, args...)...)
}
