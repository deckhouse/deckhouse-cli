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

package usecase

import (
	"context"
	"io"

	"github.com/deckhouse/deckhouse-cli/internal/system/domain"
)

// ModuleService provides module operations
type ModuleService interface {
	// List lists all modules
	List(ctx context.Context) ([]domain.Module, error)
	// Enable enables a module
	Enable(ctx context.Context, name string) error
	// Disable disables a module
	Disable(ctx context.Context, name string) error
	// GetValues gets module values
	GetValues(ctx context.Context, name string) (*domain.ModuleValues, error)
	// GetSnapshots gets module snapshots
	GetSnapshots(ctx context.Context, name string) (*domain.ModuleSnapshot, error)
}

// ConfigService provides configuration operations
type ConfigService interface {
	// GetConfig gets cluster configuration
	GetConfig(ctx context.Context, configType domain.ConfigurationType) (*domain.ClusterConfiguration, error)
	// UpdateConfig updates cluster configuration
	UpdateConfig(ctx context.Context, configType domain.ConfigurationType, content string) error
}

// QueueService provides queue operations
type QueueService interface {
	// List lists all queues
	List(ctx context.Context) ([]domain.Queue, error)
	// GetMainQueue gets main queue info
	GetMainQueue(ctx context.Context) (*domain.Queue, error)
}

// LogService provides log streaming
type LogService interface {
	// StreamLogs streams logs from deckhouse
	StreamLogs(ctx context.Context, follow bool, output io.Writer) error
}

// DebugInfoCollector collects debug information
type DebugInfoCollector interface {
	// Collect collects debug info and writes to tarball
	Collect(ctx context.Context, outputPath string) error
}

// Logger provides logging
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

