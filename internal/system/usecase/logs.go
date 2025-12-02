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
	"fmt"
	"io"
)

// LogsUseCase handles log streaming
type LogsUseCase struct {
	logService LogService
	logger     Logger
}

// NewLogsUseCase creates a new LogsUseCase
func NewLogsUseCase(logService LogService, logger Logger) *LogsUseCase {
	return &LogsUseCase{
		logService: logService,
		logger:     logger,
	}
}

// LogsParams contains parameters for log streaming
type LogsParams struct {
	Follow bool
}

// Execute streams logs
func (uc *LogsUseCase) Execute(ctx context.Context, params *LogsParams, output io.Writer) error {
	if err := uc.logService.StreamLogs(ctx, params.Follow, output); err != nil {
		return fmt.Errorf("stream logs: %w", err)
	}
	return nil
}

