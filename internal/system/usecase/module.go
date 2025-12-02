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

	"github.com/deckhouse/deckhouse-cli/internal/system/domain"
)

// ModuleListUseCase handles module listing
type ModuleListUseCase struct {
	moduleService ModuleService
	logger        Logger
}

// NewModuleListUseCase creates a new ModuleListUseCase
func NewModuleListUseCase(moduleService ModuleService, logger Logger) *ModuleListUseCase {
	return &ModuleListUseCase{
		moduleService: moduleService,
		logger:        logger,
	}
}

// Execute lists all modules
func (uc *ModuleListUseCase) Execute(ctx context.Context) ([]domain.Module, error) {
	modules, err := uc.moduleService.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list modules: %w", err)
	}
	return modules, nil
}

// ModuleEnableUseCase handles module enabling
type ModuleEnableUseCase struct {
	moduleService ModuleService
	logger        Logger
}

// NewModuleEnableUseCase creates a new ModuleEnableUseCase
func NewModuleEnableUseCase(moduleService ModuleService, logger Logger) *ModuleEnableUseCase {
	return &ModuleEnableUseCase{
		moduleService: moduleService,
		logger:        logger,
	}
}

// Execute enables a module
func (uc *ModuleEnableUseCase) Execute(ctx context.Context, moduleName string) error {
	if err := uc.moduleService.Enable(ctx, moduleName); err != nil {
		return fmt.Errorf("enable module %s: %w", moduleName, err)
	}
	uc.logger.Info("Module enabled", "name", moduleName)
	return nil
}

// ModuleDisableUseCase handles module disabling
type ModuleDisableUseCase struct {
	moduleService ModuleService
	logger        Logger
}

// NewModuleDisableUseCase creates a new ModuleDisableUseCase
func NewModuleDisableUseCase(moduleService ModuleService, logger Logger) *ModuleDisableUseCase {
	return &ModuleDisableUseCase{
		moduleService: moduleService,
		logger:        logger,
	}
}

// Execute disables a module
func (uc *ModuleDisableUseCase) Execute(ctx context.Context, moduleName string) error {
	if err := uc.moduleService.Disable(ctx, moduleName); err != nil {
		return fmt.Errorf("disable module %s: %w", moduleName, err)
	}
	uc.logger.Info("Module disabled", "name", moduleName)
	return nil
}

// ModuleValuesUseCase handles module values retrieval
type ModuleValuesUseCase struct {
	moduleService ModuleService
	logger        Logger
}

// NewModuleValuesUseCase creates a new ModuleValuesUseCase
func NewModuleValuesUseCase(moduleService ModuleService, logger Logger) *ModuleValuesUseCase {
	return &ModuleValuesUseCase{
		moduleService: moduleService,
		logger:        logger,
	}
}

// Execute gets module values
func (uc *ModuleValuesUseCase) Execute(ctx context.Context, moduleName string) (*domain.ModuleValues, error) {
	values, err := uc.moduleService.GetValues(ctx, moduleName)
	if err != nil {
		return nil, fmt.Errorf("get values for module %s: %w", moduleName, err)
	}
	return values, nil
}

// ModuleSnapshotsUseCase handles module snapshots retrieval
type ModuleSnapshotsUseCase struct {
	moduleService ModuleService
	logger        Logger
}

// NewModuleSnapshotsUseCase creates a new ModuleSnapshotsUseCase
func NewModuleSnapshotsUseCase(moduleService ModuleService, logger Logger) *ModuleSnapshotsUseCase {
	return &ModuleSnapshotsUseCase{
		moduleService: moduleService,
		logger:        logger,
	}
}

// Execute gets module snapshots
func (uc *ModuleSnapshotsUseCase) Execute(ctx context.Context, moduleName string) (*domain.ModuleSnapshot, error) {
	snapshots, err := uc.moduleService.GetSnapshots(ctx, moduleName)
	if err != nil {
		return nil, fmt.Errorf("get snapshots for module %s: %w", moduleName, err)
	}
	return snapshots, nil
}

