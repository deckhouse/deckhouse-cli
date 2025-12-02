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

// EditConfigUseCase handles configuration editing
type EditConfigUseCase struct {
	configService ConfigService
	logger        Logger
}

// NewEditConfigUseCase creates a new EditConfigUseCase
func NewEditConfigUseCase(configService ConfigService, logger Logger) *EditConfigUseCase {
	return &EditConfigUseCase{
		configService: configService,
		logger:        logger,
	}
}

// EditParams contains parameters for editing configuration
type EditParams struct {
	ConfigType domain.ConfigurationType
}

// Execute gets configuration for editing
func (uc *EditConfigUseCase) Execute(ctx context.Context, params *EditParams) (*domain.ClusterConfiguration, error) {
	config, err := uc.configService.GetConfig(ctx, params.ConfigType)
	if err != nil {
		return nil, fmt.Errorf("get config %s: %w", params.ConfigType, err)
	}
	return config, nil
}

// SaveConfig saves updated configuration
func (uc *EditConfigUseCase) SaveConfig(ctx context.Context, configType domain.ConfigurationType, content string) error {
	if err := uc.configService.UpdateConfig(ctx, configType, content); err != nil {
		return fmt.Errorf("update config %s: %w", configType, err)
	}
	uc.logger.Info("Configuration updated", "type", string(configType))
	return nil
}

