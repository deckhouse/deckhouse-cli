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

package usecase

import (
	"context"
	"fmt"
)

// PullUseCase orchestrates the pull operation for Deckhouse components
type PullUseCase struct {
	// Domain services
	platformPuller PlatformPuller
	modulesPuller  ModulesPuller
	securityPuller SecurityPuller

	// Infrastructure
	bundlePacker BundlePacker
	logger       Logger

	// Configuration
	opts *PullOpts
}

// PlatformPuller handles platform-specific pull operations
type PlatformPuller interface {
	// Pull downloads platform images and creates the platform bundle
	Pull(ctx context.Context) error
}

// ModulesPuller handles module-specific pull operations
type ModulesPuller interface {
	// Pull downloads module images and creates module bundles
	Pull(ctx context.Context) error
}

// SecurityPuller handles security database pull operations
type SecurityPuller interface {
	// Pull downloads security databases and creates the security bundle
	Pull(ctx context.Context) error
}

// NewPullUseCase creates a new PullUseCase with the provided dependencies
func NewPullUseCase(
	platformPuller PlatformPuller,
	modulesPuller ModulesPuller,
	securityPuller SecurityPuller,
	bundlePacker BundlePacker,
	logger Logger,
	opts *PullOpts,
) *PullUseCase {
	return &PullUseCase{
		platformPuller: platformPuller,
		modulesPuller:  modulesPuller,
		securityPuller: securityPuller,
		bundlePacker:   bundlePacker,
		logger:         logger,
		opts:           opts,
	}
}

// Execute runs the pull operation
func (uc *PullUseCase) Execute(ctx context.Context) error {
	if err := uc.pullPlatform(ctx); err != nil {
		return err
	}

	if err := uc.pullSecurity(ctx); err != nil {
		return err
	}

	if err := uc.pullModules(ctx); err != nil {
		return err
	}

	return nil
}

func (uc *PullUseCase) pullPlatform(ctx context.Context) error {
	if uc.opts.SkipPlatform {
		uc.logger.Info("Skipping platform pull (--no-platform flag)")
		return nil
	}

	if uc.platformPuller == nil {
		return fmt.Errorf("platform puller is not configured")
	}

	return uc.logger.Process("Pull Deckhouse Platform", func() error {
		if err := uc.platformPuller.Pull(ctx); err != nil {
			return fmt.Errorf("pull platform: %w", err)
		}
		return nil
	})
}

func (uc *PullUseCase) pullSecurity(ctx context.Context) error {
	if uc.opts.SkipSecurity {
		uc.logger.Info("Skipping security databases pull (--no-security flag)")
		return nil
	}

	if uc.securityPuller == nil {
		return fmt.Errorf("security puller is not configured")
	}

	return uc.logger.Process("Pull Security Databases", func() error {
		if err := uc.securityPuller.Pull(ctx); err != nil {
			return fmt.Errorf("pull security databases: %w", err)
		}
		return nil
	})
}

func (uc *PullUseCase) pullModules(ctx context.Context) error {
	if uc.opts.SkipModules && !uc.opts.OnlyExtraImages {
		uc.logger.Info("Skipping modules pull (--no-modules flag)")
		return nil
	}

	if uc.modulesPuller == nil {
		return fmt.Errorf("modules puller is not configured")
	}

	processName := "Pull Modules"
	if uc.opts.OnlyExtraImages {
		processName = "Pull Extra Images"
	}

	return uc.logger.Process(processName, func() error {
		if err := uc.modulesPuller.Pull(ctx); err != nil {
			return fmt.Errorf("pull modules: %w", err)
		}
		return nil
	})
}

