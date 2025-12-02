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

// PushUseCase orchestrates the push operation for Deckhouse components
type PushUseCase struct {
	// Domain services
	platformPusher PlatformPusher
	modulesPusher  ModulesPusher
	securityPusher SecurityPusher

	// Infrastructure
	logger Logger

	// Configuration
	opts *PushOpts
}

// PlatformPusher handles platform-specific push operations
type PlatformPusher interface {
	// Push uploads platform images from bundle to registry
	Push(ctx context.Context) error
}

// ModulesPusher handles module-specific push operations
type ModulesPusher interface {
	// Push uploads module images from bundle to registry
	Push(ctx context.Context) error
}

// SecurityPusher handles security database push operations
type SecurityPusher interface {
	// Push uploads security databases from bundle to registry
	Push(ctx context.Context) error
}

// NewPushUseCase creates a new PushUseCase with the provided dependencies
func NewPushUseCase(
	platformPusher PlatformPusher,
	modulesPusher ModulesPusher,
	securityPusher SecurityPusher,
	logger Logger,
	opts *PushOpts,
) *PushUseCase {
	return &PushUseCase{
		platformPusher: platformPusher,
		modulesPusher:  modulesPusher,
		securityPusher: securityPusher,
		logger:         logger,
		opts:           opts,
	}
}

// Execute runs the push operation
func (uc *PushUseCase) Execute(ctx context.Context) error {
	if err := uc.pushPlatform(ctx); err != nil {
		return err
	}

	if err := uc.pushSecurity(ctx); err != nil {
		return err
	}

	if err := uc.pushModules(ctx); err != nil {
		return err
	}

	return nil
}

func (uc *PushUseCase) pushPlatform(ctx context.Context) error {
	if uc.platformPusher == nil {
		uc.logger.Debug("Platform pusher not configured, skipping")
		return nil
	}

	return uc.logger.Process("Push Deckhouse Platform", func() error {
		if err := uc.platformPusher.Push(ctx); err != nil {
			return fmt.Errorf("push platform: %w", err)
		}
		return nil
	})
}

func (uc *PushUseCase) pushSecurity(ctx context.Context) error {
	if uc.securityPusher == nil {
		uc.logger.Debug("Security pusher not configured, skipping")
		return nil
	}

	return uc.logger.Process("Push Security Databases", func() error {
		if err := uc.securityPusher.Push(ctx); err != nil {
			return fmt.Errorf("push security databases: %w", err)
		}
		return nil
	})
}

func (uc *PushUseCase) pushModules(ctx context.Context) error {
	if uc.modulesPusher == nil {
		uc.logger.Debug("Modules pusher not configured, skipping")
		return nil
	}

	return uc.logger.Process("Push Modules", func() error {
		if err := uc.modulesPusher.Push(ctx); err != nil {
			return fmt.Errorf("push modules: %w", err)
		}
		return nil
	})
}

// PushOpts contains all configuration for the push operation
type PushOpts struct {
	// BundleDir is the directory containing the bundle to push
	BundleDir string
	// WorkingDir is the temporary directory for intermediate files
	WorkingDir string

	// Registry configuration
	RegistryHost string
	RegistryPath string
	
	// ModulesPathSuffix is the path suffix for modules in the registry
	ModulesPathSuffix string

	// Parallelism configuration
	BlobParallelism  int
	ImageParallelism int
}

