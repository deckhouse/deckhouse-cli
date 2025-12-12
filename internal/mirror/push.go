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

package mirror

import (
	"context"
	"fmt"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/platform"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/security"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

// PushServiceOptions contains configuration options for PushService
type PushServiceOptions struct {
	// BundleDir is the directory containing the bundle to push
	BundleDir string
	// WorkingDir is the temporary directory for unpacking bundles
	WorkingDir string
	// ModulesPathSuffix is the path suffix for modules in registry
	ModulesPathSuffix string
}

// PushService orchestrates pushing Deckhouse components to registry
type PushService struct {
	platformService *platform.PushService
	securityService *security.PushService
	modulesService  *modules.PushService
}

// NewPushService creates a new PushService
func NewPushService(
	client registry.Client,
	options *PushServiceOptions,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *PushService {
	if options == nil {
		options = &PushServiceOptions{}
	}

	return &PushService{
		platformService: platform.NewPushService(
			client,
			&platform.PushOptions{
				BundleDir:  options.BundleDir,
				WorkingDir: options.WorkingDir,
			},
			logger.Named("platform"),
			userLogger,
		),
		securityService: security.NewPushService(
			client,
			&security.PushOptions{
				BundleDir:  options.BundleDir,
				WorkingDir: options.WorkingDir,
			},
			logger.Named("security"),
			userLogger,
		),
		modulesService: modules.NewPushService(
			client.WithSegment(options.ModulesPathSuffix),
			&modules.PushOptions{
				BundleDir:  options.BundleDir,
				WorkingDir: options.WorkingDir,
			},
			logger.Named("modules"),
			userLogger,
		),
	}
}

// Push uploads Deckhouse components to registry
func (svc *PushService) Push(ctx context.Context) error {
	// Push platform package
	if err := svc.platformService.Push(ctx); err != nil {
		return fmt.Errorf("push platform: %w", err)
	}

	// Push security package
	if err := svc.securityService.Push(ctx); err != nil {
		return fmt.Errorf("push security databases: %w", err)
	}

	// Push module packages
	if err := svc.modulesService.Push(ctx); err != nil {
		return fmt.Errorf("push modules: %w", err)
	}

	return nil
}
