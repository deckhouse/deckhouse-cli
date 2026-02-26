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

	"github.com/deckhouse/deckhouse-cli/internal/mirror/installer"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/platform"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/security"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// PullServiceOptions contains configuration options for PullService
type PullServiceOptions struct {
	// SkipPlatform skips pulling platform images
	SkipPlatform bool
	// SkipSecurity skips pulling security databases
	SkipSecurity bool
	// SkipModules skips pulling module images
	SkipModules bool
	// SkipInstaller skips pulling installer images
	SkipInstaller bool
	// InstallerTag is the tag for the installer image
	InstallerTag string
	// OnlyExtraImages pulls only extra images for modules (without main module images)
	OnlyExtraImages bool
	// IgnoreSuspend allows mirroring even if release channels are suspended
	IgnoreSuspend bool
	// ModuleFilter is the filter for module selection (whitelist/blacklist)
	ModuleFilter *modules.Filter
	// BundleDir is the directory to store the bundle
	BundleDir string
	// BundleChunkSize is the max size of bundle chunks in bytes (0 = no chunking)
	BundleChunkSize int64
}

type PullService struct {
	registryService *registryservice.Service

	platformService  *platform.Service
	securityService  *security.Service
	modulesService   *modules.Service
	installerService *installer.Service

	options *PullServiceOptions

	// layout manages the OCI image layouts for different components
	layout *ImageLayouts

	// logger is for internal debug logging
	logger *dkplog.Logger
	// userLogger is for user-facing informational messages
	userLogger *log.SLogger
}

func NewPullService(
	registryService *registryservice.Service,
	tmpDir string,
	targetTag string,
	options *PullServiceOptions,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *PullService {
	if options == nil {
		options = &PullServiceOptions{}
	}

	return &PullService{
		registryService: registryService,

		platformService: platform.NewService(
			registryService,
			tmpDir,
			&platform.Options{
				TargetTag:       targetTag,
				BundleDir:       options.BundleDir,
				BundleChunkSize: options.BundleChunkSize,
				IgnoreSuspend:   options.IgnoreSuspend,
			},
			logger,
			userLogger,
		),
		securityService: security.NewService(
			registryService,
			tmpDir,
			&security.Options{
				BundleDir:       options.BundleDir,
				BundleChunkSize: options.BundleChunkSize,
			},
			logger,
			userLogger,
		),
		modulesService: modules.NewService(
			registryService,
			tmpDir,
			&modules.Options{
				Filter:          options.ModuleFilter,
				OnlyExtraImages: options.OnlyExtraImages,
				BundleDir:       options.BundleDir,
				BundleChunkSize: options.BundleChunkSize,
			},
			logger,
			userLogger,
		),
		installerService: installer.NewService(
			registryService,
			tmpDir,
			&installer.Options{
				TargetTag:       options.InstallerTag,
				BundleDir:       options.BundleDir,
				BundleChunkSize: options.BundleChunkSize,
			},
			logger,
			userLogger,
		),

		options: options,

		layout: NewImageLayouts(),

		logger:     logger,
		userLogger: userLogger,
	}
}

// Pull downloads Deckhouse components from registry
func (svc *PullService) Pull(ctx context.Context) error {
	if !svc.options.SkipPlatform {
		err := svc.platformService.PullPlatform(ctx)
		if err != nil {
			return fmt.Errorf("pull platform: %w", err)
		}
	}

	if !svc.options.SkipSecurity {
		err := svc.securityService.PullSecurity(ctx)
		if err != nil {
			return fmt.Errorf("pull security databases: %w", err)
		}
	}

	if !svc.options.SkipModules || svc.options.OnlyExtraImages {
		err := svc.modulesService.PullModules(ctx)
		if err != nil {
			return fmt.Errorf("pull modules: %w", err)
		}
	}

	if !svc.options.SkipInstaller {
		err := svc.installerService.PullInstaller(ctx)
		if err != nil {
			return fmt.Errorf("pull installer: %w", err)
		}
	}

	return nil
}
