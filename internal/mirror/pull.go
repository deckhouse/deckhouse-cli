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
	"time"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/installer"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/packages"
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
	// SkipPackages skips pulling package images
	SkipPackages bool
	// SkipInstaller skips pulling installer images
	SkipInstaller bool
	// InstallerTag is the tag for the installer image
	InstallerTag string
	// OnlyExtraImages pulls only extra images for modules (without main module images)
	OnlyExtraImages bool
	// IgnoreSuspend allows mirroring even if release channels are suspended
	IgnoreSuspend bool
	// PlatformConstraint selects platform releases by semver constraint
	// (--include-platform). When non-nil it replaces the default
	// rock-solid..alpha discovery window for the platform service. Exact-tag
	// constraints are routed through TargetTag inside platform.PullPlatform.
	PlatformConstraint modules.VersionConstraint
	// ModuleFilter is the filter for module selection (whitelist/blacklist)
	ModuleFilter *modules.Filter
	// PackageFilter is the filter for package selection (whitelist/blacklist).
	// Packages reuse the modules filter because selection logic is identical.
	PackageFilter *modules.Filter
	// BundleDir is the directory to store the bundle
	BundleDir string
	// BundleChunkSize is the max size of bundle chunks in bytes (0 = no chunking)
	BundleChunkSize int64
	// SkipVexImages allows skipping VEX images
	SkipVexImages bool
	// Timeout is the timeout for the pull operation
	Timeout time.Duration
	// DryRun prints the pull plan without downloading any image blobs
	DryRun bool
	// ProxyRegistry switches platform/module discovery from a single
	// catalog ListTags call (which proxy registries typically return
	// empty for) to a sequential probe of explicit version tags. The
	// CLI guarantees that --include-platform and/or --include-module
	// are supplied so the probe has a defined starting point.
	ProxyRegistry bool
}

type PullService struct {
	registryService *registryservice.Service

	platformService  *platform.Service
	securityService  *security.Service
	modulesService   *modules.Service
	packagesService  *packages.Service
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
				TargetTag:         targetTag,
				IncludeConstraint: options.PlatformConstraint,
				BundleDir:         options.BundleDir,
				BundleChunkSize:   options.BundleChunkSize,
				IgnoreSuspend:     options.IgnoreSuspend,
				SkipVexImages:     options.SkipVexImages,
				Timeout:           options.Timeout,
				DryRun:            options.DryRun,
				ProxyRegistry:     options.ProxyRegistry,
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
				Timeout:         options.Timeout,
				DryRun:          options.DryRun,
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
				SkipVexImages:   options.SkipVexImages,
				BundleDir:       options.BundleDir,
				BundleChunkSize: options.BundleChunkSize,
				Timeout:         options.Timeout,
				DryRun:          options.DryRun,
				ProxyRegistry:   options.ProxyRegistry,
			},
			logger,
			userLogger,
		),
		packagesService: packages.NewService(
			registryService,
			tmpDir,
			&packages.Options{
				Filter:          options.PackageFilter,
				OnlyExtraImages: options.OnlyExtraImages,
				SkipVexImages:   options.SkipVexImages,
				BundleDir:       options.BundleDir,
				BundleChunkSize: options.BundleChunkSize,
				Timeout:         options.Timeout,
				DryRun:          options.DryRun,
				ProxyRegistry:   options.ProxyRegistry,
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
				Timeout:         options.Timeout,
				DryRun:          options.DryRun,
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
	if svc.options.SkipVexImages {
		svc.userLogger.WarnLn("The skip-vex-images flag was detected: Vulnerability scanning may not work correctly when this flag is used.")
	}

	if !svc.options.SkipPlatform {
		err := svc.platformService.PullPlatform(ctx)
		if err != nil {
			return fmt.Errorf("pull platform: %w", err)
		}
	}

	if !svc.options.SkipInstaller {
		err := svc.installerService.PullInstaller(ctx)
		if err != nil {
			return fmt.Errorf("pull installer: %w", err)
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

	if !svc.options.SkipPackages || svc.options.OnlyExtraImages {
		err := svc.packagesService.PullPackages(ctx)
		if err != nil {
			return fmt.Errorf("pull packages: %w", err)
		}
	}

	return nil
}
