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

	"github.com/Masterminds/semver/v3"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/installer"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/packages"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/platform"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/plugins"
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
	// PluginFilter carries --include-plugin entries (whitelist, additive to
	// the module-driven plugin auto-selection). May be nil.
	PluginFilter *modules.Filter
	// PluginBuiltins are d8 built-in command names that satisfy a same-named
	// plugin dependency by presence (never pulled).
	PluginBuiltins []string
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
	pluginsService   *plugins.Service

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
		pluginsService: plugins.NewService(
			registryService,
			tmpDir,
			&plugins.Options{
				Filter:          options.PluginFilter,
				Builtins:        builtinsSet(options.PluginBuiltins),
				BundleDir:       options.BundleDir,
				BundleChunkSize: options.BundleChunkSize,
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

// Pull downloads Deckhouse components from registry.
//
// It returns a PullSummary describing what was pulled (or planned, in dry-run).
// The summary is assembled incrementally as each phase completes, and is
// returned even on error so that callers can render a partial summary after a
// graceful cancellation.
func (svc *PullService) Pull(ctx context.Context) (*PullSummary, error) {
	summary := &PullSummary{DryRun: svc.options.DryRun}

	if svc.options.SkipVexImages {
		svc.userLogger.WarnLn("The skip-vex-images flag was detected: Vulnerability scanning may not work correctly when this flag is used.")
	}

	if svc.options.SkipPlatform {
		summary.Platform.Skipped = true
	} else {
		if err := svc.platformService.PullPlatform(ctx); err != nil {
			return summary, fmt.Errorf("pull platform: %w", err)
		}

		ps := svc.platformService.Stats()
		summary.Platform = ComponentStats{Attempted: ps.Attempted, Images: ps.Images, Versions: ps.Versions, Channels: ps.Channels}
	}

	if svc.options.SkipInstaller {
		summary.Installer.Skipped = true
	} else {
		if err := svc.installerService.PullInstaller(ctx); err != nil {
			return summary, fmt.Errorf("pull installer: %w", err)
		}

		is := svc.installerService.Stats()
		summary.Installer = ComponentStats{Attempted: is.Attempted, Images: is.Images, Versions: []string{is.Tag}}
	}

	if svc.options.SkipSecurity {
		summary.Security.Skipped = true
	} else {
		if err := svc.securityService.PullSecurity(ctx); err != nil {
			return summary, fmt.Errorf("pull security databases: %w", err)
		}

		summary.Security = toSecurityStats(svc.securityService.Stats())
	}

	if !svc.options.SkipModules || svc.options.OnlyExtraImages {
		if err := svc.modulesService.PullModules(ctx); err != nil {
			return summary, fmt.Errorf("pull modules: %w", err)
		}

		summary.Modules = toModulesStats(svc.modulesService.Stats())
	} else {
		summary.Modules.Skipped = true
	}

	if !svc.options.SkipPackages || svc.options.OnlyExtraImages {
		if err := svc.packagesService.PullPackages(ctx); err != nil {
			return summary, fmt.Errorf("pull packages: %w", err)
		}

		summary.Packages = toPackagesStats(svc.packagesService.Stats())
	} else {
		summary.Packages.Skipped = true
	}

	// The package release-image catalog (package-versions) is always cloned
	// into the bundle, regardless of which components are mirrored or whether
	// packages are skipped via --no-packages. This keeps the bundle's package
	// release metadata in sync on every mirror operation.
	if err := svc.packagesService.PullPackageVersions(ctx); err != nil {
		return summary, fmt.Errorf("pull package release images: %w", err)
	}

	// Plugins resolve against what the earlier phases put into the bundle
	// (module and platform versions), so this phase runs last.
	if svc.options.OnlyExtraImages {
		summary.Plugins.Skipped = true
	} else {
		if err := svc.pluginsService.PullPlugins(ctx, svc.pluginsInput()); err != nil {
			return summary, fmt.Errorf("pull plugins: %w", err)
		}

		summary.Plugins = toPluginsStats(svc.pluginsService.Stats())
	}

	return summary, nil
}

// pluginsInput assembles the plugins phase input from what the earlier phases
// actually selected: module versions from the modules stats, platform
// versions from the platform stats. Both are recorded at resolution time, so
// the handoff works in dry-run too.
func (svc *PullService) pluginsInput() plugins.PullInput {
	return buildPluginsInput(svc.modulesService.Stats(), svc.platformService.Stats().Versions)
}

func buildPluginsInput(modulesStats modules.ModulesStats, platformVersions []string) plugins.PullInput {
	in := plugins.PullInput{}

	for _, module := range modulesStats.Modules {
		versions := parseSemvers(module.Versions)
		if len(versions) == 0 {
			continue
		}

		in.Modules = append(in.Modules, plugins.ModuleInBundle{Name: module.Name, Versions: versions})
	}

	in.PlatformVersions = parseSemvers(platformVersions)

	return in
}

// parseSemvers parses version tags, dropping unparseable ones (e.g. channel
// aliases) - plugin contracts constrain semver versions only.
func parseSemvers(raw []string) []*semver.Version {
	versions := make([]*semver.Version, 0, len(raw))

	for _, tag := range raw {
		version, err := semver.NewVersion(tag)
		if err != nil {
			continue
		}

		versions = append(versions, version)
	}

	return versions
}

func builtinsSet(names []string) map[string]struct{} {
	if len(names) == 0 {
		return nil
	}

	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}

	return set
}

// The mapper functions below copy each service's package-local stat struct into
// the corresponding summary type. The structs are duplicated to keep the
// service packages decoupled from package mirror, which imports them (so the
// dependency cannot be reversed).

func toSecurityStats(s security.SecurityStats) SecurityStats {
	return SecurityStats{
		Attempted:          s.Attempted,
		Available:          s.Available,
		Databases:          s.Databases,
		AvailableDatabases: s.AvailableDatabases,
	}
}

func toModulesStats(s modules.ModulesStats) ModulesStats {
	mods := make([]ModuleStat, 0, len(s.Modules))
	for _, m := range s.Modules {
		mods = append(mods, ModuleStat{Name: m.Name, Images: m.Images, VEX: m.VEX, Versions: m.Versions})
	}

	return ModulesStats{
		Attempted:       s.Attempted,
		OnlyExtraImages: s.OnlyExtraImages,
		Modules:         mods,
		TotalImages:     s.TotalImages,
		TotalVEX:        s.TotalVEX,
	}
}

func toPluginsStats(s plugins.PluginsStats) PluginsStats {
	stats := PluginsStats{
		Attempted:   s.Attempted,
		Warnings:    s.Warnings,
		TotalImages: s.TotalImages,
	}

	for _, p := range s.Plugins {
		versions := make([]PluginVersionStat, 0, len(p.Versions))

		for _, v := range p.Versions {
			reasons := make([]PluginReason, 0, len(v.Reasons))
			for _, r := range v.Reasons {
				reasons = append(reasons, PluginReason{Kind: r.Kind.String(), Subject: r.Subject, Constraint: r.Constraint})
			}

			versions = append(versions, PluginVersionStat{Version: v.Version, Reasons: reasons})
		}

		stats.Plugins = append(stats.Plugins, PluginStat{Name: p.Name, Images: p.Images, Versions: versions})
	}

	for _, skip := range s.Skipped {
		stats.SkippedPlugins = append(stats.SkippedPlugins, SkippedPluginStat{Name: skip.Name, Reason: skip.Reason})
	}

	return stats
}

func toPackagesStats(s packages.PackagesStats) PackagesStats {
	pkgs := make([]PackageStat, 0, len(s.Packages))
	for _, p := range s.Packages {
		pkgs = append(pkgs, PackageStat{Name: p.Name, Images: p.Images, VEX: p.VEX, Versions: p.Versions})
	}

	return PackagesStats{
		Attempted:       s.Attempted,
		OnlyExtraImages: s.OnlyExtraImages,
		Packages:        pkgs,
		TotalImages:     s.TotalImages,
		TotalVEX:        s.TotalVEX,
	}
}
