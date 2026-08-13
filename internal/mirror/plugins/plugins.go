/*
Copyright 2026 Flant JSC

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

package plugins

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/Masterminds/semver/v3"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/types"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	dkpreg "github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/pack"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry/task"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

const (
	// pluginsDirName is the working-dir subdirectory holding per-plugin OCI
	// layouts during a pull.
	pluginsDirName = "plugins"

	pullRetryAttempts = 5
	pullRetryDelay    = 10 * time.Second
)

// Options contains configuration options for the plugins service.
type Options struct {
	// Filter carries --include-plugin expressions (whitelist, additive to the
	// module-driven auto-selection). May be nil.
	Filter *modules.Filter
	// Builtins are d8 built-in command names that satisfy a same-named plugin
	// dependency by presence (never pulled).
	Builtins map[string]struct{}
	// BundleDir is the directory to store the bundle.
	BundleDir string
	// BundleChunkSize is the max size of bundle chunks in bytes (0 = no chunking).
	BundleChunkSize int64
	// DryRun prints the pull plan without downloading any image blobs.
	DryRun bool
	// ProxyRegistry means the registry serves no catalog: auto-selection is
	// impossible, only explicit exact pins are resolved.
	ProxyRegistry bool
}

// PullInput is the cross-phase handoff: what the earlier pull phases put into
// the bundle. Built by the pull orchestrator, never by this package.
type PullInput struct {
	// Modules are the mirrored modules with their bundled versions.
	Modules []ModuleInBundle
	// PlatformVersions are the mirrored Deckhouse platform versions.
	PlatformVersions []*semver.Version
}

// Service is the plugins phase of mirror pull: resolve which plugin versions
// the bundle needs, pull them (multi-platform indexes whole), pack one
// plugin-<name>.tar per plugin.
type Service struct {
	workingDir string

	// pluginsService handles plugin registry operations.
	pluginsService *registryservice.PluginsService
	// resolver picks the plugin versions to mirror.
	resolver Resolver
	// layouts holds per-plugin OCI layouts, created lazily.
	layouts map[pluginName]*regimage.ImageLayout

	options *Options

	// stats accumulates pull accounting for the summary.
	stats *pluginsPullStats

	logger     *dkplog.Logger
	userLogger *log.SLogger
}

// NewService creates the plugins phase service.
func NewService(
	registryService *registryservice.Service,
	workingDir string,
	options *Options,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *Service {
	pluginsService := registryService.PluginService()

	return &Service{
		workingDir: workingDir,

		pluginsService: pluginsService,
		resolver:       NewResolver(NewCatalog(pluginsService, logger), logger),
		layouts:        make(map[pluginName]*regimage.ImageLayout),

		options: options,

		stats: newPluginsPullStats(),

		logger:     logger,
		userLogger: userLogger,
	}
}

// PullPlugins mirrors the plugins the bundle needs: plugins whose contracts
// name the mirrored modules (per bundled module version), their mandatory
// plugin dependencies, and explicit --include-plugin entries.
func (svc *Service) PullPlugins(ctx context.Context, in PullInput) error {
	svc.stats.attempted = true

	modulesIn := in.Modules
	if svc.options.ProxyRegistry {
		// A proxy registry serves no catalog, so auto-selection cannot
		// enumerate plugins. Explicit exact pins still work: they address
		// manifests by tag.
		if len(modulesIn) > 0 {
			svc.userLogger.WarnLn("Plugin auto-selection is not available with --proxy-registry; use --include-plugin <name>@=<version> to mirror plugins.")
		}

		modulesIn = nil
	}

	resolution, err := svc.resolver.Resolve(ctx, ResolveInput{
		Modules:          modulesIn,
		PlatformVersions: in.PlatformVersions,
		Filter:           svc.options.Filter,
		Builtins:         svc.options.Builtins,
	})
	if err != nil {
		return err
	}

	svc.stats.recordResolution(resolution)

	for _, warning := range resolution.Warnings {
		svc.userLogger.WarnLn(warning)
	}

	for _, skip := range resolution.Skipped {
		svc.userLogger.Warnf("Skipping plugin %s: %s", skip.Name, skip.Reason)
	}

	if len(resolution.Plugins) == 0 {
		svc.userLogger.InfoLn("No plugins to mirror")

		return nil
	}

	if svc.options.DryRun {
		svc.printDryRunPlan(resolution)

		return nil
	}

	if err := svc.pullPlugins(ctx, resolution); err != nil {
		return err
	}

	// Image counts must be captured before packing: bundle.Pack deletes the
	// layout files as it tars them.
	svc.stats.captureImages(svc.layouts)

	return svc.packPlugins(ctx, resolution)
}

func (svc *Service) pullPlugins(ctx context.Context, resolution *Resolution) error {
	total := 0
	for _, plugin := range resolution.Plugins {
		total += len(plugin.Versions)
	}

	current := 0

	for _, plugin := range resolution.Plugins {
		for _, sv := range plugin.Versions {
			current++

			tag := sv.Version.Original()
			ref := svc.pluginRef(plugin.Name, tag)

			err := retry.RunTask(
				ctx,
				svc.userLogger,
				fmt.Sprintf("[%d / %d] Pulling %s", current, total, ref),
				task.WithConstantRetries(pullRetryAttempts, pullRetryDelay, func(ctx context.Context) error {
					return svc.pullVersion(ctx, plugin.Name, tag)
				}))
			if err != nil {
				return fmt.Errorf("pull plugin %s@%s: %w", plugin.Name, tag, err)
			}
		}
	}

	return nil
}

// pullVersion pulls one plugin version into the plugin's OCI layout. A
// multi-platform index is stored whole: children are fetched by digest, so
// their bytes (and the contract annotation) stay exactly as published.
func (svc *Service) pullVersion(ctx context.Context, name pluginName, tag versionTag) error {
	pluginSvc := svc.pluginsService.Plugin(name)

	layout, err := svc.layoutFor(name)
	if err != nil {
		return err
	}

	result, err := pluginSvc.GetManifest(ctx, tag)
	if err != nil {
		return fmt.Errorf("get manifest: %w", err)
	}

	if !result.GetMediaType().IsIndex() {
		img, err := pluginSvc.GetImage(ctx, tag)
		if err != nil {
			return fmt.Errorf("get image: %w", err)
		}

		return layout.AddImage(img, tag)
	}

	indexManifest, err := result.GetIndexManifest()
	if err != nil {
		return fmt.Errorf("read index manifest: %w", err)
	}

	idx, err := rebuildIndex(ctx, pluginSvc, indexManifest, result.GetMediaType())
	if err != nil {
		return err
	}

	return layout.AddIndex(idx, tag, svc.pluginRef(name, tag))
}

// rebuildIndex reassembles a multi-platform index from its children. Children
// are fetched by digest (byte-exact); only the top-level index manifest is
// re-marshaled locally, with its media type and annotations carried over.
func rebuildIndex(ctx context.Context, pluginSvc *registryservice.PluginService, indexManifest dkpreg.IndexManifest, mediaType types.MediaType) (v1.ImageIndex, error) {
	children := indexManifest.GetManifests()

	adds := make([]mutate.IndexAddendum, 0, len(children))

	for _, child := range children {
		img, err := pluginSvc.GetImage(ctx, "@"+child.GetDigest().String())
		if err != nil {
			return nil, fmt.Errorf("get platform image %s: %w", child.GetDigest(), err)
		}

		adds = append(adds, mutate.IndexAddendum{
			Add: img,
			Descriptor: v1.Descriptor{
				MediaType:   child.GetMediaType(),
				URLs:        child.GetURLs(),
				Annotations: child.GetAnnotations(),
				Platform:    child.GetPlatform(),
			},
		})
	}

	idx := mutate.AppendManifests(empty.Index, adds...)

	if annotations := indexManifest.GetAnnotations(); len(annotations) > 0 {
		annotated, ok := mutate.Annotations(idx, annotations).(v1.ImageIndex)
		if !ok {
			return nil, fmt.Errorf("annotate rebuilt index: unexpected mutate result type")
		}

		idx = annotated
	}

	return mutate.IndexMediaType(idx, mediaType), nil
}

func (svc *Service) packPlugins(ctx context.Context, resolution *Resolution) error {
	for _, plugin := range resolution.Plugins {
		// Honor cancellation between plugins so a Ctrl+C during the pack
		// phase doesn't keep producing more tars.
		if err := ctx.Err(); err != nil {
			return err
		}

		if _, ok := svc.layouts[plugin.Name]; !ok {
			continue
		}

		pkgName := "plugin-" + plugin.Name + ".tar"

		if err := svc.userLogger.Process(fmt.Sprintf("Pack %s", pkgName), func() error {
			// The tar prefix places the layout at deckhouse-cli/plugins/<name>
			// inside the bundle - the path mirror push uploads verbatim and
			// the registry-packages-proxy expects on the target side.
			pluginDir := filepath.Join(svc.workingDir, pluginsDirName, plugin.Name)
			tarPrefix := filepath.Join("deckhouse-cli", "plugins", plugin.Name)

			return pack.Bundle(ctx, svc.options.BundleDir, pkgName, svc.options.BundleChunkSize, func(w io.Writer) error {
				return bundle.PackWithPrefix(ctx, pluginDir, tarPrefix, w)
			})
		}); err != nil {
			return err
		}
	}

	return nil
}

// printDryRunPlan prints the refs that would be pulled, without downloading.
func (svc *Service) printDryRunPlan(resolution *Resolution) {
	svc.userLogger.InfoLn("[dry-run] Plugins that would be pulled:")

	for _, plugin := range resolution.Plugins {
		for _, sv := range plugin.Versions {
			svc.userLogger.InfoLn("  " + svc.pluginRef(plugin.Name, sv.Version.Original()))
		}
	}
}

func (svc *Service) layoutFor(name pluginName) (*regimage.ImageLayout, error) {
	if layout, ok := svc.layouts[name]; ok {
		return layout, nil
	}

	layout, err := regimage.NewImageLayout(filepath.Join(svc.workingDir, pluginsDirName, name))
	if err != nil {
		return nil, fmt.Errorf("create layout for plugin %s: %w", name, err)
	}

	svc.layouts[name] = layout

	return layout, nil
}

// pluginRef is the full registry reference of one plugin version, e.g.
// "registry.deckhouse.io/deckhouse/deckhouse-cli/plugins/foo:v1.2.3".
func (svc *Service) pluginRef(name pluginName, tag versionTag) string {
	return svc.pluginsService.Plugin(name).GetRoot() + ":" + tag
}
