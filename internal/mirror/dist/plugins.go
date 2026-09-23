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

package dist

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"github.com/Masterminds/semver/v3"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/pack"
	pluginlayout "github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry/task"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// pluginsDirName is the working-dir subdirectory holding per-plugin OCI
// layouts during a pull.
const pluginsDirName = "plugins"

// PullInput is the cross-phase handoff: what the earlier pull phases put into
// the bundle. Built by the pull orchestrator, never by this package.
type PullInput struct {
	// Modules are the mirrored modules with their bundled versions.
	Modules []ModuleInBundle
	// PlatformVersions are the mirrored Deckhouse platform versions.
	PlatformVersions []*semver.Version
}

// PullPlugins mirrors the plugins the bundle needs: plugins whose contracts
// name the mirrored modules (per bundled module version), their mandatory
// plugin dependencies, and explicit --include-plugin entries.
func (svc *Service) PullPlugins(ctx context.Context, in PullInput) error {
	svc.pluginStats.attempted = true

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
		NoCatalog:        svc.options.ProxyRegistry,
	})
	if err != nil {
		return err
	}

	svc.pluginStats.recordResolution(resolution)

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
	svc.pluginStats.captureImages(svc.layouts)

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

// pullVersion pulls one plugin version into the plugin's OCI layout.
func (svc *Service) pullVersion(ctx context.Context, name pluginName, tag versionTag) error {
	pluginLayout, err := svc.layoutFor(name)
	if err != nil {
		return err
	}

	return pullTag(ctx, svc.pluginsService.Plugin(name), pluginLayout, tag, svc.pluginRef(name, tag))
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
			tarPrefix := filepath.Join(internal.D8CLISegment, internal.D8PluginsSegment, plugin.Name)

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

	// The resolver validates every name it emits; this guard keeps the
	// filesystem join safe should a new name source bypass it.
	if err := pluginlayout.ValidatePluginName(name); err != nil {
		return nil, err
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
