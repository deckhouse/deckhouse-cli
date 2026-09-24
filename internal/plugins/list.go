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

package plugins

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
)

// PluginInfo is one installed plugin's name, version and description for display.
type PluginInfo struct {
	Name        string
	Version     string
	Description string
}

// List returns the plugins installed on disk. For the plugins published in the
// registry and available to install, see AvailablePlugins.
func (m *Manager) List() []PluginInfo {
	installed, err := m.fetchInstalledPlugins()
	if err != nil {
		m.logger.Warn("Failed to fetch installed plugins", slog.String("error", err.Error()))

		return []PluginInfo{}
	}

	return installed
}

// fetchInstalledPlugins reads the installed plugins from the plugins root on disk.
func (m *Manager) fetchInstalledPlugins() ([]PluginInfo, error) {
	plugins, err := os.ReadDir(layout.PluginsRoot(m.pluginDirectory))
	if err != nil {
		return nil, fmt.Errorf("failed to read plugins directory: %w", err)
	}

	res := make([]PluginInfo, 0, len(plugins))

	for _, plugin := range plugins {
		version, err := m.getInstalledPluginVersion(plugin.Name())
		if err != nil {
			res = append(res, PluginInfo{
				Name:        plugin.Name(),
				Version:     "ERROR",
				Description: err.Error(),
			})

			continue
		}

		contract, err := m.InstalledPluginContract(plugin.Name())
		if err != nil {
			res = append(res, PluginInfo{
				Name:        plugin.Name(),
				Version:     version.Original(),
				Description: "failed to get description",
			})

			continue
		}

		displayInfo := PluginInfo{
			Name:        plugin.Name(),
			Version:     version.Original(),
			Description: contract.Description,
		}

		res = append(res, displayInfo)
	}

	return res, nil
}

// RemotePluginInfo is one plugin published in the registry: its name, the newest
// stable version on offer, and whether it is already installed locally. Note says
// why Version is empty - a plugin that could only be named is still listed, never
// silently dropped.
type RemotePluginInfo struct {
	Name      string
	Version   string
	Installed bool
	Note      string
}

// ErrCatalogUnsupported means the transport in use cannot enumerate the published
// plugins. It is a property of the transport, not a runtime failure: the proxy
// allowlist admits deckhouse-cli and deckhouse-cli/plugins/<name> and refuses the
// bare deckhouse-cli/plugins path, so over TransportRPP the request is never made.
// Addressing a plugin by exact name is unaffected - install, update and versions
// work on every transport.
var ErrCatalogUnsupported = errors.New("listing published plugins is not supported by this transport")

// AvailablePlugins enumerates the plugins published in the registry, by name.
//
// The catalog is the tag list of the plugins repository, where each plugin has an
// image tagged with its name; a source that can reach it declares pluginCatalog.
// Only a failure to enumerate at all is returned as an error - a plugin whose
// versions cannot be resolved still appears, carrying a Note that says so.
func (m *Manager) AvailablePlugins(ctx context.Context) ([]RemotePluginInfo, error) {
	if m.service == nil {
		return nil, errors.New("plugin source is not initialized")
	}

	catalog, ok := m.service.(pluginCatalog)
	if !ok {
		return nil, fmt.Errorf("%w (%s)", ErrCatalogUnsupported, m.service.Transport())
	}

	names, err := catalog.ListPluginNames(ctx)
	if err != nil {
		return nil, err
	}

	sort.Strings(names)

	available := make([]RemotePluginInfo, 0, len(names))
	for _, name := range names {
		available = append(available, m.remotePluginInfo(ctx, name))
	}

	return available, nil
}

// remotePluginInfo resolves one published plugin's newest stable version. Every
// failure lands in the row's Note rather than aborting the listing: a plugin whose
// versions cannot be read is still worth showing by name.
func (m *Manager) remotePluginInfo(ctx context.Context, name string) RemotePluginInfo {
	info := RemotePluginInfo{Name: name}

	// The name arrives as a registry tag, so it is external input: anything that
	// could not address a plugin repository is reported, never turned into a route.
	if err := layout.ValidatePluginName(name); err != nil {
		info.Note = "not a valid plugin name"

		return info
	}

	info.Installed, _ = m.checkInstalled(name)

	tags, err := m.listTags(ctx, name)
	if err != nil {
		m.logger.Debug("cannot list versions of a published plugin",
			slog.String("plugin", name), slog.String("error", err.Error()))

		info.Note = "versions unavailable"

		return info
	}

	candidates := stableVersions(sortedSemverDesc(tags))
	if len(candidates) == 0 {
		info.Note = "no versions found"

		return info
	}

	// The newest stable tag may be one of the per-platform child images, so collapse
	// it to the release itself rather than advertising a single platform's build.
	clean, _ := SplitPlatform(candidates[0])
	info.Version = clean.Original()

	return info
}
