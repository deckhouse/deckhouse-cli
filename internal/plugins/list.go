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
	"fmt"
	"log/slog"
	"os"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
)

// PluginInfo holds all information needed to display a plugin
type PluginInfo struct {
	Name        string
	Version     string
	Description string
}

// List returns the installed plugins. The registry-packages-proxy serves only
// allow-listed images by exact name and exposes no catalog endpoint, so the set
// of available plugins cannot be listed - a plugin is inspected by name with
// `d8 plugins versions <name>`.
func (m *Manager) List() []PluginInfo {
	installed, err := m.fetchInstalledPlugins()
	if err != nil {
		m.logger.Warn("Failed to fetch installed plugins", slog.String("error", err.Error()))

		return []PluginInfo{}
	}

	return installed
}

// fetchInstalledPlugins retrieves installed plugins from filesystem
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
