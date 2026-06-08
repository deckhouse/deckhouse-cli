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
	"fmt"
	"log/slog"
	"os"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
)

// maxPluginDescLen caps a description in the available-plugins listing so a long
// contract description does not break the table layout (over the limit it is
// elided with a trailing "...").
const maxPluginDescLen = 40

// PluginInfo holds all information needed to display a plugin
type PluginInfo struct {
	Name        string
	Version     string
	Description string
}

// ListResult holds all data for the list command
type ListResult struct {
	Installed     []PluginInfo
	Available     []PluginInfo
	RegistryError error
}

// List fetches and prepares all data needed for display
func (m *Manager) List(ctx context.Context, showInstalledOnly, showAvailableOnly bool) *ListResult {
	data := &ListResult{
		Installed: []PluginInfo{},
		Available: []PluginInfo{},
	}

	// Fetch installed plugins if needed
	if !showAvailableOnly {
		installed, err := m.fetchInstalledPlugins()
		if err != nil {
			m.logger.Warn("Failed to fetch installed plugins", slog.String("error", err.Error()))
		} else {
			data.Installed = installed
		}
	}

	// Fetch available plugins from registry if needed
	if !showInstalledOnly {
		available, err := m.fetchAvailablePlugins(ctx)
		if err != nil {
			m.logger.Warn("Failed to fetch available plugins", slog.String("error", err.Error()))
			data.RegistryError = err
		} else {
			data.Available = available
		}
	}

	return data
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

// fetchAvailablePlugins retrieves and prepares available plugins from registry
func (m *Manager) fetchAvailablePlugins(ctx context.Context) ([]PluginInfo, error) {
	pluginNames, err := m.service.ListPlugins(ctx)
	if err != nil {
		// The error (including errListPluginsUnsupported from the proxy source) is
		// surfaced as RegistryError by the caller, which prints a "catalog listing
		// unavailable, install by name" hint - the whole listing is not failed.
		m.logger.Warn("Failed to list plugins", slog.String("error", err.Error()))

		return nil, fmt.Errorf("failed to list plugins: %w", err)
	}

	if len(pluginNames) == 0 {
		return []PluginInfo{}, nil
	}

	plugins := make([]PluginInfo, 0, len(pluginNames))

	// Fetch contract for each plugin to get version and description
	for _, pluginName := range pluginNames {
		plugin := PluginInfo{
			Name: pluginName,
		}

		// fetch versions to get latest version
		latestVersion, err := m.LatestVersion(ctx, pluginName)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch latest version: %w", err)
		}

		// Get the latest version contract
		contract, err := m.service.GetPluginContract(ctx, pluginName, latestVersion.Original())
		if err != nil {
			// Log the error for debugging
			m.logger.Warn("Failed to get plugin contract",
				slog.String("plugin", pluginName),
				slog.String("tag", latestVersion.Original()),
				slog.String("error", err.Error()))

			// Show ERROR in version column and error description in description column
			plugin.Version = "ERROR"
			plugin.Description = "failed to get plugin contract"
		} else {
			plugin.Version = latestVersion.Original()
			plugin.Description = contract.Description

			if len(plugin.Description) > maxPluginDescLen {
				plugin.Description = plugin.Description[:maxPluginDescLen-3] + "..."
			}
		}

		plugins = append(plugins, plugin)
	}

	return plugins, nil
}
