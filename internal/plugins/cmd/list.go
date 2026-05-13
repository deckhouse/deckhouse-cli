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

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/cmd/layout"
)

// pluginDisplayInfo holds all information needed to display a plugin
type pluginDisplayInfo struct {
	Name        string
	Version     string
	Description string
}

// pluginsListData holds all data for the list command
type pluginsListData struct {
	Installed     []pluginDisplayInfo
	Available     []pluginDisplayInfo
	RegistryError error
}

func (pc *PluginsCommand) pluginsListCommand() *cobra.Command {
	var showInstalledOnly bool
	var showAvailableOnly bool

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List Deckhouse CLI plugins",
		Long:  "Display detailed information about installed plugins and available plugins from the registry",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			// Prepare all data before printing
			data := pc.preparePluginsListData(ctx, showInstalledOnly, showAvailableOnly)

			// Print all prepared data
			pc.printPluginsList(data, showInstalledOnly, showAvailableOnly)

			return nil
		},
	}

	cmd.Flags().BoolVar(&showInstalledOnly, "installed", false, "Show only installed plugins")
	cmd.Flags().BoolVar(&showAvailableOnly, "available", false, "Show only available plugins from registry")

	return cmd
}

// preparePluginsListData fetches and prepares all data needed for display
func (pc *PluginsCommand) preparePluginsListData(ctx context.Context, showInstalledOnly, showAvailableOnly bool) *pluginsListData {
	data := &pluginsListData{
		Installed: []pluginDisplayInfo{},
		Available: []pluginDisplayInfo{},
	}

	// Fetch installed plugins if needed
	if !showAvailableOnly {
		installed, err := pc.fetchInstalledPlugins()
		if err != nil {
			pc.logger.Warn("Failed to fetch installed plugins", slog.String("error", err.Error()))
		} else {
			data.Installed = installed
		}
	}

	// Fetch available plugins from registry if needed
	if !showInstalledOnly {
		available, err := pc.fetchAvailablePlugins(ctx)
		if err != nil {
			pc.logger.Warn("Failed to fetch available plugins", slog.String("error", err.Error()))
			data.RegistryError = err
		} else {
			data.Available = available
		}
	}

	return data
}

// fetchInstalledPlugins retrieves installed plugins from filesystem
func (pc *PluginsCommand) fetchInstalledPlugins() ([]pluginDisplayInfo, error) {
	plugins, err := os.ReadDir(layout.PluginsRoot(pc.pluginDirectory))
	if err != nil {
		return nil, fmt.Errorf("failed to read plugins directory: %w", err)
	}

	res := make([]pluginDisplayInfo, 0, len(plugins))

	for _, plugin := range plugins {
		version, err := pc.getInstalledPluginVersion(plugin.Name())
		if err != nil {
			res = append(res, pluginDisplayInfo{
				Name:        plugin.Name(),
				Version:     "ERROR",
				Description: err.Error(),
			})
			continue
		}

		contract, err := pc.getInstalledPluginContract(plugin.Name())
		if err != nil {
			res = append(res, pluginDisplayInfo{
				Name:        plugin.Name(),
				Version:     version.Original(),
				Description: "failed to get description",
			})
			continue
		}

		displayInfo := pluginDisplayInfo{
			Name:        plugin.Name(),
			Version:     version.Original(),
			Description: contract.Description,
		}

		res = append(res, displayInfo)
	}

	return res, nil
}

// fetchAvailablePlugins retrieves and prepares available plugins from registry
func (pc *PluginsCommand) fetchAvailablePlugins(ctx context.Context) ([]pluginDisplayInfo, error) {
	pluginNames, err := pc.service.ListPlugins(ctx)
	if err != nil {
		pc.logger.Warn("Failed to list plugins", slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to list plugins: %w", err)
	}

	if len(pluginNames) == 0 {
		return []pluginDisplayInfo{}, nil
	}

	plugins := make([]pluginDisplayInfo, 0, len(pluginNames))

	// Fetch contract for each plugin to get version and description
	for _, pluginName := range pluginNames {
		plugin := pluginDisplayInfo{
			Name: pluginName,
		}

		// fetch versions to get latest version
		latestVersion, err := pc.fetchLatestVersion(ctx, pluginName)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch latest version: %w", err)
		}

		// Get the latest version contract
		contract, err := pc.service.GetPluginContract(ctx, pluginName, latestVersion.Original())
		if err != nil {
			// Log the error for debugging
			pc.logger.Warn("Failed to get plugin contract",
				slog.String("plugin", pluginName),
				slog.String("tag", latestVersion.Original()),
				slog.String("error", err.Error()))

			// Show ERROR in version column and error description in description column
			plugin.Version = "ERROR"
			plugin.Description = "failed to get plugin contract"
		} else {
			plugin.Version = latestVersion.Original()
			plugin.Description = contract.Description

			// Truncate description if too long
			if len(plugin.Description) > 40 {
				plugin.Description = plugin.Description[:37] + "..."
			}
		}

		plugins = append(plugins, plugin)
	}

	return plugins, nil
}

// printPluginsList prints all prepared data
func (pc *PluginsCommand) printPluginsList(data *pluginsListData, showInstalledOnly, showAvailableOnly bool) {
	// Print installed plugins section
	if !showAvailableOnly {
		pc.printInstalledSection(data)
	}

	// Print available plugins section
	if !showInstalledOnly {
		pc.printAvailableSection(data)
	}
}

// printInstalledSection prints the installed plugins section
func (pc *PluginsCommand) printInstalledSection(data *pluginsListData) {
	fmt.Println("Installed Plugins:")
	fmt.Println("-------------------------------------------")
	fmt.Printf("%-20s %-15s %-40s\n", "NAME", "VERSION", "DESCRIPTION")
	fmt.Println("-------------------------------------------")

	if len(data.Installed) == 0 {
		fmt.Println("No plugins installed")
	} else {
		for _, plugin := range data.Installed {
			fmt.Printf("%-20s %-15s %-40s\n", plugin.Name, plugin.Version, plugin.Description)
		}
	}

	fmt.Println()
	fmt.Printf("Total: %d plugin(s) installed\n", len(data.Installed))
	fmt.Println()
}

// printAvailableSection prints the available plugins section
func (pc *PluginsCommand) printAvailableSection(data *pluginsListData) {
	fmt.Println("Available Plugins in Registry:")
	fmt.Println("-------------------------------------------")

	// Handle registry error
	if data.RegistryError != nil {
		fmt.Println()
		fmt.Println("⚠ Unable to connect to plugin registry")
		fmt.Println()
		fmt.Println("The registry may not be accessible or catalog listing may be disabled.")
		fmt.Println("You can still use specific plugins if you know their names:")
		fmt.Println("  - Use 'plugins contract <name>' to view plugin details")
		fmt.Println("  - Use 'plugins install <name>' to install a plugin")
		return
	}

	// Handle empty registry
	if len(data.Available) == 0 {
		fmt.Println("No plugins found in registry")
		return
	}

	// Print plugins table
	fmt.Printf("%-20s %-15s %-40s\n", "NAME", "VERSION", "DESCRIPTION")
	fmt.Println("-------------------------------------------")

	for _, plugin := range data.Available {
		fmt.Printf("%-20s %-15s %-40s\n", plugin.Name, plugin.Version, plugin.Description)
	}

	// Print summary
	fmt.Println()
	fmt.Printf("Total: %d plugin(s) available\n", len(data.Available))

	fmt.Println()
	fmt.Println("Use 'plugins contract <name>' to see detailed information about a plugin")
	fmt.Println("Use 'plugins install <name>' to install a plugin")
}
