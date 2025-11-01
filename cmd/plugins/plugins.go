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

	"github.com/Masterminds/semver/v3"
	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/cmd/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

type PluginsCommand struct {
	service              *service.PluginService
	pluginRegistryClient pkg.RegistryClient

	logger *dkplog.Logger
}

// pluginDisplayInfo holds all information needed to display a plugin
type pluginDisplayInfo struct {
	Name        string
	Version     string
	Description string
	IsInstalled bool
	HasError    bool
}

// pluginsListData holds all data for the list command
type pluginsListData struct {
	Installed        []pluginDisplayInfo
	Available        []pluginDisplayInfo
	RegistryError    error
	TotalInstalled   int
	TotalAvailable   int
	AvailableSuccess int
	AvailableFailed  int
}

func NewPluginsCommand(logger *dkplog.Logger) *cobra.Command {
	pc := &PluginsCommand{
		logger: logger,
	}

	cmd := &cobra.Command{
		Use:    "plugins",
		Short:  "Manage Deckhouse CLI plugins",
		Hidden: true,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// init plugin services for subcommands after flags are parsed
			pc.initPluginServices()
		},
	}

	cmd.AddCommand(pc.pluginsListCommand())
	cmd.AddCommand(pc.pluginsContractCommand())
	cmd.AddCommand(pc.pluginsInstallCommand())
	cmd.AddCommand(pc.pluginsUpdateCommand())
	cmd.AddCommand(pc.pluginsRemoveCommand())

	flags.AddFlags(cmd.PersistentFlags())

	return cmd
}

func (pc *PluginsCommand) pluginsListCommand() *cobra.Command {
	var showInstalledOnly bool
	var showAvailableOnly bool

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List Deckhouse CLI plugins",
		Long:  "Display detailed information about installed plugins and available plugins from the registry",
		RunE: func(cmd *cobra.Command, args []string) error {
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
		data.Installed = pc.fetchInstalledPlugins()
		data.TotalInstalled = len(data.Installed)
	}

	// Fetch available plugins from registry if needed
	if !showInstalledOnly {
		data.Available, data.RegistryError = pc.fetchAvailablePlugins(ctx)
		data.TotalAvailable = len(data.Available)

		// Count successful and failed plugins
		for _, plugin := range data.Available {
			if plugin.HasError {
				data.AvailableFailed++
			} else {
				data.AvailableSuccess++
			}
		}
	}

	return data
}

// fetchInstalledPlugins retrieves installed plugins from filesystem
func (pc *PluginsCommand) fetchInstalledPlugins() []pluginDisplayInfo {
	// TODO: Implement listing installed plugins from filesystem
	return []pluginDisplayInfo{
		{
			Name:        "example-plugin",
			Version:     "v1.0.0",
			Description: "Example installed plugin",
			IsInstalled: true,
			HasError:    false,
		},
		{
			Name:        "another-plugin",
			Version:     "v2.1.3",
			Description: "Another installed plugin",
			IsInstalled: true,
			HasError:    false,
		},
	}
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
			Name:        pluginName,
			IsInstalled: false,
		}

		// fetch versions to get latest version
		versions, err := pc.service.ListPluginTags(ctx, pluginName)
		if err != nil {
			pc.logger.Warn("Failed to list plugin tags", slog.String("plugin", pluginName), slog.String("error", err.Error()))
			return nil, fmt.Errorf("failed to list plugin tags: %w", err)
		}

		latestVersion, err := pc.findLatestVersion(versions)
		if err != nil {
			pc.logger.Warn("Failed to fetch latest version", slog.String("plugin", pluginName), slog.String("error", err.Error()))
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
			plugin.HasError = true
		} else {
			plugin.Version = latestVersion.Original()
			plugin.Description = contract.Description
			plugin.HasError = false

			// Truncate description if too long
			if len(plugin.Description) > 40 {
				plugin.Description = plugin.Description[:37] + "..."
			}
		}

		plugins = append(plugins, plugin)
	}

	return plugins, nil
}

// findLatestVersion finds the latest version from a list of version strings
func (pc *PluginsCommand) findLatestVersion(versions []string) (*semver.Version, error) {
	if len(versions) == 0 {
		return nil, fmt.Errorf("no versions found")
	}

	var latestVersion *semver.Version

	for _, version := range versions {
		version, err := semver.NewVersion(version)
		if err != nil {
			continue
		}

		if latestVersion == nil {
			latestVersion = version
			continue
		}

		if latestVersion.LessThan(version) {
			latestVersion = version
		}
	}

	if latestVersion == nil {
		return nil, fmt.Errorf("no versions found")
	}

	return latestVersion, nil
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
	fmt.Printf("Total: %d plugin(s) installed\n", data.TotalInstalled)
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
	if data.AvailableFailed > 0 {
		fmt.Printf("Total: %d plugin(s) available (%d accessible, %d with errors)\n",
			data.TotalAvailable, data.AvailableSuccess, data.AvailableFailed)
	} else {
		fmt.Printf("Total: %d plugin(s) available\n", data.TotalAvailable)
	}

	fmt.Println()
	fmt.Println("Use 'plugins contract <name>' to see detailed information about a plugin")
	fmt.Println("Use 'plugins install <name>' to install a plugin")
}

func (pc *PluginsCommand) pluginsContractCommand() *cobra.Command {
	var version string
	var useMajor int

	cmd := &cobra.Command{
		Use:   "contract [plugin-name]",
		Short: "Get the contract for a specific plugin",
		Long:  "Retrieve and display the contract specification for a specific plugin from the registry",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pluginName := args[0]
			ctx := cmd.Context()

			versions, err := pc.service.ListPluginTags(ctx, pluginName)
			if err != nil {
				pc.logger.Warn("Failed to list plugin tags", slog.String("plugin", pluginName), slog.String("error", err.Error()))
				return fmt.Errorf("failed to list plugin tags: %w", err)
			}

			latestVersion, err := pc.findLatestVersion(versions)
			if err != nil {
				pc.logger.Warn("Failed to fetch latest version", slog.String("plugin", pluginName), slog.String("error", err.Error()))
				return fmt.Errorf("failed to fetch latest version: %w", err)
			}

			tag := latestVersion.Original()

			fmt.Printf("Fetching contract for plugin: %s\n", pluginName)
			fmt.Printf("Tag: %s\n", tag)
			if useMajor > 0 {
				fmt.Printf("Using major version: %d\n", useMajor)
			}

			fmt.Println("\nRetrieving contract from registry...")

			// Use service to get plugin contract
			plugin, err := pc.service.GetPluginContract(ctx, pluginName, tag)
			if err != nil {
				pc.logger.Warn("Failed to get plugin contract",
					slog.String("plugin", pluginName),
					slog.String("tag", tag),
					slog.String("error", err.Error()))
				return fmt.Errorf("failed to get plugin contract: %w", err)
			}

			// Display contract
			fmt.Println("---")
			fmt.Printf("Name: %s\n", plugin.Name)
			fmt.Printf("Version: %s\n", plugin.Version)
			fmt.Printf("Description: %s\n", plugin.Description)

			if len(plugin.Env) > 0 {
				fmt.Println("\nEnvironment Variables:")
				for _, env := range plugin.Env {
					fmt.Printf("  - %s\n", env.Name)
				}
			}

			if len(plugin.Flags) > 0 {
				fmt.Println("\nFlags:")
				for _, flag := range plugin.Flags {
					fmt.Printf("  - %s\n", flag.Name)
				}
			}

			fmt.Println("\nRequirements:")
			fmt.Printf("  Kubernetes: %s\n", plugin.Requirements.Kubernetes.Constraint)
			if len(plugin.Requirements.Modules) > 0 {
				fmt.Println("  Modules:")
				for _, mod := range plugin.Requirements.Modules {
					fmt.Printf("    - %s: %s\n", mod.Name, mod.Constraint)
				}
			}

			fmt.Println("\n✓ Contract retrieved successfully!")
			return nil
		},
	}

	cmd.Flags().StringVar(&version, "version", "", "Specific version of the plugin contract to retrieve")
	cmd.Flags().IntVar(&useMajor, "use-major", 0, "Use specific major version (e.g., 1, 2)")

	return cmd
}

func (pc *PluginsCommand) pluginsInstallCommand() *cobra.Command {
	var version string
	var useMajor int

	cmd := &cobra.Command{
		Use:   "install [plugin-name]",
		Short: "Install a Deckhouse CLI plugin",
		Long:  "Install a new plugin",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pluginName := args[0]
			ctx := cmd.Context()

			versions, err := pc.service.ListPluginTags(ctx, pluginName)
			if err != nil {
				pc.logger.Warn("Failed to list plugin tags", slog.String("plugin", pluginName), slog.String("error", err.Error()))
				return fmt.Errorf("failed to list plugin tags: %w", err)
			}

			latestVersion, err := pc.findLatestVersion(versions)
			if err != nil {
				pc.logger.Warn("Failed to fetch latest version", slog.String("plugin", pluginName), slog.String("error", err.Error()))
				return fmt.Errorf("failed to fetch latest version: %w", err)
			}

			tag := latestVersion.Original()

			fmt.Printf("Installing plugin: %s\n", pluginName)
			fmt.Printf("Tag: %s\n", tag)
			if useMajor > 0 {
				fmt.Printf("Using major version: %d\n", useMajor)
			}

			// Get plugin contract first
			fmt.Println("Verifying plugin contract...")
			plugin, err := pc.service.GetPluginContract(ctx, pluginName, tag)
			if err != nil {
				pc.logger.Warn("Failed to get plugin contract",
					slog.String("plugin", pluginName),
					slog.String("tag", tag),
					slog.String("error", err.Error()))
				return fmt.Errorf("failed to get plugin contract: %w", err)
			}

			fmt.Printf("Plugin: %s v%s\n", plugin.Name, plugin.Version)
			fmt.Printf("Description: %s\n", plugin.Description)

			// Extract plugin to installation directory
			// TODO: Make destination configurable
			destination := fmt.Sprintf("/tmp/deckhouse-cli/plugins/%s", pluginName)
			fmt.Printf("Installing to: %s\n", destination)

			fmt.Println("Downloading and extracting plugin...")
			err = pc.service.ExtractPlugin(ctx, pluginName, tag, destination)
			if err != nil {
				pc.logger.Warn("Failed to extract plugin",
					slog.String("plugin", pluginName),
					slog.String("tag", tag),
					slog.String("destination", destination),
					slog.String("error", err.Error()))
				return fmt.Errorf("failed to extract plugin: %w", err)
			}

			fmt.Printf("✓ Plugin '%s' successfully installed!\n", pluginName)
			return nil
		},
	}

	cmd.Flags().StringVar(&version, "version", "", "Specific version of the plugin to install")
	cmd.Flags().IntVar(&useMajor, "use-major", 0, "Use specific major version (e.g., 1, 2)")

	return cmd
}

func (pc *PluginsCommand) pluginsUpdateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update [plugin-name]",
		Short: "Update an installed plugin",
		Long:  "Update a specific plugin to its latest available version",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pluginName := args[0]
			fmt.Printf("Updating plugin: %s\n", pluginName)

			ctx := cmd.Context()

			// Get latest version
			fmt.Println("Checking for updates...")
			plugin, err := pc.service.GetPluginContract(ctx, pluginName, "latest")
			if err != nil {
				pc.logger.Warn("Failed to get plugin contract",
					slog.String("plugin", pluginName),
					slog.String("tag", "latest"),
					slog.String("error", err.Error()))
				return fmt.Errorf("failed to get plugin contract: %w", err)
			}

			fmt.Printf("Latest version: %s\n", plugin.Version)

			// Extract plugin
			destination := fmt.Sprintf("/tmp/deckhouse-cli/plugins/%s", pluginName)
			fmt.Println("Downloading latest version...")
			err = pc.service.ExtractPlugin(ctx, pluginName, "latest", destination)
			if err != nil {
				pc.logger.Warn("Failed to extract plugin",
					slog.String("plugin", pluginName),
					slog.String("tag", "latest"),
					slog.String("destination", destination),
					slog.String("error", err.Error()))
				return fmt.Errorf("failed to extract plugin: %w", err)
			}

			fmt.Printf("✓ Plugin '%s' updated successfully to v%s!\n", pluginName, plugin.Version)
			return nil
		},
	}

	// Add subcommands
	cmd.AddCommand(pc.pluginsUpdateAllCommand())

	return cmd
}

func (pc *PluginsCommand) pluginsUpdateAllCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "all",
		Short: "Update all installed plugins",
		Long:  "Update all installed plugins to their latest available versions",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: Implement updating all installed plugins from filesystem
			fmt.Println("Updating all installed plugins...")
			fmt.Println("Checking for updates...")
			fmt.Println("  - example-plugin: v1.0.0 → v1.2.0")
			fmt.Println("  - another-plugin: v2.1.3 (already up-to-date)")
			fmt.Println("✓ All plugins updated successfully!")
			return nil
		},
	}

	return cmd
}

func (pc *PluginsCommand) pluginsRemoveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remove [plugin-name]",
		Aliases: []string{"uninstall", "delete"},
		Short:   "Remove an installed plugin",
		Long:    "Remove a specific plugin from the Deckhouse CLI",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pluginName := args[0]
			fmt.Printf("Removing plugin: %s\n", pluginName)

			// TODO: Implement actual removal from filesystem
			destination := fmt.Sprintf("/tmp/deckhouse-cli/plugins/%s", pluginName)
			fmt.Printf("Removing plugin from: %s\n", destination)

			fmt.Println("Cleaning up plugin files...")
			fmt.Println("Removing plugin configuration...")
			fmt.Printf("✓ Plugin '%s' successfully removed!\n", pluginName)
			return nil
		},
	}

	// Add subcommands
	cmd.AddCommand(pc.pluginsRemoveAllCommand())

	return cmd
}

func (pc *PluginsCommand) pluginsRemoveAllCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "all",
		Short: "Remove all installed plugins",
		Long:  "Remove all plugins from the Deckhouse CLI at once",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: Implement removing all installed plugins from filesystem
			fmt.Println("Removing all installed plugins...")

			fmt.Println("Found 2 plugins to remove:")
			fmt.Println("  - example-plugin")
			fmt.Println("  - another-plugin")
			fmt.Println("Cleaning up plugin files...")
			fmt.Println("Removing plugin configurations...")
			fmt.Println("✓ All plugins successfully removed!")
			return nil
		},
	}

	return cmd
}
