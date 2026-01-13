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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/cmd/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

type PluginsCommand struct {
	service              *service.PluginService
	pluginRegistryClient registry.Client
	pluginDirectory      string

	logger *dkplog.Logger
}

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

func NewPluginsCommand(logger *dkplog.Logger) *PluginsCommand {
	return &PluginsCommand{
		pluginDirectory: flags.DeckhousePluginsDir,
		logger:          logger,
	}
}

func NewCommand(logger *dkplog.Logger) *cobra.Command {
	pc := NewPluginsCommand(logger)

	cmd := &cobra.Command{
		Use:    "plugins",
		Short:  "Manage Deckhouse CLI plugins",
		Hidden: true,
		PersistentPreRun: func(_ *cobra.Command, _ []string) {
			// init plugin services for subcommands after flags are parsed
			pc.InitPluginServices()

			err := os.MkdirAll(flags.DeckhousePluginsDir+"/plugins", 0755)
			// if permission failed
			if errors.Is(err, os.ErrPermission) {
				pc.logger.Warn("use homedir instead of default d8 plugins path", dkplog.Err(err))

				homeDir, err := os.UserHomeDir()
				if err != nil {
					logger.Warn("failed to receive home dir to create plugins dir", slog.String("error", err.Error()))
					return
				}

				pc.pluginDirectory = path.Join(homeDir, ".deckhouse-cli")
			}
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
	plugins, err := os.ReadDir(path.Join(pc.pluginDirectory, "plugins"))
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

func (pc *PluginsCommand) getInstalledPluginContract(pluginName string) (*internal.Plugin, error) {
	contractFile := path.Join(pc.pluginDirectory, "cache", "contracts", pluginName+".json")

	file, err := os.Open(contractFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read contract file: %w", err)
	}
	defer file.Close()

	contract := new(service.PluginContract)
	dec := json.NewDecoder(file)
	err = dec.Decode(contract)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal contract: %w", err)
	}

	return service.ContractToDomain(contract), nil
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

			latestVersion, err := pc.fetchLatestVersion(ctx, pluginName)
			if err != nil {
				return fmt.Errorf("failed to fetch latest version: %w", err)
			}

			tag := latestVersion.Original()

			pc.logger.Debug("Fetching contract for plugin", slog.String("plugin", pluginName), slog.String("tag", tag))

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
			buffer := bytes.NewBuffer(nil)
			enc := yaml.NewEncoder(buffer)
			enc.SetIndent(2)
			err = enc.Encode(plugin)
			if err != nil {
				return fmt.Errorf("failed to encode requirements: %w", err)
			}
			fmt.Println(buffer.String())

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
	var resolvePluginsConflicts bool

	cmd := &cobra.Command{
		Use:   "install [plugin-name]",
		Short: "Install a Deckhouse CLI plugin",
		Long:  "Install a new plugin",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pluginName := args[0]
			ctx := cmd.Context()

			return pc.InstallPlugin(ctx, pluginName, &installPluginOptions{
				version:                 version,
				useMajor:                useMajor,
				resolvePluginsConflicts: resolvePluginsConflicts,
			})
		},
	}

	cmd.Flags().StringVar(&version, "version", "", "Specific version of the plugin to install")
	cmd.Flags().IntVar(&useMajor, "use-major", -1, "Use specific major version (e.g., 1, 2)")
	cmd.Flags().BoolVar(&resolvePluginsConflicts, "resolve-plugins-conflicts", false, "Resolve conflicts between plugins requirements")

	return cmd
}

type installPluginOptions struct {
	version                 string
	useMajor                int
	resolvePluginsConflicts bool
}

// function checks if plugin can be installed, creates folders layout and then installs plugin, creates symlink "current" and caches contract.json
// version - semver version string (e.g. v1.0.0), default: "" (use latest version)
// useMajor - major version to install, default: -1 (use latest major version)
// resolvePluginsConflicts - resolve conflicts between installed plugins, default: false
func (pc *PluginsCommand) InstallPlugin(ctx context.Context, pluginName string, opts *installPluginOptions) error {
	// check if version is specified
	var installVersion *semver.Version

	if opts == nil {
		opts = &installPluginOptions{
			useMajor: -1,
		}
	}

	if opts.version != "" {
		var err error
		installVersion, err = semver.NewVersion(opts.version)
		if err != nil {
			return fmt.Errorf("failed to parse version: %w", err)
		}

		return pc.installPlugin(ctx, pluginName, installVersion, opts.resolvePluginsConflicts)
	}

	versions, err := pc.service.ListPluginTags(ctx, pluginName)
	if err != nil {
		pc.logger.Warn("Failed to list plugin tags", slog.String("plugin", pluginName), slog.String("error", err.Error()))
		return fmt.Errorf("failed to list plugin tags: %w", err)
	}

	if opts.useMajor >= 0 {
		versions = pc.filterMajorVersion(versions, opts.useMajor)
		if len(versions) == 0 {
			return fmt.Errorf("no versions found for major version: %d", opts.useMajor)
		}
	}

	installVersion, err = pc.findLatestVersion(versions)
	if err != nil {
		pc.logger.Warn("Failed to fetch latest version", slog.String("plugin", pluginName), slog.String("error", err.Error()))
		return fmt.Errorf("failed to fetch latest version: %w", err)
	}

	return pc.installPlugin(ctx, pluginName, installVersion, opts.resolvePluginsConflicts)
}

func (pc *PluginsCommand) installPlugin(ctx context.Context, pluginName string, version *semver.Version, resolvePluginsConflicts bool) error {
	// create plugin directory if it doesn't exist
	// example path: /opt/deckhouse/lib/deckhouse-cli/plugins/example-plugin
	pluginDir := path.Join(pc.pluginDirectory, "plugins", pluginName)
	err := os.MkdirAll(pluginDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create plugin directory: %w", err)
	}

	majorVersion := strconv.Itoa(int(version.Major()))

	// example path: /opt/deckhouse/lib/deckhouse-cli/plugins/example-plugin/v1
	versionDir := path.Join(pluginDir, "v"+majorVersion)
	err = os.MkdirAll(versionDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create plugin directory: %w", err)
	}

	// if locked - exit
	// example path: /opt/deckhouse/lib/deckhouse-cli/plugins/example-plugin/v1/example-plugin.lock
	lockFilePath := path.Join(versionDir, pluginName+".lock")
	_, err = os.Stat(lockFilePath)
	if err == nil {
		// File exists, plugin is locked
		return fmt.Errorf("plugin is locked by: %s", lockFilePath)
	}
	// Some other error occurred (permissions, etc.)
	if !os.IsNotExist(err) {
		return fmt.Errorf("failed to check lock file %s: %w", lockFilePath, err)
	}

	// create lock lockFile
	lockFile, err := os.Create(lockFilePath)
	if err != nil {
		return fmt.Errorf("failed to create lock file: %w", err)
	}
	lockFile.Close()
	defer os.Remove(lockFilePath)

	tag := version.Original()

	fmt.Printf("Installing plugin: %s\n", pluginName)
	fmt.Printf("Tag: %s\n", tag)

	// get contract
	plugin, err := pc.service.GetPluginContract(ctx, pluginName, tag)
	if err != nil {
		return fmt.Errorf("failed to get plugin contract: %w", err)
	}

	fmt.Printf("Plugin: %s %s\n", plugin.Name, plugin.Version)
	fmt.Printf("Description: %s\n", plugin.Description)

	// validate requirements
	pc.logger.Debug("validating requirements", slog.String("plugin", plugin.Name))
	failedConstraints, err := pc.validateRequirements(plugin)
	if err != nil {
		return fmt.Errorf("failed to validate requirements: %w", err)
	}
	if len(failedConstraints) > 0 && !resolvePluginsConflicts {
		return fmt.Errorf("plugin requirements not satisfied")
	}
	if len(failedConstraints) > 0 && resolvePluginsConflicts {
		// try to resolve conflicts
		err = pc.resolvePluginConflicts(ctx, failedConstraints)
		if err != nil {
			return fmt.Errorf("failed to resolve conflicts: %w", err)
		}
	}

	// check if binary exists (if yes - rename it to .old)
	// example path: /opt/deckhouse/lib/deckhouse-cli/plugins/example-plugin/v1/example-plugin
	pluginBinaryPath := path.Join(versionDir, pluginName)
	pluginBinaryInfo, err := os.Stat(pluginBinaryPath)
	if err == nil && !pluginBinaryInfo.IsDir() {
		err = os.Rename(pluginBinaryPath, pluginBinaryPath+".old")
		if err != nil {
			return fmt.Errorf("failed to save old version: %w", err)
		}
	}

	// extract plugin to installation directory
	fmt.Printf("Installing to: %s\n", pluginBinaryPath)

	fmt.Println("Downloading and extracting plugin...")
	err = pc.service.ExtractPlugin(ctx, pluginName, tag, pluginBinaryPath)
	if err != nil {
		pc.logger.Warn("Failed to extract plugin",
			slog.String("plugin", pluginName),
			slog.String("tag", tag),
			slog.String("destination", pluginBinaryPath),
			slog.String("error", err.Error()))
		return fmt.Errorf("failed to extract plugin: %w", err)
	}

	// symlink "current" to the installed version (delete old symlink if exists)
	// example path: /opt/deckhouse/lib/deckhouse-cli/plugins/example-plugin/current
	currentSymlink := path.Join(pluginDir, "current")
	_ = os.Remove(currentSymlink)

	absPath, err := filepath.Abs(pluginBinaryPath)
	if err != nil {
		return fmt.Errorf("failed to compute absolute path: %w", err)
	}

	err = os.Symlink(absPath, currentSymlink)
	if err != nil {
		return fmt.Errorf("failed to create symlink: %w", err)
	}

	// cache contract
	// example path: /opt/deckhouse/lib/deckhouse-cli/cache/contracts
	contractDir := path.Join(pc.pluginDirectory, "cache", "contracts")
	err = os.MkdirAll(contractDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create contract directory: %w", err)
	}

	// example path: /opt/deckhouse/lib/deckhouse-cli/cache/contracts/example-plugin.json
	contractFilePath := path.Join(contractDir, pluginName+".json")
	contract := service.DomainToContract(plugin)
	contractFile, err := os.OpenFile(contractFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open contract file: %w", err)
	}
	defer contractFile.Close()

	enc := json.NewEncoder(contractFile)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)

	err = enc.Encode(contract)
	if err != nil {
		return fmt.Errorf("failed to cache contract: %w", err)
	}

	fmt.Printf("✓ Plugin '%s' successfully installed!\n", pluginName)
	return nil
}

func (pc *PluginsCommand) filterMajorVersion(versions []string, majorVersion int) []string {
	res := make([]string, 0, 1)

	for _, ver := range versions {
		version, err := semver.NewVersion(ver)
		if err != nil {
			continue
		}

		if version.Major() == uint64(majorVersion) {
			res = append(res, ver)
		}
	}

	return res
}

func (pc *PluginsCommand) fetchLatestVersion(ctx context.Context, pluginName string) (*semver.Version, error) {
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
	return latestVersion, nil
}

func (pc *PluginsCommand) resolvePluginConflicts(ctx context.Context, failedConstraints FailedConstraints) error {
	installOptions := &installPluginOptions{
		resolvePluginsConflicts: true,
	}

	// for each failed constraint, try to install the plugin
	for pluginName := range failedConstraints {
		pc.logger.Debug("resolving plugin conflict", slog.String("plugin", pluginName))

		err := pc.InstallPlugin(ctx, pluginName, installOptions)
		if err != nil {
			return fmt.Errorf("failed to install plugin: %w", err)
		}
	}

	return nil
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

			return pc.InstallPlugin(ctx, pluginName, nil)
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
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			fmt.Println("Updating all installed plugins...")

			plugins, err := os.ReadDir(path.Join(pc.pluginDirectory, "plugins"))
			if err != nil {
				return fmt.Errorf("failed to read plugins directory: %w", err)
			}

			for _, plugin := range plugins {
				err := pc.InstallPlugin(ctx, plugin.Name(), nil)
				if err != nil {
					return fmt.Errorf("failed to update plugin: %w", err)
				}
			}

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
		RunE: func(_ *cobra.Command, args []string) error {
			pluginName := args[0]
			fmt.Printf("Removing plugin: %s\n", pluginName)

			pluginDir := path.Join(pc.pluginDirectory, "plugins", pluginName)
			fmt.Printf("Removing plugin from: %s\n", pluginDir)

			err := os.RemoveAll(pluginDir)
			if err != nil {
				return fmt.Errorf("failed to remove plugin directory: %w", err)
			}

			fmt.Println("Cleaning up plugin files...")

			os.Remove(path.Join(pc.pluginDirectory, "cache", "contracts", pluginName+".json"))

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
		RunE: func(_ *cobra.Command, _ []string) error {
			fmt.Println("Removing all installed plugins...")

			plugins, err := os.ReadDir(path.Join(pc.pluginDirectory, "plugins"))
			if err != nil {
				return fmt.Errorf("failed to read plugins directory: %w", err)
			}

			fmt.Println("Found", len(plugins), "plugins to remove:")

			for _, plugin := range plugins {
				pluginDir := path.Join(pc.pluginDirectory, "plugins", plugin.Name())
				fmt.Printf("Removing plugin from: %s\n", pluginDir)

				err := os.RemoveAll(pluginDir)
				if err != nil {
					return fmt.Errorf("failed to remove plugin directory: %w", err)
				}

				fmt.Printf("Cleaning up plugin files for '%s'...\n", plugin.Name())

				os.Remove(path.Join(pc.pluginDirectory, "cache", "contracts", plugin.Name()+".json"))

				fmt.Printf("✓ Plugin '%s' successfully removed!\n", plugin.Name())
			}

			fmt.Println("✓ All plugins successfully removed!")

			return nil
		},
	}

	return cmd
}

func (pc *PluginsCommand) getInstalledPluginVersion(pluginName string) (*semver.Version, error) {
	pluginBinaryPath := path.Join(pc.pluginDirectory, "plugins", pluginName, "current")
	cmd := exec.Command(pluginBinaryPath, "--version")

	output, err := cmd.Output()
	if err != nil {
		pc.logger.Warn("failed to call plugin with '--version'", slog.String("plugin", pluginName), slog.String("error", err.Error()))

		// try to call plugin with "version" command
		// this is for compatibility with plugins that don't support "--version"
		cmd = exec.Command(pluginBinaryPath, "version")

		output, err = cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("failed to call plugin: %w", err)
		}
	}

	version, err := semver.NewVersion(strings.TrimSpace(string(output)))
	if err != nil {
		return nil, fmt.Errorf("failed to parse version: %w", err)
	}

	return version, nil
}

// map of plugin name to failed constraints
type FailedConstraints map[string]*semver.Constraints

func (pc *PluginsCommand) validateRequirements(plugin *internal.Plugin) (FailedConstraints, error) {
	// validate plugin requirements
	pc.logger.Debug("validating plugin requirements", slog.String("plugin", plugin.Name))

	err := pc.validatePluginConflicts(plugin)
	if err != nil {
		return nil, fmt.Errorf("plugin conflicts: %w", err)
	}

	failedConstraints, err := pc.validatePluginRequirement(plugin)
	if err != nil {
		return nil, fmt.Errorf("plugin requirements: %w", err)
	}

	// validate module requirements
	pc.logger.Debug("validating module requirements", slog.String("plugin", plugin.Name))

	err = pc.validateModuleRequirement(plugin)
	if err != nil {
		return nil, fmt.Errorf("module requirements: %w", err)
	}

	return failedConstraints, nil
}

// check that installing version not make conflict with existing plugins requirements
func (pc *PluginsCommand) validatePluginConflicts(plugin *internal.Plugin) error {
	plugins, err := os.ReadDir(path.Join(pc.pluginDirectory, "plugins"))
	if err != nil {
		return fmt.Errorf("failed to read plugins directory: %w", err)
	}

	for _, pluginDir := range plugins {
		pluginName := pluginDir.Name()

		contract, err := pc.getInstalledPluginContract(pluginName)
		if err != nil {
			return fmt.Errorf("failed to get installed plugin contract: %w", err)
		}

		err = validatePluginConflict(plugin, contract)
		if err != nil {
			return fmt.Errorf("validate plugin conflict: %w", err)
		}
	}

	return nil
}

func validatePluginConflict(plugin *internal.Plugin, installedPlugin *internal.Plugin) error {
	for _, requirement := range installedPlugin.Requirements.Plugins {
		// installed plugin requirement is the same as the plugin we are validating
		if requirement.Name == plugin.Name {
			constraint, err := semver.NewConstraint(requirement.Constraint)
			if err != nil {
				return fmt.Errorf("failed to parse constraint: %w", err)
			}

			version, err := semver.NewVersion(installedPlugin.Version)
			if err != nil {
				return fmt.Errorf("failed to parse version: %w", err)
			}

			if !constraint.Check(version) {
				return fmt.Errorf("installing plugin %s %s will make conflict with existing plugin %s %s",
					plugin.Name,
					plugin.Version,
					installedPlugin.Name,
					constraint.String())
			}
		}
	}

	return nil
}

func (pc *PluginsCommand) validatePluginRequirement(plugin *internal.Plugin) (FailedConstraints, error) {
	result := make(FailedConstraints)

	for _, pluginRequirement := range plugin.Requirements.Plugins {
		// check if plugin is installed
		installed, err := pc.checkInstalled(pluginRequirement.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to check if plugin is installed: %w", err)
		}
		if !installed {
			result[pluginRequirement.Name] = nil
			continue
		}

		// check constraint
		if pluginRequirement.Constraint != "" {
			installedVersion, err := pc.getInstalledPluginVersion(pluginRequirement.Name)
			if err != nil {
				return nil, fmt.Errorf("failed to get installed version: %w", err)
			}

			constraint, err := semver.NewConstraint(pluginRequirement.Constraint)
			if err != nil {
				return nil, fmt.Errorf("failed to parse constraint: %w", err)
			}

			if !constraint.Check(installedVersion) {
				pc.logger.Warn("plugin requirement not satisfied",
					slog.String("plugin", plugin.Name),
					slog.String("requirement", pluginRequirement.Name),
					slog.String("constraint", pluginRequirement.Constraint),
					slog.String("installedVersion", installedVersion.Original()))

				result[pluginRequirement.Name] = constraint
			}
		}
	}

	return result, nil
}

func (pc *PluginsCommand) validateModuleRequirement(_ *internal.Plugin) error {
	// TODO: Implement module requirement validation
	return nil
}
