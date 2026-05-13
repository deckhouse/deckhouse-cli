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
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/Masterminds/semver/v3"
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/cmd/layout"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

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

			opts := []installPluginOption{
				installWithVersion(version),
				installWithMajorVersion(useMajor),
			}

			if resolvePluginsConflicts {
				opts = append(opts, installWithResolvePluginsConflicts())
			}

			return pc.InstallPlugin(ctx, pluginName, opts...)
		},
	}

	cmd.Flags().StringVar(&version, "version", "", "Specific version of the plugin to install")
	cmd.Flags().IntVar(&useMajor, "use-major", -1, "Use specific major version (e.g., 1, 2)")
	cmd.Flags().BoolVar(&resolvePluginsConflicts, "resolve-plugins-conflicts", false, "Resolve conflicts between plugins requirements")

	return cmd
}

type installPluginOptions struct {
	version                 string
	majorVersion            int
	resolvePluginsConflicts bool
}

type installPluginOption func(*installPluginOptions)

func installWithMajorVersion(majorVersion int) installPluginOption {
	return func(opts *installPluginOptions) {
		opts.majorVersion = majorVersion
	}
}

func installWithVersion(version string) installPluginOption {
	return func(opts *installPluginOptions) {
		opts.version = version
	}
}

func installWithResolvePluginsConflicts() installPluginOption {
	return func(opts *installPluginOptions) {
		opts.resolvePluginsConflicts = true
	}
}

// InstallPlugin checks if plugin can be installed, creates folders layout and then installs plugin, creates symlink "current" and caches contract.json.
// version - semver version string (e.g. v1.0.0), default: "" (use latest version)
// useMajor - major version to install, default: -1 (use latest major version)
// resolvePluginsConflicts - resolve conflicts between installed plugins, default: false
func (pc *PluginsCommand) InstallPlugin(ctx context.Context, pluginName string, opts ...installPluginOption) error {
	// check if version is specified
	var installVersion *semver.Version

	options := &installPluginOptions{
		majorVersion: -1,
	}

	for _, opt := range opts {
		opt(options)
	}

	if options.version != "" {
		var err error
		installVersion, err = semver.NewVersion(options.version)
		if err != nil {
			return fmt.Errorf("failed to parse version: %w", err)
		}

		return pc.installPlugin(ctx, pluginName, installVersion, options.resolvePluginsConflicts)
	}

	versions, err := pc.service.ListPluginTags(ctx, pluginName)
	if err != nil {
		pc.logger.Warn("Failed to list plugin tags", slog.String("plugin", pluginName), slog.String("error", err.Error()))
		return fmt.Errorf("failed to list plugin tags: %w", err)
	}

	if options.majorVersion >= 0 {
		versions = pc.filterMajorVersion(versions, options.majorVersion)
		if len(versions) == 0 {
			return fmt.Errorf("no versions found for major version: %d", options.majorVersion)
		}
	}

	installVersion, err = pc.findLatestVersion(versions)
	if err != nil {
		pc.logger.Warn("Failed to fetch latest version", slog.String("plugin", pluginName), slog.String("error", err.Error()))
		return fmt.Errorf("failed to fetch latest version: %w", err)
	}

	return pc.installPlugin(ctx, pluginName, installVersion, options.resolvePluginsConflicts)
}

// pluginPaths bundles the filesystem locations an install operates on.
// Created once by preparePluginDirs and threaded through the install pipeline.
type pluginPaths struct {
	pluginDir   string // <plugin-dir>/plugins/<name>
	versionDir  string // <plugin-dir>/plugins/<name>/v<major>
	binaryPath  string // <plugin-dir>/plugins/<name>/v<major>/<name>
	lockPath    string // <plugin-dir>/plugins/<name>/v<major>/<name>.lock
	currentLink string // <plugin-dir>/plugins/<name>/current
}

// installPlugin is the install pipeline orchestrator. Each step delegates to
// a focused helper below; the order is significant and stays identical to
// the pre-split monolith.
func (pc *PluginsCommand) installPlugin(ctx context.Context, pluginName string, version *semver.Version, resolvePluginsConflicts bool) error {
	paths, err := pc.preparePluginDirs(pluginName, version)
	if err != nil {
		return err
	}

	release, err := pc.acquireInstallLock(paths.lockPath)
	if err != nil {
		return err
	}
	defer release()

	plugin, err := pc.fetchAndDisplayContract(ctx, pluginName, version)
	if err != nil {
		return err
	}

	if err := pc.validateAndResolveConflicts(ctx, plugin, resolvePluginsConflicts); err != nil {
		return err
	}

	if err := pc.backupOldBinary(paths.binaryPath); err != nil {
		return err
	}

	if err := pc.downloadAndExtract(ctx, pluginName, version, paths.binaryPath); err != nil {
		return err
	}

	if err := pc.linkCurrent(paths); err != nil {
		return err
	}

	if err := pc.cacheContract(pluginName, plugin); err != nil {
		return err
	}

	fmt.Printf("✓ Plugin '%s' successfully installed!\n", pluginName)
	return nil
}

// preparePluginDirs creates plugins/<name>/v<major> on disk and returns the
// paths derived from <pluginName, version> used by the rest of the pipeline.
func (pc *PluginsCommand) preparePluginDirs(pluginName string, version *semver.Version) (pluginPaths, error) {
	major := int(version.Major())
	paths := pluginPaths{
		pluginDir:   layout.PluginDir(pc.pluginDirectory, pluginName),
		versionDir:  layout.VersionDir(pc.pluginDirectory, pluginName, major),
		binaryPath:  layout.BinaryPath(pc.pluginDirectory, pluginName, major),
		lockPath:    layout.LockPath(pc.pluginDirectory, pluginName, major),
		currentLink: layout.CurrentLinkPath(pc.pluginDirectory, pluginName),
	}

	if err := os.MkdirAll(paths.pluginDir, 0755); err != nil {
		return pluginPaths{}, fmt.Errorf("failed to create plugin directory: %w", err)
	}
	if err := os.MkdirAll(paths.versionDir, 0755); err != nil {
		return pluginPaths{}, fmt.Errorf("failed to create plugin directory: %w", err)
	}

	return paths, nil
}

// acquireInstallLock creates the lock file at lockFilePath; if it already
// exists, returns an error without touching it. The caller must invoke the
// returned release func when finished (typically via defer).
func (pc *PluginsCommand) acquireInstallLock(lockFilePath string) (func(), error) {
	_, err := os.Stat(lockFilePath)
	if err == nil {
		// File exists, plugin is locked
		return nil, fmt.Errorf("plugin is locked by: %s", lockFilePath)
	}
	// Some other error occurred (permissions, etc.)
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to check lock file %s: %w", lockFilePath, err)
	}

	lockFile, err := os.Create(lockFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create lock file: %w", err)
	}
	lockFile.Close()

	return func() { os.Remove(lockFilePath) }, nil
}

// fetchAndDisplayContract pulls the contract for <pluginName, version> from
// the registry and prints the installing-plugin banner.
func (pc *PluginsCommand) fetchAndDisplayContract(ctx context.Context, pluginName string, version *semver.Version) (*internal.Plugin, error) {
	tag := version.Original()

	fmt.Printf("Installing plugin: %s\n", pluginName)
	fmt.Printf("Tag: %s\n", tag)

	plugin, err := pc.service.GetPluginContract(ctx, pluginName, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get plugin contract: %w", err)
	}

	fmt.Printf("Plugin: %s %s\n", plugin.Name, plugin.Version)
	fmt.Printf("Description: %s\n", plugin.Description)

	return plugin, nil
}

// validateAndResolveConflicts runs validateRequirements; if requirements are
// not satisfied and resolvePluginsConflicts is true, attempts to fix them
// recursively; otherwise returns an error.
func (pc *PluginsCommand) validateAndResolveConflicts(ctx context.Context, plugin *internal.Plugin, resolvePluginsConflicts bool) error {
	pc.logger.Debug("validating requirements", slog.String("plugin", plugin.Name))

	failedConstraints, err := pc.validateRequirements(plugin)
	if err != nil {
		return fmt.Errorf("failed to validate requirements: %w", err)
	}
	if len(failedConstraints) > 0 && !resolvePluginsConflicts {
		return fmt.Errorf("plugin requirements not satisfied")
	}
	if len(failedConstraints) > 0 && resolvePluginsConflicts {
		if err := pc.resolvePluginConflicts(ctx, failedConstraints); err != nil {
			return fmt.Errorf("failed to resolve conflicts: %w", err)
		}
	}
	return nil
}

// backupOldBinary renames an already-installed binary to <binaryPath>.old so
// a fresh extract has a clean destination. No-op if no binary present yet.
func (pc *PluginsCommand) backupOldBinary(binaryPath string) error {
	info, err := os.Stat(binaryPath)
	if err != nil || info.IsDir() {
		return nil
	}
	if err := os.Rename(binaryPath, binaryPath+".old"); err != nil {
		return fmt.Errorf("failed to save old version: %w", err)
	}
	return nil
}

// downloadAndExtract pulls the plugin image tag and writes the embedded
// binary to <binaryPath>.
func (pc *PluginsCommand) downloadAndExtract(ctx context.Context, pluginName string, version *semver.Version, binaryPath string) error {
	tag := version.Original()
	fmt.Printf("Installing to: %s\n", binaryPath)
	fmt.Println("Downloading and extracting plugin...")

	if err := pc.service.ExtractPlugin(ctx, pluginName, tag, binaryPath); err != nil {
		pc.logger.Warn("Failed to extract plugin",
			slog.String("plugin", pluginName),
			slog.String("tag", tag),
			slog.String("destination", binaryPath),
			slog.String("error", err.Error()))
		return fmt.Errorf("failed to extract plugin: %w", err)
	}
	return nil
}

// linkCurrent (re)points <pluginDir>/current to the freshly installed binary
// using an absolute target path.
func (pc *PluginsCommand) linkCurrent(paths pluginPaths) error {
	_ = os.Remove(paths.currentLink)

	absPath, err := filepath.Abs(paths.binaryPath)
	if err != nil {
		return fmt.Errorf("failed to compute absolute path: %w", err)
	}

	if err := os.Symlink(absPath, paths.currentLink); err != nil {
		return fmt.Errorf("failed to create symlink: %w", err)
	}
	return nil
}

// cacheContract writes the plugin contract JSON to
// <plugin-dir>/cache/contracts/<name>.json for later lookups by
// validatePluginConflicts and `d8 plugins list`.
func (pc *PluginsCommand) cacheContract(pluginName string, plugin *internal.Plugin) error {
	if err := os.MkdirAll(layout.ContractsDir(pc.pluginDirectory), 0755); err != nil {
		return fmt.Errorf("failed to create contract directory: %w", err)
	}

	contract := service.DomainToContract(plugin)
	contractFile, err := os.OpenFile(layout.ContractFile(pc.pluginDirectory, pluginName), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open contract file: %w", err)
	}
	defer contractFile.Close()

	enc := json.NewEncoder(contractFile)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)

	if err := enc.Encode(contract); err != nil {
		return fmt.Errorf("failed to cache contract: %w", err)
	}
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

func (pc *PluginsCommand) resolvePluginConflicts(ctx context.Context, failedConstraints FailedConstraints) error {
	// for each failed constraint, try to install the plugin
	for pluginName := range failedConstraints {
		pc.logger.Debug("resolving plugin conflict", slog.String("plugin", pluginName))

		err := pc.InstallPlugin(ctx, pluginName, installWithResolvePluginsConflicts())
		if err != nil {
			return fmt.Errorf("failed to install plugin: %w", err)
		}
	}

	return nil
}
