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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"

	"github.com/Masterminds/semver/v3"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/cmd/layout"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// getInstalledPluginContract reads the cached contract from
// <plugin-dir>/cache/contracts/<plugin>.json and converts it to a domain object.
func (pc *PluginsCommand) getInstalledPluginContract(pluginName string) (*internal.Plugin, error) {
	contractFile := layout.ContractFile(pc.pluginDirectory, pluginName)

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

// getInstalledPluginVersion runs the installed plugin binary with "--version"
// (or "version" as fallback) and parses the output as a semver value.
func (pc *PluginsCommand) getInstalledPluginVersion(pluginName string) (*semver.Version, error) {
	pluginBinaryPath := layout.CurrentLinkPath(pc.pluginDirectory, pluginName)
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

// fetchLatestVersion lists tags from the registry for a plugin and returns
// the highest semver version.
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

// FailedConstraints holds plugin requirements that were not satisfied during
// installation: a nil value means the plugin is missing entirely, a non-nil
// value carries the constraint that the currently installed version fails.
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
	contractDir, err := os.ReadDir(layout.ContractsDir(pc.pluginDirectory))
	// if no plugins installed, nothing to conflict
	if err != nil && errors.Is(err, os.ErrNotExist) {
		pc.logger.Debug("failed to read contract directory", slog.String("error", err.Error()))
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read contract directory: %w", err)
	}

	for _, contractFile := range contractDir {
		pluginName := strings.TrimSuffix(contractFile.Name(), layout.ContractFileExt)

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
	for _, requirement := range installedPlugin.Requirements.Plugins.Mandatory {
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

	for _, pluginRequirement := range plugin.Requirements.Plugins.Mandatory {
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
