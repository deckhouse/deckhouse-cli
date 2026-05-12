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
	pc.logger.Debug("validating plugin requirements", slog.String("plugin", plugin.Name))

	if err := pc.validatePluginConflicts(plugin); err != nil {
		return nil, fmt.Errorf("plugin conflicts: %w", err)
	}

	failedConstraints, err := pc.validatePluginRequirementMandatory(plugin)
	if err != nil {
		return nil, fmt.Errorf("plugin requirements (mandatory): %w", err)
	}

	if err := pc.validatePluginRequirementConditional(plugin); err != nil {
		return nil, fmt.Errorf("plugin requirements (conditional): %w", err)
	}

	// Cluster-side requirements - currently log-only, no enforcement.
	// Real validation lands when cluster connectivity is added.
	if err := pc.validateKubernetesRequirement(plugin); err != nil {
		return nil, fmt.Errorf("kubernetes requirement: %w", err)
	}
	if err := pc.validateDeckhouseRequirement(plugin); err != nil {
		return nil, fmt.Errorf("deckhouse requirement: %w", err)
	}

	pc.logger.Debug("validating module requirements", slog.String("plugin", plugin.Name))
	if err := pc.validateModuleRequirement(plugin); err != nil {
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

// validatePluginConflict checks whether installing `plugin` violates any
// constraint that the already-installed `installedPlugin` places on it.
//
// Both Mandatory and Conditional sections of installedPlugin's requirements
// are inspected - if an existing plugin requires us, we must satisfy its
// constraint regardless of whether the requirement is mandatory or conditional.
func validatePluginConflict(plugin *internal.Plugin, installedPlugin *internal.Plugin) error {
	candidates := make([]internal.PluginRequirement, 0,
		len(installedPlugin.Requirements.Plugins.Mandatory)+len(installedPlugin.Requirements.Plugins.Conditional))
	candidates = append(candidates, installedPlugin.Requirements.Plugins.Mandatory...)
	candidates = append(candidates, installedPlugin.Requirements.Plugins.Conditional...)

	for _, requirement := range candidates {
		if requirement.Name != plugin.Name {
			continue
		}
		constraint, err := semver.NewConstraint(requirement.Constraint)
		if err != nil {
			return fmt.Errorf("failed to parse constraint: %w", err)
		}
		// Check the NEW plugin's version against the constraint -
		// not installedPlugin.Version (that was a long-standing bug).
		version, err := semver.NewVersion(plugin.Version)
		if err != nil {
			return fmt.Errorf("failed to parse version: %w", err)
		}
		if !constraint.Check(version) {
			return fmt.Errorf("installing plugin %s %s conflicts with existing plugin %s which requires %s %s",
				plugin.Name,
				plugin.Version,
				installedPlugin.Name,
				plugin.Name,
				constraint.String())
		}
	}

	return nil
}

// validatePluginRequirementMandatory enforces mandatory plugin requirements:
//
// For mandatory requirements:
// - if the dependency is not installed, return a soft failure (FailedConstraints)
// - if the dependency is installed but fails the constraint, return a hard error
func (pc *PluginsCommand) validatePluginRequirementMandatory(plugin *internal.Plugin) (FailedConstraints, error) {
	result := make(FailedConstraints)

	for _, pluginRequirement := range plugin.Requirements.Plugins.Mandatory {
		installed, err := pc.checkInstalled(pluginRequirement.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to check if plugin is installed: %w", err)
		}
		if !installed {
			result[pluginRequirement.Name] = nil
			continue
		}
		if pluginRequirement.Constraint == "" {
			continue
		}
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

	return result, nil
}

// validatePluginRequirementConditional enforces conditional plugin requirements:
//
// For conditional requirements:
//   - if the dependency is not installed, skip silently;
//   - if the dependency is installed but fails the constraint, return a hard error
func (pc *PluginsCommand) validatePluginRequirementConditional(plugin *internal.Plugin) error {
	for _, pluginRequirement := range plugin.Requirements.Plugins.Conditional {
		installed, err := pc.checkInstalled(pluginRequirement.Name)
		if err != nil {
			return fmt.Errorf("failed to check if plugin is installed: %w", err)
		}
		if !installed {
			continue
		}
		if pluginRequirement.Constraint == "" {
			continue
		}
		installedVersion, err := pc.getInstalledPluginVersion(pluginRequirement.Name)
		if err != nil {
			return fmt.Errorf("failed to get installed version: %w", err)
		}
		constraint, err := semver.NewConstraint(pluginRequirement.Constraint)
		if err != nil {
			return fmt.Errorf("failed to parse constraint: %w", err)
		}
		if !constraint.Check(installedVersion) {
			return fmt.Errorf("conditional plugin requirement not satisfied: plugin %s %s installed but %s requires %s",
				pluginRequirement.Name,
				installedVersion.Original(),
				plugin.Name,
				pluginRequirement.Constraint)
		}
	}
	return nil
}

// validateKubernetesRequirement is a log-only stub (yet).
//
// For Kubernetes requirement:
// - if the constraint is empty, skip silently
// - if the constraint is not empty, log a warning
func (pc *PluginsCommand) validateKubernetesRequirement(plugin *internal.Plugin) error {
	if plugin.Requirements.Kubernetes.Constraint == "" {
		return nil
	}
	pc.logger.Warn("plugin declares a Kubernetes version requirement but enforcement is not implemented yet",
		slog.String("plugin", plugin.Name),
		slog.String("constraint", plugin.Requirements.Kubernetes.Constraint),
	)
	return nil
}

// validateDeckhouseRequirement is a log-only stub. See validateKubernetesRequirement
// for rationale; enforcement will land once Deckhouse version discovery is in place.
func (pc *PluginsCommand) validateDeckhouseRequirement(plugin *internal.Plugin) error {
	if plugin.Requirements.Deckhouse.Constraint == "" {
		return nil
	}
	pc.logger.Warn("plugin declares a Deckhouse version requirement but enforcement is not implemented yet",
		slog.String("plugin", plugin.Name),
		slog.String("constraint", plugin.Requirements.Deckhouse.Constraint),
	)
	return nil
}

// validateModuleRequirement is a log-only stub. Mandatory, Conditional and
// AnyOf sections are all surfaced via Warn so authors and operators see
// the declared expectations even though d8 does not yet inspect the cluster
// to verify them.
func (pc *PluginsCommand) validateModuleRequirement(plugin *internal.Plugin) error {
	mods := plugin.Requirements.Modules
	if len(mods.Mandatory) == 0 && len(mods.Conditional) == 0 && len(mods.AnyOf) == 0 {
		return nil
	}

	for _, m := range mods.Mandatory {
		pc.logger.Warn("plugin declares a mandatory module requirement but enforcement is not implemented yet",
			slog.String("plugin", plugin.Name),
			slog.String("module", m.Name),
			slog.String("constraint", m.Constraint),
		)
	}
	for _, m := range mods.Conditional {
		pc.logger.Warn("plugin declares a conditional module requirement but enforcement is not implemented yet",
			slog.String("plugin", plugin.Name),
			slog.String("module", m.Name),
			slog.String("constraint", m.Constraint),
		)
	}
	for i, grp := range mods.AnyOf {
		names := make([]string, 0, len(grp.Modules))
		for _, m := range grp.Modules {
			names = append(names, m.Name)
		}
		pc.logger.Warn("plugin declares an anyOf module group but enforcement is not implemented yet",
			slog.String("plugin", plugin.Name),
			slog.Int("group_index", i),
			slog.String("group_description", grp.Description),
			slog.Any("modules", names),
		)
	}
	return nil
}
