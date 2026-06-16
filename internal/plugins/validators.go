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
	"sort"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/deckhouse/deckhouse-cli/internal"
	d8flags "github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/requirements"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// clusterProbeTimeout bounds the cluster snapshot. Discovery().ServerVersion()
// has no context variant, so the bound is applied via rest.Config.Timeout.
const clusterProbeTimeout = 30 * time.Second

// InstalledPluginContract reads the cached contract from
// <plugin-dir>/cache/contracts/<plugin>.json and converts it to a domain object.
func (m *Manager) InstalledPluginContract(pluginName string) (*internal.Plugin, error) {
	contractFile := layout.ContractFile(m.pluginDirectory, pluginName)

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

// getInstalledPluginVersion runs the installed plugin's current binary and parses
// its reported version. It delegates to pluginBinaryVersion so the probe logic and
// its timeout are shared with the install path (no duplicate, no unbounded exec).
func (m *Manager) getInstalledPluginVersion(pluginName string) (*semver.Version, error) {
	ctx, cancel := context.WithTimeout(context.Background(), pluginProbeTimeout)
	defer cancel()

	return pluginBinaryVersion(ctx, layout.CurrentLinkPath(m.pluginDirectory, pluginName))
}

// LatestVersion lists tags from the registry for a plugin and returns the
// highest STABLE semver version - the same notion of "latest" that install
// selection uses (select.go), so `plugins list`/`contract` never advertise a
// pre-release that a default install would not pick.
func (m *Manager) LatestVersion(ctx context.Context, pluginName string) (*semver.Version, error) {
	versions, err := m.service.ListPluginTags(ctx, pluginName)
	if err != nil {
		return nil, fmt.Errorf("failed to list plugin tags: %w", err)
	}

	candidates := stableVersions(sortedSemverDesc(versions))
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no stable versions found for plugin %q", pluginName)
	}

	return candidates[0], nil
}

// failedConstraints holds plugin requirements that were not satisfied during
// installation: a nil value means the plugin is missing entirely, a non-nil
// value carries the constraint that the currently installed version fails.
type failedConstraints map[string]*semver.Constraints

// describe renders the unsatisfied requirements as a sorted, bulleted list - one
// per line, indented to sit under an error header:
//
//   - delivery-kit (not installed)
//   - foo (must satisfy >=2.0.0)
//
// nil value = dependency missing; non-nil = installed but fails the constraint.
func (fc failedConstraints) describe() string {
	parts := make([]string, 0, len(fc))

	for name, constraint := range fc {
		if constraint == nil {
			parts = append(parts, fmt.Sprintf("  - %s (not installed)", name))
		} else {
			parts = append(parts, fmt.Sprintf("  - %s (must satisfy %s)", name, constraint))
		}
	}

	sort.Strings(parts)

	return strings.Join(parts, "\n")
}

func (m *Manager) validateRequirements(ctx context.Context, plugin *internal.Plugin) (failedConstraints, error) {
	m.logger.Debug("validating plugin requirements", slog.String("plugin", plugin.Name))

	if err := m.validatePluginConflicts(plugin); err != nil {
		return nil, fmt.Errorf("plugin conflicts: %w", err)
	}

	failedConstraints, err := m.validatePluginRequirementMandatory(plugin)
	if err != nil {
		return nil, fmt.Errorf("plugin requirements (mandatory): %w", err)
	}

	if err := m.validatePluginRequirementConditional(plugin); err != nil {
		return nil, fmt.Errorf("plugin requirements (conditional): %w", err)
	}

	if err := m.validateClusterRequirements(ctx, plugin); err != nil {
		return nil, err
	}

	return failedConstraints, nil
}

// validatePluginConflicts checks that installing the plugin does not violate any
// constraint placed on it by already-installed plugins.
func (m *Manager) validatePluginConflicts(plugin *internal.Plugin) error {
	contractDir, err := os.ReadDir(layout.ContractsDir(m.pluginDirectory))
	if err != nil && errors.Is(err, os.ErrNotExist) {
		// No contracts dir yet: no installed plugins, nothing to conflict with.
		m.logger.Debug("no installed plugins, skipping conflict check")
		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to read contract directory: %w", err)
	}

	for _, contractFile := range contractDir {
		pluginName := strings.TrimSuffix(contractFile.Name(), layout.ContractFileExt)

		contract, err := m.InstalledPluginContract(pluginName)
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
		// Check the NEW plugin's version against the constraint, not the installed
		// plugin's version.
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
//   - if the dependency is not installed, record a soft failure in failedConstraints;
//   - if the dependency is installed but fails the constraint, record a soft failure;
//   - return a non-nil error only for operational failures (install check, version
//     lookup, invalid constraint).
func (m *Manager) validatePluginRequirementMandatory(plugin *internal.Plugin) (failedConstraints, error) {
	result := make(failedConstraints)

	for _, pluginRequirement := range plugin.Requirements.Plugins.Mandatory {
		installed, err := m.checkInstalled(pluginRequirement.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to check if plugin is installed: %w", err)
		}

		if !installed {
			m.logger.Debug("plugin requirement not installed",
				slog.String("plugin", plugin.Name),
				slog.String("requirement", pluginRequirement.Name))
			result[pluginRequirement.Name] = nil

			continue
		}

		if pluginRequirement.Constraint == "" {
			continue
		}

		installedVersion, err := m.getInstalledPluginVersion(pluginRequirement.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get installed version: %w", err)
		}

		constraint, err := semver.NewConstraint(pluginRequirement.Constraint)
		if err != nil {
			return nil, fmt.Errorf("failed to parse constraint: %w", err)
		}

		if !constraint.Check(installedVersion) {
			m.logger.Debug("plugin requirement not satisfied",
				slog.String("plugin", plugin.Name),
				slog.String("requirement", pluginRequirement.Name),
				slog.String("constraint", pluginRequirement.Constraint),
				slog.String("installed_version", installedVersion.Original()))
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
func (m *Manager) validatePluginRequirementConditional(plugin *internal.Plugin) error {
	for _, pluginRequirement := range plugin.Requirements.Plugins.Conditional {
		installed, err := m.checkInstalled(pluginRequirement.Name)
		if err != nil {
			return fmt.Errorf("failed to check if plugin is installed: %w", err)
		}

		if !installed {
			continue
		}

		if pluginRequirement.Constraint == "" {
			continue
		}

		installedVersion, err := m.getInstalledPluginVersion(pluginRequirement.Name)
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

// clusterState builds (and caches) the snapshot used by the cluster-side checks,
// using the kubeconfig identity from the plugin flags. A failure here is fatal for
// the caller: if a plugin declares a requirement we cannot verify, we must not
// install it blindly.
func (m *Manager) clusterState(ctx context.Context) (*requirements.ClusterState, error) {
	if m.clusterStateCache != nil {
		return m.clusterStateCache, nil
	}

	restConfig, _, err := utilk8s.SetupK8sClientSet(d8flags.Kubeconfig, d8flags.KubeContext)
	if err != nil {
		return nil, fmt.Errorf("set up kubernetes client: %w", err)
	}

	// Build both clients from a timeout-bounded config so the un-cancellable version
	// probe cannot hang indefinitely on an unreachable API server.
	restConfig.Timeout = clusterProbeTimeout

	kubeCl, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("set up kubernetes client: %w", err)
	}

	dynamicCl, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("set up dynamic client: %w", err)
	}

	state, err := requirements.LoadClusterState(ctx, kubeCl, dynamicCl, m.logger)
	if err != nil {
		return nil, err
	}

	m.clusterStateCache = state

	return state, nil
}

// clusterChecks returns the cluster-side check list bound to m's logger. The
// ordered list is the single source of truth shared by enforcement
// (validateClusterRequirements) and selection (clusterCompatible).
func (m *Manager) clusterChecks() []requirements.Check {
	return requirements.NewChecker(m.logger).Checks()
}

// validateClusterRequirements enforces the cluster-side requirements (Kubernetes,
// Deckhouse and module versions). It inspects the cluster only when the plugin
// actually declares such a requirement, so plugins without them install without a
// cluster connection. A snapshot that cannot be built is a hard error: a declared
// requirement we cannot verify must not be silently ignored.
func (m *Manager) validateClusterRequirements(ctx context.Context, plugin *internal.Plugin) error {
	if !requirements.HasClusterRequirements(plugin) {
		return nil
	}

	if d8flags.SkipClusterChecks {
		m.logger.Warn("skipping cluster-side requirement checks (--skip-cluster-checks set)",
			slog.String("plugin", plugin.Name))

		return nil
	}

	state, err := m.clusterState(ctx)
	if err != nil {
		return fmt.Errorf("cannot reach the cluster to verify %q requirements "+
			"(set "+d8flags.EnvSkipClusterChecks+"=1, or pass --skip-cluster-checks to 'd8 plugins ...', to skip verification): %w",
			plugin.Name, err)
	}

	for _, check := range m.clusterChecks() {
		if err := check.Run(plugin, state); err != nil {
			return fmt.Errorf("%s: %w", check.Name, err)
		}
	}

	return nil
}
