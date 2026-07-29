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

package requirements

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/Masterminds/semver/v3"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal"
)

// Checker runs the cluster-side requirement checks. It carries only the logger
// used by the skip-with-a-warning paths (unknown Deckhouse version, module
// without a version).
type Checker struct {
	logger *dkplog.Logger
}

func NewChecker(logger *dkplog.Logger) *Checker {
	return &Checker{logger: logger}
}

// Check is one named cluster-side validator. The ordered list returned by
// Checker.Checks is the single source of truth shared by enforcement
// (install/run gating) and version selection in internal/plugins, so a new
// check is added in one place and cannot drift between the two.
type Check struct {
	Name string
	Run  func(*internal.Plugin, *ClusterState) error
}

func (c *Checker) Checks() []Check {
	return []Check{
		{"kubernetes requirement", c.validateKubernetesRequirement},
		{"deckhouse requirement", c.validateDeckhouseRequirement},
		{"module requirements", c.validateModuleRequirement},
	}
}

// unmetRequirementError marks a requirement the cluster genuinely does not satisfy,
// as opposed to an operational error (e.g. a malformed constraint in the contract).
// Version selection treats "unmet" as "try an older version" but must propagate
// operational errors instead of silently masking a broken contract.
type unmetRequirementError struct{ msg string }

func (e unmetRequirementError) Error() string { return e.msg }

func unmetf(format string, args ...any) error {
	return unmetRequirementError{msg: fmt.Sprintf(format, args...)}
}

// IsUnmet reports whether err marks a genuinely unmet requirement (as opposed
// to an operational failure that must not be masked).
func IsUnmet(err error) bool {
	var unmet unmetRequirementError

	return errors.As(err, &unmet)
}

// HasClusterRequirements reports whether the plugin declares any requirement that
// needs cluster state to verify (Kubernetes / Deckhouse / modules).
func HasClusterRequirements(plugin *internal.Plugin) bool {
	requirements := plugin.Requirements

	return requirements.Kubernetes.Constraint != "" ||
		requirements.Deckhouse.Constraint != "" ||
		len(requirements.Modules.Mandatory) > 0 ||
		len(requirements.Modules.Conditional) > 0 ||
		len(requirements.Modules.AnyOf) > 0 ||
		len(requirements.Modules.NoneOf) > 0
}

// normalizedForConstraint prepares a version for constraint matching.
// Build metadata is always dropped. The pre-release segment depends on its kind:
//   - genuine RC (rc/alpha/beta/etc.): kept, so boundary constraints treat an RC as below its GA;
//   - CI/build markers ("v1.77.0-main+abc", "v1.28.3-eks-1-30"): stripped, so a plain floor like ">= 1.0" matches them.
//
// Trade-off: for genuine RCs, ">= 1.30" excludes 1.30.0-rc.1.
func normalizedForConstraint(v *semver.Version) *semver.Version {
	pre := v.Prerelease()
	if pre != "" && IsGenuinePrerelease(pre) {
		return semver.New(v.Major(), v.Minor(), v.Patch(), pre, "")
	}

	return semver.New(v.Major(), v.Minor(), v.Patch(), "", "")
}

// IsGenuinePrerelease reports whether a pre-release segment denotes a real
// pre-release (rc/alpha/beta/preview/snapshot) rather than a CI/build marker.
// Version selection uses the same notion to keep pre-releases out of the
// default pick.
func IsGenuinePrerelease(pre string) bool {
	first := strings.ToLower(strings.SplitN(pre, ".", 2)[0])

	for _, marker := range []string{"alpha", "beta", "rc", "pre", "preview", "snapshot"} {
		if strings.HasPrefix(first, marker) {
			return true
		}
	}

	return false
}

// validateKubernetesRequirement fails if the cluster Kubernetes version does not
// satisfy the plugin's constraint, or cannot be determined.
func (c *Checker) validateKubernetesRequirement(plugin *internal.Plugin, state *ClusterState) error {
	if plugin.Requirements.Kubernetes.Constraint == "" {
		return nil
	}

	if state.Kubernetes == nil {
		return fmt.Errorf("plugin %s requires Kubernetes %s, but the cluster Kubernetes version could not be determined",
			plugin.Name, plugin.Requirements.Kubernetes.Constraint)
	}

	constraint, err := semver.NewConstraint(plugin.Requirements.Kubernetes.Constraint)
	if err != nil {
		return fmt.Errorf("parse kubernetes constraint %q: %w", plugin.Requirements.Kubernetes.Constraint, err)
	}

	if !constraint.Check(normalizedForConstraint(state.Kubernetes)) {
		return unmetf("plugin %s requires Kubernetes %s, but the cluster runs %s",
			plugin.Name, plugin.Requirements.Kubernetes.Constraint, state.Kubernetes.Original())
	}

	return nil
}

// validateDeckhouseRequirement fails if the cluster Deckhouse version does not
// satisfy the plugin's constraint. A non-release cluster version (e.g. "dev",
// recorded as nil) is skipped with a warning rather than blocking the install.
func (c *Checker) validateDeckhouseRequirement(plugin *internal.Plugin, state *ClusterState) error {
	if plugin.Requirements.Deckhouse.Constraint == "" {
		return nil
	}

	if state.Deckhouse == nil {
		c.logger.Warn("skipping Deckhouse version requirement: cluster version is not a release semver",
			slog.String("plugin", plugin.Name),
			slog.String("constraint", plugin.Requirements.Deckhouse.Constraint))

		return nil
	}

	constraint, err := semver.NewConstraint(plugin.Requirements.Deckhouse.Constraint)
	if err != nil {
		return fmt.Errorf("parse deckhouse constraint %q: %w", plugin.Requirements.Deckhouse.Constraint, err)
	}

	if !constraint.Check(normalizedForConstraint(state.Deckhouse)) {
		return unmetf("plugin %s requires Deckhouse %s, but the cluster runs %s",
			plugin.Name, plugin.Requirements.Deckhouse.Constraint, state.Deckhouse.Original())
	}

	return nil
}

// validateModuleRequirement enforces module requirements against the cluster:
//   - Mandatory: the module must be enabled and satisfy its version constraint;
//   - Conditional: checked only when the module is enabled;
//   - AnyOf: at least one module in each group must be enabled and satisfy its constraint;
//   - NoneOf: no module in any group may be enabled within its forbidden version range.
func (c *Checker) validateModuleRequirement(plugin *internal.Plugin, state *ClusterState) error {
	for _, requirement := range plugin.Requirements.Modules.Mandatory {
		module, enabled := enabledModule(state, requirement.Name)
		if !enabled {
			return unmetf("plugin %s requires module %q to be enabled, but it is not", plugin.Name, requirement.Name)
		}

		if err := c.checkModuleConstraint(plugin.Name, requirement, module); err != nil {
			return err
		}
	}

	for _, requirement := range plugin.Requirements.Modules.Conditional {
		module, enabled := enabledModule(state, requirement.Name)
		if !enabled {
			continue
		}

		if err := c.checkModuleConstraint(plugin.Name, requirement, module); err != nil {
			return err
		}
	}

	for index, group := range plugin.Requirements.Modules.AnyOf {
		if err := c.checkAnyOfModules(plugin.Name, index, group, state); err != nil {
			return err
		}
	}

	for index, group := range plugin.Requirements.Modules.NoneOf {
		if err := c.checkNoneOfModules(plugin.Name, index, group, state); err != nil {
			return err
		}
	}

	return nil
}

// enabledModule returns the module's state and whether it is present and enabled.
func enabledModule(state *ClusterState, name string) (ModuleState, bool) {
	module, present := state.Modules[name]

	return module, present && module.Enabled
}

// evaluateModuleVersion checks an enabled module's version against the requirement.
// It returns (satisfied, versionKnown, err):
//   - err is non-nil only for an operational failure (a malformed constraint string);
//   - versionKnown is false when the module reports no version;
//   - satisfied is meaningful only when versionKnown is true (or the constraint is empty).
func evaluateModuleVersion(requirement internal.ModuleRequirement, module ModuleState) (bool, bool, error) {
	if requirement.Constraint == "" {
		return true, true, nil
	}

	constraint, err := semver.NewConstraint(requirement.Constraint)
	if err != nil {
		return false, false, fmt.Errorf("parse module %q constraint %q: %w", requirement.Name, requirement.Constraint, err)
	}

	if module.Version == nil {
		return false, false, nil
	}

	return constraint.Check(normalizedForConstraint(module.Version)), true, nil
}

// checkModuleConstraint verifies a mandatory/conditional module's version. A module
// that reports no version is skipped with a warning (its presence/enabled state is
// already enforced by the caller) rather than failing the install.
func (c *Checker) checkModuleConstraint(pluginName string, requirement internal.ModuleRequirement, module ModuleState) error {
	satisfied, versionKnown, err := evaluateModuleVersion(requirement, module)
	if err != nil {
		return err
	}

	if !versionKnown {
		c.logger.Warn("skipping module version check: module reports no version",
			slog.String("plugin", pluginName),
			slog.String("module", requirement.Name),
			slog.String("constraint", requirement.Constraint))

		return nil
	}

	if !satisfied {
		return unmetf("plugin %s requires module %q %s, but the cluster has %s",
			pluginName, requirement.Name, requirement.Constraint, module.Version.Original())
	}

	return nil
}

// checkAnyOfModules passes if at least one module in the group is enabled and
// satisfies its constraint; otherwise it returns a descriptive error.
// An enabled-but-unversioned module does NOT satisfy a versioned alternative here
// (unlike the mandatory/conditional paths): another candidate may be verifiable.
// A malformed constraint is operational and propagates, not swallowed as "none satisfied".
func (c *Checker) checkAnyOfModules(pluginName string, index int, group internal.ModuleGroup, state *ClusterState) error {
	if len(group.Modules) == 0 {
		return nil
	}

	names := make([]string, 0, len(group.Modules))

	for _, requirement := range group.Modules {
		names = append(names, requirement.Name)

		module, enabled := enabledModule(state, requirement.Name)
		if !enabled {
			continue
		}

		satisfied, versionKnown, err := evaluateModuleVersion(requirement, module)
		if err != nil {
			return err
		}

		if versionKnown && satisfied {
			return nil
		}
	}

	return unmetf("plugin %s requires at least one of [%s] (%s), but none is satisfied",
		pluginName, strings.Join(names, ", "), groupID(group.Name, index))
}

// groupID returns a stable identifier for a requirement group in diagnostics:
// the group's (required) name, with a positional fallback for a group built
// without one (e.g. in tests, which bypass contract validation).
func groupID(name string, index int) string {
	if name == "" {
		return fmt.Sprintf("group %d", index)
	}

	return name
}

// checkNoneOfModules fails if any module in the group is enabled and its version
// falls in the forbidden range. An empty member constraint forbids the module at
// any version. An enabled module that reports no version is skipped with a warning
// when a version constraint is set (it cannot be judged). A malformed constraint is
// operational and propagates, not swallowed as "not forbidden".
func (c *Checker) checkNoneOfModules(pluginName string, index int, group internal.ModuleGroup, state *ClusterState) error {
	for _, requirement := range group.Modules {
		module, enabled := enabledModule(state, requirement.Name)
		if !enabled {
			continue
		}

		forbidden, versionKnown, err := evaluateModuleVersion(requirement, module)
		if err != nil {
			return err
		}

		if !versionKnown {
			c.logger.Warn("skipping noneOf version check: module reports no version",
				slog.String("plugin", pluginName),
				slog.String("module", requirement.Name),
				slog.String("constraint", requirement.Constraint))

			continue
		}

		if forbidden {
			return unmetf("plugin %s forbids module %q (%s), but it is enabled in the cluster",
				pluginName, requirement.Name, groupID(group.Name, index))
		}
	}

	return nil
}
