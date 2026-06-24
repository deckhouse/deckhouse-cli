/*
Copyright 2026 Flant JSC

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
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/Masterminds/semver/v3"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
	"github.com/deckhouse/deckhouse-cli/internal/rpp"
)

// maxResolveDepth bounds the dependency recursion. The visited-set already stops
// cycles; this is a backstop against a pathological/malicious contract chain.
const maxResolveDepth = 16

// planStep is one install action: lay down pluginName at version.
type planStep struct {
	pluginName string
	version    *semver.Version
}

// resolutionPlan is a dependency-first, deduplicated set of installs needed to
// satisfy a plugin's plugin->plugin requirements. steps is ordered so every
// dependency precedes its dependents; byName dedups and records the decided
// version per plugin.
type resolutionPlan struct {
	steps  []planStep
	byName map[string]*semver.Version
}

func newResolutionPlan() *resolutionPlan {
	return &resolutionPlan{byName: make(map[string]*semver.Version)}
}

func (p *resolutionPlan) isEmpty() bool {
	return len(p.steps) == 0
}

// add records a decided install. Callers append a dependency only after its own
// sub-steps, so steps stays dependency-first.
func (p *resolutionPlan) add(name string, version *semver.Version) {
	p.steps = append(p.steps, planStep{pluginName: name, version: version})
	p.byName[name] = version
}

// mergeNewSteps folds a sub-plan's new steps and decisions into dst. src is seeded
// from dst.byName, so its steps are exactly the newly added ones.
func mergeNewSteps(dst, src *resolutionPlan) {
	dst.steps = append(dst.steps, src.steps...)
	maps.Copy(dst.byName, src.byName)
}

// reasonKind classifies why a candidate was rejected, so the terminal error can
// group dependency problems under one "unresolved dependencies" message and keep
// non-dependency failures (cluster, conflict) out of it.
type reasonKind int

const (
	reasonOther reasonKind = iota
	reasonDepNotPublished
	reasonDepNoVersion
	reasonDepCycle
	reasonClusterIncompatible
	reasonContractUnavailable
	reasonReverseConflict
)

func (k reasonKind) isDependency() bool {
	switch k {
	case reasonDepNotPublished, reasonDepNoVersion, reasonDepCycle:
		return true
	default:
		return false
	}
}

// unsatisfiableReason explains why a candidate cannot be resolved. It is a soft
// "skip this candidate" signal, distinct from an operational error (which must
// hard-stop and is returned as a plain error).
type unsatisfiableReason struct {
	kind       reasonKind
	pluginName string
	constraint string
	detail     string
	// path is the dependency chain from the requested plugin down to pluginName.
	// Renders as "via a -> b -> c". Empty or single-element for a directly
	// requested plugin.
	path []string
}

func (r *unsatisfiableReason) summary() string {
	msg := r.detail
	if r.pluginName != "" {
		if len(r.path) > 1 {
			msg = fmt.Sprintf("dependency %q (via %s): %s", r.pluginName, strings.Join(r.path, " -> "), r.detail)
		} else {
			msg = fmt.Sprintf("dependency %q: %s", r.pluginName, r.detail)
		}
	}

	if r.constraint != "" {
		msg += fmt.Sprintf(" (needs %s)", r.constraint)
	}

	return msg
}

// planFor computes the dependency plan needed to install plugin into the current
// on-disk state. It is read-only: it consults the registry (cached), the installed
// contracts on disk and cluster state, but writes nothing. allowMajorCross lets an
// installed dependency be upgraded across its own major to satisfy a constraint
// (the cascade enabled by --use-major); otherwise a dependency stays within its
// installed major.
//
// Returns:
//   - (plan, nil, nil) when the chain is resolvable,
//   - (nil, reason, nil) when plugin cannot be satisfied (caller skips the candidate),
//   - (nil, nil, err) only on an operational failure that must hard-stop.
func (m *Manager) planFor(ctx context.Context, plugin *internal.Plugin, allowMajorCross bool) (*resolutionPlan, *unsatisfiableReason, error) {
	plan := newResolutionPlan()
	visited := map[string]bool{plugin.Name: true}

	reason, err := m.resolveInto(ctx, plugin, plan, visited, allowMajorCross, 0, []string{plugin.Name})
	if err != nil {
		return nil, nil, err
	}

	if reason != nil {
		return nil, reason, nil
	}

	return plan, nil, nil
}

// resolveInto walks plugin's plugin-requirements, growing plan in place. visited is
// the set of plugin names currently on the recursion stack (the cycle guard).
func (m *Manager) resolveInto(
	ctx context.Context,
	plugin *internal.Plugin,
	plan *resolutionPlan,
	visited map[string]bool,
	allowMajorCross bool,
	depth int,
	path []string,
) (*unsatisfiableReason, error) {
	if depth > maxResolveDepth {
		return nil, fmt.Errorf("plugin dependency chain for %q exceeds the maximum depth of %d", plugin.Name, maxResolveDepth)
	}

	// Reverse conflicts: an already-installed plugin may constrain plugin to a
	// version it does not satisfy. Not resolvable by installing deps - skip candidate.
	reason, err := m.reverseConflictReason(plugin)
	if err != nil {
		return nil, err
	}

	if reason != nil {
		return reason, nil
	}

	// Conditional deps: enforced only when the dependency is installed or planned.
	for _, req := range plugin.Requirements.Plugins.Conditional {
		reason, err := m.conditionalReason(req, plan)
		if err != nil {
			return nil, err
		}

		if reason != nil {
			return reason, nil
		}
	}

	// Mandatory deps: must be satisfiable (installed/planned at a good version, or
	// installable/upgradable).
	for _, req := range plugin.Requirements.Plugins.Mandatory {
		reason, err := m.resolveMandatoryDep(ctx, req, plan, visited, allowMajorCross, depth, path)
		if err != nil {
			return nil, err
		}

		if reason != nil {
			return reason, nil
		}
	}

	return nil, nil
}

// reverseConflictReason reports the first already-installed plugin whose constraint
// on plugin is violated by plugin's candidate version.
func (m *Manager) reverseConflictReason(plugin *internal.Plugin) (*unsatisfiableReason, error) {
	contractDir, err := os.ReadDir(layout.ContractsDir(m.pluginDirectory))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, fmt.Errorf("read contract directory: %w", err)
	}

	for _, file := range contractDir {
		name := strings.TrimSuffix(file.Name(), layout.ContractFileExt)

		installed, err := m.InstalledPluginContract(name)
		if err != nil {
			return nil, fmt.Errorf("read installed contract %q: %w", name, err)
		}

		if err := validatePluginConflict(plugin, installed); err != nil {
			return &unsatisfiableReason{kind: reasonReverseConflict, pluginName: name, detail: "reverse conflict: " + err.Error()}, nil
		}
	}

	return nil, nil
}

// conditionalReason enforces a conditional dependency: it matters only when the
// dependency is installed or already planned. An installed/planned but
// out-of-constraint conditional dependency makes the candidate unsatisfiable (a
// conditional dependency is not auto-upgraded).
func (m *Manager) conditionalReason(req internal.PluginRequirement, plan *resolutionPlan) (*unsatisfiableReason, error) {
	version, present, err := m.effectiveVersion(req.Name, plan)
	if err != nil {
		return nil, err
	}

	if !present || req.Constraint == "" {
		return nil, nil
	}

	constraint, err := semver.NewConstraint(req.Constraint)
	if err != nil {
		return nil, fmt.Errorf("parse constraint %q for plugin %q: %w", req.Constraint, req.Name, err)
	}

	if !constraint.Check(version) {
		return &unsatisfiableReason{
			pluginName: req.Name,
			constraint: req.Constraint,
			detail:     fmt.Sprintf("conditional dependency installed at %s does not satisfy", version.Original()),
		}, nil
	}

	return nil, nil
}

// resolveMandatoryDep ensures one mandatory dependency is satisfied, planning an
// install/upgrade when needed.
func (m *Manager) resolveMandatoryDep(
	ctx context.Context,
	req internal.PluginRequirement,
	plan *resolutionPlan,
	visited map[string]bool,
	allowMajorCross bool,
	depth int,
	path []string,
) (*unsatisfiableReason, error) {
	if visited[req.Name] {
		return &unsatisfiableReason{kind: reasonDepCycle, pluginName: req.Name, path: append(slices.Clone(path), req.Name), detail: "dependency cycle"}, nil
	}

	var constraint *semver.Constraints

	if req.Constraint != "" {
		parsed, err := semver.NewConstraint(req.Constraint)
		if err != nil {
			return nil, fmt.Errorf("parse constraint %q for plugin %q: %w", req.Constraint, req.Name, err)
		}

		constraint = parsed
	}

	version, present, err := m.effectiveVersion(req.Name, plan)
	if err != nil {
		return nil, err
	}

	if present {
		if constraint == nil || constraint.Check(version) {
			// Satisfied. An installed dependency's own chain was validated at its
			// install time; a planned one was resolved when it was planned.
			return nil, nil
		}

		// A planned version cannot be changed without conflicting another dependent.
		if _, planned := plan.byName[req.Name]; planned {
			return &unsatisfiableReason{
				pluginName: req.Name,
				constraint: req.Constraint,
				detail:     fmt.Sprintf("already planned at %s", version.Original()),
			}, nil
		}
		// Installed but out of constraint: fall through to upgrade it.
	}

	reason, err := m.selectDepVersion(ctx, req.Name, constraint, plan, visited, allowMajorCross, depth, path)
	if err != nil {
		return nil, err
	}

	return reason, nil
}

// effectiveVersion returns the version a dependency would have given the current
// plan and disk: a planned version wins, else the installed version. present is
// false when the dependency is neither planned nor installed.
func (m *Manager) effectiveVersion(name string, plan *resolutionPlan) (*semver.Version, bool, error) {
	if version, ok := plan.byName[name]; ok {
		return version, true, nil
	}

	installed, err := m.checkInstalled(name)
	if err != nil {
		return nil, false, fmt.Errorf("check whether %q is installed: %w", name, err)
	}

	if !installed {
		return nil, false, nil
	}

	version, err := m.getInstalledPluginVersion(name)
	if err != nil {
		return nil, false, fmt.Errorf("read installed version of %q: %w", name, err)
	}

	return version, true, nil
}

// selectDepVersion picks (and plans) a version for a dependency that must be
// installed or upgraded: the newest tag that satisfies constraint, is
// cluster-compatible, and whose own chain resolves. An installed dependency is
// bounded to its current major (unless allowMajorCross) and never downgraded.
func (m *Manager) selectDepVersion(
	ctx context.Context,
	depName string,
	constraint *semver.Constraints,
	plan *resolutionPlan,
	visited map[string]bool,
	allowMajorCross bool,
	depth int,
	path []string,
) (*unsatisfiableReason, error) {
	depPath := append(slices.Clone(path), depName)

	tags, err := m.listTags(ctx, depName)
	if err != nil {
		// An unpublished mandatory dependency makes this candidate unsatisfiable
		// (skip to an older version), not an operational failure. Other errors
		// (auth, proxy, transport) still hard-stop.
		if errors.Is(err, rpp.ErrNotFound) {
			return &unsatisfiableReason{
				kind:       reasonDepNotPublished,
				pluginName: depName,
				path:       depPath,
				detail:     fmt.Sprintf("not published as deckhouse-cli/plugins/%s", depName),
			}, nil
		}

		return nil, fmt.Errorf("list tags for %q: %w", depName, err)
	}

	majorBound := -1

	var floor *semver.Version

	installed, err := m.checkInstalled(depName)
	if err != nil {
		return nil, fmt.Errorf("check whether %q is installed: %w", depName, err)
	}

	if installed {
		current, err := m.getInstalledPluginVersion(depName)
		if err != nil {
			return nil, fmt.Errorf("read installed version of %q: %w", depName, err)
		}

		floor = current
		if !allowMajorCross {
			majorBound = int(current.Major())
		}
	}

	candidates := filterDepTags(tags, majorBound, floor)

	// Mark the dependency active on the recursion stack while its subtree resolves.
	visited[depName] = true
	defer delete(visited, depName)

	deepCheck := func(ctx context.Context, contract *internal.Plugin) (bool, *unsatisfiableReason, error) {
		sub := newResolutionPlan()
		maps.Copy(sub.byName, plan.byName)

		reason, err := m.resolveInto(ctx, contract, sub, visited, allowMajorCross, depth+1, depPath)
		if err != nil {
			return false, nil, err
		}

		if reason != nil {
			return false, reason, nil
		}

		mergeNewSteps(plan, sub)

		return true, nil, nil
	}

	version, rejected, err := m.selectCompatible(ctx, depName, candidates, constraint, deepCheck)
	if err != nil {
		return nil, err
	}

	if version == nil {
		// A single version blocked by its own (deeper) dependency: surface that leaf
		// reason directly so the chain names the real culprit, not just this dep.
		if len(rejected) == 1 && rejected[0].reason != nil && rejected[0].reason.kind.isDependency() {
			return rejected[0].reason, nil
		}

		return &unsatisfiableReason{
			kind:       reasonDepNoVersion,
			pluginName: depName,
			path:       depPath,
			constraint: constraintString(constraint),
			detail:     "no compatible version",
		}, nil
	}

	plan.add(depName, version)

	return nil, nil
}

// filterDepTags keeps tags that parse as semver, optionally restricting to
// majorBound (>= 0) and dropping anything below floor (never downgrade an
// installed dependency).
func filterDepTags(tags []string, majorBound int, floor *semver.Version) []string {
	out := make([]string, 0, len(tags))

	for _, tag := range tags {
		version, err := semver.NewVersion(tag)
		if err != nil {
			continue
		}

		if majorBound >= 0 && int(version.Major()) != majorBound {
			continue
		}

		if floor != nil && version.LessThan(floor) {
			continue
		}

		out = append(out, tag)
	}

	return out
}

func constraintString(constraint *semver.Constraints) string {
	if constraint == nil {
		return ""
	}

	return constraint.String()
}

// selectTopWithPlan returns the newest version of pluginName whose cluster
// requirements AND plugin->plugin chain are resolvable, together with the plan to
// realize that chain.
func (m *Manager) selectTopWithPlan(
	ctx context.Context,
	pluginName string,
	versions []string,
	allowMajorCross bool,
) (*semver.Version, *resolutionPlan, error) {
	plan := newResolutionPlan()

	deepCheck := func(ctx context.Context, contract *internal.Plugin) (bool, *unsatisfiableReason, error) {
		candidatePlan, reason, err := m.planFor(ctx, contract, allowMajorCross)
		if err != nil {
			return false, nil, err
		}

		if reason != nil {
			return false, reason, nil
		}

		plan = candidatePlan

		return true, nil, nil
	}

	version, rejected, err := m.selectCompatible(ctx, pluginName, versions, nil, deepCheck)
	if err != nil {
		return nil, nil, err
	}

	if version == nil {
		return nil, nil, noCompatibleError(pluginName, rejected)
	}

	if len(rejected) > 0 {
		skipped := make([]string, len(rejected))
		for i, rc := range rejected {
			skipped[i] = fmt.Sprintf("%s (%s)", rc.version, rc.reason.summary())
		}

		fmt.Printf("Selected %s (newer version(s) skipped: %s)\n", version.Original(), strings.Join(skipped, "; "))
	}

	return version, plan, nil
}
