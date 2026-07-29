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
	"sort"
	"strings"

	"github.com/Masterminds/semver/v3"

	"github.com/deckhouse/deckhouse-cli/internal"
	d8flags "github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/requirements"
	"github.com/deckhouse/deckhouse-cli/pkg/diagnostic"
)

// PluginContract fetches a plugin contract, memoizing it per name@tag for the
// duration of the command. Requirements-aware selection probes several versions'
// contracts, and the chosen one is fetched again by the install pipeline; the
// cache keeps that to a single pull per version.
func (m *Manager) PluginContract(ctx context.Context, pluginName, tag string) (*internal.Plugin, error) {
	key := pluginName + "@" + tag

	if cached, ok := m.contractCache[key]; ok {
		return cached, nil
	}

	contract, err := m.service.GetPluginContract(ctx, pluginName, tag)
	if err != nil {
		return nil, err
	}

	if m.contractCache == nil {
		m.contractCache = make(map[string]*internal.Plugin)
	}

	m.contractCache[key] = contract

	return contract, nil
}

// listTags returns a plugin's published tags, memoized per name for the command
// run. The dependency planner probes the same dep across several candidate paths,
// so this keeps the listing to one registry call per plugin.
func (m *Manager) listTags(ctx context.Context, pluginName string) ([]string, error) {
	if tags, ok := m.tagsCache[pluginName]; ok {
		return tags, nil
	}

	tags, err := m.service.ListPluginTags(ctx, pluginName)
	if err != nil {
		return nil, err
	}

	if m.tagsCache == nil {
		m.tagsCache = make(map[string][]string)
	}

	m.tagsCache[pluginName] = tags

	return tags, nil
}

// deepCheckFunc decides whether a cluster-compatible candidate is acceptable. It
// returns ok=false with a structured reason to skip to an older version, or err
// for an operational failure that must stop selection (never masked as "try older").
type deepCheckFunc func(ctx context.Context, contract *internal.Plugin) (ok bool, reason *unsatisfiableReason, err error)

// rejectedCandidate is a version skipped during selection, kept with its
// structured reason so the terminal error can group dependency problems.
type rejectedCandidate struct {
	version string
	reason  *unsatisfiableReason
}

// selectCompatible walks stable versions newest->oldest and returns the first that
// (a) matches constraint (nil = any), (b) is cluster-compatible, and (c) passes
// deepCheck (nil = skip). It returns:
//   - (version, rejected, nil) on success - rejected lists the newer versions skipped,
//   - (nil, rejected, nil) when no candidate qualifies (the caller reports it),
//   - (nil, nil, err) only on an operational failure (cluster/contract hard error).
//
// A too-new release needing a newer cluster is skipped for an older, working one.
// Genuine pre-releases (rc/alpha/beta) are excluded (install them via --version).
// Scope of the built-in checks is cluster-only (Kubernetes/Deckhouse/modules);
// plugin->plugin dependency resolvability is layered in via deepCheck (the planner).
func (m *Manager) selectCompatible(
	ctx context.Context,
	pluginName string,
	tags []string,
	constraint *semver.Constraints,
	deepCheck deepCheckFunc,
) (*semver.Version, []rejectedCandidate, error) {
	candidates := stableVersions(sortedSemverDesc(tags))

	rejected := make([]rejectedCandidate, 0, len(candidates))

	for _, version := range candidates {
		if constraint != nil && !constraint.Check(version) {
			continue
		}

		contract, err := m.PluginContract(ctx, pluginName, version.Original())
		if err != nil {
			// A missing contract is not an error - GetPluginContract returns name+version
			// for a contract-less image. So any error here is operational (registry/proxy
			// unreachable): hard stop, never silently demote to an older version.
			return nil, nil, err
		}

		compatible, reason, err := m.clusterCompatible(ctx, contract)
		if err != nil {
			// Cluster unreachable or a broken contract (operational) - hard stop, same
			// as enforcement; do not silently mask it by trying an older version.
			return nil, nil, err
		}

		if !compatible {
			rejected = append(rejected, rejectedCandidate{version.Original(), &unsatisfiableReason{kind: reasonClusterIncompatible, detail: reason}})

			continue
		}

		if deepCheck != nil {
			ok, reason, err := deepCheck(ctx, contract)
			if err != nil {
				return nil, nil, err
			}

			if !ok {
				rejected = append(rejected, rejectedCandidate{version.Original(), reason})

				continue
			}
		}

		return version, rejected, nil
	}

	return nil, rejected, nil
}

// noCompatibleError builds the terminal "nothing usable" error as a HelpfulError.
// When every rejection is dependency-related it leads with "unresolved
// dependencies" and one suggestion per missing dependency; otherwise it falls back
// to a per-version listing. The top-level handler renders it.
func noCompatibleError(pluginName string, rejected []rejectedCandidate) error {
	if len(rejected) == 0 {
		return &diagnostic.HelpfulError{
			Category: fmt.Sprintf("no stable version of plugin %q is published", pluginName),
			Suggestions: []diagnostic.Suggestion{{
				Cause:     "only pre-releases (rc, alpha, beta) exist",
				Solutions: []string{fmt.Sprintf("install a pre-release explicitly: d8 plugins install %s --version <version>", pluginName)},
			}},
		}
	}

	if allDependencyReasons(rejected) {
		return &diagnostic.HelpfulError{
			Category:    fmt.Sprintf("cannot install plugin %q: unresolved dependencies", pluginName),
			Suggestions: dependencySuggestions(rejected),
		}
	}

	suggestions := make([]diagnostic.Suggestion, 0, len(rejected)+1)
	for _, rc := range rejected {
		suggestions = append(suggestions, diagnostic.Suggestion{Cause: fmt.Sprintf("%s: %s", rc.version, rc.reason.summary())})
	}

	suggestions = append(suggestions, diagnostic.Suggestion{
		Cause: "no version could be installed",
		Solutions: []string{
			fmt.Sprintf("inspect a version's requirements: d8 plugins contract %s", pluginName),
			fmt.Sprintf("or install an exact version: d8 plugins install %s --version <version>", pluginName),
		},
	})

	return &diagnostic.HelpfulError{
		Category:    fmt.Sprintf("cannot install plugin %q: no installable version", pluginName),
		Suggestions: suggestions,
	}
}

// allDependencyReasons reports whether every rejection is an unresolved-dependency
// problem (so the message can lead with "unresolved dependencies").
func allDependencyReasons(rejected []rejectedCandidate) bool {
	for _, rc := range rejected {
		if rc.reason == nil || !rc.reason.kind.isDependency() {
			return false
		}
	}

	return true
}

// dependencySuggestions builds one suggestion per distinct unresolved dependency
// (deduped by kind+name, since several rejected versions often share one).
func dependencySuggestions(rejected []rejectedCandidate) []diagnostic.Suggestion {
	seen := make(map[string]bool, len(rejected))
	suggestions := make([]diagnostic.Suggestion, 0, len(rejected))

	for _, rc := range rejected {
		key := fmt.Sprintf("%d:%s", rc.reason.kind, rc.reason.pluginName)
		if seen[key] {
			continue
		}

		seen[key] = true

		suggestions = append(suggestions, dependencySuggestion(rc.reason))
	}

	return suggestions
}

// dependencySuggestion renders one missing dependency as a cause + fix.
func dependencySuggestion(r *unsatisfiableReason) diagnostic.Suggestion {
	dep := r.pluginName

	switch r.kind {
	case reasonDepNotPublished:
		return diagnostic.Suggestion{
			Cause:     withChain(fmt.Sprintf("required plugin %q is not published", dep), r.path),
			Solutions: []string{fmt.Sprintf("publish it under deckhouse-cli/plugins/%s", dep)},
		}
	case reasonDepNoVersion:
		cause := fmt.Sprintf("no compatible version of required plugin %q", dep)
		if r.constraint != "" {
			cause = fmt.Sprintf("no version of required plugin %q satisfies %s", dep, r.constraint)
		}

		return diagnostic.Suggestion{
			Cause:     withChain(cause, r.path),
			Solutions: []string{fmt.Sprintf("publish a matching version of %q", dep)},
		}
	case reasonDepCycle:
		return diagnostic.Suggestion{
			Cause:     fmt.Sprintf("dependency cycle: %s", strings.Join(r.path, " -> ")),
			Solutions: []string{"break the cycle in the plugins' contracts"},
		}
	default:
		return diagnostic.Suggestion{Cause: r.summary()}
	}
}

// withChain appends "(needed by: a -> b -> c)" for a transitive dependency (the
// path is deeper than the direct request); a direct dependency needs no chain.
func withChain(cause string, path []string) string {
	if len(path) > 2 {
		return cause + fmt.Sprintf("\n    (needed by: %s)", strings.Join(path, " -> "))
	}

	return cause
}

// clusterCompatible reports whether the plugin's cluster-side requirements are
// met, reusing the enforcement validators (clusterChecks) read-only. A genuine
// unmet requirement yields (false, reason, nil).
// The err return covers cases where compatibility cannot be determined (cluster
// unreachable) or an operational contract error (e.g. a malformed constraint).
// These must NOT be masked as merely "incompatible" and trigger a downgrade.
func (m *Manager) clusterCompatible(ctx context.Context, plugin *internal.Plugin) (bool, string, error) {
	if !requirements.HasClusterRequirements(plugin) || d8flags.SkipClusterChecks {
		return true, "", nil
	}

	state, err := m.clusterState(ctx)
	if err != nil {
		return false, "", fmt.Errorf("cannot reach the cluster to select a compatible version "+
			"(use --skip-cluster-checks to pick the latest regardless): %w", err)
	}

	for _, check := range m.clusterChecks() {
		if err := check.Run(plugin, state); err != nil {
			if requirements.IsUnmet(err) {
				return false, err.Error(), nil
			}

			return false, "", fmt.Errorf("%s: %w", check.Name, err)
		}
	}

	return true, "", nil
}

// stableVersions drops genuine pre-releases (rc/alpha/beta), keeping CI/build
// markers like "v1.77.0-main"; the default pick should not land on a pre-release.
func stableVersions(versions []*semver.Version) []*semver.Version {
	stable := make([]*semver.Version, 0, len(versions))

	for _, version := range versions {
		if version.Prerelease() != "" && requirements.IsGenuinePrerelease(version.Prerelease()) {
			continue
		}

		stable = append(stable, version)
	}

	return stable
}

// sortedSemverDesc parses tags as semver, drops the unparseable ones, and returns
// them sorted newest first.
func sortedSemverDesc(tags []string) []*semver.Version {
	versions := make([]*semver.Version, 0, len(tags))

	for _, tag := range tags {
		version, err := semver.NewVersion(tag)
		if err != nil {
			continue
		}

		versions = append(versions, version)
	}

	sort.Slice(versions, func(i, j int) bool { return versions[i].GreaterThan(versions[j]) })

	return versions
}
