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
	"sort"
	"strings"

	"github.com/Masterminds/semver/v3"

	"github.com/deckhouse/deckhouse-cli/internal"
	d8flags "github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/requirements"
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
// returns ok=false with a short reason to skip to an older version, or err for an
// operational failure that must stop selection (never masked as "try older").
type deepCheckFunc func(ctx context.Context, contract *internal.Plugin) (ok bool, reason string, err error)

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
) (*semver.Version, []string, error) {
	candidates := stableVersions(sortedSemverDesc(tags))

	rejected := make([]string, 0, len(candidates))

	for _, version := range candidates {
		if constraint != nil && !constraint.Check(version) {
			continue
		}

		contract, err := m.PluginContract(ctx, pluginName, version.Original())
		if err != nil {
			// GetPluginContract has no typed not-found, so a transient registry error
			// is indistinguishable from "no contract"; both demote to an older version.
			m.logger.Warn("skipping version: contract unavailable",
				slog.String("plugin", pluginName), slog.String("version", version.Original()), slog.String("error", err.Error()))
			rejected = append(rejected, fmt.Sprintf("%s (contract unavailable)", version.Original()))

			continue
		}

		compatible, reason, err := m.clusterCompatible(ctx, contract)
		if err != nil {
			// Cluster unreachable or a broken contract (operational) - hard stop, same
			// as enforcement; do not silently mask it by trying an older version.
			return nil, nil, err
		}

		if !compatible {
			rejected = append(rejected, fmt.Sprintf("%s (%s)", version.Original(), reason))

			continue
		}

		if deepCheck != nil {
			ok, reason, err := deepCheck(ctx, contract)
			if err != nil {
				return nil, nil, err
			}

			if !ok {
				rejected = append(rejected, fmt.Sprintf("%s (%s)", version.Original(), reason))

				continue
			}
		}

		return version, rejected, nil
	}

	return nil, rejected, nil
}

// noCompatibleError builds the "nothing usable" error from the rejected list.
func noCompatibleError(pluginName string, rejected []string) error {
	if len(rejected) == 0 {
		return fmt.Errorf("no stable version found for plugin %q (use --version to install a pre-release)", pluginName)
	}

	return fmt.Errorf("no compatible version of plugin %q for this cluster; rejected: %s",
		pluginName, strings.Join(rejected, "; "))
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
