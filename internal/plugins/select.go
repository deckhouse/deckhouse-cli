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

// selectLatestCompatible returns the newest stable version (from the given tags)
// whose CLUSTER-side requirements are satisfied. It walks versions newest to oldest
// and returns the first compatible one, so a too-new release that needs a newer
// cluster is skipped in favour of an older, working version. Genuine pre-releases
// (rc/alpha/beta) are excluded from the default pick (install them via --version).
//
// Scope: this considers ONLY cluster requirements (Kubernetes/Deckhouse/modules).
// plugin->plugin conflicts and requirements are still enforced later at install
// time (validateRequirements) and are not backtracked over during selection.
func (m *Manager) selectLatestCompatible(ctx context.Context, pluginName string, tags []string) (*semver.Version, error) {
	candidates := stableVersions(sortedSemverDesc(tags))
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no stable version found for plugin %q (use --version to install a pre-release)", pluginName)
	}

	rejected := make([]string, 0, len(candidates))

	for i, version := range candidates {
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
			return nil, err
		}

		if compatible {
			if i != 0 {
				fmt.Printf("Selected %s (newest compatible; %s is not compatible with this cluster)\n",
					version.Original(), candidates[0].Original())
			}

			return version, nil
		}

		rejected = append(rejected, fmt.Sprintf("%s (%s)", version.Original(), reason))
	}

	return nil, fmt.Errorf("no compatible version of plugin %q for this cluster; rejected: %s",
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
