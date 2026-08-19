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

package mirror

import (
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/plugins"
)

// TestBuildPluginsInput pins the modules->plugins handoff: module versions
// from the modules stats become typed semver versions, unparseable tags and
// version-less modules are dropped, platform versions ride along.
func TestBuildPluginsInput(t *testing.T) {
	stats := modules.ModulesStats{
		Attempted: true,
		Modules: []modules.ModuleStat{
			{Name: "postgresql", Versions: []string{"v1.0.0", "v1.5.0", "not-a-version"}},
			{Name: "broken-only", Versions: []string{"alpha-junk"}},
			{Name: "no-versions"},
		},
	}

	in := buildPluginsInput(stats, []string{"v1.71.3", "stable"})

	require.Len(t, in.Modules, 1, "modules without a single parseable version must be dropped")
	assert.Equal(t, "postgresql", in.Modules[0].Name)

	versions := make([]string, 0, len(in.Modules[0].Versions))
	for _, v := range in.Modules[0].Versions {
		versions = append(versions, v.Original())
	}

	assert.Equal(t, []string{"v1.0.0", "v1.5.0"}, versions)

	require.Len(t, in.PlatformVersions, 1, "channel aliases must be dropped from platform versions")
	assert.Equal(t, "v1.71.3", in.PlatformVersions[0].Original())
}

// TestToPluginsStats pins the phase-stats -> summary mapping, including the
// reason-kind labels the renderer keys on.
func TestToPluginsStats(t *testing.T) {
	stats := toPluginsStats(plugins.PluginsStats{
		Attempted: true,
		Plugins: []plugins.PluginStat{{
			Name:   "postgresql-mgr",
			Images: 2,
			Versions: []plugins.PluginVersionStat{{
				Version: "v1.2.0",
				Reasons: []plugins.Reason{
					{Kind: plugins.ReasonModule, Subject: "postgresql", Constraint: ">=1.5.0"},
					{Kind: plugins.ReasonExplicit, Subject: "--include-plugin postgresql-mgr"},
				},
			}},
		}, {
			Name:   "db-connector",
			Images: 1,
			Versions: []plugins.PluginVersionStat{{
				Version: "v0.9.1",
				Reasons: []plugins.Reason{{Kind: plugins.ReasonDependency, Subject: "postgresql-mgr@v1.2.0", Constraint: ">=0.9.0"}},
			}},
		}},
		Skipped:     []plugins.SkippedPlugin{{Name: "backup-tool", Reason: "requires postgresql >=3.0.0"}},
		Warnings:    []string{"some advisory"},
		TotalImages: 3,
	})

	assert.True(t, stats.Attempted)
	assert.Equal(t, 3, stats.TotalImages)
	assert.Equal(t, []string{"some advisory"}, stats.Warnings)

	require.Len(t, stats.Plugins, 2)
	require.Len(t, stats.Plugins[0].Versions, 1)
	assert.Equal(t, []PluginReason{
		{Kind: "module", Subject: "postgresql", Constraint: ">=1.5.0"},
		{Kind: "explicit", Subject: "--include-plugin postgresql-mgr"},
	}, stats.Plugins[0].Versions[0].Reasons)
	assert.Equal(t, "dependency", stats.Plugins[1].Versions[0].Reasons[0].Kind)

	require.Len(t, stats.SkippedPlugins, 1)
	assert.Equal(t, SkippedPluginStat{Name: "backup-tool", Reason: "requires postgresql >=3.0.0"}, stats.SkippedPlugins[0])
}

// TestParseSemvers: unparseable tags are dropped, originals preserved.
func TestParseSemvers(t *testing.T) {
	versions := parseSemvers([]string{"v1.2.3", "junk", "v2.0.0-main"})

	require.Len(t, versions, 2)
	assert.Equal(t, "v1.2.3", versions[0].Original())
	assert.Equal(t, "v2.0.0-main", versions[1].Original())
	assert.IsType(t, &semver.Version{}, versions[0])
}
