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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal"
)

// TestAvailablePluginsResolvesLatest: the catalog is the tag list of the plugins
// repository, and each name is resolved to its newest stable release.
func TestAvailablePluginsResolvesLatest(t *testing.T) {
	m := plannerManager(t, &multiPluginSource{
		tags: map[string][]string{
			// Listed second by the source, but the result is sorted by name.
			"stronghold": {"v1.2.3", "v1.3.0", "v1.3.0-rc.1"},
			"package":    {"v0.0.33", "v0.0.34"},
		},
	})

	available, err := m.AvailablePlugins(context.Background())
	require.NoError(t, err)

	require.Len(t, available, 2)
	assert.Equal(t, "package", available[0].Name)
	assert.Equal(t, "v0.0.34", available[0].Version)
	assert.Empty(t, available[0].Note)

	// The release candidate must not win over the stable release.
	assert.Equal(t, "stronghold", available[1].Name)
	assert.Equal(t, "v1.3.0", available[1].Version)
}

// TestAvailablePluginsCollapsesPlatformTags: a release published only as
// per-platform images still shows as the release, not one platform's build.
func TestAvailablePluginsCollapsesPlatformTags(t *testing.T) {
	m := plannerManager(t, &multiPluginSource{
		tags: map[string][]string{
			"package": {"v0.0.34-linux-amd64", "v0.0.34-darwin-arm64"},
		},
	})

	available, err := m.AvailablePlugins(context.Background())
	require.NoError(t, err)

	require.Len(t, available, 1)
	assert.Equal(t, "v0.0.34", available[0].Version)
	assert.Empty(t, available[0].Note)
}

// TestAvailablePluginsReportsPerPluginFailures is the partial-output guarantee: a
// plugin whose versions cannot be resolved is still listed by name, with the reason
// in its own row, and never takes the rest of the listing down with it.
func TestAvailablePluginsReportsPerPluginFailures(t *testing.T) {
	m := plannerManager(t, &multiPluginSource{
		tags: map[string][]string{
			"healthy":    {"v1.0.0"},
			"unreadable": {},
			"nostable":   {"v1.0.0-rc.1"},
			"untagged":   {"latest"},
		},
		tagErrors: map[string]error{"unreadable": errors.New("proxy exploded")},
	})

	available, err := m.AvailablePlugins(context.Background())
	require.NoError(t, err)

	byName := make(map[string]RemotePluginInfo, len(available))
	for _, plugin := range available {
		byName[plugin.Name] = plugin
	}

	require.Len(t, byName, 4)

	assert.Equal(t, "v1.0.0", byName["healthy"].Version)
	assert.Empty(t, byName["healthy"].Note)

	assert.Empty(t, byName["unreadable"].Version)
	assert.Equal(t, "versions unavailable", byName["unreadable"].Note)

	// Listed, but nothing installable: only a genuine pre-release is published.
	assert.Empty(t, byName["nostable"].Version)
	assert.Equal(t, "no versions found", byName["nostable"].Note)

	// A tag that is not semver at all leaves the plugin with no usable version.
	assert.Empty(t, byName["untagged"].Version)
	assert.Equal(t, "no versions found", byName["untagged"].Note)
}

// TestAvailablePluginsRejectsUnusableName: names arrive as registry tags, so a value
// that could not address a plugin repository is reported, never turned into a route.
func TestAvailablePluginsRejectsUnusableName(t *testing.T) {
	m := plannerManager(t, &multiPluginSource{
		tags: map[string][]string{"../escape": {"v1.0.0"}},
	})

	available, err := m.AvailablePlugins(context.Background())
	require.NoError(t, err)

	require.Len(t, available, 1)
	assert.Equal(t, "../escape", available[0].Name)
	assert.Empty(t, available[0].Version)
	assert.Equal(t, "not a valid plugin name", available[0].Note)
}

// TestAvailablePluginsMarksInstalled cross-references the on-disk installs so the
// listing says which published plugins are already present.
func TestAvailablePluginsMarksInstalled(t *testing.T) {
	m := plannerManager(t, &multiPluginSource{
		tags: map[string][]string{"package": {"v0.0.34"}, "stronghold": {"v1.0.0"}},
	})
	installPluginFixture(t, m.pluginDirectory, "package", 0)

	available, err := m.AvailablePlugins(context.Background())
	require.NoError(t, err)

	require.Len(t, available, 2)
	assert.True(t, available[0].Installed, "package is installed")
	assert.False(t, available[1].Installed, "stronghold is not")
}

// TestAvailablePluginsCatalogFailure: a transport that can enumerate but fails to
// is a plain error, distinct from one that cannot enumerate at all.
func TestAvailablePluginsCatalogFailure(t *testing.T) {
	m := plannerManager(t, &multiPluginSource{})
	m.service = &catalogFailureSource{multiPluginSource: &multiPluginSource{}}

	_, err := m.AvailablePlugins(context.Background())
	assert.Error(t, err)
	assert.NotErrorIs(t, err, ErrCatalogUnsupported, "a failed listing is not an absent capability")
}

// TestAvailablePluginsUnsupportedTransport is the RPP case: the proxy cannot serve
// the plugins path, so the source does not implement pluginCatalog and the request
// is never made. The error names the transport.
func TestAvailablePluginsUnsupportedTransport(t *testing.T) {
	m := plannerManager(t, &multiPluginSource{})
	m.service = &noCatalogSource{}

	_, err := m.AvailablePlugins(context.Background())
	assert.ErrorIs(t, err, ErrCatalogUnsupported)
	assert.ErrorContains(t, err, string(TransportRPP))
}

// TestAvailablePluginsWithoutSource guards the command path that reaches the manager
// after InitPluginServices failed.
func TestAvailablePluginsWithoutSource(t *testing.T) {
	m := testManager()

	_, err := m.AvailablePlugins(context.Background())
	assert.ErrorContains(t, err, "not initialized")
}

// catalogFailureSource enumerates nothing: the plugins repository tag listing fails
// for a reason other than the route being absent (transport, auth, proxy).
type catalogFailureSource struct {
	*multiPluginSource
}

func (s *catalogFailureSource) ListPluginNames(context.Context) ([]string, error) {
	return nil, errors.New("proxy unreachable")
}

// noCatalogSource stands in for the proxy transport: it satisfies pluginSource and
// deliberately NOT pluginCatalog. It is written out in full rather than embedding a
// catalog-capable fake, whose ListPluginNames would be promoted and make the
// capability assertion succeed.
type noCatalogSource struct{}

var _ pluginSource = (*noCatalogSource)(nil)

func (s *noCatalogSource) Transport() Transport { return TransportRPP }

func (s *noCatalogSource) ListPluginTags(context.Context, string) ([]string, error) {
	return nil, nil
}

func (s *noCatalogSource) GetPluginContract(context.Context, string, string) (*internal.Plugin, error) {
	return nil, nil
}

func (s *noCatalogSource) ExtractPlugin(context.Context, string, string, string) error {
	return nil
}
