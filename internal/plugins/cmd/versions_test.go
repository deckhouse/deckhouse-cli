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

package pluginscmd

import (
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/fatih/color"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/plugins"
)

// mustSemvers builds platform-less releases - the shape a plugin published as a
// single platform-independent tag produces.
func mustSemvers(t *testing.T, raw ...string) []plugins.PluginVersion {
	t.Helper()

	versions := make([]plugins.PluginVersion, 0, len(raw))

	for _, r := range raw {
		v, err := semver.NewVersion(r)
		require.NoError(t, err)

		versions = append(versions, plugins.PluginVersion{Version: v})
	}

	return versions
}

// withPlatforms builds one release published for several platforms.
func withPlatforms(t *testing.T, raw string, platforms ...string) plugins.PluginVersion {
	t.Helper()

	v, err := semver.NewVersion(raw)
	require.NoError(t, err)

	return plugins.PluginVersion{Version: v, Platforms: platforms}
}

func withoutColor(t *testing.T) {
	t.Helper()

	prev := color.NoColor
	color.NoColor = true
	t.Cleanup(func() { color.NoColor = prev })
}

func TestFormatPluginVersionListGroupsAroundCurrent(t *testing.T) {
	withoutColor(t)

	lines, listed := formatPluginVersionList(
		mustSemvers(t, "v0.1.2", "v0.0.21", "v0.0.20"), semver.MustParse("v0.0.21"))

	assert.True(t, listed)
	assert.Equal(t, []string{
		"  v0.1.2   newer",
		"* v0.0.21  current",
		"  v0.0.20",
	}, lines)
}

func TestFormatPluginVersionListCurrentNotPublished(t *testing.T) {
	withoutColor(t)

	lines, listed := formatPluginVersionList(
		mustSemvers(t, "v0.1.2", "v0.1.1"), semver.MustParse("v0.0.21"))

	assert.False(t, listed)
	assert.Equal(t, []string{
		"  v0.1.2  newer",
		"  v0.1.1  newer",
	}, lines)
}

func TestFormatPluginVersionListNotInstalledIsPlain(t *testing.T) {
	withoutColor(t)

	lines, listed := formatPluginVersionList(mustSemvers(t, "v0.1.2", "v0.0.21"), nil)

	assert.False(t, listed)
	assert.Equal(t, []string{
		"  v0.1.2",
		"  v0.0.21",
	}, lines)
}

// TestFormatPluginVersionListShowsPlatforms also pins the alignment: the group
// column keeps a fixed width so the platform lists form a column of their own.
func TestFormatPluginVersionListShowsPlatforms(t *testing.T) {
	withoutColor(t)

	lines, listed := formatPluginVersionList([]plugins.PluginVersion{
		withPlatforms(t, "v0.0.35", "linux/amd64"),
		withPlatforms(t, "v0.0.34", "linux/amd64", "darwin/arm64"),
		withPlatforms(t, "v0.0.33", "linux/amd64"),
	}, semver.MustParse("v0.0.34"))

	assert.True(t, listed)
	assert.Equal(t, []string{
		"  v0.0.35  newer    linux/amd64",
		"* v0.0.34  current  linux/amd64, darwin/arm64",
		"  v0.0.33           linux/amd64",
	}, lines)
}

// TestFormatPluginVersionListPlatformlessIsUnpadded guards the plugin published as
// one platform-independent tag: with no platforms to list, the fixed-width columns
// must trim away entirely rather than leave a ragged tail of spaces.
func TestFormatPluginVersionListPlatformlessIsUnpadded(t *testing.T) {
	withoutColor(t)

	lines, _ := formatPluginVersionList(
		mustSemvers(t, "v0.1.2", "v0.0.21"), semver.MustParse("v0.0.21"))

	assert.Equal(t, []string{
		"  v0.1.2   newer",
		"* v0.0.21  current",
	}, lines)
}
