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
)

func mustSemvers(t *testing.T, raw ...string) []*semver.Version {
	t.Helper()

	versions := make([]*semver.Version, 0, len(raw))

	for _, r := range raw {
		v, err := semver.NewVersion(r)
		require.NoError(t, err)

		versions = append(versions, v)
	}

	return versions
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
