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

package selfupdatecmd

import (
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/fatih/color"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustVersions(t *testing.T, raw ...string) []*semver.Version {
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

func TestFormatVersionListGroupsAroundCurrent(t *testing.T) {
	withoutColor(t)

	lines, listed := formatVersionList(
		mustVersions(t, "v0.14.1", "v0.14.0", "v0.13.1", "v0.13.0"), "v0.13.1", nil)

	assert.True(t, listed)
	assert.Equal(t, []string{
		"  v0.14.1  newer",
		"  v0.14.0  newer",
		"* v0.13.1  current",
		"  v0.13.0",
	}, lines)
}

func TestFormatVersionListCurrentNotPublished(t *testing.T) {
	withoutColor(t)

	lines, listed := formatVersionList(mustVersions(t, "v0.14.1", "v0.14.0"), "v0.13.5", nil)

	assert.False(t, listed)
	assert.Equal(t, []string{
		"  v0.14.1  newer",
		"  v0.14.0  newer",
	}, lines)
}

func TestFormatVersionListDevBuildIsPlain(t *testing.T) {
	withoutColor(t)

	lines, listed := formatVersionList(mustVersions(t, "v0.14.1", "v0.13.0"), "local-dev", nil)

	assert.False(t, listed)
	assert.Equal(t, []string{
		"  v0.14.1",
		"  v0.13.0",
	}, lines)
}

func TestFormatVersionListMarksInstalled(t *testing.T) {
	withoutColor(t)

	lines, listed := formatVersionList(
		mustVersions(t, "v0.14.1", "v0.13.1", "v0.13.0"), "v0.13.1",
		mustVersions(t, "v0.14.1", "v0.13.0"))

	assert.True(t, listed)
	assert.Equal(t, []string{
		"  v0.14.1  newer  installed",
		"* v0.13.1  current",
		"  v0.13.0  installed",
	}, lines)
}

func TestStoredOnly(t *testing.T) {
	extra := storedOnly(
		mustVersions(t, "v0.14.1", "v0.10.0"),
		mustVersions(t, "v0.14.1", "v0.13.0"))

	require.Len(t, extra, 1)
	assert.Equal(t, "v0.10.0", extra[0].Original())
}
