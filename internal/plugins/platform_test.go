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
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSplitPlatform pins which prereleases are read as a platform. The dangerous
// case is the genuine prerelease: treating "rc.1" as a platform would collapse a
// release candidate into the stable release of the same number and hide it.
func TestSplitPlatform(t *testing.T) {
	cases := []struct {
		raw      string
		version  string
		platform string
	}{
		{raw: "v0.0.34-linux-amd64", version: "v0.0.34", platform: "linux/amd64"},
		{raw: "v0.0.34-darwin-arm64", version: "v0.0.34", platform: "darwin/arm64"},
		{raw: "1.2.3-windows-386", version: "1.2.3", platform: "windows/386"},

		// No prerelease at all: a platform-independent tag.
		{raw: "v1.2.3", version: "v1.2.3", platform: ""},

		// Genuine prereleases must survive untouched.
		{raw: "v2.0.0-rc.1", version: "v2.0.0", platform: ""},
		{raw: "v2.0.0-alpha", version: "v2.0.0", platform: ""},

		// Neither half names a real GOOS/GOARCH.
		{raw: "v1.2.3-foo-bar", version: "v1.2.3", platform: ""},
		{raw: "v1.2.3-linux-pentium", version: "v1.2.3", platform: ""},

		// A pre-release that also carries a platform keeps its pre-release identity:
		// only the os-arch tail is the platform.
		{raw: "v1.2.3-rc.1-linux-amd64", version: "v1.2.3-rc.1", platform: "linux/amd64"},
		{raw: "v0.0.1-test-windows-amd64", version: "v0.0.1-test", platform: "windows/amd64"},
		{raw: "v0.0.1-test-darwin-arm64", version: "v0.0.1-test", platform: "darwin/arm64"},

		// A two-token prerelease whose head is not a GOOS is not a platform.
		{raw: "v1.2.3-beta-1", version: "v1.2.3-beta-1", platform: ""},
	}

	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			parsed, err := semver.NewVersion(tc.raw)
			require.NoError(t, err)

			clean, platform := SplitPlatform(parsed)

			assert.Equal(t, tc.platform, platform)

			if tc.platform == "" {
				// Untouched: the original string, prerelease and all.
				assert.Equal(t, tc.raw, clean.Original())

				return
			}

			// Stripped: only the platform tail is gone, and the original "v" prefix
			// and any real prerelease survive.
			assert.Equal(t, tc.version, clean.Original())
		})
	}
}

// TestCollapsePlatformTags folds the per-platform tags of one release into a single
// entry while preserving the newest-first order handed to it.
func TestCollapsePlatformTags(t *testing.T) {
	collapsed := collapsePlatformTags(sortedSemverDesc([]string{
		"v0.0.34-linux-amd64",
		"v0.0.34-darwin-arm64",
		"v0.0.34-linux-arm64",
		"v0.0.33-linux-amd64",
		"v0.0.33-darwin-arm64",
	}))

	require.Len(t, collapsed, 2)

	assert.Equal(t, "v0.0.34", collapsed[0].Version.Original())
	assert.ElementsMatch(t,
		[]string{"linux/amd64", "linux/arm64", "darwin/arm64"}, collapsed[0].Platforms)

	assert.Equal(t, "v0.0.33", collapsed[1].Version.Original())
	assert.ElementsMatch(t, []string{"linux/amd64", "darwin/arm64"}, collapsed[1].Platforms)
}

// TestCollapsePlatformTagsKeepsPrereleasesDistinct: a release candidate is its own
// release, never merged into the stable version that shares its numbers - even
// though both are published per platform.
func TestCollapsePlatformTagsKeepsPrereleasesDistinct(t *testing.T) {
	collapsed := collapsePlatformTags(sortedSemverDesc([]string{
		"v2.0.0-linux-amd64",
		"v2.0.0-darwin-arm64",
		"v2.0.0-rc.1-linux-amd64",
		"v2.0.0-rc.1",
	}))

	// Descending semver puts "rc.1..." above "linux-amd64"/"darwin-arm64" (all are
	// prerelease identifiers, compared as ASCII), so the candidate leads.
	require.Len(t, collapsed, 2)

	assert.Equal(t, "v2.0.0-rc.1", collapsed[0].Version.Original())
	assert.Equal(t, []string{"linux/amd64"}, collapsed[0].Platforms)

	assert.Equal(t, "v2.0.0", collapsed[1].Version.Original())
	assert.ElementsMatch(t, []string{"linux/amd64", "darwin/arm64"}, collapsed[1].Platforms)
}

// TestCollapsePlatformTagsPrereleaseWithIndex reproduces the reported listing: a
// pre-release published as an index plus one tag per platform must fold into a
// single "v0.0.1-test" row, not five.
func TestCollapsePlatformTagsPrereleaseWithIndex(t *testing.T) {
	collapsed := collapsePlatformTags(sortedSemverDesc([]string{
		"v0.0.1-test-windows-amd64",
		"v0.0.1-test-linux-amd64",
		"v0.0.1-test-darwin-arm64",
		"v0.0.1-test-darwin-amd64",
		"v0.0.1-test",
	}))

	require.Len(t, collapsed, 1)
	assert.Equal(t, "v0.0.1-test", collapsed[0].Version.Original())
	assert.ElementsMatch(t, []string{
		"windows/amd64", "linux/amd64", "darwin/arm64", "darwin/amd64",
	}, collapsed[0].Platforms)
}

// TestCollapsePlatformTagsPlatformlessTag: a plugin published as one
// platform-independent tag yields an entry with no platforms.
func TestCollapsePlatformTagsPlatformlessTag(t *testing.T) {
	collapsed := collapsePlatformTags(sortedSemverDesc([]string{"v1.2.3", "v1.2.2"}))

	require.Len(t, collapsed, 2)
	assert.Equal(t, "v1.2.3", collapsed[0].Version.Original())
	assert.Empty(t, collapsed[0].Platforms)
	assert.Empty(t, collapsed[1].Platforms)
}
