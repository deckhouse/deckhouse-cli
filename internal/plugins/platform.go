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
	"strings"

	"github.com/Masterminds/semver/v3"
)

// PluginVersion is one published release: the version with any platform suffix
// removed, plus the platforms that release was published for. Platforms is empty
// for a plugin published as a single platform-independent tag.
type PluginVersion struct {
	Version   *semver.Version
	Platforms []string
}

// knownGOOS and knownGOARCH bound what may be read as a platform suffix. Without
// them a genuine prerelease such as v2.0.0-rc.1 would be mistaken for one and its
// release collapsed into the stable version of the same number.
var (
	knownGOOS = map[string]struct{}{
		"aix": {}, "android": {}, "darwin": {}, "dragonfly": {}, "freebsd": {},
		"hurd": {}, "illumos": {}, "ios": {}, "js": {}, "linux": {}, "nacl": {},
		"netbsd": {}, "openbsd": {}, "plan9": {}, "solaris": {}, "wasip1": {},
		"windows": {}, "zos": {},
	}

	knownGOARCH = map[string]struct{}{
		"386": {}, "amd64": {}, "amd64p32": {}, "arm": {}, "arm64": {},
		"arm64be": {}, "armbe": {}, "loong64": {}, "mips": {}, "mips64": {},
		"mips64le": {}, "mips64p32": {}, "mips64p32le": {}, "mipsle": {},
		"ppc": {}, "ppc64": {}, "ppc64le": {}, "riscv": {}, "riscv64": {},
		"s390": {}, "s390x": {}, "sparc": {}, "sparc64": {}, "wasm": {},
	}
)

// platformFromPrerelease reads a prerelease as an "<os>-<arch>" pair and returns it
// as "os/arch". Only that exact shape is recognized: a prerelease carrying anything
// else - including a real prerelease that also names a platform, like
// "rc.1-linux-amd64" - is left alone rather than half-parsed.
func platformFromPrerelease(prerelease string) (string, bool) {
	os, arch, found := strings.Cut(prerelease, "-")
	if !found {
		return "", false
	}

	if _, ok := knownGOOS[os]; !ok {
		return "", false
	}

	if _, ok := knownGOARCH[arch]; !ok {
		return "", false
	}

	return os + "/" + arch, true
}

// SplitPlatform separates a version's platform suffix from the version itself.
// Plugin images are published one tag per platform, so a single release reaches the
// registry as v0.0.34-linux-amd64, v0.0.34-darwin-arm64 and so on - the os-arch pair
// riding in the semver prerelease slot. It returns the version without that suffix
// and the platform as "os/arch"; a version whose prerelease is a genuine prerelease
// comes back untouched with an empty platform.
func SplitPlatform(version *semver.Version) (*semver.Version, string) {
	prerelease := version.Prerelease()
	if prerelease == "" {
		return version, ""
	}

	platform, ok := platformFromPrerelease(prerelease)
	if !ok {
		return version, ""
	}

	// Trim the suffix off the original text rather than rebuilding the version, so
	// the "v" prefix and any other original formatting survive into the output.
	clean, err := semver.NewVersion(strings.TrimSuffix(version.Original(), "-"+prerelease))
	if err != nil {
		return version, ""
	}

	return clean, platform
}

// collapsePlatformTags folds the per-platform tags of one release into a single
// entry, keeping the newest-first order of the input and, within an entry, the order
// the platforms were listed in. Versions with no platform suffix pass through as
// entries with no platforms.
func collapsePlatformTags(versions []*semver.Version) []PluginVersion {
	collapsed := make([]PluginVersion, 0, len(versions))
	positions := make(map[string]int, len(versions))

	for _, version := range versions {
		clean, platform := SplitPlatform(version)

		position, seen := positions[clean.String()]
		if !seen {
			positions[clean.String()] = len(collapsed)

			entry := PluginVersion{Version: clean}
			if platform != "" {
				entry.Platforms = []string{platform}
			}

			collapsed = append(collapsed, entry)

			continue
		}

		if platform != "" {
			collapsed[position].Platforms = append(collapsed[position].Platforms, platform)
		}
	}

	return collapsed
}
