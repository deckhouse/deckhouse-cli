/*
Copyright 2024 Flant JSC

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

package operatemodule

import (
	"sort"
	"strings"

	"github.com/Masterminds/semver/v3"
)

// NormalizeVersion ensures the version has a 'v' prefix.
func NormalizeVersion(version string) string {
	if version == "" {
		return version
	}
	if !strings.HasPrefix(version, "v") {
		return "v" + version
	}
	return version
}

// NearestVersions contains the closest versions below and above a target version.
type NearestVersions struct {
	Lower *ModuleReleaseInfo // closest version below target, nil if none exists
	Upper *ModuleReleaseInfo // closest version above target, nil if none exists
}

// SortReleasesByVersion sorts releases by semantic version in ascending order.
// Falls back to lexicographic comparison if version parsing fails.
func SortReleasesByVersion(releases []ModuleReleaseInfo) {
	sort.Slice(releases, func(i, j int) bool {
		vi, errI := semver.NewVersion(NormalizeVersion(releases[i].Version))
		vj, errJ := semver.NewVersion(NormalizeVersion(releases[j].Version))
		if errI != nil || errJ != nil {
			return releases[i].Version < releases[j].Version
		}
		return vi.LessThan(vj)
	})
}

// FindNearestVersions finds the closest versions below and above the target version.
func FindNearestVersions(releases []ModuleReleaseInfo, targetVersion string) NearestVersions {
	targetVersion = NormalizeVersion(targetVersion)

	targetSemver, err := semver.NewVersion(targetVersion)
	if err != nil {
		return NearestVersions{}
	}

	var result NearestVersions
	var lowerVersion, upperVersion *semver.Version

	for i := range releases {
		r := &releases[i]
		v, err := semver.NewVersion(NormalizeVersion(r.Version))
		if err != nil {
			continue
		}

		// Find the closest version below target
		if v.LessThan(targetSemver) {
			if lowerVersion == nil || v.GreaterThan(lowerVersion) {
				lowerVersion = v
				result.Lower = r
			}
		}

		// Find the closest version above target
		if v.GreaterThan(targetSemver) {
			if upperVersion == nil || v.LessThan(upperVersion) {
				upperVersion = v
				result.Upper = r
			}
		}
	}

	return result
}

// extractVersionFromName extracts version from release name.
// Expected format: moduleName-vX.Y.Z (e.g., "csi-hpe-v0.3.10" -> "v0.3.10").
func extractVersionFromName(name, moduleName string) string {
	prefix := moduleName + "-"
	if strings.HasPrefix(name, prefix) {
		return strings.TrimPrefix(name, prefix)
	}
	return ""
}
