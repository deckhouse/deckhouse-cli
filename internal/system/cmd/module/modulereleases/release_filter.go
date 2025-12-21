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

package modulereleases

import (
	"sort"

	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/api/v1alpha1"
)

// ReleaseMatchFunc is a predicate function for filtering releases.
// Returns true if the release should be included in the result.
type ReleaseMatchFunc func(r ModuleReleaseInfo) bool

// CanBeApproved returns true for pending releases that are not yet approved.
// Used by the approve command to filter releases available for approval.
func CanBeApproved(r ModuleReleaseInfo) bool {
	return r.Phase == v1alpha1.ModuleReleasePhasePending && !r.IsApproved
}

// CanBeAppliedNow returns true for pending releases without apply-now annotation.
// Used by the apply-now command to filter releases available for immediate deployment.
func CanBeAppliedNow(r ModuleReleaseInfo) bool {
	return r.Phase == v1alpha1.ModuleReleasePhasePending && !r.IsApplyNow
}

// FindReleases returns releases matching the given predicate, sorted by version.
// It's simply a filtered version of ListModuleReleases (convenience function).
func FindReleases(dynamicClient dynamic.Interface, moduleName string, match ReleaseMatchFunc) ([]ModuleReleaseInfo, error) {
	releases, err := ListModuleReleases(dynamicClient, moduleName)
	if err != nil {
		return nil, err
	}

	var result []ModuleReleaseInfo
	for _, r := range releases {
		if match(r) {
			result = append(result, r)
		}
	}

	SortReleasesByVersion(result)
	return result, nil
}

// FindVersions returns a sorted list of versions for releases matching the predicate.
// All versions are normalized to have a "v" prefix for consistent completion.
func FindVersions(dynamicClient dynamic.Interface, moduleName string, match ReleaseMatchFunc) ([]string, error) {
	releases, err := FindReleases(dynamicClient, moduleName, match)
	if err != nil {
		return nil, err
	}

	versions := make([]string, 0, len(releases))
	for _, r := range releases {
		versions = append(versions, NormalizeVersion(r.Version))
	}
	return versions, nil
}

// ListModuleNames returns a sorted list of unique module names from all releases.
func ListModuleNames(dynamicClient dynamic.Interface) ([]string, error) {
	releases, err := ListModuleReleases(dynamicClient, "")
	if err != nil {
		return nil, err
	}

	moduleSet := make(map[string]struct{})
	for _, r := range releases {
		if r.ModuleName != "" {
			moduleSet[r.ModuleName] = struct{}{}
		}
	}

	modules := make([]string, 0, len(moduleSet))
	for m := range moduleSet {
		modules = append(modules, m)
	}
	sort.Strings(modules)

	return modules, nil
}
