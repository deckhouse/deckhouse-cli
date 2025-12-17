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

package operatemodule

import (
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"
	"k8s.io/client-go/dynamic"
)

// Output message prefixes with colors for better status readability.
var (
	MsgOK    = color.New(color.FgGreen).Sprint("[OK]")
	MsgWarn  = color.New(color.FgYellow).Sprint("[WARN]")
	MsgError = color.New(color.FgRed).Sprint("[ERROR]")
)

// similarModulePrefixLen is the number of characters to match when suggesting
// similar module names. Used to help users who mistyped a module name.
const similarModulePrefixLen = 3

// maxModulesToList is the maximum number of modules to show in "all available modules" list.
const maxModulesToList = 10

// SuggestSuitableReleasesOnNotFound prints helpful suggestions when a release is not found.
// It can suggest releases that are available for the given match predicate.
//
// For example, all the releases that are in Pending phase and not yet approved.
// You can select any rule with the ReleaseMatchFunc predicate.
func SuggestSuitableReleasesOnNotFound(dynamicClient dynamic.Interface, moduleName, version string, match ReleaseMatchFunc) error {
	fmt.Fprintf(os.Stderr, "\n%s Release '%s-%s' not found.\n", MsgError, moduleName, version)

	suitableReleases, err := FindReleases(dynamicClient, moduleName, match)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n   Could not fetch available releases: %v\n", err)
		return ReleaseNotFoundError(moduleName, version)
	}

	// At first, suggest the nearest version which is possibly was meant by the user.
	PrintNearestVersionSuggestions(suitableReleases, version)

	// Then, suggest a list of available pending releases in a sorted order.
	if len(suitableReleases) > 0 {
		PrintPendingReleases(suitableReleases)
	} else {
		PrintNoReleasesHelp(dynamicClient, moduleName)
	}

	fmt.Fprintln(os.Stderr)
	return ReleaseNotFoundError(moduleName, version)
}

// ReleaseNotFoundError returns a formatted error for a missing release.
func ReleaseNotFoundError(moduleName, version string) error {
	return fmt.Errorf("release '%s-%s' not found", moduleName, version)
}

// PrintNearestVersionSuggestions prints suggestions for nearest versions.
func PrintNearestVersionSuggestions(releases []ModuleReleaseInfo, targetVersion string) {
	nearest := FindNearestVersions(releases, targetVersion)
	if nearest.Lower == nil && nearest.Upper == nil {
		return
	}

	fmt.Fprintln(os.Stderr, "\nPerhaps you meant one of these?")
	if nearest.Lower != nil {
		fmt.Fprintf(os.Stderr, "   • %s (previous version)\n", nearest.Lower.Version)
	}
	if nearest.Upper != nil {
		fmt.Fprintf(os.Stderr, "   • %s (next version)\n", nearest.Upper.Version)
	}
}

// PrintPendingReleases prints a list of available pending releases.
func PrintPendingReleases(releases []ModuleReleaseInfo) {
	fmt.Fprintln(os.Stderr, "\nAvailable pending releases:")
	for _, r := range releases {
		fmt.Fprintf(os.Stderr, "   • %s\n", r.Version)
	}
}

// PrintNoReleasesHelp prints help when no pending releases are found.
func PrintNoReleasesHelp(dynamicClient dynamic.Interface, moduleName string) {
	allReleases, _ := ListModuleReleases(dynamicClient, moduleName)

	if len(allReleases) > 0 {
		fmt.Fprintf(os.Stderr, "\nNo pending releases available for module '%s'.\n", moduleName)
		fmt.Fprintln(os.Stderr, "All releases may already be deployed.")
		return
	}

	fmt.Fprintf(os.Stderr, "\nNo releases found for module '%s'.\n", moduleName)
	fmt.Fprintln(os.Stderr, "Check if the module name is correct.")
	PrintSimilarModules(dynamicClient, moduleName)
}

// PrintSimilarModules prints modules with similar names to help with typos.
func PrintSimilarModules(dynamicClient dynamic.Interface, moduleName string) {
	if moduleName == "" {
		return
	}

	modules, _ := ListModuleNames(dynamicClient)
	if len(modules) == 0 {
		return
	}

	// Find modules with similar prefix
	prefix := moduleName[:min(similarModulePrefixLen, len(moduleName))]
	var similar []string
	for _, m := range modules {
		if strings.HasPrefix(m, prefix) {
			similar = append(similar, m)
		}
	}

	if len(similar) > 0 {
		fmt.Fprintln(os.Stderr, "\nSimilar modules:")
		for _, m := range similar {
			fmt.Fprintf(os.Stderr, "   • %s\n", m)
		}
	}

	if len(modules) <= maxModulesToList {
		fmt.Fprintln(os.Stderr, "\nAll available modules:")
		for _, m := range modules {
			fmt.Fprintf(os.Stderr, "   • %s\n", m)
		}
	}
}
