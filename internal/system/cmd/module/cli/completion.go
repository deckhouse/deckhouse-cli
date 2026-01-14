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

package cli

import (
	"strings"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/modulereleases"
)

// CompleteForApprove provides shell completion for the approve command.
// It suggests pending releases that are not yet approved.
func CompleteForApprove(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return completeModuleReleaseAndVersion(cmd, args, toComplete, modulereleases.CanBeApproved)
}

// CompleteForApplyNow provides shell completion for the apply-now command.
// It suggests pending releases that don't have the apply-now annotation.
func CompleteForApplyNow(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return completeModuleReleaseAndVersion(cmd, args, toComplete, modulereleases.CanBeAppliedNow)
}

// completeModuleReleaseAndVersion provides shell completion for module names and versions.
// It takes a ReleaseMatchFunc to filter releases by the given predicate.
func completeModuleReleaseAndVersion(cmd *cobra.Command, args []string, toComplete string, match modulereleases.ReleaseMatchFunc) ([]string, cobra.ShellCompDirective) {
	dynamicClient, err := GetDynamicClient(cmd)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	const (
		completingModuleName = 0 // First argument: module name
		completingVersion    = 1 // Second argument: version
	)

	// Suggest module name / version by its prefix, entered by the user in the terminal.
	switch len(args) {
	case completingModuleName:
		modules, err := modulereleases.ListModuleNames(dynamicClient)
		if err != nil {
			return nil, cobra.ShellCompDirectiveError
		}
		return filterByPrefix(modules, toComplete), cobra.ShellCompDirectiveNoFileComp
	case completingVersion:
		versions, err := modulereleases.FindVersions(dynamicClient, args[0], match)
		if err != nil {
			return nil, cobra.ShellCompDirectiveError
		}
		return filterVersionsByPrefix(versions, toComplete), cobra.ShellCompDirectiveNoFileComp
	default:
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
}

// filterByPrefix filters completion suggestions by user's partially typed input.
func filterByPrefix(items []string, prefix string) []string {
	if prefix == "" {
		return items
	}
	var filtered []string
	for _, item := range items {
		if strings.HasPrefix(item, prefix) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

// filterVersionsByPrefix filters version suggestions based on user input.
// Returns versions in the same format as user started typing:
//   - "v1" -> ["v1.0.0", "v1.2.0"]
//   - "1"  -> ["1.0.0", "1.2.0"]
func filterVersionsByPrefix(versions []string, prefix string) []string {
	if prefix == "" {
		return versions
	}

	// Strip "v" from both prefix and versions for comparison
	prefixWithoutV := strings.TrimPrefix(prefix, "v")
	returnWithV := strings.HasPrefix(prefix, "v")

	var filtered []string
	for _, version := range versions {
		versionWithoutV := strings.TrimPrefix(version, "v")
		if strings.HasPrefix(versionWithoutV, prefixWithoutV) {
			if returnWithV {
				filtered = append(filtered, "v"+versionWithoutV)
			} else {
				filtered = append(filtered, versionWithoutV)
			}
		}
	}
	return filtered
}
