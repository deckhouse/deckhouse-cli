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

package group

import (
	"github.com/spf13/cobra"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

// memberKinds is the enum accepted as the positional member-kind argument
// on "group add-member" / "group remove-member" commands.
var memberKinds = []string{"user", "group"}

// completeGroupOnly returns group names only on the first positional arg.
func completeGroupOnly(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) >= 1 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return utilk8s.CompleteResourceNames(cmd, iamtypes.GroupGVR, "", toComplete)
}

// completeMemberArgs completes "GROUP [user|group] NAME" triplets for
// add-member / remove-member commands.
func completeMemberArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	switch len(args) {
	case 0:
		return utilk8s.CompleteResourceNames(cmd, iamtypes.GroupGVR, "", toComplete)
	case 1:
		return utilk8s.FilterByPrefix(memberKinds, toComplete), cobra.ShellCompDirectiveNoFileComp
	case 2:
		switch args[1] {
		case "user":
			return utilk8s.CompleteResourceNames(cmd, iamtypes.UserGVR, "", toComplete)
		case "group":
			return utilk8s.CompleteResourceNames(cmd, iamtypes.GroupGVR, "", toComplete)
		}
	}
	return nil, cobra.ShellCompDirectiveNoFileComp
}
