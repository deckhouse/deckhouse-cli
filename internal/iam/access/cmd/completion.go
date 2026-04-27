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

package access

import (
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

// subjectKinds is the enum accepted as the first positional arg on
// "d8 iam access grant", "d8 iam access revoke" and "d8 iam access explain".
var subjectKinds = []string{"user", "group"}

// allAccessLevelsList is exposed for flag completion. It points at the same
// ordered slice that drives validation in types.go, so adding a new level
// updates both sites at once.
var allAccessLevelsList = allAccessLevelsOrdered

// completeSubjectAndName completes "user|group NAME" positional pairs.
func completeSubjectAndName(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	switch len(args) {
	case 0:
		return utilk8s.FilterByPrefix(subjectKinds, toComplete), cobra.ShellCompDirectiveNoFileComp
	case 1:
		switch args[0] {
		case "user":
			return utilk8s.CompleteResourceNames(cmd, iamtypes.UserGVR, "", toComplete)
		case "group":
			return utilk8s.CompleteResourceNames(cmd, iamtypes.GroupGVR, "", toComplete)
		}
	}
	return nil, cobra.ShellCompDirectiveNoFileComp
}

func completeAccessLevels(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return utilk8s.FilterByPrefix(allAccessLevelsList, toComplete), cobra.ShellCompDirectiveNoFileComp
}

func completeNamespacesFlag(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return utilk8s.CompleteNamespaces(cmd, toComplete)
}

// completeRuleRef completes rule references in the form used by
// "d8 iam access rules get". It progressively offers:
//
//	"CAR/<name>" — full list of ClusterAuthorizationRules
//	"AR/<ns>/<name>" — full list of AuthorizationRules from every namespace
//
// Short prefixes (CAR, AR) are preferred in completion output because they
// are less typing, but the long prefixes remain valid input.
func completeRuleRef(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) >= 1 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	// Decide which branches to populate based on the prefix already committed
	// to by the user. For a partially-typed prefix (e.g. "C", "CA") we still
	// want both — cobra's downstream filtering will narrow things down.
	wantCAR, wantAR := true, true
	switch {
	case strings.HasPrefix(toComplete, "CAR/"),
		strings.HasPrefix(toComplete, "ClusterAuthorizationRule/"):
		wantAR = false
	case strings.HasPrefix(toComplete, "AR/"),
		strings.HasPrefix(toComplete, "AuthorizationRule/"):
		wantCAR = false
	}

	var suggestions []string

	if wantCAR {
		list, err := dyn.Resource(iamtypes.ClusterAuthorizationRuleGVR).List(cmd.Context(), metav1.ListOptions{})
		if err == nil {
			for i := range list.Items {
				suggestions = append(suggestions, "CAR/"+list.Items[i].GetName())
			}
		}
	}
	if wantAR {
		list, err := dyn.Resource(iamtypes.AuthorizationRuleGVR).Namespace("").List(cmd.Context(), metav1.ListOptions{})
		if err == nil {
			for i := range list.Items {
				obj := &list.Items[i]
				suggestions = append(suggestions, "AR/"+obj.GetNamespace()+"/"+obj.GetName())
			}
		}
	}

	return utilk8s.FilterByPrefix(suggestions, toComplete), cobra.ShellCompDirectiveNoFileComp
}
