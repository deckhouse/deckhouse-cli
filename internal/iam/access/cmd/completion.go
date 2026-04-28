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

// subjectKinds is the first positional arg of grant/revoke.
var subjectKinds = []string{"user", "group"}

// allAccessLevelsList aliases the ordered slice in types.go so completion
// and validation share a single source of truth.
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

// completeScopeFlag offers static suggestions for --scope. The labels= form
// is template-only because we cannot reasonably enumerate every possible
// K=V pair the user might want.
func completeScopeFlag(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	candidates := []string{"cluster", "all-namespaces", "labels="}
	return utilk8s.FilterByPrefix(candidates, toComplete), cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
}

// completeRuleRef completes "CAR/<name>" and "AR/<ns>/<name>" for
// `d8 iam get rule REF`. Short prefixes (CAR, AR) are preferred in output;
// long prefixes (ClusterAuthorizationRule, AuthorizationRule) are also
// accepted as valid input.
func completeRuleRef(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	// Pick which branch to fetch by the committed prefix. Short prefixes
	// like "C"/"CA" still query both — cobra filters the result.
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
