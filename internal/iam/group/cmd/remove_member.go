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
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var removeMemberExample = templates.Examples(`
  # Remove a user from a group (kind "user" is implicit for two positional args)
  d8 iam group remove-member admins anton

  # Remove a user explicitly
  d8 iam group remove-member admins user anton

  # Remove a nested group
  d8 iam group remove-member platform group admins`)

func newRemoveMemberCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "remove-member GROUP [user|group] MEMBER",
		Short:             "Remove a member (user or group) from a local group",
		Example:           removeMemberExample,
		Args:              cobra.RangeArgs(2, 3),
		ValidArgsFunction: completeMemberArgs,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			groupName, kindStr, memberName, err := parseMemberArgs(args)
			if err != nil {
				return err
			}

			memberKind, err := normalizeMemberKind(kindStr)
			if err != nil {
				return err
			}

			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}

			ctx := cmd.Context()
			groupClient := dyn.Resource(groupGVR)

			obj, err := groupClient.Get(ctx, groupName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("getting group %q: %w", groupName, err)
			}

			rawMembers, _, _ := unstructured.NestedSlice(obj.Object, "spec", "members")
			found := false
			var newMembers []any
			for _, item := range rawMembers {
				m, ok := item.(map[string]any)
				if !ok {
					newMembers = append(newMembers, item)
					continue
				}
				if fmt.Sprint(m["kind"]) == memberKind && fmt.Sprint(m["name"]) == memberName {
					found = true
					continue
				}
				newMembers = append(newMembers, item)
			}

			if !found {
				cmd.Printf("Nothing to do: %s %q is not a member of group %q\n", strings.ToLower(memberKind), memberName, groupName)
				return nil
			}

			if err := unstructured.SetNestedSlice(obj.Object, newMembers, "spec", "members"); err != nil {
				return fmt.Errorf("setting members: %w", err)
			}

			_, err = groupClient.Update(ctx, obj, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("updating group %q: %w", groupName, err)
			}

			cmd.Printf("Removed %s %q from group %q\n", strings.ToLower(memberKind), memberName, groupName)
			return nil
		},
	}

	return cmd
}
