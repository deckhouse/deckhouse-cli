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
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"
	"k8s.io/kubectl/pkg/util/templates"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
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

			removed, err := RemoveMember(cmd.Context(), dyn, groupName, memberKind, memberName)
			if err != nil {
				return err
			}

			memberKindStr := strings.ToLower(string(memberKind))
			if !removed {
				cmd.Printf("Nothing to do: %s %q is not a member of group %q\n", memberKindStr, memberName, groupName)
				return nil
			}
			cmd.Printf("Removed %s %q from group %q\n", memberKindStr, memberName, groupName)
			return nil
		},
	}

	return cmd
}

// RemoveMember idempotently removes (memberKind, memberName) from the
// spec.members of groupName, retrying on conflict. It returns:
//   - removed=true  when an Update succeeded and dropped the member;
//   - removed=false on a no-op (member was not present);
//   - a non-nil error wrapping the underlying cause for any other failure.
//
// Exposed at package level so tests can drive the logic with a fake
// dynamic client; the cobra command above is a thin wrapper around it.
func RemoveMember(ctx context.Context, dyn dynamic.Interface,
	groupName string, memberKind iamtypes.SubjectKind, memberName string) (bool, error) {
	groupClient := dyn.Resource(iamtypes.GroupGVR)
	memberKindStr := string(memberKind)
	removed := false

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
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
			if fmt.Sprint(m["kind"]) == memberKindStr && fmt.Sprint(m["name"]) == memberName {
				found = true
				continue
			}
			newMembers = append(newMembers, item)
		}

		if !found {
			removed = false
			return nil
		}

		if err := unstructured.SetNestedSlice(obj.Object, newMembers, "spec", "members"); err != nil {
			return fmt.Errorf("setting members: %w", err)
		}
		// Surface conflict errors raw so retry.RetryOnConflict can classify
		// and retry them on a fresh Get.
		if _, err := groupClient.Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
			return err
		}
		removed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return removed, nil
}
