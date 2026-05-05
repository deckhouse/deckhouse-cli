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

package user

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"
	"k8s.io/kubectl/pkg/util/templates"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var deleteLong = templates.LongDesc(`
Delete a local static user from Deckhouse.

The User CR is removed and, by default, the user is also removed from every
local Group CR that lists them under spec.members. Pass --keep-memberships
to leave the membership references in place (e.g. for audit retention or to
re-create the user with the same memberships later).

© Flant JSC 2026`)

func newDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "delete <name>",
		Short:             "Delete a local static user",
		Long:              deleteLong,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeUserNames,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			keepMemberships, _ := cmd.Flags().GetBool("keep-memberships")

			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}

			// Order matters: cleanup runs BEFORE Delete so a failure in either
			// step leaves the operator with a clear, recoverable picture:
			//   - cleanup fails → User CR is still present, retry the whole
			//     command with the same flags;
			//   - cleanup succeeds, Delete fails → User CR still present, but
			//     groups already lost the membership reference; the operator
			//     can either re-add via `d8 iam group add-member` or just
			//     re-run delete (cleanup is idempotent).
			// The reverse order would leave dangling group references with no
			// User CR to identify them.
			if !keepMemberships {
				if err := cleanupUserFromGroups(cmd.Context(), cmd, dyn, name); err != nil {
					return err
				}
			}

			if err := dyn.Resource(iamtypes.UserGVR).Delete(cmd.Context(), name, metav1.DeleteOptions{}); err != nil {
				return fmt.Errorf("deleting User %q: %w", name, err)
			}

			if keepMemberships {
				fmt.Fprintf(cmd.ErrOrStderr(), "Warning: --keep-memberships set; user %q may still be referenced in Group memberships. Use \"d8 iam group remove-member\" to clean up.\n", name)
			}
			cmd.Printf("User %s deleted\n", name)
			return nil
		},
	}
	cmd.Flags().Bool("keep-memberships", false, "Do not remove the user from local Group spec.members; only delete the User CR")
	return cmd
}

// cleanupUserFromGroups removes (kind=User, name=userName) from every local
// Group's spec.members. Each Group is updated in its own retry-on-conflict
// loop so a parallel writer to one group does not block progress on the
// others. Errors are aggregated into a multi-line message but cleanup makes
// best-effort progress across remaining groups before reporting them.
func cleanupUserFromGroups(ctx context.Context, cmd *cobra.Command, dyn dynamic.Interface, userName string) error {
	groupClient := dyn.Resource(iamtypes.GroupGVR)

	groups, err := groupClient.List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing groups for cleanup: %w", err)
	}

	cleaned := 0
	var errs *multierror.Error
	userKindStr := string(iamtypes.KindUser)

	for i := range groups.Items {
		groupName := groups.Items[i].GetName()

		// Quick filter: skip groups that don't reference the user, so we
		// don't issue an Update RPC for unrelated groups. The retry loop
		// re-fetches anyway, so this only affects the unmodified path.
		if !groupContainsMember(&groups.Items[i], userKindStr, userName) {
			continue
		}

		removed, err := removeUserFromGroup(ctx, groupClient, groupName, userName)
		if err != nil {
			errs = multierror.Append(errs, fmt.Errorf("group %q: %w", groupName, err))
			continue
		}
		if removed {
			cleaned++
			fmt.Fprintf(cmd.ErrOrStderr(), "Removed user %q from group %q\n", userName, groupName)
		}
	}

	if errs.ErrorOrNil() != nil {
		return fmt.Errorf("failed to clean up user %q from %d group(s): %w",
			userName, errs.Len(), errs)
	}
	if cleaned > 0 {
		fmt.Fprintf(cmd.ErrOrStderr(), "Cleaned %d group membership(s) of user %q\n", cleaned, userName)
	}
	return nil
}

// removeUserFromGroup removes the User member from a single group with
// retry-on-conflict. Returns true if a member was actually removed (false on
// no-op when the user wasn't in spec.members at the time of the final Get).
func removeUserFromGroup(ctx context.Context, groupClient dynamic.NamespaceableResourceInterface,
	groupName, userName string) (bool, error) {
	userKindStr := string(iamtypes.KindUser)
	removed := false

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		obj, err := groupClient.Get(ctx, groupName, metav1.GetOptions{})
		if err != nil {
			return err
		}

		raw, _, _ := unstructured.NestedSlice(obj.Object, "spec", "members")
		filtered := make([]any, 0, len(raw))
		found := false
		for _, item := range raw {
			m, ok := item.(map[string]any)
			if !ok {
				filtered = append(filtered, item)
				continue
			}
			if fmt.Sprint(m["kind"]) == userKindStr && fmt.Sprint(m["name"]) == userName {
				found = true
				continue
			}
			filtered = append(filtered, item)
		}

		if !found {
			removed = false
			return nil
		}

		if err := unstructured.SetNestedSlice(obj.Object, filtered, "spec", "members"); err != nil {
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
	return removed, err
}

func groupContainsMember(obj *unstructured.Unstructured, kindStr, name string) bool {
	raw, _, _ := unstructured.NestedSlice(obj.Object, "spec", "members")
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if fmt.Sprint(m["kind"]) == kindStr && fmt.Sprint(m["name"]) == name {
			return true
		}
	}
	return false
}
