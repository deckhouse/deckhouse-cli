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

var addMemberExample = templates.Examples(`
  # Add a user to a group (kind "user" is implicit for two positional args)
  d8 iam group add-member admins anton

  # Add a user explicitly
  d8 iam group add-member admins user anton

  # Add a nested group
  d8 iam group add-member platform group admins

  # Add a user, creating the target group if it doesn't exist
  d8 iam group add-member admins anton --create-group`)

func newAddMemberCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "add-member GROUP [user|group] MEMBER",
		Short:             "Add a member (user or group) to a local group",
		Example:           addMemberExample,
		Args:              cobra.RangeArgs(2, 3),
		ValidArgsFunction: completeMemberArgs,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE:              runAddMember,
	}

	cmd.Flags().Bool("create-group", false, "Create the target group if it does not exist")
	return cmd
}

func runAddMember(cmd *cobra.Command, args []string) error {
	groupName, kindStr, memberName, err := parseMemberArgs(args)
	if err != nil {
		return err
	}
	createGroup, _ := cmd.Flags().GetBool("create-group")

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
		if !createGroup {
			return fmt.Errorf("group %q not found (use --create-group to create it): %w", groupName, err)
		}
		obj = buildGroupObject(groupName)
		obj, err = groupClient.Create(ctx, obj, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("creating group %q: %w", groupName, err)
		}
		fmt.Fprintf(cmd.ErrOrStderr(), "Group %q created\n", groupName)
	}

	// Cycle detection for group->group membership
	if memberKind == "Group" {
		hasCycle, cyclePath, err := detectCycle(cmd, dyn, groupName, memberName)
		if err != nil {
			return fmt.Errorf("cycle detection failed: %w", err)
		}
		if hasCycle {
			return fmt.Errorf("adding group %q to %q would create a cycle: %s", memberName, groupName, strings.Join(cyclePath, " -> "))
		}
	}

	members, _ := getGroupMembers(obj)
	for _, m := range members {
		if fmt.Sprint(m["kind"]) == memberKind && fmt.Sprint(m["name"]) == memberName {
			cmd.Printf("Member %s/%s already exists in group %s\n", memberKind, memberName, groupName)
			return nil
		}
	}

	rawMembers, _, _ := unstructured.NestedSlice(obj.Object, "spec", "members")
	rawMembers = append(rawMembers, map[string]any{
		"kind": memberKind,
		"name": memberName,
	})
	if err := unstructured.SetNestedSlice(obj.Object, rawMembers, "spec", "members"); err != nil {
		return fmt.Errorf("setting members: %w", err)
	}

	_, err = groupClient.Update(ctx, obj, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("updating group %q: %w", groupName, err)
	}

	cmd.Printf("Added %s %q to group %q\n", strings.ToLower(memberKind), memberName, groupName)
	return nil
}

func normalizeMemberKind(kind string) (string, error) {
	switch strings.ToLower(kind) {
	case "user":
		return "User", nil
	case "group":
		return "Group", nil
	default:
		return "", fmt.Errorf("invalid member kind %q: must be user or group", kind)
	}
}

// parseMemberArgs accepts:
//
//	GROUP MEMBER             → kind defaults to "user"
//	GROUP (user|group) MEMBER → explicit kind
//
// Returns (groupName, kindStr, memberName, err).
func parseMemberArgs(args []string) (string, string, string, error) {
	switch len(args) {
	case 2:
		return args[0], "user", args[1], nil
	case 3:
		return args[0], args[1], args[2], nil
	default:
		return "", "", "", fmt.Errorf("expected GROUP MEMBER or GROUP (user|group) MEMBER, got %d arguments", len(args))
	}
}
