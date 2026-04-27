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
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/kubectl/pkg/util/templates"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
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

	res, err := EnsureMember(cmd.Context(), dyn, groupName, memberKind, memberName, EnsureMemberOpts{
		CreateGroupIfMissing: createGroup,
		CycleCheck:           true,
	})
	if err != nil {
		// EnsureMember wraps the API NotFound error verbatim, so the standard
		// k8s error helpers see through the wrap and we don't need to grep
		// the message string.
		if !createGroup && apierrors.IsNotFound(err) {
			return fmt.Errorf("%w (use --create-group to create it)", err)
		}
		return err
	}

	switch {
	case res.GroupCreated:
		fmt.Fprintf(cmd.ErrOrStderr(), "Group %q created\n", groupName)
		cmd.Printf("Added %s %q to group %q\n", strings.ToLower(string(memberKind)), memberName, groupName)
	case res.AlreadyMember:
		cmd.Printf("Member %s/%s already exists in group %s\n", memberKind, memberName, groupName)
	case res.Added:
		cmd.Printf("Added %s %q to group %q\n", strings.ToLower(string(memberKind)), memberName, groupName)
	}
	return nil
}

func normalizeMemberKind(kind string) (iamtypes.SubjectKind, error) {
	switch strings.ToLower(kind) {
	case "user":
		return iamtypes.KindUser, nil
	case "group":
		return iamtypes.KindGroup, nil
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
		return "", "", "", errors.New("expected GROUP MEMBER or GROUP (user|group) MEMBER")
	}
}
