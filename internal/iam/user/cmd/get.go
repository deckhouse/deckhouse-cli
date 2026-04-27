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
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/cli-runtime/pkg/printers"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

func newGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "get <name>",
		Short:             "Get details of a local static user",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeUserNames,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			outputFmt, _ := cmd.Flags().GetString("output")

			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}

			obj, err := dyn.Resource(iamtypes.UserGVR).Get(cmd.Context(), name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("getting User %q: %w", name, err)
			}

			switch outputFmt {
			case "json", "yaml":
				return utilk8s.PrintObject(cmd.OutOrStdout(), obj, outputFmt)
			default:
				return printUserDetail(cmd, obj)
			}
		},
	}

	cmd.Flags().StringP("output", "o", "table", "Output format: table|json|yaml")
	_ = cmd.RegisterFlagCompletionFunc("output", utilk8s.CompleteOutputFormats("table", "json", "yaml"))
	return cmd
}

func printUserDetail(cmd *cobra.Command, u *unstructured.Unstructured) error {
	w := cmd.OutOrStdout()
	name := u.GetName()
	email, _, _ := unstructured.NestedString(u.Object, "spec", "email")
	ttl, _, _ := unstructured.NestedString(u.Object, "spec", "ttl")
	userID, _, _ := unstructured.NestedString(u.Object, "spec", "userID")

	fmt.Fprintf(w, "User: %s\n", name)
	if email != "" {
		fmt.Fprintf(w, "Email: %s\n", email)
	}
	if userID != "" {
		fmt.Fprintf(w, "UserID: %s\n", userID)
	}
	fmt.Fprintf(w, "Created: %s\n", u.GetCreationTimestamp().Format("2006-01-02 15:04:05"))

	fmt.Fprintln(w, "\nStatus:")
	expireAt, _, _ := unstructured.NestedString(u.Object, "status", "expireAt")
	if ttl != "" {
		fmt.Fprintf(w, "  TTL:         %s\n", ttl)
	} else {
		fmt.Fprintln(w, "  TTL:         <none>")
	}
	if expireAt != "" {
		fmt.Fprintf(w, "  Expire at:   %s\n", expireAt)
	} else {
		fmt.Fprintln(w, "  Expire at:   <none>")
	}

	locked := false
	if v, found, _ := unstructured.NestedBool(u.Object, "status", "lock", "state"); found {
		locked = v
	}
	if locked {
		fmt.Fprintln(w, "  Lock:        locked")
		if until, _, _ := unstructured.NestedString(u.Object, "status", "lock", "until"); until != "" {
			fmt.Fprintf(w, "  Lock until:  %s\n", until)
		}
		if reason, _, _ := unstructured.NestedString(u.Object, "status", "lock", "reason"); reason != "" {
			fmt.Fprintf(w, "  Lock reason: %s\n", reason)
		}
		if msg, _, _ := unstructured.NestedString(u.Object, "status", "lock", "message"); msg != "" {
			fmt.Fprintf(w, "  Lock message: %s\n", msg)
		}
	} else {
		fmt.Fprintln(w, "  Lock:        unlocked")
	}

	groups, _, _ := unstructured.NestedStringSlice(u.Object, "status", "groups")
	fmt.Fprintf(w, "\nGroups (from status) (%d):\n", len(groups))
	if len(groups) == 0 {
		fmt.Fprintln(w, "  <none>")
	}
	for _, g := range groups {
		fmt.Fprintf(w, "  - %s\n", g)
	}

	fmt.Fprintln(w, "\nTip: run \"d8 iam access explain user "+name+"\" to see effective access.")
	return nil
}

func printUserTable(cmd *cobra.Command, users []*unstructured.Unstructured) error {
	tw := printers.GetNewTabWriter(cmd.OutOrStdout())
	fmt.Fprintln(tw, "NAME\tEMAIL\tGROUPS\tEXPIRE_AT\tLOCKED")
	for _, u := range users {
		name := u.GetName()
		email, _, _ := unstructured.NestedString(u.Object, "spec", "email")
		expireAt, _, _ := unstructured.NestedString(u.Object, "status", "expireAt")

		groups, _, _ := unstructured.NestedStringSlice(u.Object, "status", "groups")
		groupStr := strings.Join(groups, ",")
		if groupStr == "" {
			groupStr = "<none>"
		}

		locked := "false"
		lockState, found, _ := unstructured.NestedBool(u.Object, "status", "lock", "state")
		if found && lockState {
			locked = "true"
		}

		if expireAt == "" {
			expireAt = "<none>"
		}

		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", name, email, groupStr, expireAt, locked)
	}
	return tw.Flush()
}
