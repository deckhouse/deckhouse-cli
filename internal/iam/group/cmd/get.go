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

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/cli-runtime/pkg/printers"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

func newGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "get <name>",
		Short:             "Get details of a local group",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeGroupOnly,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			outputFmt, _ := cmd.Flags().GetString("output")

			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}

			obj, err := dyn.Resource(groupGVR).Get(cmd.Context(), name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("getting Group %q: %w", name, err)
			}

			switch outputFmt {
			case "json", "yaml":
				return utilk8s.PrintObject(cmd.OutOrStdout(), obj, outputFmt)
			default:
				return printGroupDetail(cmd, obj)
			}
		},
	}

	cmd.Flags().StringP("output", "o", "table", "Output format: table|json|yaml")
	_ = cmd.RegisterFlagCompletionFunc("output", utilk8s.CompleteOutputFormats("table", "json", "yaml"))
	return cmd
}

func printGroupDetail(cmd *cobra.Command, obj *unstructured.Unstructured) error {
	w := cmd.OutOrStdout()
	name := obj.GetName()
	specName, _, _ := unstructured.NestedString(obj.Object, "spec", "name")
	fmt.Fprintf(w, "Group: %s\n", name)
	if specName != "" && specName != name {
		fmt.Fprintf(w, "Display name: %s\n", specName)
	}

	members, _ := getGroupMembers(obj)
	var users, groups []string
	for _, m := range members {
		kind := fmt.Sprint(m["kind"])
		mName := fmt.Sprint(m["name"])
		switch kind {
		case "Group":
			groups = append(groups, mName)
		default:
			users = append(users, mName)
		}
	}

	fmt.Fprintf(w, "\nUser members (%d):\n", len(users))
	if len(users) == 0 {
		fmt.Fprintln(w, "  <none>")
	}
	for _, u := range users {
		fmt.Fprintf(w, "  - %s\n", u)
	}

	fmt.Fprintf(w, "\nNested groups (%d):\n", len(groups))
	if len(groups) == 0 {
		fmt.Fprintln(w, "  <none>")
	}
	for _, g := range groups {
		fmt.Fprintf(w, "  - %s\n", g)
	}

	// Show status errors if present (try both spec.status.errors and status.errors)
	errors := getStatusErrors(obj)
	if len(errors) > 0 {
		fmt.Fprintf(w, "\nStatus errors (%d):\n", len(errors))
		for _, e := range errors {
			fmt.Fprintf(w, "  - %s\n", e)
		}
	}

	fmt.Fprintln(w, "\nTip: run \"d8 iam access explain group "+name+"\" to see effective grants.")
	return nil
}

func getStatusErrors(obj *unstructured.Unstructured) []string {
	var result []string

	// Try spec.status.errors (as per CRD schema)
	paths := [][]string{
		{"spec", "status", "errors"},
		{"status", "errors"},
	}
	for _, path := range paths {
		errs, found, _ := unstructured.NestedSlice(obj.Object, path...)
		if !found {
			continue
		}
		for _, e := range errs {
			em, ok := e.(map[string]any)
			if !ok {
				continue
			}
			msg := fmt.Sprint(em["message"])
			if ref, ok := em["objectRef"].(map[string]any); ok {
				msg = fmt.Sprintf("[%s/%s] %s", ref["kind"], ref["name"], msg)
			}
			result = append(result, msg)
		}
		if len(result) > 0 {
			return result
		}
	}
	return result
}

func printGroupTable(cmd *cobra.Command, groups []*unstructured.Unstructured) error {
	tw := printers.GetNewTabWriter(cmd.OutOrStdout())
	fmt.Fprintln(tw, "GROUP\tMEMBERS\tNESTED\tERRORS")
	for _, g := range groups {
		name := g.GetName()
		members, _ := getGroupMembers(g)
		userCount := 0
		nestedCount := 0
		for _, m := range members {
			if fmt.Sprint(m["kind"]) == "Group" {
				nestedCount++
			} else {
				userCount++
			}
		}
		errorCount := len(getStatusErrors(g))
		errStr := "0"
		if errorCount > 0 {
			errStr = fmt.Sprintf("%d", errorCount)
		}

		fmt.Fprintf(tw, "%s\t%d\t%d\t%s\n", name, userCount, nestedCount, errStr)
	}
	return tw.Flush()
}
