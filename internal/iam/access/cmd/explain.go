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
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var explainLong = templates.LongDesc(`
Explain why a user or group has certain access.

This command traces access through identity resolution, group memberships,
direct and inherited grants, and shows warnings about potential issues
like group cycles, orphaned references, and manual (non-d8-managed) objects.

© Flant JSC 2026`)

var explainExample = templates.Examples(`
  # Explain access for a user
  d8 iam access explain user anton

  # Explain access for a group (JSON output)
  d8 iam access explain group admins -o json`)

func newExplainCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "explain (user|group) <name>",
		Short:             "Explain why a user or group has certain access",
		Long:              explainLong,
		Example:           explainExample,
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completeSubjectAndName,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE:              runExplain,
	}

	cmd.Flags().StringP("output", "o", "text", "Output format: text|json")
	_ = cmd.RegisterFlagCompletionFunc("output", utilk8s.CompleteOutputFormats("text", "json"))
	return cmd
}

func runExplain(cmd *cobra.Command, args []string) error {
	kindStr := args[0]
	name := args[1]
	outputFmt, _ := cmd.Flags().GetString("output")

	subjectKind, err := parseSubjectKind(kindStr)
	if err != nil {
		return err
	}

	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return err
	}

	inv, err := buildInventory(cmd.Context(), dyn)
	if err != nil {
		return err
	}

	switch subjectKind {
	case iamtypes.KindUser:
		return explainUser(cmd, inv, name, outputFmt)
	case iamtypes.KindGroup:
		return explainGroup(cmd, inv, name, outputFmt)
	}
	return nil
}

func explainUser(cmd *cobra.Command, inv *accessInventory, userName, outputFmt string) error {
	email, ok := inv.Users[userName]
	if !ok {
		return fmt.Errorf("user %q not found", userName)
	}

	directGroups, transitiveGroups := inv.ResolveUserGroups(userName)
	directGrants, inheritedGrants := inv.UserGrants(userName)
	allGrants := make([]normalizedGrant, 0, len(directGrants)+len(inheritedGrants))
	allGrants = append(allGrants, directGrants...)
	allGrants = append(allGrants, inheritedGrants...)
	summary := computeEffectiveSummary(allGrants)
	cycles := inv.DetectGroupCycles()
	warnings := collectUserWarnings(transitiveGroups, directGrants, inheritedGrants, cycles)

	if outputFmt == "json" {
		item := buildUserAccessJSON(inv, userName)
		item.Warnings = warnings
		data, err := json.MarshalIndent(item, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), string(data))
		return nil
	}

	w := cmd.OutOrStdout()

	fmt.Fprintln(w, "=== Identity Resolution ===")
	fmt.Fprintf(w, "User CR: %s\n", userName)
	fmt.Fprintf(w, "Resolved principal (email): %s\n", email)

	fmt.Fprintln(w, "\n=== Group Memberships ===")
	fmt.Fprintln(w, "Direct groups:")
	printBulletList(w, directGroups)
	if len(transitiveGroups) > len(directGroups) {
		fmt.Fprintln(w, "Transitive groups (via nested membership):")
		directSet := make(map[string]bool, len(directGroups))
		for _, g := range directGroups {
			directSet[g] = true
		}
		var extra []string
		for _, g := range transitiveGroups {
			if !directSet[g] {
				extra = append(extra, g)
			}
		}
		sort.Strings(extra)
		printBulletList(w, extra)
	}

	fmt.Fprintln(w, "\n=== Direct Grants (matched by email) ===")
	if len(directGrants) == 0 {
		fmt.Fprintln(w, "  <none>")
	}
	for _, g := range directGrants {
		printGrantToWriter(w, &g, "")
	}

	fmt.Fprintln(w, "\n=== Inherited Grants (via group membership) ===")
	if len(inheritedGrants) == 0 {
		fmt.Fprintln(w, "  <none>")
	}
	for _, g := range inheritedGrants {
		via := findViaGroup(inv, userName, g.SubjectPrincipal)
		printGrantToWriter(w, &g, via)
	}

	fmt.Fprintln(w, "\n=== Effective Access Summary ===")
	printEffectiveSummary(w, summary)

	if len(warnings) > 0 {
		fmt.Fprintln(w, "\n=== Warnings ===")
		for _, warn := range warnings {
			fmt.Fprintf(w, "  ! %s\n", warn)
		}
	}

	return nil
}

func explainGroup(cmd *cobra.Command, inv *accessInventory, groupName, outputFmt string) error {
	if _, ok := inv.GroupMembers[groupName]; !ok {
		return fmt.Errorf("group %q not found", groupName)
	}

	grants := inv.GroupGrants(groupName)
	summary := computeEffectiveSummary(grants)
	cycles := inv.DetectGroupCycles()
	warnings := collectGroupWarnings(inv, groupName, cycles)

	if outputFmt == "json" {
		return printGroupExplainJSON(cmd, inv, groupName, warnings)
	}

	w := cmd.OutOrStdout()

	fmt.Fprintln(w, "=== Group Identity ===")
	fmt.Fprintf(w, "Group CR: %s\n", groupName)

	userMembers, groupMembers := partitionMembersByKind(inv.GroupMembers[groupName])

	fmt.Fprintln(w, "\n=== Members ===")
	fmt.Fprintf(w, "Users (%d):\n", len(userMembers))
	printBulletList(w, userMembers)
	fmt.Fprintf(w, "Nested groups (%d):\n", len(groupMembers))
	printBulletList(w, groupMembers)

	fmt.Fprintln(w, "\n=== Grants ===")
	if len(grants) == 0 {
		fmt.Fprintln(w, "  <none>")
	}
	for _, g := range grants {
		printGrantToWriter(w, &g, "")
	}

	fmt.Fprintln(w, "\n=== Effective Access Summary ===")
	printEffectiveSummary(w, summary)

	if len(warnings) > 0 {
		fmt.Fprintln(w, "\n=== Warnings ===")
		for _, warn := range warnings {
			fmt.Fprintf(w, "  ! %s\n", warn)
		}
	}

	return nil
}

func collectUserWarnings(groups []string, directGrants, inheritedGrants []normalizedGrant, cycles map[string][]string) []string {
	var warnings []string

	// Check for cycles in user's groups
	for _, g := range groups {
		if cycle, ok := cycles[g]; ok {
			warnings = append(warnings, fmt.Sprintf("group cycle detected: %s", strings.Join(cycle, " -> ")))
		}
	}

	for _, g := range directGrants {
		if !g.ManagedByD8 {
			warnings = append(warnings, fmt.Sprintf("direct grant from manual object: %s", formatGrantSource(&g)))
		}
	}
	for _, g := range inheritedGrants {
		if !g.ManagedByD8 {
			warnings = append(warnings, fmt.Sprintf("inherited grant from manual object: %s (via group %s)", formatGrantSource(&g), g.SubjectPrincipal))
		}
	}

	return warnings
}

func collectGroupWarnings(inv *accessInventory, groupName string, cycles map[string][]string) []string {
	var warnings []string

	if cycle, ok := cycles[groupName]; ok {
		warnings = append(warnings, fmt.Sprintf("group cycle detected: %s", strings.Join(cycle, " -> ")))
	}

	// Check if any user members are orphaned
	members := inv.GroupMembers[groupName]
	for _, m := range members {
		if m.Kind == iamtypes.KindUser {
			if _, ok := inv.Users[m.Name]; !ok {
				warnings = append(warnings, fmt.Sprintf("user member %q not found as a local User CR (may be orphaned)", m.Name))
			}
		}
		if m.Kind == iamtypes.KindGroup {
			if _, ok := inv.GroupMembers[m.Name]; !ok {
				warnings = append(warnings, fmt.Sprintf("nested group %q not found as a local Group CR", m.Name))
			}
		}
	}

	return warnings
}

func printGroupExplainJSON(cmd *cobra.Command, inv *accessInventory, groupName string, warnings []string) error {
	members := inv.GroupMembers[groupName]
	grants := inv.GroupGrants(groupName)
	summary := computeEffectiveSummary(grants)

	type memberEntry struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	}
	type groupExplainJSON struct {
		Kind      string        `json:"kind"`
		Name      string        `json:"name"`
		Members   []memberEntry `json:"members"`
		Grants    []grantJSON   `json:"grants"`
		Effective effectiveJSON `json:"effectiveSummary"`
		Warnings  []string      `json:"warnings"`
	}

	item := groupExplainJSON{
		Kind:      "GroupAccessExplanation",
		Name:      groupName,
		Effective: summaryToJSON(summary),
		Warnings:  warnings,
	}
	for _, m := range members {
		item.Members = append(item.Members, memberEntry{Kind: string(m.Kind), Name: m.Name})
	}
	if item.Members == nil {
		item.Members = []memberEntry{}
	}
	for _, g := range grants {
		item.Grants = append(item.Grants, grantToJSON(&g, ""))
	}
	if item.Grants == nil {
		item.Grants = []grantJSON{}
	}
	if item.Warnings == nil {
		item.Warnings = []string{}
	}

	data, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(data))
	return nil
}
