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
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/printers"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

// runInventory builds the access inventory once per command invocation and
// hands it (plus the resolved -o value) to fn. Every list/get command in
// this package goes through this helper so each only declares the actual
// rendering, not the open-client / open-inventory boilerplate.
func runInventory(cmd *cobra.Command, fn func(inv *accessInventory, outputFmt string) error) error {
	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return err
	}
	inv, err := buildInventory(cmd.Context(), dyn)
	if err != nil {
		return err
	}
	outputFmt, _ := cmd.Flags().GetString("output")
	return fn(inv, outputFmt)
}

// NewListUsersCommand returns the cobra command behind "d8 iam list users".
// Exported so package listget can register it under "d8 iam list".
func NewListUsersCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "users",
		Aliases:       []string{"user"},
		Short:         "List all users with their effective access",
		Args:          cobra.NoArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runInventory(cmd, func(inv *accessInventory, outputFmt string) error {
				if outputFmt == "json" {
					return printStructured(cmd.OutOrStdout(), buildUsersJSON(inv), outputFmt)
				}
				return printUsersTable(cmd, inv)
			})
		},
	}
	utilk8s.AddOutputFlag(cmd, "table", "table", "json")
	return cmd
}

// NewListGroupsCommand returns the cobra command behind "d8 iam list groups".
func NewListGroupsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "groups",
		Aliases:       []string{"group"},
		Short:         "List all groups with their effective access",
		Args:          cobra.NoArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runInventory(cmd, func(inv *accessInventory, outputFmt string) error {
				if outputFmt == "json" {
					return printStructured(cmd.OutOrStdout(), buildGroupsJSON(inv), outputFmt)
				}
				return printGroupsTable(cmd, inv)
			})
		},
	}
	utilk8s.AddOutputFlag(cmd, "table", "table", "json")
	return cmd
}

// NewGetUserCommand returns the cobra command behind "d8 iam get user <name>".
// Output combines the user's identity, group memberships (direct +
// transitive), direct/inherited grants, the effective summary, and any
// warnings (group cycles, manual rules, orphaned subjects).
func NewGetUserCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "user <name>",
		Aliases: []string{"users"},
		Short:   "Show detailed access for a specific user",
		Args:    cobra.ExactArgs(1),
		ValidArgsFunction: func(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return utilk8s.CompleteResourceNames(cmd, iamtypes.UserGVR, "", toComplete)
		},
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			userName := args[0]
			return runInventory(cmd, func(inv *accessInventory, outputFmt string) error {
				if _, ok := inv.Users[userName]; !ok {
					return fmt.Errorf("user %q not found", userName)
				}
				warnings := userWarnings(inv, userName)
				switch outputFmt {
				case "json", "yaml":
					payload := buildUserAccessJSON(inv, userName)
					payload.Warnings = denil(warnings)
					return printStructured(cmd.OutOrStdout(), payload, outputFmt)
				case "table", "":
					return printUserDetail(cmd, inv, userName, warnings)
				default:
					return fmt.Errorf("%w %q; use table|json|yaml", errUnsupportedFormat, outputFmt)
				}
			})
		},
	}
	utilk8s.AddOutputFlag(cmd, "table", "table", "json", "yaml")
	return cmd
}

// NewGetGroupCommand returns the cobra command behind "d8 iam get group <name>".
func NewGetGroupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "group <name>",
		Aliases: []string{"groups"},
		Short:   "Show detailed access for a specific group",
		Args:    cobra.ExactArgs(1),
		ValidArgsFunction: func(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return utilk8s.CompleteResourceNames(cmd, iamtypes.GroupGVR, "", toComplete)
		},
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			groupName := args[0]
			return runInventory(cmd, func(inv *accessInventory, outputFmt string) error {
				if _, ok := inv.GroupMembers[groupName]; !ok {
					return fmt.Errorf("group %q not found", groupName)
				}
				warnings := groupWarnings(inv, groupName)
				switch outputFmt {
				case "json", "yaml":
					payload := buildGroupDetailJSON(inv, groupName)
					payload.Warnings = denil(warnings)
					return printStructured(cmd.OutOrStdout(), payload, outputFmt)
				case "table", "":
					return printGroupDetail(cmd, inv, groupName, warnings)
				default:
					return fmt.Errorf("%w %q; use table|json|yaml", errUnsupportedFormat, outputFmt)
				}
			})
		},
	}
	utilk8s.AddOutputFlag(cmd, "table", "table", "json", "yaml")
	return cmd
}

// --- Table output ---

func printUsersTable(cmd *cobra.Command, inv *accessInventory) error {
	tw := printers.GetNewTabWriter(cmd.OutOrStdout())
	fmt.Fprintln(tw, "USER\tEMAIL\tGROUPS\tDIRECT\tINHERIT\tEFFECTIVE")

	users := make([]string, 0, len(inv.Users))
	for u := range inv.Users {
		users = append(users, u)
	}
	sort.Strings(users)

	for _, userName := range users {
		email := inv.Users[userName]
		directGroups, _ := inv.ResolveUserGroups(userName)
		directGrants, inheritedGrants := inv.UserGrants(userName)
		allGrants := make([]normalizedGrant, 0, len(directGrants)+len(inheritedGrants))
		allGrants = append(allGrants, directGrants...)
		allGrants = append(allGrants, inheritedGrants...)
		summary := computeEffectiveSummary(allGrants)

		groupStr := "<none>"
		if len(directGroups) > 0 {
			groupStr = strings.Join(directGroups, ",")
		}

		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%s\n",
			userName, email, groupStr,
			len(directGrants), len(inheritedGrants),
			summary.String())
	}
	return tw.Flush()
}

func printGroupsTable(cmd *cobra.Command, inv *accessInventory) error {
	tw := printers.GetNewTabWriter(cmd.OutOrStdout())
	fmt.Fprintln(tw, "GROUP\tMEMBERS\tNESTED\tGRANTS\tEFFECTIVE")

	groups := make([]string, 0, len(inv.GroupMembers))
	for g := range inv.GroupMembers {
		groups = append(groups, g)
	}
	sort.Strings(groups)

	for _, groupName := range groups {
		members := inv.GroupMembers[groupName]
		userCount, nestedCount := 0, 0
		for _, m := range members {
			if m.Kind == iamtypes.KindGroup {
				nestedCount++
			} else {
				userCount++
			}
		}

		grants := inv.GroupGrants(groupName)
		summary := computeEffectiveSummary(grants)

		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%s\n",
			groupName, userCount, nestedCount, len(grants), summary.String())
	}
	return tw.Flush()
}

func printUserDetail(cmd *cobra.Command, inv *accessInventory, userName string, warnings []string) error {
	w := cmd.OutOrStdout()
	email := inv.Users[userName]
	directGroups, transitiveGroups := inv.ResolveUserGroups(userName)
	directGrants, inheritedGrants := inv.UserGrants(userName)
	allGrants := make([]normalizedGrant, 0, len(directGrants)+len(inheritedGrants))
	allGrants = append(allGrants, directGrants...)
	allGrants = append(allGrants, inheritedGrants...)
	summary := computeEffectiveSummary(allGrants)

	fmt.Fprintf(w, "User: %s\n", userName)
	fmt.Fprintf(w, "Email: %s\n", email)

	fmt.Fprintf(w, "\nGroups (direct):\n")
	printBulletList(w, directGroups)

	if len(transitiveGroups) > len(directGroups) {
		fmt.Fprintf(w, "\nGroups (transitive):\n")
		printBulletList(w, transitiveGroups)
	}

	fmt.Fprintf(w, "\nDirect access:\n")
	if len(directGrants) == 0 {
		fmt.Fprintln(w, "  <none>")
	}
	for _, g := range directGrants {
		printGrantToWriter(w, &g, "")
	}

	fmt.Fprintf(w, "\nInherited access:\n")
	if len(inheritedGrants) == 0 {
		fmt.Fprintln(w, "  <none>")
	}
	for _, g := range inheritedGrants {
		via := findViaGroup(inv, userName, g.SubjectPrincipal)
		printGrantToWriter(w, &g, via)
	}

	fmt.Fprintf(w, "\nEffective access summary:\n")
	printEffectiveSummary(w, summary)
	printWarnings(w, warnings)

	return nil
}

func printGroupDetail(cmd *cobra.Command, inv *accessInventory, groupName string, warnings []string) error {
	w := cmd.OutOrStdout()
	members := inv.GroupMembers[groupName]
	grants := inv.GroupGrants(groupName)
	summary := computeEffectiveSummary(grants)

	fmt.Fprintf(w, "Group: %s\n", groupName)

	userMembers, groupMembers := partitionMembersByKind(members)

	fmt.Fprintf(w, "\nUser members (%d):\n", len(userMembers))
	printBulletList(w, userMembers)

	fmt.Fprintf(w, "\nNested groups (%d):\n", len(groupMembers))
	printBulletList(w, groupMembers)

	fmt.Fprintf(w, "\nGrants (%d):\n", len(grants))
	if len(grants) == 0 {
		fmt.Fprintln(w, "  <none>")
	}
	for _, g := range grants {
		printGrantToWriter(w, &g, "")
	}

	fmt.Fprintf(w, "\nEffective access summary:\n")
	printEffectiveSummary(w, summary)
	printWarnings(w, warnings)

	return nil
}

func printGrantToWriter(w io.Writer, g *normalizedGrant, via string) {
	scope := string(g.ScopeType)
	if g.ScopeType == iamtypes.ScopeNamespace && len(g.ScopeNamespaces) > 0 {
		scope = fmt.Sprintf("namespaces %s", strings.Join(g.ScopeNamespaces, ", "))
	}
	managed := ""
	if g.ManagedByD8 {
		managed = " [d8-managed]"
	}

	fmt.Fprintf(w, "  - %s\n", g.AccessLevel)
	fmt.Fprintf(w, "    Scope: %s\n", scope)
	fmt.Fprintf(w, "    Source: %s%s\n", formatGrantSource(g), managed)
	if via != "" {
		fmt.Fprintf(w, "    Via: group %s\n", via)
	}
	if g.AllowScale {
		fmt.Fprintln(w, "    allow-scale: true")
	}
	if g.PortForwarding {
		fmt.Fprintln(w, "    port-forwarding: true")
	}
}

func formatGrantSource(g *normalizedGrant) string {
	return formatRuleRef(g.SourceKind, g.SourceNamespace, g.SourceName)
}

// printEffectiveSummary renders the cluster/namespaced/port-forwarding/
// allow-scale block. Capability lines append capabilityNote() when the
// capability is implicit (SuperAdmin wildcard) rather than an explicit flag.
func printEffectiveSummary(w io.Writer, summary *effectiveSummary) {
	if summary.ClusterLevel != "" {
		fmt.Fprintf(w, "  cluster scope: %s\n", summary.ClusterLevel)
	}
	for ns, level := range summary.Namespaced {
		fmt.Fprintf(w, "  namespaced scope: %s(%s)\n", level, ns)
	}
	fmt.Fprintf(w, "  port-forwarding: %v%s\n", summary.PortForwarding, capabilityNote(summary.PortForwardingImplicit))
	fmt.Fprintf(w, "  allow-scale: %v%s\n", summary.AllowScale, capabilityNote(summary.AllowScaleImplicit))
}

func printWarnings(w io.Writer, warnings []string) {
	if len(warnings) == 0 {
		return
	}
	fmt.Fprintln(w, "\nWarnings:")
	for _, warn := range warnings {
		fmt.Fprintf(w, "  ! %s\n", warn)
	}
}

func partitionMembersByKind(members []memberRef) ([]string, []string) {
	var users, groups []string
	for _, m := range members {
		if m.Kind == iamtypes.KindGroup {
			groups = append(groups, m.Name)
		} else {
			users = append(users, m.Name)
		}
	}
	return users, groups
}

func printBulletList(w io.Writer, items []string) {
	if len(items) == 0 {
		fmt.Fprintln(w, "  <none>")
		return
	}
	for _, item := range items {
		fmt.Fprintf(w, "  - %s\n", item)
	}
}

func findViaGroup(inv *accessInventory, userName, grantGroupName string) string {
	directGroups, transitiveGroups := inv.ResolveUserGroups(userName)
	for _, g := range directGroups {
		if g == grantGroupName {
			return g
		}
	}
	for _, g := range transitiveGroups {
		if g == grantGroupName {
			return g + " (transitive)"
		}
	}
	return grantGroupName
}

// --- Warnings (folded in from the former "iam access explain") ---

// userWarnings returns the same warning set that "iam access explain user"
// used to surface: cycles in any group the user transits, and grants from
// manual (non-d8-managed) rules so the operator knows that revoke cannot
// touch them.
func userWarnings(inv *accessInventory, userName string) []string {
	if _, ok := inv.Users[userName]; !ok {
		return nil
	}
	cycles := inv.DetectGroupCycles()
	_, transitiveGroups := inv.ResolveUserGroups(userName)
	directGrants, inheritedGrants := inv.UserGrants(userName)

	var out []string
	for _, g := range transitiveGroups {
		if cycle, ok := cycles[g]; ok {
			out = append(out, fmt.Sprintf("group cycle detected: %s", strings.Join(cycle, " -> ")))
		}
	}
	for _, g := range directGrants {
		if !g.ManagedByD8 {
			out = append(out, fmt.Sprintf("direct grant from manual object: %s", formatGrantSource(&g)))
		}
	}
	for _, g := range inheritedGrants {
		if !g.ManagedByD8 {
			out = append(out, fmt.Sprintf("inherited grant from manual object: %s (via group %s)",
				formatGrantSource(&g), g.SubjectPrincipal))
		}
	}
	return out
}

// groupWarnings reports cycles touching the group and members that point at
// User/Group CRs that do not exist locally (orphaned references).
func groupWarnings(inv *accessInventory, groupName string) []string {
	if _, ok := inv.GroupMembers[groupName]; !ok {
		return nil
	}
	cycles := inv.DetectGroupCycles()

	var out []string
	if cycle, ok := cycles[groupName]; ok {
		out = append(out, fmt.Sprintf("group cycle detected: %s", strings.Join(cycle, " -> ")))
	}
	for _, m := range inv.GroupMembers[groupName] {
		switch m.Kind {
		case iamtypes.KindUser:
			if _, ok := inv.Users[m.Name]; !ok {
				out = append(out, fmt.Sprintf("user member %q not found as a local User CR (may be orphaned)", m.Name))
			}
		case iamtypes.KindGroup:
			if _, ok := inv.GroupMembers[m.Name]; !ok {
				out = append(out, fmt.Sprintf("nested group %q not found as a local Group CR", m.Name))
			}
		}
	}
	return out
}

// --- JSON output: shared types ---
//
// Defining these once at package level (instead of inline in each
// printXxxJSON) keeps the wire format documented in one place and lets
// build-helpers share concrete types like memberJSON / grantJSON.
// Slice fields go through denil() so JSON emits "[]" instead of "null"
// — that contract is part of our CLI surface, locked down by tests.

type userAccessJSON struct {
	Kind    string         `json:"kind"`
	Subject subjectJSONRef `json:"subject"`
	Groups  groupsJSONRef  `json:"groups"`

	DirectGrants    []grantJSON   `json:"directGrants"`
	InheritedGrants []grantJSON   `json:"inheritedGrants"`
	Effective       effectiveJSON `json:"effectiveSummary"`
	Warnings        []string      `json:"warnings"`
}

type subjectJSONRef struct {
	Kind      string `json:"kind"`
	RefName   string `json:"refName"`
	Principal string `json:"principal"`
}

type groupsJSONRef struct {
	Direct     []string `json:"direct"`
	Transitive []string `json:"transitive"`
}

type grantJSON struct {
	ViaGroup       string     `json:"viaGroup,omitempty"`
	Source         sourceJSON `json:"source"`
	AccessLevel    string     `json:"accessLevel"`
	Scope          scopeJSON  `json:"scope"`
	AllowScale     bool       `json:"allowScale"`
	PortForwarding bool       `json:"portForwarding"`
	ManagedByD8    bool       `json:"managedByD8"`
}

type sourceJSON struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

type scopeJSON struct {
	Type       string   `json:"type"`
	Namespaces []string `json:"namespaces,omitempty"`
}

type effectiveJSON struct {
	Cluster                string        `json:"cluster,omitempty"`
	Namespaces             []nsLevelJSON `json:"namespaces,omitempty"`
	AllowScale             bool          `json:"allowScale"`
	PortForwarding         bool          `json:"portForwarding"`
	AllowScaleImplicit     bool          `json:"allowScaleImplicit,omitempty"`
	PortForwardingImplicit bool          `json:"portForwardingImplicit,omitempty"`
}

type nsLevelJSON struct {
	AccessLevel string   `json:"accessLevel"`
	Namespaces  []string `json:"namespaces"`
}

type memberJSON struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type groupSummaryJSON struct {
	Name      string        `json:"name"`
	Members   int           `json:"memberCount"`
	Nested    int           `json:"nestedGroupCount"`
	Grants    int           `json:"grantCount"`
	Effective effectiveJSON `json:"effectiveSummary"`
}

type groupDetailJSON struct {
	Name      string        `json:"name"`
	Members   []memberJSON  `json:"members"`
	Grants    []grantJSON   `json:"grants"`
	Effective effectiveJSON `json:"effectiveSummary"`
	Warnings  []string      `json:"warnings"`
}

// --- JSON output: build helpers ---

func grantToJSON(g *normalizedGrant, via string) grantJSON {
	return grantJSON{
		ViaGroup: via,
		Source: sourceJSON{
			Kind:      g.SourceKind,
			Name:      g.SourceName,
			Namespace: g.SourceNamespace,
		},
		AccessLevel:    g.AccessLevel,
		Scope:          scopeJSON{Type: string(g.ScopeType), Namespaces: g.ScopeNamespaces},
		AllowScale:     g.AllowScale,
		PortForwarding: g.PortForwarding,
		ManagedByD8:    g.ManagedByD8,
	}
}

func summaryToJSON(s *effectiveSummary) effectiveJSON {
	ej := effectiveJSON{
		Cluster:                s.ClusterLevel,
		AllowScale:             s.AllowScale,
		PortForwarding:         s.PortForwarding,
		AllowScaleImplicit:     s.AllowScaleImplicit,
		PortForwardingImplicit: s.PortForwardingImplicit,
	}

	levelNS := make(map[string][]string)
	for ns, level := range s.Namespaced {
		levelNS[level] = append(levelNS[level], ns)
	}
	for level, nss := range levelNS {
		sort.Strings(nss)
		ej.Namespaces = append(ej.Namespaces, nsLevelJSON{AccessLevel: level, Namespaces: nss})
	}

	return ej
}

func buildUsersJSON(inv *accessInventory) []userAccessJSON {
	users := make([]string, 0, len(inv.Users))
	for u := range inv.Users {
		users = append(users, u)
	}
	sort.Strings(users)

	items := make([]userAccessJSON, 0, len(users))
	for _, userName := range users {
		items = append(items, buildUserAccessJSON(inv, userName))
	}
	return items
}

func buildUserAccessJSON(inv *accessInventory, userName string) userAccessJSON {
	email := inv.Users[userName]
	directGroups, transitiveGroups := inv.ResolveUserGroups(userName)
	directGrants, inheritedGrants := inv.UserGrants(userName)
	allGrants := make([]normalizedGrant, 0, len(directGrants)+len(inheritedGrants))
	allGrants = append(allGrants, directGrants...)
	allGrants = append(allGrants, inheritedGrants...)
	summary := computeEffectiveSummary(allGrants)

	directGrantsJSON := make([]grantJSON, 0, len(directGrants))
	for _, g := range directGrants {
		directGrantsJSON = append(directGrantsJSON, grantToJSON(&g, ""))
	}
	inheritedGrantsJSON := make([]grantJSON, 0, len(inheritedGrants))
	for _, g := range inheritedGrants {
		via := findViaGroup(inv, userName, g.SubjectPrincipal)
		inheritedGrantsJSON = append(inheritedGrantsJSON, grantToJSON(&g, via))
	}

	return userAccessJSON{
		Kind: "AccessExplanation",
		Subject: subjectJSONRef{
			Kind:      string(iamtypes.KindUser),
			RefName:   userName,
			Principal: email,
		},
		Groups: groupsJSONRef{
			Direct:     denil(directGroups),
			Transitive: denil(transitiveGroups),
		},
		DirectGrants:    denil(directGrantsJSON),
		InheritedGrants: denil(inheritedGrantsJSON),
		Effective:       summaryToJSON(summary),
		Warnings:        []string{},
	}
}

func buildGroupsJSON(inv *accessInventory) []groupSummaryJSON {
	groups := make([]string, 0, len(inv.GroupMembers))
	for g := range inv.GroupMembers {
		groups = append(groups, g)
	}
	sort.Strings(groups)

	items := make([]groupSummaryJSON, 0, len(groups))
	for _, gName := range groups {
		members := inv.GroupMembers[gName]
		userCount, nestedCount := 0, 0
		for _, m := range members {
			if m.Kind == iamtypes.KindGroup {
				nestedCount++
			} else {
				userCount++
			}
		}
		grants := inv.GroupGrants(gName)
		summary := computeEffectiveSummary(grants)
		items = append(items, groupSummaryJSON{
			Name:      gName,
			Members:   userCount,
			Nested:    nestedCount,
			Grants:    len(grants),
			Effective: summaryToJSON(summary),
		})
	}
	return items
}

func buildGroupDetailJSON(inv *accessInventory, groupName string) groupDetailJSON {
	members := inv.GroupMembers[groupName]
	grants := inv.GroupGrants(groupName)
	summary := computeEffectiveSummary(grants)

	memberItems := make([]memberJSON, 0, len(members))
	for _, m := range members {
		memberItems = append(memberItems, memberJSON{Kind: string(m.Kind), Name: m.Name})
	}
	grantItems := make([]grantJSON, 0, len(grants))
	for _, g := range grants {
		grantItems = append(grantItems, grantToJSON(&g, ""))
	}

	return groupDetailJSON{
		Name:      groupName,
		Members:   denil(memberItems),
		Grants:    denil(grantItems),
		Effective: summaryToJSON(summary),
		Warnings:  []string{},
	}
}
