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
	"io"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var listLong = templates.LongDesc(`
List effective access for users and groups.

This is not a raw CR listing. It aggregates information from Users, Groups,
AuthorizationRules, and ClusterAuthorizationRules to show effective access.

See the list of subcommands below or run "d8 iam access list SUBCOMMAND --help".

© Flant JSC 2026`)

func newListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list (users|groups|user <name>|group <name>)",
		Short: "List effective access for users and groups",
		Long:  listLong,
	}

	cmd.AddCommand(
		newListUsersCommand(),
		newListGroupsCommand(),
		newListUserCommand(),
		newListGroupCommand(),
	)
	return cmd
}

func newListUsersCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "users",
		Short:         "List all users with their effective access",
		Args:          cobra.NoArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			outputFmt, _ := cmd.Flags().GetString("output")

			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}

			inv, err := buildInventory(cmd.Context(), dyn)
			if err != nil {
				return err
			}

			if outputFmt == "json" {
				return printUsersJSON(cmd, inv)
			}

			return printUsersTable(cmd, inv)
		},
	}
	cmd.Flags().StringP("output", "o", "table", "Output format: table|json")
	_ = cmd.RegisterFlagCompletionFunc("output", utilk8s.CompleteOutputFormats("table", "json"))
	return cmd
}

func newListGroupsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "groups",
		Short:         "List all groups with their effective access",
		Args:          cobra.NoArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			outputFmt, _ := cmd.Flags().GetString("output")

			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}

			inv, err := buildInventory(cmd.Context(), dyn)
			if err != nil {
				return err
			}

			if outputFmt == "json" {
				return printGroupsJSON(cmd, inv)
			}

			return printGroupsTable(cmd, inv)
		},
	}
	cmd.Flags().StringP("output", "o", "table", "Output format: table|json")
	_ = cmd.RegisterFlagCompletionFunc("output", utilk8s.CompleteOutputFormats("table", "json"))
	return cmd
}

func newListUserCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "user <name>",
		Short: "Show detailed access for a specific user",
		Args:  cobra.ExactArgs(1),
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) >= 1 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return utilk8s.CompleteResourceNames(cmd, userGVR, "", toComplete)
		},
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			userName := args[0]
			outputFmt, _ := cmd.Flags().GetString("output")

			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}

			inv, err := buildInventory(cmd.Context(), dyn)
			if err != nil {
				return err
			}

			if _, ok := inv.Users[userName]; !ok {
				return fmt.Errorf("user %q not found", userName)
			}

			if outputFmt == "json" {
				return printUserDetailJSON(cmd, inv, userName)
			}

			return printUserDetail(cmd, inv, userName)
		},
	}
	cmd.Flags().StringP("output", "o", "table", "Output format: table|json")
	_ = cmd.RegisterFlagCompletionFunc("output", utilk8s.CompleteOutputFormats("table", "json"))
	return cmd
}

func newListGroupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "group <name>",
		Short: "Show detailed access for a specific group",
		Args:  cobra.ExactArgs(1),
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) >= 1 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return utilk8s.CompleteResourceNames(cmd, groupGVR, "", toComplete)
		},
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			groupName := args[0]
			outputFmt, _ := cmd.Flags().GetString("output")

			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}

			inv, err := buildInventory(cmd.Context(), dyn)
			if err != nil {
				return err
			}

			if _, ok := inv.GroupMembers[groupName]; !ok {
				return fmt.Errorf("group %q not found", groupName)
			}

			if outputFmt == "json" {
				return printGroupDetailJSON(cmd, inv, groupName)
			}

			return printGroupDetail(cmd, inv, groupName)
		},
	}
	cmd.Flags().StringP("output", "o", "table", "Output format: table|json")
	_ = cmd.RegisterFlagCompletionFunc("output", utilk8s.CompleteOutputFormats("table", "json"))
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
			if m.Kind == "Group" {
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

func printUserDetail(cmd *cobra.Command, inv *accessInventory, userName string) error {
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

	return nil
}

func printGroupDetail(cmd *cobra.Command, inv *accessInventory, groupName string) error {
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

	return nil
}

func printGrantToWriter(w io.Writer, g *normalizedGrant, via string) {
	scope := g.ScopeType
	if g.ScopeType == "namespace" && len(g.ScopeNamespaces) > 0 {
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

// formatGrantSource renders the source CAR/AR reference as "Kind/Name" for
// cluster-scoped objects and "Kind/Namespace/Name" for namespaced ones.
// Used by text output in list, explain, and warning messages.
func formatGrantSource(g *normalizedGrant) string {
	if g.SourceNamespace != "" {
		return fmt.Sprintf("%s/%s/%s", g.SourceKind, g.SourceNamespace, g.SourceName)
	}
	return fmt.Sprintf("%s/%s", g.SourceKind, g.SourceName)
}

// printEffectiveSummary renders the "cluster scope / namespaced scope /
// port-forwarding / allow-scale" block used by both "access list" and
// "access explain". The capability lines include the implicit-source note
// when the capability is inherited from the SuperAdmin wildcard rather than
// an explicit CAR flag.
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

// partitionMembersByKind splits Group.spec.members into user and nested-group
// name slices, preserving the original order in each bucket.
func partitionMembersByKind(members []memberRef) ([]string, []string) {
	var users, groups []string
	for _, m := range members {
		if m.Kind == "Group" {
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
	directGroups, _ := inv.ResolveUserGroups(userName)
	for _, g := range directGroups {
		if g == grantGroupName {
			return g
		}
	}
	// If not a direct group, find indirect path
	_, transitiveGroups := inv.ResolveUserGroups(userName)
	for _, g := range transitiveGroups {
		if g == grantGroupName {
			return g + " (transitive)"
		}
	}
	return grantGroupName
}

// --- JSON output ---

type userAccessJSON struct {
	Kind    string `json:"kind"`
	Subject struct {
		Kind      string `json:"kind"`
		RefName   string `json:"refName"`
		Principal string `json:"principal"`
	} `json:"subject"`
	Groups struct {
		Direct     []string `json:"direct"`
		Transitive []string `json:"transitive"`
	} `json:"groups"`
	DirectGrants    []grantJSON   `json:"directGrants"`
	InheritedGrants []grantJSON   `json:"inheritedGrants"`
	Effective       effectiveJSON `json:"effectiveSummary"`
	Warnings        []string      `json:"warnings"`
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

func grantToJSON(g *normalizedGrant, via string) grantJSON {
	return grantJSON{
		ViaGroup: via,
		Source: sourceJSON{
			Kind:      g.SourceKind,
			Name:      g.SourceName,
			Namespace: g.SourceNamespace,
		},
		AccessLevel:    g.AccessLevel,
		Scope:          scopeJSON{Type: g.ScopeType, Namespaces: g.ScopeNamespaces},
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

func printUsersJSON(cmd *cobra.Command, inv *accessInventory) error {
	items := make([]userAccessJSON, 0, len(inv.Users))
	users := make([]string, 0, len(inv.Users))
	for u := range inv.Users {
		users = append(users, u)
	}
	sort.Strings(users)

	for _, userName := range users {
		items = append(items, buildUserAccessJSON(inv, userName))
	}

	data, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(data))
	return nil
}

func printUserDetailJSON(cmd *cobra.Command, inv *accessInventory, userName string) error {
	item := buildUserAccessJSON(inv, userName)
	data, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(data))
	return nil
}

func buildUserAccessJSON(inv *accessInventory, userName string) userAccessJSON {
	email := inv.Users[userName]
	directGroups, transitiveGroups := inv.ResolveUserGroups(userName)
	directGrants, inheritedGrants := inv.UserGrants(userName)
	allGrants := make([]normalizedGrant, 0, len(directGrants)+len(inheritedGrants))
	allGrants = append(allGrants, directGrants...)
	allGrants = append(allGrants, inheritedGrants...)
	summary := computeEffectiveSummary(allGrants)

	item := userAccessJSON{Kind: "AccessExplanation"}
	item.Subject.Kind = "User"
	item.Subject.RefName = userName
	item.Subject.Principal = email
	item.Groups.Direct = directGroups
	item.Groups.Transitive = transitiveGroups
	if item.Groups.Direct == nil {
		item.Groups.Direct = []string{}
	}
	if item.Groups.Transitive == nil {
		item.Groups.Transitive = []string{}
	}

	for _, g := range directGrants {
		item.DirectGrants = append(item.DirectGrants, grantToJSON(&g, ""))
	}
	for _, g := range inheritedGrants {
		via := findViaGroup(inv, userName, g.SubjectPrincipal)
		item.InheritedGrants = append(item.InheritedGrants, grantToJSON(&g, via))
	}
	if item.DirectGrants == nil {
		item.DirectGrants = []grantJSON{}
	}
	if item.InheritedGrants == nil {
		item.InheritedGrants = []grantJSON{}
	}

	item.Effective = summaryToJSON(summary)
	item.Warnings = []string{}
	return item
}

func printGroupsJSON(cmd *cobra.Command, inv *accessInventory) error {
	type groupJSON struct {
		Name      string        `json:"name"`
		Members   int           `json:"memberCount"`
		Nested    int           `json:"nestedGroupCount"`
		Grants    int           `json:"grantCount"`
		Effective effectiveJSON `json:"effectiveSummary"`
	}

	groups := make([]string, 0, len(inv.GroupMembers))
	for g := range inv.GroupMembers {
		groups = append(groups, g)
	}
	sort.Strings(groups)

	items := make([]groupJSON, 0, len(groups))
	for _, gName := range groups {
		members := inv.GroupMembers[gName]
		userCount, nestedCount := 0, 0
		for _, m := range members {
			if m.Kind == "Group" {
				nestedCount++
			} else {
				userCount++
			}
		}
		grants := inv.GroupGrants(gName)
		summary := computeEffectiveSummary(grants)
		items = append(items, groupJSON{
			Name:      gName,
			Members:   userCount,
			Nested:    nestedCount,
			Grants:    len(grants),
			Effective: summaryToJSON(summary),
		})
	}

	data, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(data))
	return nil
}

func printGroupDetailJSON(cmd *cobra.Command, inv *accessInventory, groupName string) error {
	members := inv.GroupMembers[groupName]
	grants := inv.GroupGrants(groupName)
	summary := computeEffectiveSummary(grants)

	type memberJSON struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	}

	type detailJSON struct {
		Name      string        `json:"name"`
		Members   []memberJSON  `json:"members"`
		Grants    []grantJSON   `json:"grants"`
		Effective effectiveJSON `json:"effectiveSummary"`
	}

	detail := detailJSON{Name: groupName, Effective: summaryToJSON(summary)}
	for _, m := range members {
		detail.Members = append(detail.Members, memberJSON(m))
	}
	if detail.Members == nil {
		detail.Members = []memberJSON{}
	}
	for _, g := range grants {
		detail.Grants = append(detail.Grants, grantToJSON(&g, ""))
	}
	if detail.Grants == nil {
		detail.Grants = []grantJSON{}
	}

	data, err := json.MarshalIndent(detail, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(data))
	return nil
}
