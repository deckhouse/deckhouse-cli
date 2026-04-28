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
	"context"
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
)

// resolveUserEmail fetches a User CR and returns its spec.email.
func resolveUserEmail(ctx context.Context, dyn dynamic.Interface, userName string) (string, error) {
	obj, err := dyn.Resource(iamtypes.UserGVR).Get(ctx, userName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("getting User %q to resolve email: %w", userName, err)
	}
	email, found, _ := unstructured.NestedString(obj.Object, "spec", "email")
	if !found || email == "" {
		return "", fmt.Errorf("User %q has no spec.email", userName)
	}
	return email, nil
}

// accessInventory holds the full in-memory model for access list/explain.
type accessInventory struct {
	Users  map[string]string // user CR name -> email
	Emails map[string]string // email -> user CR name

	// GroupMembers: group name -> list of (kind, name) members
	GroupMembers map[string][]memberRef

	// Grants: normalized grants from all authz rules
	Grants []normalizedGrant
}

// memberRef is a (kind, name) pair from Group.spec.members.
type memberRef struct {
	Kind iamtypes.SubjectKind
	Name string
}

// buildInventory fetches all relevant CRs and builds the access inventory.
func buildInventory(ctx context.Context, dyn dynamic.Interface) (*accessInventory, error) {
	inv := &accessInventory{
		Users:        make(map[string]string),
		Emails:       make(map[string]string),
		GroupMembers: make(map[string][]memberRef),
	}

	// Fetch users
	userList, err := dyn.Resource(iamtypes.UserGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing Users: %w", err)
	}
	for _, u := range userList.Items {
		name := u.GetName()
		email, _, _ := unstructured.NestedString(u.Object, "spec", "email")
		if email != "" {
			inv.Users[name] = email
			inv.Emails[email] = name
		}
	}

	// Fetch groups
	groupList, err := dyn.Resource(iamtypes.GroupGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing Groups: %w", err)
	}
	for _, g := range groupList.Items {
		gName := g.GetName()
		rawMembers, _, _ := unstructured.NestedSlice(g.Object, "spec", "members")
		var members []memberRef
		for _, item := range rawMembers {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			members = append(members, memberRef{
				Kind: iamtypes.SubjectKind(fmt.Sprint(m["kind"])),
				Name: fmt.Sprint(m["name"]),
			})
		}
		inv.GroupMembers[gName] = members
	}

	// Fetch ClusterAuthorizationRules
	carList, err := dyn.Resource(iamtypes.ClusterAuthorizationRuleGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing ClusterAuthorizationRules: %w", err)
	}
	for _, car := range carList.Items {
		inv.Grants = append(inv.Grants, normalizeAuthRule(&car)...)
	}

	// Fetch AuthorizationRules from all namespaces
	arList, err := dyn.Resource(iamtypes.AuthorizationRuleGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing AuthorizationRules: %w", err)
	}
	for _, ar := range arList.Items {
		inv.Grants = append(inv.Grants, normalizeAuthRule(&ar)...)
	}

	return inv, nil
}

// normalizeAuthRule converts an AuthorizationRule or ClusterAuthorizationRule
// into a flat list of normalizedGrant rows (one per non-ServiceAccount subject).
// The CAR vs AR distinction lives entirely in the source kind and the scope:
// CARs default to ScopeCluster but switch to ScopeAllNamespaces or ScopeLabels
// based on namespaceSelector content; ARs are always ScopeNamespace.
func normalizeAuthRule(obj *unstructured.Unstructured) []normalizedGrant {
	managedByD8 := obj.GetLabels()[iamtypes.LabelManagedBy] == iamtypes.ManagedByValueCLI
	accessLevel, _, _ := unstructured.NestedString(obj.Object, "spec", "accessLevel")
	allowScale, _, _ := unstructured.NestedBool(obj.Object, "spec", "allowScale")
	portForwarding, _, _ := unstructured.NestedBool(obj.Object, "spec", "portForwarding")

	base := normalizedGrant{
		SourceName:     obj.GetName(),
		AccessLevel:    accessLevel,
		AllowScale:     allowScale,
		PortForwarding: portForwarding,
		ManagedByD8:    managedByD8,
	}

	switch obj.GetKind() {
	case iamtypes.KindClusterAuthorizationRule:
		base.SourceKind = iamtypes.KindClusterAuthorizationRule
		base.ScopeType = iamtypes.ScopeCluster
		if matchAny, found, _ := unstructured.NestedBool(obj.Object, "spec", "namespaceSelector", "matchAny"); found && matchAny {
			base.ScopeType = iamtypes.ScopeAllNamespaces
		} else if matchLabels, found, _ := unstructured.NestedMap(obj.Object, "spec", "namespaceSelector", "labelSelector", "matchLabels"); found && len(matchLabels) > 0 {
			base.ScopeType = iamtypes.ScopeLabels
		}
	case iamtypes.KindAuthorizationRule:
		base.SourceKind = iamtypes.KindAuthorizationRule
		base.SourceNamespace = obj.GetNamespace()
		base.ScopeType = iamtypes.ScopeNamespace
		base.ScopeNamespaces = []string{obj.GetNamespace()}
	default:
		return nil
	}

	var grants []normalizedGrant
	for _, sub := range readSubjectRefs(obj) {
		if sub.Kind == iamtypes.KindServiceAccount {
			continue
		}
		g := base
		g.SubjectKind = sub.Kind
		g.SubjectPrincipal = sub.Name
		grants = append(grants, g)
	}
	return grants
}

// ResolveUserGroups computes direct and transitive group memberships for a user.
func (inv *accessInventory) ResolveUserGroups(userName string) ([]string, []string) {
	directSet := make(map[string]bool)
	for gName, members := range inv.GroupMembers {
		for _, m := range members {
			if m.Kind == iamtypes.KindUser && m.Name == userName {
				directSet[gName] = true
			}
		}
	}

	visited := make(map[string]bool)
	var walk func(string)
	walk = func(g string) {
		if visited[g] {
			return
		}
		visited[g] = true
		for parentGroup, members := range inv.GroupMembers {
			for _, m := range members {
				if m.Kind == iamtypes.KindGroup && m.Name == g {
					walk(parentGroup)
				}
			}
		}
	}

	for g := range directSet {
		walk(g)
	}

	direct := make([]string, 0, len(directSet))
	for g := range directSet {
		direct = append(direct, g)
	}
	sort.Strings(direct)

	transitive := make([]string, 0, len(visited))
	for g := range visited {
		transitive = append(transitive, g)
	}
	sort.Strings(transitive)
	return direct, transitive
}

// UserGrants returns direct and inherited grants for a user.
func (inv *accessInventory) UserGrants(userName string) ([]normalizedGrant, []normalizedGrant) {
	email := inv.Users[userName]
	_, allGroups := inv.ResolveUserGroups(userName)

	groupSet := make(map[string]bool)
	for _, g := range allGroups {
		groupSet[g] = true
	}

	var directGrants, inheritedGrants []normalizedGrant
	for _, grant := range inv.Grants {
		if grant.SubjectKind == iamtypes.KindUser && grant.SubjectPrincipal == email {
			directGrants = append(directGrants, grant)
		} else if grant.SubjectKind == iamtypes.KindGroup && groupSet[grant.SubjectPrincipal] {
			inheritedGrants = append(inheritedGrants, grant)
		}
	}
	return directGrants, inheritedGrants
}

// GroupGrants returns grants assigned directly to a group.
func (inv *accessInventory) GroupGrants(groupName string) []normalizedGrant {
	var grants []normalizedGrant
	for _, grant := range inv.Grants {
		if grant.SubjectKind == iamtypes.KindGroup && grant.SubjectPrincipal == groupName {
			grants = append(grants, grant)
		}
	}
	return grants
}

// effectiveSummary computes the max access level per scope and OR-aggregated booleans.
type effectiveSummary struct {
	ClusterLevel   string
	Namespaced     map[string]string // namespace -> max level
	AllowScale     bool
	PortForwarding bool
	// *Implicit flags record that the capability comes from the SuperAdmin wildcard
	// (apiGroups/*/*/*) in user-authz:super-admin ClusterRole, not from an explicit
	// allowScale / portForwarding field on a CAR. This is critical because such a
	// ClusterRole allows pods/portforward and */scale irrespective of CAR flags.
	AllowScaleImplicit     bool
	PortForwardingImplicit bool
}

func computeEffectiveSummary(grants []normalizedGrant) *effectiveSummary {
	summary := &effectiveSummary{
		Namespaced: make(map[string]string),
	}

	var clusterLevels []string
	nsLevels := make(map[string][]string)

	for _, g := range grants {
		if g.AllowScale {
			summary.AllowScale = true
		}
		if g.PortForwarding {
			summary.PortForwarding = true
		}
		switch g.ScopeType {
		case iamtypes.ScopeCluster, iamtypes.ScopeAllNamespaces, iamtypes.ScopeLabels:
			// ScopeLabels still creates a CAR; without resolving the
			// labelSelector against the live namespace list we cannot
			// attribute it to specific namespaces, so treat it as a
			// cluster-level grant for the summary view.
			clusterLevels = append(clusterLevels, g.AccessLevel)
		case iamtypes.ScopeNamespace:
			for _, ns := range g.ScopeNamespaces {
				nsLevels[ns] = append(nsLevels[ns], g.AccessLevel)
			}
		}
	}

	summary.ClusterLevel = maxAccessLevel(clusterLevels)
	for ns, levels := range nsLevels {
		summary.Namespaced[ns] = maxAccessLevel(levels)
	}

	// SuperAdmin binds the user-authz:super-admin ClusterRole, which grants
	// apiGroups/resources/verbs=* and nonResourceURLs=*. That implicitly covers
	// pods/portforward and deployments|statefulsets/scale regardless of the CAR
	// flags, so the effective answer must be true.
	if summary.ClusterLevel == "SuperAdmin" {
		if !summary.PortForwarding {
			summary.PortForwarding = true
			summary.PortForwardingImplicit = true
		}
		if !summary.AllowScale {
			summary.AllowScale = true
			summary.AllowScaleImplicit = true
		}
	}

	return summary
}

// capabilityNote returns a human-readable source annotation used when a capability
// is present implicitly via the SuperAdmin wildcard rather than an explicit CAR flag.
func capabilityNote(implicit bool) string {
	if implicit {
		return " (implicit via SuperAdmin wildcard)"
	}
	return ""
}

func (s *effectiveSummary) String() string {
	var parts []string
	if s.ClusterLevel != "" {
		parts = append(parts, s.ClusterLevel+"[*]")
	}

	// Group namespaces by access level
	levelNS := make(map[string][]string)
	for ns, level := range s.Namespaced {
		levelNS[level] = append(levelNS[level], ns)
	}
	for level, nss := range levelNS {
		sort.Strings(nss)
		parts = append(parts, fmt.Sprintf("%s[%s]", level, strings.Join(nss, ",")))
	}

	if len(parts) == 0 {
		return "<none>"
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

// DetectGroupCycles finds cycles in the group membership graph.
func (inv *accessInventory) DetectGroupCycles() map[string][]string {
	cycles := make(map[string][]string)
	visited := make(map[string]int) // 0=unvisited, 1=visiting, 2=done
	var path []string

	var dfs func(string)
	dfs = func(g string) {
		if visited[g] == 2 {
			return
		}
		if visited[g] == 1 {
			// Found cycle
			cycleStart := -1
			for i, p := range path {
				if p == g {
					cycleStart = i
					break
				}
			}
			if cycleStart >= 0 {
				cycle := append([]string{}, path[cycleStart:]...)
				cycle = append(cycle, g)
				cycles[g] = cycle
			}
			return
		}
		visited[g] = 1
		path = append(path, g)

		for _, m := range inv.GroupMembers[g] {
			if m.Kind == iamtypes.KindGroup {
				dfs(m.Name)
			}
		}

		path = path[:len(path)-1]
		visited[g] = 2
	}

	for g := range inv.GroupMembers {
		if visited[g] == 0 {
			dfs(g)
		}
	}

	return cycles
}
