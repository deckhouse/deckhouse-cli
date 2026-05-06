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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
)

// EnsureMemberOpts controls EnsureMember behavior. Zero value is valid:
// CreateGroupIfMissing=false, CycleCheck=false (no-op for User members).
type EnsureMemberOpts struct {
	// CreateGroupIfMissing creates the parent Group CR if it does not exist.
	CreateGroupIfMissing bool
	// CycleCheck loads the membership graph and refuses to add the member
	// if it would introduce a cycle. Only meaningful when MemberKind==Group;
	// User members can never form a cycle.
	CycleCheck bool
}

// EnsureMember idempotently adds a (memberKind, memberName) pair to the
// spec.members of groupName. It is the single implementation shared by
// `d8 iam group add-member` and `d8 iam user create --member-of`.
//
// Returns an EnsureMemberResult describing what changed; the caller is
// responsible for printing user-facing messages.
func EnsureMember(ctx context.Context, dyn dynamic.Interface,
	groupName string, memberKind iamtypes.SubjectKind, memberName string, opts EnsureMemberOpts) (EnsureMemberResult, error) {
	groupClient := dyn.Resource(iamtypes.GroupGVR)
	memberKindStr := string(memberKind)

	// Probe to handle the create-if-missing path. The actual Get→mutate→Update
	// cycle below runs inside retry.RetryOnConflict so a parallel writer can't
	// make our Update lose to its commit.
	if _, err := groupClient.Get(ctx, groupName, metav1.GetOptions{}); err != nil {
		switch {
		case apierrors.IsNotFound(err):
			if !opts.CreateGroupIfMissing {
				return EnsureMemberResult{}, fmt.Errorf("group %q not found: %w", groupName, err)
			}
			newObj := buildGroupObject(groupName)
			setSpecMembers(newObj, []any{
				map[string]any{"kind": memberKindStr, "name": memberName},
			})
			if _, err := groupClient.Create(ctx, newObj, metav1.CreateOptions{}); err != nil {
				return EnsureMemberResult{}, fmt.Errorf("creating group %q: %w", groupName, err)
			}
			return EnsureMemberResult{GroupCreated: true, Added: true}, nil
		default:
			// Any other Get failure (Forbidden, Timeout, transient API error)
			// must not silently route into Create: that would either overwrite
			// an existing group whose Get we couldn't read, or create one for
			// a user who lacks permission to even see it.
			return EnsureMemberResult{}, fmt.Errorf("getting group %q: %w", groupName, err)
		}
	}

	var result EnsureMemberResult
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		obj, err := groupClient.Get(ctx, groupName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("getting group %q: %w", groupName, err)
		}

		if opts.CycleCheck && memberKind == iamtypes.KindGroup {
			// Re-check on every retry: a conflict can be exactly because some
			// other writer changed the membership graph in a way that newly
			// introduces a cycle.
			hasCycle, cyclePath, err := detectCycleCtx(ctx, dyn, groupName, memberName)
			if err != nil {
				return fmt.Errorf("cycle detection failed: %w", err)
			}
			if hasCycle {
				return fmt.Errorf("adding group %q to %q would create a cycle: %s",
					memberName, groupName, strings.Join(cyclePath, " -> "))
			}
		}

		members, _, _ := unstructured.NestedSlice(obj.Object, "spec", "members")
		for _, m := range members {
			member, ok := m.(map[string]any)
			if !ok {
				continue
			}
			if fmt.Sprint(member["kind"]) == memberKindStr && fmt.Sprint(member["name"]) == memberName {
				result = EnsureMemberResult{AlreadyMember: true}
				return nil
			}
		}

		members = append(members, map[string]any{"kind": memberKindStr, "name": memberName})
		if err := unstructured.SetNestedSlice(obj.Object, members, "spec", "members"); err != nil {
			return fmt.Errorf("setting members on group %q: %w", groupName, err)
		}
		// Return the raw Update error: retry.RetryOnConflict inspects it via
		// apierrors.IsConflict (which traverses %w chains), and a conflict here
		// is exactly the case we want to retry on a fresh Get.
		if _, err := groupClient.Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
			return err
		}
		result = EnsureMemberResult{Added: true}
		return nil
	})
	if err != nil {
		return EnsureMemberResult{}, err
	}
	return result, nil
}

// EnsureMemberResult tells the caller what happened so it can print the right
// message ("created", "already member", etc.) without re-reading state.
type EnsureMemberResult struct {
	GroupCreated  bool
	Added         bool
	AlreadyMember bool
}

// detectCycleCtx walks the existing group membership graph to check whether
// adding (parentGroup -> childGroup) would create a cycle. It only follows
// Group->Group edges; User members can never form a cycle.
func detectCycleCtx(ctx context.Context, dyn dynamic.Interface, parentGroup, childGroup string) (bool, []string, error) {
	allGroups, err := dyn.Resource(iamtypes.GroupGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, nil, fmt.Errorf("listing groups for cycle detection: %w", err)
	}

	groupKindStr := string(iamtypes.KindGroup)
	adj := make(map[string][]string)
	for i := range allGroups.Items {
		g := &allGroups.Items[i]
		gName := g.GetName()
		members, _ := getGroupMembers(g)
		for _, m := range members {
			if fmt.Sprint(m["kind"]) == groupKindStr {
				adj[gName] = append(adj[gName], fmt.Sprint(m["name"]))
			}
		}
	}
	adj[parentGroup] = append(adj[parentGroup], childGroup)

	visited := make(map[string]bool)
	var path []string
	var dfs func(string) bool
	dfs = func(node string) bool {
		if node == parentGroup {
			path = append(path, node)
			return true
		}
		if visited[node] {
			return false
		}
		visited[node] = true
		path = append(path, node)
		for _, next := range adj[node] {
			if dfs(next) {
				return true
			}
		}
		path = path[:len(path)-1]
		return false
	}
	if dfs(childGroup) {
		return true, path, nil
	}
	return false, nil, nil
}

func setSpecMembers(obj *unstructured.Unstructured, members []any) {
	_ = unstructured.SetNestedSlice(obj.Object, members, "spec", "members")
}
