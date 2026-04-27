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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

var groupGVR = schema.GroupVersionResource{
	Group:    "deckhouse.io",
	Version:  "v1alpha1",
	Resource: "groups",
}

// getGroupMembers extracts spec.members from a Group object.
func getGroupMembers(obj *unstructured.Unstructured) ([]map[string]any, error) {
	raw, found, err := unstructured.NestedSlice(obj.Object, "spec", "members")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	result := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		result = append(result, m)
	}
	return result, nil
}

// detectCycle checks if adding a group member (childGroup -> parentGroup) would create a cycle.
// It loads all groups from the cluster and walks the membership graph.
func detectCycle(cmd *cobra.Command, dyn dynamic.Interface, parentGroup, childGroup string) (bool, []string, error) {
	ctx := cmd.Context()
	allGroups, err := dyn.Resource(groupGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, nil, fmt.Errorf("listing groups for cycle detection: %w", err)
	}

	adj := make(map[string][]string)
	for _, g := range allGroups.Items {
		gName := g.GetName()
		members, _ := getGroupMembers(&g)
		for _, m := range members {
			if fmt.Sprint(m["kind"]) == "Group" {
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
