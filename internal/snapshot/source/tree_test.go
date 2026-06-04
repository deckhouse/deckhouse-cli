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

package source

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

var snapshotGVK = schema.GroupVersionKind{
	Group:   snapshotGroup,
	Version: snapshotVersion,
	Kind:    snapshotKind,
}

func makeSnapshot(name, ns, boundContent, readyStatus string, children []any) *unstructured.Unstructured {
	obj := new(unstructured.Unstructured)
	obj.SetGroupVersionKind(snapshotGVK)
	obj.SetName(name)
	obj.SetNamespace(ns)

	statusMap := map[string]any{
		"boundSnapshotContentName": boundContent,
	}

	if readyStatus != "" {
		statusMap["conditions"] = []any{
			map[string]any{
				"type":   "Ready",
				"status": readyStatus,
			},
		}
	}

	if len(children) > 0 {
		statusMap["childrenSnapshotRefs"] = children
	}

	_ = unstructured.SetNestedField(obj.Object, statusMap, "status")

	return obj
}

func fakeScheme() *runtime.Scheme {
	return runtime.NewScheme()
}

func TestCheckReady(t *testing.T) {
	tests := []struct {
		name       string
		snapshot   *unstructured.Unstructured
		wantReady  bool
		wantReason string
	}{
		{
			name:      "ready with bound content",
			snapshot:  makeSnapshot("s", "ns", "snap-content-1", "True", nil),
			wantReady: true,
		},
		{
			name:       "no bound content",
			snapshot:   makeSnapshot("s", "ns", "", "True", nil),
			wantReady:  false,
			wantReason: "boundSnapshotContentName is empty",
		},
		{
			name:      "ready false",
			snapshot:  makeSnapshot("s", "ns", "snap-content-1", "False", nil),
			wantReady: false,
		},
		{
			name:      "no conditions",
			snapshot:  makeSnapshot("s", "ns", "snap-content-1", "", nil),
			wantReady: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reason, ok := checkReady(tc.snapshot)

			if ok != tc.wantReady {
				t.Fatalf("checkReady ready = %v, want %v (reason: %q)", ok, tc.wantReady, reason)
			}
		})
	}
}

func TestBuildTree_NotFound(t *testing.T) {
	scheme := fakeScheme()
	client := fake.NewClientBuilder().WithScheme(scheme).Build()

	_, err := BuildTree(context.Background(), client, "demo", "missing-snap")
	if err == nil {
		t.Fatal("expected error for missing snapshot, got nil")
	}

	var nfe *ErrNotFound
	if !asError(err, &nfe) {
		t.Fatalf("expected ErrNotFound, got %T: %v", err, err)
	}
}

func TestBuildTree_NotReady(t *testing.T) {
	snap := makeSnapshot("my-snap", "demo", "", "False", nil)

	scheme := fakeScheme()
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(snap).
		Build()

	_, err := BuildTree(context.Background(), client, "demo", "my-snap")
	if err == nil {
		t.Fatal("expected error for not-ready snapshot, got nil")
	}

	var nre *ErrNotReady
	if !asError(err, &nre) {
		t.Fatalf("expected ErrNotReady, got %T: %v", err, err)
	}
}

func TestBuildTree_Ready_NoChildren(t *testing.T) {
	snap := makeSnapshot("my-snap", "demo", "snap-content-1", "True", nil)

	scheme := fakeScheme()
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(snap).
		Build()

	root, err := BuildTree(context.Background(), client, "demo", "my-snap")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if root.ID != "Snapshot--my-snap" {
		t.Fatalf("root.ID = %q, want %q", root.ID, "Snapshot--my-snap")
	}

	if root.BoundSnapshotContentName != "snap-content-1" {
		t.Fatalf("root.BoundSnapshotContentName = %q, want %q", root.BoundSnapshotContentName, "snap-content-1")
	}

	if len(root.Children) != 0 {
		t.Fatalf("expected 0 children, got %d", len(root.Children))
	}
}

func TestSelectSubtree_Full(t *testing.T) {
	root := &Node{ID: "root", Children: []*Node{{ID: "child"}}}

	got, err := SelectSubtree(root, TreeOptions{})
	if err != nil {
		t.Fatalf("SelectSubtree: %v", err)
	}

	if got != root {
		t.Fatal("expected root to be returned for empty NodeFilter")
	}
}

func TestSelectSubtree_Found(t *testing.T) {
	child := &Node{ID: "Snapshot--child"}
	root := &Node{ID: "Snapshot--root", Children: []*Node{child}}

	got, err := SelectSubtree(root, TreeOptions{NodeFilter: "Snapshot--child"})
	if err != nil {
		t.Fatalf("SelectSubtree: %v", err)
	}

	if got != child {
		t.Fatalf("expected child node, got %v", got)
	}
}

func TestSelectSubtree_NotFound(t *testing.T) {
	root := &Node{ID: "Snapshot--root", Children: []*Node{{ID: "Snapshot--child"}}}

	_, err := SelectSubtree(root, TreeOptions{NodeFilter: "Snapshot--missing"})
	if err == nil {
		t.Fatal("expected error for missing node, got nil")
	}
}

func TestFlatNodes(t *testing.T) {
	grandchild := &Node{ID: "gc"}
	child := &Node{ID: "c", Children: []*Node{grandchild}}
	root := &Node{ID: "r", Children: []*Node{child}}

	flat := FlatNodes(root)

	if len(flat) != 3 {
		t.Fatalf("FlatNodes: got %d nodes, want 3", len(flat))
	}

	ids := make([]string, len(flat))
	for i, n := range flat {
		ids[i] = n.ID
	}

	want := []string{"r", "c", "gc"}

	for i, id := range ids {
		if id != want[i] {
			t.Fatalf("FlatNodes[%d] = %q, want %q", i, id, want[i])
		}
	}
}

func TestNodeID(t *testing.T) {
	id := archive.NodeID("Snapshot", "my-snap")

	if id != "Snapshot--my-snap" {
		t.Fatalf("archive.NodeID = %q, want %q", id, "Snapshot--my-snap")
	}
}

// asError is a minimal errors.As-like helper that avoids importing "errors".
func asError[T error](err error, target *T) bool {
	if err == nil {
		return false
	}

	if t, ok := err.(T); ok {
		*target = t

		return true
	}

	return false
}
