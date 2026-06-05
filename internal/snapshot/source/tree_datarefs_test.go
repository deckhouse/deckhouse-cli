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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

var snapshotContentGVK = schema.GroupVersionKind{
	Group:   snapshotGroup,
	Version: snapshotVersion,
	Kind:    snapshotContentKind,
}

// makeUnstructuredSnapshotContent creates a SnapshotContent object with
// optional status.dataRefs entries.
func makeUnstructuredSnapshotContent(name string, dataRefs []any) *unstructured.Unstructured {
	obj := new(unstructured.Unstructured)
	obj.SetGroupVersionKind(snapshotContentGVK)
	obj.SetName(name)

	if len(dataRefs) > 0 {
		statusMap := map[string]any{
			"dataRefs": dataRefs,
		}
		_ = unstructured.SetNestedField(obj.Object, statusMap, "status")
	}

	return obj
}

// TestFetchDataRefs_WithVSCRef verifies that fetchDataRefs correctly parses
// a dataRefs entry whose artifact.kind == "VolumeSnapshotContent".
func TestFetchDataRefs_WithVSCRef(t *testing.T) {
	dataRefs := []any{
		map[string]any{
			"target": map[string]any{
				"pvcName":      "my-pvc",
				"pvcNamespace": "demo",
			},
			"artifact": map[string]any{
				"kind": "VolumeSnapshotContent",
				"name": "snapcontent-abc",
			},
		},
	}

	sc := makeUnstructuredSnapshotContent("sc-1", dataRefs)

	client := fake.NewClientBuilder().
		WithScheme(fakeScheme()).
		WithObjects(sc).
		Build()

	refs, err := fetchDataRefs(context.Background(), client, "sc-1")
	if err != nil {
		t.Fatalf("fetchDataRefs: %v", err)
	}

	if len(refs) != 1 {
		t.Fatalf("got %d refs, want 1", len(refs))
	}

	if refs[0].VSCName != "snapcontent-abc" {
		t.Errorf("VSCName = %q, want %q", refs[0].VSCName, "snapcontent-abc")
	}

	if refs[0].PVCName != "my-pvc" {
		t.Errorf("PVCName = %q, want %q", refs[0].PVCName, "my-pvc")
	}

	if refs[0].PVCNamespace != "demo" {
		t.Errorf("PVCNamespace = %q, want %q", refs[0].PVCNamespace, "demo")
	}
}

// TestFetchDataRefs_NonVSCKindSkipped verifies that artifact entries with a
// kind other than VolumeSnapshotContent are ignored.
func TestFetchDataRefs_NonVSCKindSkipped(t *testing.T) {
	dataRefs := []any{
		map[string]any{
			"target": map[string]any{
				"pvcName":      "other-pvc",
				"pvcNamespace": "demo",
			},
			"artifact": map[string]any{
				"kind": "PersistentVolumeClaim",
				"name": "some-pvc",
			},
		},
	}

	sc := makeUnstructuredSnapshotContent("sc-2", dataRefs)

	client := fake.NewClientBuilder().
		WithScheme(fakeScheme()).
		WithObjects(sc).
		Build()

	refs, err := fetchDataRefs(context.Background(), client, "sc-2")
	if err != nil {
		t.Fatalf("fetchDataRefs: %v", err)
	}

	if len(refs) != 0 {
		t.Fatalf("expected 0 refs (non-VSC kind skipped), got %d", len(refs))
	}
}

// TestFetchDataRefs_Empty verifies that an absent dataRefs field yields nil.
func TestFetchDataRefs_Empty(t *testing.T) {
	sc := makeUnstructuredSnapshotContent("sc-empty", nil)

	client := fake.NewClientBuilder().
		WithScheme(fakeScheme()).
		WithObjects(sc).
		Build()

	refs, err := fetchDataRefs(context.Background(), client, "sc-empty")
	if err != nil {
		t.Fatalf("fetchDataRefs: %v", err)
	}

	if len(refs) != 0 {
		t.Fatalf("expected 0 refs, got %d", len(refs))
	}
}

// TestFetchDataRefs_NotFound verifies that a missing SnapshotContent returns an error.
func TestFetchDataRefs_NotFound(t *testing.T) {
	client := fake.NewClientBuilder().WithScheme(fakeScheme()).Build()

	_, err := fetchDataRefs(context.Background(), client, "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing SnapshotContent, got nil")
	}
}

// TestBuildTree_WithDataRefs verifies that BuildTree populates Node.DataRefs
// and Node.HasData = true when the bound SnapshotContent has volume dataRefs.
func TestBuildTree_WithDataRefs(t *testing.T) {
	dataRefs := []any{
		map[string]any{
			"target": map[string]any{
				"pvcName":      "vol-pvc",
				"pvcNamespace": "demo",
			},
			"artifact": map[string]any{
				"kind": "VolumeSnapshotContent",
				"name": "vsc-disk",
			},
		},
	}

	snap := makeSnapshot("my-snap", "demo", "sc-root", "True", nil)
	sc := makeUnstructuredSnapshotContent("sc-root", dataRefs)

	client := fake.NewClientBuilder().
		WithScheme(fakeScheme()).
		WithObjects(snap, sc).
		Build()

	root, err := BuildTree(context.Background(), client, "demo", "my-snap")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if !root.HasData {
		t.Fatal("expected HasData = true, got false")
	}

	if len(root.DataRefs) != 1 {
		t.Fatalf("expected 1 DataRef, got %d", len(root.DataRefs))
	}

	if root.DataRefs[0].VSCName != "vsc-disk" {
		t.Errorf("DataRefs[0].VSCName = %q, want %q", root.DataRefs[0].VSCName, "vsc-disk")
	}
}

// TestBuildTree_NoData verifies that a node without volume data has
// HasData = false and an empty DataRefs slice.
func TestBuildTree_NoData(t *testing.T) {
	snap := makeSnapshot("my-snap", "demo", "sc-root", "True", nil)
	sc := makeUnstructuredSnapshotContent("sc-root", nil)

	client := fake.NewClientBuilder().
		WithScheme(fakeScheme()).
		WithObjects(snap, sc).
		Build()

	root, err := BuildTree(context.Background(), client, "demo", "my-snap")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if root.HasData {
		t.Fatal("expected HasData = false for node without volume data")
	}

	if len(root.DataRefs) != 0 {
		t.Fatalf("expected 0 DataRefs, got %d", len(root.DataRefs))
	}
}

// TestToNodeRecord_WithDataRefs verifies that ToNodeRecord correctly converts
// a Node with DataRefs to an archive.NodeRecord.
func TestToNodeRecord_WithDataRefs(t *testing.T) {
	node := &Node{
		ID:        "Snapshot--my-snap",
		Kind:      "Snapshot",
		Name:      "my-snap",
		Namespace: "demo",
		Children:  nil,
		HasData:   true,
		DataRefs: []DataRef{
			{VSCName: "vsc-123", PVCName: "pvc-disk", PVCNamespace: "demo"},
		},
	}

	rec := ToNodeRecord(node)

	if !rec.HasData {
		t.Error("expected HasData = true in NodeRecord")
	}

	if len(rec.DataRefs) != 1 {
		t.Fatalf("expected 1 DataRef in NodeRecord, got %d", len(rec.DataRefs))
	}

	if rec.DataRefs[0].VSCName != "vsc-123" {
		t.Errorf("DataRefs[0].VSCName = %q, want %q", rec.DataRefs[0].VSCName, "vsc-123")
	}

	if rec.DataRefs[0].PVCName != "pvc-disk" {
		t.Errorf("DataRefs[0].PVCName = %q, want %q", rec.DataRefs[0].PVCName, "pvc-disk")
	}
}

// TestToNodeRecord_NoDataRefs verifies that a node without DataRefs produces
// HasData = false in the NodeRecord (no hardcoded-false regression).
func TestToNodeRecord_NoDataRefs(t *testing.T) {
	node := &Node{
		ID:       "Snapshot--plain",
		Kind:     "Snapshot",
		Name:     "plain",
		Children: nil,
		HasData:  false,
		DataRefs: nil,
	}

	rec := ToNodeRecord(node)

	if rec.HasData {
		t.Error("expected HasData = false in NodeRecord for node without DataRefs")
	}

	if len(rec.DataRefs) != 0 {
		t.Fatalf("expected 0 DataRefs in NodeRecord, got %d", len(rec.DataRefs))
	}
}

// TestToNodeRecord_ChildrenPreserved verifies that child IDs round-trip through
// ToNodeRecord when DataRefs are also present.
func TestToNodeRecord_ChildrenPreserved(t *testing.T) {
	child := &Node{ID: archive.NodeID("VirtualDiskSnapshot", "disk")}
	node := &Node{
		ID:       "Snapshot--root",
		Kind:     "Snapshot",
		Name:     "root",
		Children: []*Node{child},
		HasData:  true,
		DataRefs: []DataRef{{VSCName: "vsc-1"}},
	}

	rec := ToNodeRecord(node)

	if len(rec.Children) != 1 || rec.Children[0] != child.ID {
		t.Errorf("Children = %v, want [%q]", rec.Children, child.ID)
	}

	if len(rec.DataRefs) != 1 {
		t.Errorf("DataRefs len = %d, want 1", len(rec.DataRefs))
	}
}
