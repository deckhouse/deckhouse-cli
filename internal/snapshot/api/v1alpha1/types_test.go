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

package v1alpha1

import (
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestSnapshotUnmarshal(t *testing.T) {
	t.Helper()

	raw := `{
		"apiVersion": "state-snapshotter.deckhouse.io/v1alpha1",
		"kind": "Snapshot",
		"metadata": {"name": "test-snap", "namespace": "default"},
		"status": {
			"boundSnapshotContentName": "sc-abc",
			"childrenSnapshotRefs": [
				{"apiVersion": "state-snapshotter.deckhouse.io/v1alpha1", "kind": "Snapshot", "name": "child-snap"}
			]
		}
	}`

	var s Snapshot

	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatalf("unmarshal Snapshot: %v", err)
	}

	if s.Name != "test-snap" {
		t.Errorf("name: got %q, want %q", s.Name, "test-snap")
	}

	if s.Status.BoundSnapshotContentName != "sc-abc" {
		t.Errorf("boundSnapshotContentName: got %q, want %q", s.Status.BoundSnapshotContentName, "sc-abc")
	}

	if len(s.Status.ChildrenSnapshotRefs) != 1 {
		t.Fatalf("childrenSnapshotRefs len: got %d, want 1", len(s.Status.ChildrenSnapshotRefs))
	}

	child := s.Status.ChildrenSnapshotRefs[0]

	if child.APIVersion != "state-snapshotter.deckhouse.io/v1alpha1" {
		t.Errorf("child apiVersion: got %q", child.APIVersion)
	}

	if child.Kind != "Snapshot" {
		t.Errorf("child kind: got %q", child.Kind)
	}

	if child.Name != "child-snap" {
		t.Errorf("child name: got %q", child.Name)
	}
}

// TestSnapshotContentUnmarshal verifies that the REAL producer JSON key "dataRef"
// (singular object, Variant A) unmarshals correctly including all extended fields.
// This test proves the JSON key contract matches state-snapshotter:
// snapshotcontent_types.go:149-153 (DataRef *SnapshotDataBinding, json:"dataRef").
func TestSnapshotContentUnmarshal(t *testing.T) {
	t.Helper()

	raw := `{
		"apiVersion": "state-snapshotter.deckhouse.io/v1alpha1",
		"kind": "SnapshotContent",
		"metadata": {"name": "sc-abc"},
		"status": {
			"manifestCheckpointName": "mcp-xyz",
			"childrenSnapshotContentRefs": [{"name": "sc-child"}],
			"dataRef": {
				"targetUID": "uid-1",
				"target": {
					"apiVersion": "v1",
					"kind": "PersistentVolumeClaim",
					"name": "pvc-1",
					"namespace": "default",
					"uid": "uid-1"
				},
				"artifact": {
					"apiVersion": "snapshot.storage.k8s.io/v1",
					"kind": "VolumeSnapshotContent",
					"name": "vsc-1"
				},
				"volumeMode": "Block",
				"storageClassName": "csi-ceph-rbd",
				"accessModes": ["ReadWriteOnce"],
				"size": "10Gi"
			}
		}
	}`

	var sc SnapshotContent

	if err := json.Unmarshal([]byte(raw), &sc); err != nil {
		t.Fatalf("unmarshal SnapshotContent: %v", err)
	}

	if sc.Status.ManifestCheckpointName != "mcp-xyz" {
		t.Errorf("manifestCheckpointName: got %q, want %q", sc.Status.ManifestCheckpointName, "mcp-xyz")
	}

	if len(sc.Status.ChildrenSnapshotContentRefs) != 1 {
		t.Fatalf("childrenSnapshotContentRefs len: got %d, want 1", len(sc.Status.ChildrenSnapshotContentRefs))
	}

	if sc.Status.ChildrenSnapshotContentRefs[0].Name != "sc-child" {
		t.Errorf("child name: got %q", sc.Status.ChildrenSnapshotContentRefs[0].Name)
	}

	if sc.Status.DataRef == nil {
		t.Fatal("dataRef must not be nil")
	}

	dr := sc.Status.DataRef

	if dr.TargetUID != "uid-1" {
		t.Errorf("targetUID: got %q, want uid-1", dr.TargetUID)
	}

	if dr.Target.Kind != "PersistentVolumeClaim" {
		t.Errorf("target kind: got %q", dr.Target.Kind)
	}

	if dr.Artifact.Kind != "VolumeSnapshotContent" {
		t.Errorf("artifact kind: got %q", dr.Artifact.Kind)
	}

	if dr.VolumeMode != "Block" {
		t.Errorf("volumeMode: got %q, want Block", dr.VolumeMode)
	}

	if dr.StorageClassName != "csi-ceph-rbd" {
		t.Errorf("storageClassName: got %q, want csi-ceph-rbd", dr.StorageClassName)
	}

	if len(dr.AccessModes) != 1 || dr.AccessModes[0] != "ReadWriteOnce" {
		t.Errorf("accessModes: got %v, want [ReadWriteOnce]", dr.AccessModes)
	}

	if dr.Size != "10Gi" {
		t.Errorf("size: got %q, want 10Gi", dr.Size)
	}

	// DataRefList bridge: returns a 0/1 slice.
	list := sc.DataRefList()
	if len(list) != 1 {
		t.Fatalf("DataRefList len: got %d, want 1", len(list))
	}

	if list[0].TargetUID != "uid-1" {
		t.Errorf("DataRefList[0].TargetUID: got %q", list[0].TargetUID)
	}

	// Absent dataRef → nil DataRefList.
	var empty SnapshotContent
	if empty.DataRefList() != nil {
		t.Errorf("DataRefList on nil DataRef: got %v, want nil", empty.DataRefList())
	}
}

func TestSnapshot_DeepCopyObject_NoAliasing(t *testing.T) {
	t.Parallel()

	orig := &Snapshot{
		Status: SnapshotStatus{
			ChildrenSnapshotRefs: []SnapshotChildRef{
				{APIVersion: "v1", Kind: "Snapshot", Name: "child-a"},
			},
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: "True", Reason: "All"},
			},
		},
	}

	cp := orig.DeepCopyObject().(*Snapshot)

	cp.Status.ChildrenSnapshotRefs[0].Name = "mutated"
	cp.Status.Conditions[0].Type = "Mutated"

	if orig.Status.ChildrenSnapshotRefs[0].Name != "child-a" {
		t.Errorf("ChildrenSnapshotRefs aliased: orig was mutated")
	}

	if orig.Status.Conditions[0].Type != "Ready" {
		t.Errorf("Conditions aliased: orig was mutated")
	}
}

func TestSnapshotContent_DeepCopyObject_NoAliasing(t *testing.T) {
	t.Parallel()

	orig := &SnapshotContent{
		Status: SnapshotContentStatus{
			ChildrenSnapshotContentRefs: []SnapshotContentChildRef{{Name: "child-a"}},
			DataRef: &SnapshotDataBinding{
				TargetUID:   "uid-1",
				AccessModes: []string{"ReadWriteOnce"},
			},
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: "True", Reason: "All"},
			},
		},
	}

	cp := orig.DeepCopyObject().(*SnapshotContent)

	cp.Status.ChildrenSnapshotContentRefs[0].Name = "mutated"
	cp.Status.DataRef.TargetUID = "mutated"
	cp.Status.DataRef.AccessModes[0] = "mutated"
	cp.Status.Conditions[0].Type = "Mutated"

	if orig.Status.ChildrenSnapshotContentRefs[0].Name != "child-a" {
		t.Errorf("ChildrenSnapshotContentRefs aliased: orig was mutated")
	}

	if orig.Status.DataRef.TargetUID != "uid-1" {
		t.Errorf("DataRef.TargetUID aliased: orig was mutated")
	}

	if orig.Status.DataRef.AccessModes[0] != "ReadWriteOnce" {
		t.Errorf("DataRef.AccessModes aliased: orig was mutated")
	}

	if orig.Status.Conditions[0].Type != "Ready" {
		t.Errorf("Conditions aliased: orig was mutated")
	}
}

func TestSnapshotList_DeepCopyObject_NoAliasing(t *testing.T) {
	t.Parallel()

	orig := &SnapshotList{
		Items: []Snapshot{
			{
				Status: SnapshotStatus{
					ChildrenSnapshotRefs: []SnapshotChildRef{{Name: "child-a"}},
				},
			},
		},
	}

	cp := orig.DeepCopyObject().(*SnapshotList)
	cp.Items[0].Status.ChildrenSnapshotRefs[0].Name = "mutated"

	if orig.Items[0].Status.ChildrenSnapshotRefs[0].Name != "child-a" {
		t.Errorf("SnapshotList item slice aliased: orig was mutated")
	}
}

func TestAddToScheme(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()

	if err := AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	cases := []struct {
		name string
		obj  runtime.Object
	}{
		{"Snapshot", &Snapshot{}},
		{"SnapshotList", &SnapshotList{}},
		{"SnapshotContent", &SnapshotContent{}},
		{"SnapshotContentList", &SnapshotContentList{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gvks, _, err := scheme.ObjectKinds(tc.obj)
			if err != nil {
				t.Fatalf("ObjectKinds(%s): %v", tc.name, err)
			}

			if len(gvks) == 0 {
				t.Errorf("ObjectKinds(%s): got empty list", tc.name)
			}
		})
	}
}
