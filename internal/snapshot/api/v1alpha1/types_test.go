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

	"k8s.io/apimachinery/pkg/runtime"
)

func TestSnapshotUnmarshal(t *testing.T) {
	t.Helper()

	raw := `{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind": "Snapshot",
		"metadata": {"name": "test-snap", "namespace": "default"},
		"status": {
			"boundSnapshotContentName": "sc-abc",
			"childrenSnapshotRefs": [
				{"apiVersion": "storage.deckhouse.io/v1alpha1", "kind": "Snapshot", "name": "child-snap"}
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

	if child.APIVersion != "storage.deckhouse.io/v1alpha1" {
		t.Errorf("child apiVersion: got %q", child.APIVersion)
	}

	if child.Kind != "Snapshot" {
		t.Errorf("child kind: got %q", child.Kind)
	}

	if child.Name != "child-snap" {
		t.Errorf("child name: got %q", child.Name)
	}
}

func TestSnapshotContentUnmarshal(t *testing.T) {
	t.Helper()

	raw := `{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind": "SnapshotContent",
		"metadata": {"name": "sc-abc"},
		"status": {
			"manifestCheckpointName": "mcp-xyz",
			"childrenSnapshotContentRefs": [{"name": "sc-child"}],
			"dataRefs": [{
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
				}
			}]
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

	if len(sc.Status.DataRefs) != 1 {
		t.Fatalf("dataRefs len: got %d, want 1", len(sc.Status.DataRefs))
	}

	dr := sc.Status.DataRefs[0]

	if dr.TargetUID != "uid-1" {
		t.Errorf("targetUID: got %q, want %q", dr.TargetUID, "uid-1")
	}

	if dr.Target.Kind != "PersistentVolumeClaim" {
		t.Errorf("target kind: got %q", dr.Target.Kind)
	}

	if dr.Artifact.Kind != "VolumeSnapshotContent" {
		t.Errorf("artifact kind: got %q", dr.Artifact.Kind)
	}
}

func TestManifestCheckpointUnmarshal(t *testing.T) {
	t.Helper()

	raw := `{
		"apiVersion": "state-snapshotter.deckhouse.io/v1alpha1",
		"kind": "ManifestCheckpoint",
		"metadata": {"name": "mcp-xyz"},
		"spec": {"sourceNamespace": "default"},
		"status": {
			"chunks": [
				{
					"name": "chunk-0",
					"index": 0,
					"objectsCount": 42,
					"sizeBytes": 4096,
					"checksum": "abc123"
				}
			],
			"totalObjects": 42,
			"totalSizeBytes": 4096
		}
	}`

	var mcp ManifestCheckpoint

	if err := json.Unmarshal([]byte(raw), &mcp); err != nil {
		t.Fatalf("unmarshal ManifestCheckpoint: %v", err)
	}

	if mcp.Name != "mcp-xyz" {
		t.Errorf("name: got %q", mcp.Name)
	}

	if len(mcp.Status.Chunks) != 1 {
		t.Fatalf("chunks len: got %d, want 1", len(mcp.Status.Chunks))
	}

	chunk := mcp.Status.Chunks[0]

	if chunk.Name != "chunk-0" {
		t.Errorf("chunk name: got %q", chunk.Name)
	}

	if chunk.Index != 0 {
		t.Errorf("chunk index: got %d", chunk.Index)
	}

	if chunk.ObjectsCount != 42 {
		t.Errorf("objectsCount: got %d", chunk.ObjectsCount)
	}

	if chunk.SizeBytes != 4096 {
		t.Errorf("sizeBytes: got %d", chunk.SizeBytes)
	}

	if chunk.Checksum != "abc123" {
		t.Errorf("checksum: got %q", chunk.Checksum)
	}
}

func TestManifestCheckpointContentChunkUnmarshal(t *testing.T) {
	t.Helper()

	raw := `{
		"apiVersion": "state-snapshotter.deckhouse.io/v1alpha1",
		"kind": "ManifestCheckpointContentChunk",
		"metadata": {"name": "chunk-abc-0"},
		"spec": {
			"checkpointName": "mcp-xyz",
			"index": 0,
			"data": "H4sIAAAAAAAA/w==",
			"objectsCount": 3,
			"checksum": "deadbeef"
		}
	}`

	var chunk ManifestCheckpointContentChunk

	if err := json.Unmarshal([]byte(raw), &chunk); err != nil {
		t.Fatalf("unmarshal ManifestCheckpointContentChunk: %v", err)
	}

	if chunk.Spec.CheckpointName != "mcp-xyz" {
		t.Errorf("checkpointName: got %q", chunk.Spec.CheckpointName)
	}

	if chunk.Spec.Index != 0 {
		t.Errorf("index: got %d", chunk.Spec.Index)
	}

	if chunk.Spec.Data != "H4sIAAAAAAAA/w==" {
		t.Errorf("data: got %q", chunk.Spec.Data)
	}

	if chunk.Spec.ObjectsCount != 3 {
		t.Errorf("objectsCount: got %d", chunk.Spec.ObjectsCount)
	}

	if chunk.Spec.Checksum != "deadbeef" {
		t.Errorf("checksum: got %q", chunk.Spec.Checksum)
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
		{"ManifestCheckpoint", &ManifestCheckpoint{}},
		{"ManifestCheckpointList", &ManifestCheckpointList{}},
		{"ManifestCheckpointContentChunk", &ManifestCheckpointContentChunk{}},
		{"ManifestCheckpointContentChunkList", &ManifestCheckpointContentChunkList{}},
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
