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

package upload

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"testing"

	unstructuredpkg "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/restore"
)

// ── sanitize ──────────────────────────────────────────────────────────────────

func TestSanitize(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"my-snap", "my-snap"},
		{"My_Snap.v2", "my-snap-v2"},
		{"Hello World", "hello-world"},
		{"test123", "test123"},
		{"A_B.C-D", "a-b-c-d"},
	}
	for _, tc := range cases {
		got := sanitize(tc.input)
		if got != tc.want {
			t.Errorf("sanitize(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// ── importRequestName ─────────────────────────────────────────────────────────

func TestImportRequestName(t *testing.T) {
	cases := []struct {
		input      string
		wantPrefix string
		maxLen     int
	}{
		{"my-snap", "sir-my-snap", 63},
		{"x", "sir-x", 63},
	}
	for _, tc := range cases {
		got := importRequestName(tc.input)
		if len(got) > tc.maxLen {
			t.Errorf("importRequestName(%q) len=%d > %d", tc.input, len(got), tc.maxLen)
		}
		// Must start with "sir-"
		if len(got) < 4 || got[:4] != "sir-" {
			t.Errorf("importRequestName(%q) = %q, want prefix 'sir-'", tc.input, got)
		}
	}
	// Very long name must be truncated to 63.
	long := "a-very-long-snapshot-name-that-exceeds-the-kubernetes-label-limit-of-63-chars"
	got := importRequestName(long)
	if len(got) > 63 {
		t.Errorf("importRequestName(long) len=%d > 63", len(got))
	}
}

// ── parseStorageClassMapping ──────────────────────────────────────────────────

func TestParseStorageClassMapping(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		m, err := parseStorageClassMapping([]string{"fast=ultra-fast", "slow=standard"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if m["fast"] != "ultra-fast" {
			t.Errorf("fast mapping got %q", m["fast"])
		}
		if m["slow"] != "standard" {
			t.Errorf("slow mapping got %q", m["slow"])
		}
	})

	t.Run("empty", func(t *testing.T) {
		m, err := parseStorageClassMapping(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(m) != 0 {
			t.Errorf("expected empty map, got %v", m)
		}
	})

	t.Run("invalid - no equals", func(t *testing.T) {
		_, err := parseStorageClassMapping([]string{"fast"})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("invalid - empty from", func(t *testing.T) {
		_, err := parseStorageClassMapping([]string{"=fast"})
		if err == nil {
			t.Fatal("expected error for empty from-class")
		}
	})

	t.Run("invalid - empty to", func(t *testing.T) {
		_, err := parseStorageClassMapping([]string{"fast="})
		if err == nil {
			t.Fatal("expected error for empty to-class")
		}
	})
}

// ── splitBlobsIntoChunks ──────────────────────────────────────────────────────

func TestSplitBlobsIntoChunks(t *testing.T) {
	makeBlobs := func(n int) []json.RawMessage {
		blobs := make([]json.RawMessage, n)
		for i := range blobs {
			blobs[i] = json.RawMessage(`{}`)
		}
		return blobs
	}

	t.Run("exact multiple", func(t *testing.T) {
		chunks := splitBlobsIntoChunks(makeBlobs(6), 2)
		if len(chunks) != 3 {
			t.Fatalf("expected 3 chunks, got %d", len(chunks))
		}
		for _, c := range chunks {
			if len(c) != 2 {
				t.Errorf("chunk size %d, want 2", len(c))
			}
		}
	})

	t.Run("remainder", func(t *testing.T) {
		chunks := splitBlobsIntoChunks(makeBlobs(5), 2)
		if len(chunks) != 3 {
			t.Fatalf("expected 3 chunks (2+2+1), got %d", len(chunks))
		}
		if len(chunks[2]) != 1 {
			t.Errorf("last chunk size %d, want 1", len(chunks[2]))
		}
	})

	t.Run("single chunk", func(t *testing.T) {
		chunks := splitBlobsIntoChunks(makeBlobs(3), 10)
		if len(chunks) != 1 {
			t.Fatalf("expected 1 chunk, got %d", len(chunks))
		}
	})

	t.Run("empty", func(t *testing.T) {
		chunks := splitBlobsIntoChunks(nil, 10)
		if len(chunks) != 0 {
			t.Fatalf("expected 0 chunks, got %d", len(chunks))
		}
	})
}

// ── gzipJSONArray ─────────────────────────────────────────────────────────────

func TestGzipJSONArray(t *testing.T) {
	blobs := []json.RawMessage{
		json.RawMessage(`{"a":1}`),
		json.RawMessage(`{"b":2}`),
	}

	encoded, err := gzipJSONArray(blobs)
	if err != nil {
		t.Fatalf("gzipJSONArray: %v", err)
	}

	// Must be valid base64.
	compressed, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("base64 decode: %v", err)
	}

	// Must be valid gzip.
	gr, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(gr); err != nil {
		t.Fatalf("gzip read: %v", err)
	}
	_ = gr.Close()

	// Decompressed must be valid JSON array of 2 elements.
	var arr []json.RawMessage
	if err := json.Unmarshal(buf.Bytes(), &arr); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	if len(arr) != 2 {
		t.Errorf("expected 2 elements, got %d", len(arr))
	}
}

// ── stagingPVCName ────────────────────────────────────────────────────────────

func TestStagingPVCName(t *testing.T) {
	t.Run("from PVCSpec.Name", func(t *testing.T) {
		op := restore.VolumeOp{
			PVCSpec: &restore.PVCSpec{Name: "my-pvc"},
			PVCName: "fallback-pvc",
			VSCName: "vsc-name",
		}
		if got := stagingPVCName(op); got != "my-pvc" {
			t.Errorf("got %q, want %q", got, "my-pvc")
		}
	})

	t.Run("from PVCName when spec name empty", func(t *testing.T) {
		op := restore.VolumeOp{
			PVCSpec: &restore.PVCSpec{Name: ""},
			PVCName: "fallback-pvc",
			VSCName: "vsc-name",
		}
		if got := stagingPVCName(op); got != "fallback-pvc" {
			t.Errorf("got %q, want %q", got, "fallback-pvc")
		}
	})

	t.Run("from VSCName as last resort", func(t *testing.T) {
		op := restore.VolumeOp{
			PVCSpec: nil,
			PVCName: "",
			VSCName: "my-vsc",
		}
		if got := stagingPVCName(op); got != "restore-my-vsc" {
			t.Errorf("got %q, want %q", got, "restore-my-vsc")
		}
	})
}

// ── buildManifestChunkObject ──────────────────────────────────────────────────

func TestBuildManifestChunkObject(t *testing.T) {
	obj := buildManifestChunkObject("chunk-0", "test-ns", "sir-snap", "node-1", 0, 3, "base64data", 5)
	if obj.GetName() != "chunk-0" {
		t.Errorf("name = %q, want chunk-0", obj.GetName())
	}
	if obj.GetNamespace() != "test-ns" {
		t.Errorf("namespace = %q, want test-ns", obj.GetNamespace())
	}
	spec, ok, err := unstructuredpkg.NestedMap(obj.Object, "spec")
	if err != nil || !ok {
		t.Fatal("spec not found")
	}
	if spec["importRequestName"] != "sir-snap" {
		t.Errorf("importRequestName = %v", spec["importRequestName"])
	}
	if spec["nodeId"] != "node-1" {
		t.Errorf("nodeId = %v", spec["nodeId"])
	}
	if spec["total"] != int64(3) {
		t.Errorf("total = %v, want 3", spec["total"])
	}
	if spec["objectsCount"] != int64(5) {
		t.Errorf("objectsCount = %v, want 5", spec["objectsCount"])
	}
}

// ── buildImportNodes ──────────────────────────────────────────────────────────

func TestBuildImportNodes(t *testing.T) {
	nodes := []archive.NodeRecord{
		{
			ID:         "root",
			APIVersion: "demo.deckhouse.io/v1alpha1",
			Kind:       "DemoVirtualMachineSnapshot",
			Name:       "my-vm-snap",
			ParentID:   "",
			Children:   []string{"child1"},
			HasData:    false,
		},
		{
			ID:         "child1",
			APIVersion: "demo.deckhouse.io/v1alpha1",
			Kind:       "DemoVirtualDiskSnapshot",
			Name:       "my-disk-snap",
			ParentID:   "root",
			Children:   nil,
			HasData:    true,
		},
	}

	result := buildImportNodes(nodes, "")
	if len(result) != 2 {
		t.Fatalf("expected 2 import nodes, got %d", len(result))
	}

	root := result[0]
	if root["id"] != "root" {
		t.Errorf("root id = %v", root["id"])
	}
	if root["hasData"] != false {
		t.Errorf("root hasData = %v", root["hasData"])
	}
	children, ok := root["children"].([]interface{})
	if !ok {
		t.Fatal("children not []interface{}")
	}
	if len(children) != 1 || children[0] != "child1" {
		t.Errorf("children = %v", children)
	}

	child := result[1]
	if child["parentId"] != "root" {
		t.Errorf("child parentId = %v", child["parentId"])
	}
	if child["hasData"] != true {
		t.Errorf("child hasData = %v", child["hasData"])
	}
}

func TestBuildImportNodes_RootNameOverride(t *testing.T) {
	nodes := []archive.NodeRecord{
		{
			ID:         "Snapshot--old-name",
			APIVersion: "storage.deckhouse.io/v1alpha1",
			Kind:       "Snapshot",
			Name:       "old-name",
			ParentID:   "",
			Children:   []string{"child1"},
		},
		{
			ID:         "child1",
			APIVersion: "demo.deckhouse.io/v1alpha1",
			Kind:       "DemoVirtualDiskSnapshot",
			Name:       "disk-snap",
			ParentID:   "Snapshot--old-name",
		},
	}

	result := buildImportNodes(nodes, "new-name")
	if len(result) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(result))
	}
	if result[0]["name"] != "new-name" {
		t.Errorf("root name = %v, want new-name", result[0]["name"])
	}
	if result[0]["id"] != "Snapshot--old-name" {
		t.Errorf("root id should be unchanged: %v", result[0]["id"])
	}
	// Child name must not be changed.
	if result[1]["name"] != "disk-snap" {
		t.Errorf("child name = %v, want disk-snap", result[1]["name"])
	}
}

// ── buildImportVolumes ────────────────────────────────────────────────────────

func TestBuildImportVolumes(t *testing.T) {
	vols := []restore.VolumeOp{
		{NodeID: "node1", PVCName: "my-pvc", VolumeMode: "Block", VSCName: "vsc1"},
		{NodeID: "node1", PVCName: "my-pvc2", VolumeMode: "Filesystem", VSCName: "vsc2"},
	}
	// Keyed by VSCName so two volumes on the same node don't collide.
	staging := map[string]string{
		"vsc1": "staging-vsc1",
		"vsc2": "staging-vsc2",
	}

	result := buildImportVolumes(vols, staging)
	if len(result) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(result))
	}

	// JSON key must match API tag `json:"stagingPvcName"`.
	if result[0]["stagingPvcName"] != "staging-vsc1" {
		t.Errorf("vsc1 stagingPvcName = %v", result[0]["stagingPvcName"])
	}
	if result[1]["stagingPvcName"] != "staging-vsc2" {
		t.Errorf("vsc2 stagingPvcName = %v", result[1]["stagingPvcName"])
	}
}

func TestBuildImportVolumesFallback(t *testing.T) {
	vols := []restore.VolumeOp{
		{NodeID: "node1", PVCName: "my-pvc", VolumeMode: "Block", VSCName: "vsc1"},
	}
	// staging map is empty; should fall back to stagingPVCName(vol) = "my-pvc"
	staging := map[string]string{}

	result := buildImportVolumes(vols, staging)
	if result[0]["stagingPvcName"] != "my-pvc" {
		t.Errorf("fallback stagingPvcName = %v, want my-pvc", result[0]["stagingPvcName"])
	}
}

// ── buildImportRequest ────────────────────────────────────────────────────────

func TestBuildImportRequest(t *testing.T) {
	nodes := []map[string]interface{}{
		{"id": "root", "kind": "DemoVirtualMachineSnapshot"},
	}
	volumes := []map[string]interface{}{
		{"nodeId": "root", "stagingPVCName": "staging-pvc-1"},
	}
	scMapping := map[string]string{"fast": "ultra-fast"}

	obj := buildImportRequest("sir-my-snap", "my-ns", "my-snap", nodes, volumes, scMapping, "720h")

	if obj.GetName() != "sir-my-snap" {
		t.Errorf("name = %q", obj.GetName())
	}
	if obj.GetNamespace() != "my-ns" {
		t.Errorf("namespace = %q", obj.GetNamespace())
	}

	rootSnap, ok, _ := unstructuredpkg.NestedString(obj.Object, "spec", "rootSnapshotName")
	if !ok || rootSnap != "my-snap" {
		t.Errorf("rootSnapshotName = %q", rootSnap)
	}

	ttl, ok, _ := unstructuredpkg.NestedString(obj.Object, "spec", "ttl")
	if !ok || ttl != "720h" {
		t.Errorf("ttl = %q", ttl)
	}

	scMap, ok, _ := unstructuredpkg.NestedStringMap(obj.Object, "spec", "storageClassMapping")
	if !ok {
		t.Fatal("storageClassMapping missing")
	}
	if scMap["fast"] != "ultra-fast" {
		t.Errorf("sc mapping fast = %q", scMap["fast"])
	}

	nodesSlice, ok, _ := unstructuredpkg.NestedSlice(obj.Object, "spec", "nodes")
	if !ok || len(nodesSlice) != 1 {
		t.Errorf("nodes len = %d", len(nodesSlice))
	}

	volsSlice, ok, _ := unstructuredpkg.NestedSlice(obj.Object, "spec", "volumes")
	if !ok || len(volsSlice) != 1 {
		t.Errorf("volumes len = %d", len(volsSlice))
	}
}
