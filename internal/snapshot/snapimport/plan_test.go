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

package snapimport

import (
	"os"
	"path/filepath"
	"testing"

	sigsyaml "sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

// archiveNode describes one node to materialise in a test archive.
type archiveNode struct {
	apiVersion      string
	kind            string
	name            string
	namespace       string
	manifests       []map[string]interface{}
	blockData       []byte
	sourceObjectRef *archive.SourceObjectRef
	volumes         []archive.VolumeInfo
}

// writeArchiveNode writes snapshot.yaml, manifests/ and optional data.bin into dir.
func writeArchiveNode(t *testing.T, dir string, n archiveNode) {
	t.Helper()

	if err := os.MkdirAll(filepath.Join(dir, archive.ManifestsDirName), 0o755); err != nil {
		t.Fatalf("mkdir node: %v", err)
	}

	if err := archive.WriteSnapshotYAML(dir, archive.SnapshotYAML{
		APIVersion:      n.apiVersion,
		Kind:            n.kind,
		Name:            n.name,
		Namespace:       n.namespace,
		SourceObjectRef: n.sourceObjectRef,
		Volumes:         n.volumes,
	}); err != nil {
		t.Fatalf("write snapshot.yaml: %v", err)
	}

	for i, m := range n.manifests {
		data, err := sigsyaml.Marshal(m)
		if err != nil {
			t.Fatalf("marshal manifest: %v", err)
		}

		fname := filepath.Join(dir, archive.ManifestsDirName, archive.ManifestFileName("obj", string(rune('a'+i)), ""))
		if err := os.WriteFile(fname, data, 0o600); err != nil {
			t.Fatalf("write manifest: %v", err)
		}
	}

	if n.blockData != nil {
		if err := os.WriteFile(filepath.Join(dir, archive.DataBlockBase), n.blockData, 0o600); err != nil {
			t.Fatalf("write data.bin: %v", err)
		}
	}
}

// childDir returns the directory a child node lives in under parent.
func childDir(parent, kind, name string) string {
	return filepath.Join(parent, archive.SnapshotsDirName, archive.NodeDirName(kind, name))
}

// buildTwoLevelArchive writes a root Snapshot with one CSI VolumeSnapshot block leaf and
// returns the root dir.
func buildTwoLevelArchive(t *testing.T) string {
	t.Helper()

	root := t.TempDir()

	writeArchiveNode(t, root, archiveNode{
		apiVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		kind:       "Snapshot",
		name:       "root",
		namespace:  "src",
		manifests:  []map[string]interface{}{{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]interface{}{"name": "cm"}}},
	})

	leaf := childDir(root, "VolumeSnapshot", "pvc-1")
	writeArchiveNode(t, leaf, archiveNode{
		apiVersion: "snapshot.storage.k8s.io/v1",
		kind:       "VolumeSnapshot",
		name:       "pvc-1",
		namespace:  "src",
		manifests:  []map[string]interface{}{{"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": map[string]interface{}{"name": "pvc-1"}}},
		blockData:  []byte("rawbytes"),
	})

	return root
}

func TestBuildPlan_PostOrder(t *testing.T) {
	root := buildTwoLevelArchive(t)

	plan, err := BuildPlan(root)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	if len(plan) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(plan))
	}

	// Post-order: leaf first, root last.
	if plan[0].Kind != "VolumeSnapshot" || plan[0].Name != "pvc-1" {
		t.Errorf("first node = %s/%s, want VolumeSnapshot/pvc-1", plan[0].Kind, plan[0].Name)
	}

	if plan[1].Kind != "Snapshot" || plan[1].Name != "root" {
		t.Errorf("last node = %s/%s, want Snapshot/root", plan[1].Kind, plan[1].Name)
	}

	if !plan[0].HasBlockData() {
		t.Errorf("leaf should have block data")
	}

	if len(plan[1].Children) != 1 {
		t.Fatalf("root should have 1 child, got %d", len(plan[1].Children))
	}

	child := plan[1].Children[0]
	if child.Kind != "VolumeSnapshot" || child.Name != "pvc-1" || child.APIVersion != "snapshot.storage.k8s.io/v1" {
		t.Errorf("unexpected child ref: %+v", child)
	}

	if len(plan[1].Manifests) != 1 || plan[1].Manifests[0].GetKind() != "ConfigMap" {
		t.Errorf("root manifests not read correctly: %+v", plan[1].Manifests)
	}
}

func TestBuildPlan_MissingSnapshotYAML(t *testing.T) {
	dir := t.TempDir()

	if _, err := BuildPlan(dir); err == nil {
		t.Fatal("expected error for archive without snapshot.yaml, got nil")
	}
}

func TestBuildPlan_DomainDataLeaf_SourceObjectRef(t *testing.T) {
	root := t.TempDir()

	writeArchiveNode(t, root, archiveNode{
		apiVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		kind:       "Snapshot",
		name:       "root",
	})

	leafDir := childDir(root, "DemoVirtualDiskSnapshot", "dvd-snap-1")

	wantRef := &archive.SourceObjectRef{
		APIVersion: "demo.deckhouse.io/v1alpha1",
		Kind:       "DemoVirtualDisk",
		Name:       "disk-a",
	}

	writeArchiveNode(t, leafDir, archiveNode{
		apiVersion:      "demo.state-snapshotter.deckhouse.io/v1alpha1",
		kind:            "DemoVirtualDiskSnapshot",
		name:            "dvd-snap-1",
		blockData:       []byte("rawbytes"),
		sourceObjectRef: wantRef,
	})

	plan, err := BuildPlan(root)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	var leaf *PlannedNode

	for i := range plan {
		if plan[i].Kind == "DemoVirtualDiskSnapshot" {
			leaf = &plan[i]

			break
		}
	}

	if leaf == nil {
		t.Fatal("DemoVirtualDiskSnapshot node not found in plan")
	}

	if leaf.SourceObjectRef == nil {
		t.Fatal("SourceObjectRef is nil; expected it to be carried from snapshot.yaml")
	}

	if leaf.SourceObjectRef.APIVersion != wantRef.APIVersion ||
		leaf.SourceObjectRef.Kind != wantRef.Kind ||
		leaf.SourceObjectRef.Name != wantRef.Name {
		t.Errorf("SourceObjectRef = %+v, want %+v", *leaf.SourceObjectRef, *wantRef)
	}

	if !leaf.isDomainDataLeaf() {
		t.Error("DemoVirtualDiskSnapshot with block data should be isDomainDataLeaf()")
	}
}

// TestBuildPlan_LeafStorageParams verifies that the captured scratch-volume parameters written
// into snapshot.yaml Volumes[0] are lifted onto the PlannedNode so EnsureDataImport can send
// them as the PopulateData DataImport's spec.storageParams.
func TestBuildPlan_LeafStorageParams(t *testing.T) {
	root := t.TempDir()
	writeArchiveNode(t, root, archiveNode{
		apiVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		kind:       "Snapshot",
		name:       "root",
	})

	leafDir := childDir(root, "VolumeSnapshot", "pvc-1")
	writeArchiveNode(t, leafDir, archiveNode{
		apiVersion: "snapshot.storage.k8s.io/v1",
		kind:       "VolumeSnapshot",
		name:       "pvc-1",
		blockData:  []byte("rawbytes"),
		volumes: []archive.VolumeInfo{{
			StorageClassName: "sc-fast",
			Size:             "10Gi",
			VolumeMode:       "Block",
		}},
	})

	plan, err := BuildPlan(root)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	var leaf *PlannedNode

	for i := range plan {
		if plan[i].Kind == "VolumeSnapshot" {
			leaf = &plan[i]

			break
		}
	}

	if leaf == nil {
		t.Fatal("VolumeSnapshot node not found in plan")
	}

	if leaf.StorageClassName != "sc-fast" || leaf.Size != "10Gi" || leaf.VolumeMode != "Block" {
		t.Errorf("leaf storage params = {storageClassName:%q, size:%q, volumeMode:%q}, want {sc-fast, 10Gi, Block}",
			leaf.StorageClassName, leaf.Size, leaf.VolumeMode)
	}

	// A structural node owns no volumes and must leave the metadata empty.
	for i := range plan {
		if plan[i].Kind == "Snapshot" {
			if plan[i].StorageClassName != "" || plan[i].Size != "" || plan[i].VolumeMode != "" {
				t.Errorf("structural node carried storage params: %+v", plan[i])
			}
		}
	}
}

func TestBuildPlan_FilesystemDataFlag(t *testing.T) {
	root := t.TempDir()
	writeArchiveNode(t, root, archiveNode{
		apiVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		kind:       "Snapshot",
		name:       "root",
	})

	tarPath := filepath.Join(root, archive.FsTarName)

	if err := os.WriteFile(tarPath, []byte("tar"), 0o600); err != nil {
		t.Fatalf("write data.tar: %v", err)
	}

	plan, err := BuildPlan(root)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	if !plan[0].FilesystemData {
		t.Errorf("expected FilesystemData=true when data.tar present")
	}

	if plan[0].TarFile != tarPath {
		t.Errorf("TarFile = %q, want %q", plan[0].TarFile, tarPath)
	}
}
