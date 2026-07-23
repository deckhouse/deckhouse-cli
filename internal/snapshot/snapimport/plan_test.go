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
	"errors"
	"os"
	"path/filepath"
	"testing"

	sigsyaml "sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

// archiveNode describes one node to materialise in a test archive.
type archiveNode struct {
	apiVersion string
	kind       string
	name       string
	namespace  string
	manifests  []map[string]interface{}
	blockData  []byte
	// blockExt is the codec extension of the block payload filename: "" writes data.bin,
	// ".zst" writes data.bin.zst, etc. Ignored when blockData is nil.
	blockExt string
	// tarData, when non-nil, is written as data.tar (a filesystem-volume payload).
	tarData         []byte
	sourceObjectRef *archive.SourceObjectRef
	volumes         []archive.VolumeInfo
}

// writeArchiveNode writes a VALID node directory: manifests/, an optional block or filesystem
// payload, and a snapshot.yaml whose checksum is computed over the node's files (so
// archive.VerifyNode passes) and whose Volumes satisfy the import integrity preflight
// (archive.ValidateNodeMetadata). A data node with no explicit volumes gets a synthesized,
// well-formed VolumeInfo whose volumeMode agrees with the payload kind. snapshot.yaml is
// written LAST, after payloads exist, because the checksum is computed over them.
func writeArchiveNode(t *testing.T, dir string, n archiveNode) {
	t.Helper()

	if err := os.MkdirAll(filepath.Join(dir, archive.ManifestsDirName), 0o755); err != nil {
		t.Fatalf("mkdir node: %v", err)
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
		if err := os.WriteFile(filepath.Join(dir, archive.DataBlockName(n.blockExt)), n.blockData, 0o600); err != nil {
			t.Fatalf("write block data: %v", err)
		}
	}

	if n.tarData != nil {
		if err := os.WriteFile(filepath.Join(dir, archive.FsTarName), n.tarData, 0o600); err != nil {
			t.Fatalf("write data.tar: %v", err)
		}
	}

	volumes := n.volumes
	if volumes == nil {
		volumes = synthVolumeInfo(n)
	}

	sum, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("compute node checksum: %v", err)
	}

	if err := archive.WriteSnapshotYAML(dir, archive.SnapshotYAML{
		APIVersion:      n.apiVersion,
		Kind:            n.kind,
		Name:            n.name,
		Namespace:       n.namespace,
		SourceObjectRef: n.sourceObjectRef,
		Checksum:        sum,
		Volumes:         volumes,
	}); err != nil {
		t.Fatalf("write snapshot.yaml: %v", err)
	}
}

// synthVolumeInfo returns a single well-formed VolumeInfo for a data node (block or filesystem
// payload present), or nil for a non-data node. The volumeMode agrees with the payload kind so
// the archive integrity preflight (archive.ValidateSnapshotYAML) accepts it.
func synthVolumeInfo(n archiveNode) []archive.VolumeInfo {
	var mode string

	switch {
	case n.blockData != nil:
		mode = archive.VolumeModeBlock
	case n.tarData != nil:
		mode = archive.VolumeModeFilesystem
	default:
		return nil
	}

	return []archive.VolumeInfo{{
		Target: archive.VolumeObjectRef{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
			Name:       n.name,
			Namespace:  n.namespace,
		},
		Artifact: archive.VolumeObjectRef{
			APIVersion: "snapshot.storage.k8s.io/v1",
			Kind:       "VolumeSnapshotContent",
			Name:       n.name + "-content",
		},
		VolumeMode:       mode,
		StorageClassName: "sc-test",
		Size:             "1Gi",
	}}
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

// TestBuildPlan_BlockExtCarriedExplicitly verifies that PlannedNode.Ext is
// resolved by archive.ClassifyBlockPayload for every recognized codec name,
// and in particular that the raw "data.bin" case resolves Ext to "" — NOT
// filepath.Ext("data.bin")'s ".bin" (the bug this task fixes: the block
// upload path previously derived ext via filepath.Ext(leaf.DataFile) and
// therefore always mis-detected the raw/none codec).
func TestBuildPlan_BlockExtCarriedExplicitly(t *testing.T) {
	tests := []struct {
		fileName string
		wantExt  string
	}{
		{"data.bin", ""},
		{"data.bin.zst", ".zst"},
		{"data.bin.gz", ".gz"},
		{"data.bin.lz4", ".lz4"},
	}

	for _, tc := range tests {
		t.Run(tc.fileName, func(t *testing.T) {
			root := t.TempDir()

			writeArchiveNode(t, root, archiveNode{
				apiVersion: "state-snapshotter.deckhouse.io/v1alpha1",
				kind:       "Snapshot",
				name:       "root",
			})

			if err := os.WriteFile(filepath.Join(root, tc.fileName), []byte("rawbytes"), 0o600); err != nil {
				t.Fatalf("write %s: %v", tc.fileName, err)
			}

			plan, err := BuildPlan(root)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}

			if !plan[0].HasBlockData() {
				t.Fatal("expected HasBlockData() == true")
			}

			if plan[0].Ext != tc.wantExt {
				t.Errorf("Ext = %q, want %q", plan[0].Ext, tc.wantExt)
			}

			wantPath := filepath.Join(root, tc.fileName)
			if plan[0].DataFile != wantPath {
				t.Errorf("DataFile = %q, want %q", plan[0].DataFile, wantPath)
			}
		})
	}
}

// TestBuildPlan_RejectsInvalidBlockPayload verifies that BuildPlan fails
// deterministically — instead of silently picking one file via a
// first-glob-match — when a node directory's block payload shape is
// unknown/chained, ambiguous (multiple files), or coexists with data.tar.
// This is the same classifier ComputeNodeChecksum uses (see
// archive.ClassifyBlockPayload), so checksum and upload always agree.
func TestBuildPlan_RejectsInvalidBlockPayload(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
	}{
		{
			name:  "unknown codec suffix",
			files: map[string]string{"data.bin.foo": "x"},
		},
		{
			name:  "chained suffix",
			files: map[string]string{"data.bin.zst.bak": "x"},
		},
		{
			name: "multiple block files",
			files: map[string]string{
				"data.bin.zst": "x",
				"data.bin.gz":  "y",
			},
		},
		{
			name: "block payload coexists with data.tar",
			files: map[string]string{
				"data.bin.zst":    "x",
				archive.FsTarName: "y",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()

			writeArchiveNode(t, root, archiveNode{
				apiVersion: "state-snapshotter.deckhouse.io/v1alpha1",
				kind:       "Snapshot",
				name:       "root",
			})

			for name, content := range tc.files {
				if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
					t.Fatalf("write %s: %v", name, err)
				}
			}

			_, err := BuildPlan(root)
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			if !errors.Is(err, archive.ErrInvalidBlockPayload) {
				t.Errorf("expected ErrInvalidBlockPayload, got: %v", err)
			}
		})
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
		apiVersion:      "sds-unified-snapshots-poc.deckhouse.io/v1alpha1",
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
