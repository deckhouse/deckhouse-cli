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
	gotar "archive/tar"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"
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

func TestBuildPlanRejectsHostFilesystemSymlinks(t *testing.T) {
	tests := []struct {
		name  string
		build func(t *testing.T) (string, string)
	}{
		{
			name: "archive root",
			build: func(t *testing.T) (string, string) {
				t.Helper()

				target := buildTwoLevelArchive(t)
				path := filepath.Join(t.TempDir(), "archive")
				if err := os.Symlink(target, path); err != nil {
					t.Fatalf("symlink archive root: %v", err)
				}

				return path, path
			},
		},
		{
			name: "snapshot yaml",
			build: func(t *testing.T) (string, string) {
				t.Helper()

				root := buildTwoLevelArchive(t)
				path := filepath.Join(root, archive.SnapshotYAMLName)
				moveOutsideAndSymlink(t, path)

				return root, path
			},
		},
		{
			name: "manifests directory",
			build: func(t *testing.T) (string, string) {
				t.Helper()

				root := buildTwoLevelArchive(t)
				path := filepath.Join(root, archive.ManifestsDirName)
				moveOutsideAndSymlink(t, path)

				return root, path
			},
		},
		{
			name: "manifest file",
			build: func(t *testing.T) (string, string) {
				t.Helper()

				root := buildTwoLevelArchive(t)
				entries, err := os.ReadDir(filepath.Join(root, archive.ManifestsDirName))
				if err != nil {
					t.Fatalf("read manifests: %v", err)
				}

				path := filepath.Join(root, archive.ManifestsDirName, entries[0].Name())
				moveOutsideAndSymlink(t, path)

				return root, path
			},
		},
		{
			name: "snapshots directory",
			build: func(t *testing.T) (string, string) {
				t.Helper()

				root := buildTwoLevelArchive(t)
				path := filepath.Join(root, archive.SnapshotsDirName)
				moveOutsideAndSymlink(t, path)

				return root, path
			},
		},
		{
			name: "child node directory",
			build: func(t *testing.T) (string, string) {
				t.Helper()

				root := buildTwoLevelArchive(t)
				path := childDir(root, "VolumeSnapshot", "pvc-1")
				moveOutsideAndSymlink(t, path)

				return root, path
			},
		},
		{
			name: "block payload",
			build: func(t *testing.T) (string, string) {
				t.Helper()

				root := buildTwoLevelArchive(t)
				path := filepath.Join(childDir(root, "VolumeSnapshot", "pvc-1"), archive.DataBlockName(""))
				moveOutsideAndSymlink(t, path)

				return root, path
			},
		},
		{
			name: "filesystem payload",
			build: func(t *testing.T) (string, string) {
				t.Helper()

				root := t.TempDir()
				writeArchiveNode(t, root, archiveNode{
					apiVersion: "snapshot.storage.k8s.io/v1",
					kind:       "VolumeSnapshot",
					name:       "pvc-1",
					tarData:    []byte("tar bytes are never read"),
				})

				path := filepath.Join(root, archive.FsTarName)
				moveOutsideAndSymlink(t, path)

				return root, path
			},
		},
		{
			name: "legacy data directory",
			build: func(t *testing.T) (string, string) {
				t.Helper()

				root := buildTwoLevelArchive(t)
				path := filepath.Join(root, archive.DataDirName)
				if err := os.Mkdir(path, 0o755); err != nil {
					t.Fatalf("mkdir legacy data: %v", err)
				}
				moveOutsideAndSymlink(t, path)

				return root, path
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root, path := tc.build(t)

			_, err := BuildPlan(root)
			if !errors.Is(err, archive.ErrNonRegularArchiveArtifact) {
				t.Fatalf("BuildPlan error = %v, want ErrNonRegularArchiveArtifact", err)
			}

			if !strings.Contains(err.Error(), path) {
				t.Errorf("error %q does not contain offending path %q", err, path)
			}
		})
	}
}

func moveOutsideAndSymlink(t *testing.T, path string) {
	t.Helper()

	outside := filepath.Join(t.TempDir(), filepath.Base(path))
	if err := os.Rename(path, outside); err != nil {
		t.Fatalf("move %s outside archive: %v", path, err)
	}

	if err := os.Symlink(outside, path); err != nil {
		t.Fatalf("symlink %s: %v", path, err)
	}
}

func TestBuildPlan_RejectsDuplicateCanonicalIdentities(t *testing.T) {
	const duplicateAPIVersion = "domain.example.io/v1"

	tests := []struct {
		name      string
		build     func(t *testing.T) string
		wantIssue string
	}{
		{
			name: "siblings_duplicate_one_parent_child",
			build: func(t *testing.T) string {
				t.Helper()

				root := t.TempDir()
				writeArchiveNode(t, root, archiveNode{
					apiVersion: snapshotAPIVersion,
					kind:       snapshotKind,
					name:       "root",
				})

				for _, physicalName := range []string{"z-physical", "a-physical"} {
					writeArchiveNode(t, filepath.Join(root, archive.SnapshotsDirName, physicalName), archiveNode{
						apiVersion: duplicateAPIVersion,
						kind:       "DemoSnapshot",
						name:       "same",
					})
				}

				return root
			},
			wantIssue: "references child domain.example.io/v1 DemoSnapshot/same 2 times",
		},
		{
			name: "separate_branches_multiple_parents",
			build: func(t *testing.T) string {
				t.Helper()

				root := t.TempDir()
				writeArchiveNode(t, root, archiveNode{
					apiVersion: snapshotAPIVersion,
					kind:       snapshotKind,
					name:       "root",
				})

				for _, branchName := range []string{"branch-b", "branch-a"} {
					branch := filepath.Join(root, archive.SnapshotsDirName, branchName)
					writeArchiveNode(t, branch, archiveNode{
						apiVersion: duplicateAPIVersion,
						kind:       "BranchSnapshot",
						name:       branchName,
					})
					writeArchiveNode(t, filepath.Join(branch, archive.SnapshotsDirName, "leaf"), archiveNode{
						apiVersion: duplicateAPIVersion,
						kind:       "DemoSnapshot",
						name:       "same",
					})
				}

				return root
			},
			wantIssue: "child domain.example.io/v1 DemoSnapshot/same has multiple physical parents",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := test.build(t)

			_, err := BuildPlan(root)
			if err == nil {
				t.Fatal("BuildPlan() error = nil, want duplicate identity rejection")
			}

			errorText := err.Error()
			for _, want := range []string{
				"invalid archive plan topology",
				"canonical identity domain.example.io/v1 DemoSnapshot/same appears in multiple directories",
				test.wantIssue,
			} {
				if !strings.Contains(errorText, want) {
					t.Errorf("BuildPlan() error = %q, want substring %q", errorText, want)
				}
			}

			firstPath := filepath.Join(root, archive.SnapshotsDirName, "a-physical")
			secondPath := filepath.Join(root, archive.SnapshotsDirName, "z-physical")
			if test.name == "siblings_duplicate_one_parent_child" &&
				strings.Index(errorText, firstPath) > strings.Index(errorText, secondPath) {
				t.Errorf("duplicate paths are not deterministic: %q", errorText)
			}
		})
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

	if leaf.SizeBytes != 10*1024*1024*1024 {
		t.Errorf("leaf SizeBytes = %d, want %d", leaf.SizeBytes, int64(10*1024*1024*1024))
	}

	if leaf.NodeChecksum == "" || leaf.DataImportIdentity == "" {
		t.Errorf("leaf identity metadata is incomplete: checksum=%q identity=%q",
			leaf.NodeChecksum, leaf.DataImportIdentity)
	}

	if leaf.PayloadKind != dataImportPayloadBlock || leaf.Codec != "none" {
		t.Errorf("leaf payload metadata = {kind:%q codec:%q}, want {block none}",
			leaf.PayloadKind, leaf.Codec)
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

func TestDataImportIdentity_CanonicalAndDimensionComplete(t *testing.T) {
	base := PlannedNode{
		APIVersion:       "snapshot.storage.k8s.io/v1",
		Kind:             "VolumeSnapshot",
		Name:             "pvc-1",
		NodeChecksum:     strings.Repeat("a", sha256HexLength),
		VolumeMode:       archive.VolumeModeBlock,
		StorageClassName: "sc-fast",
		SizeBytes:        1024,
		PayloadKind:      dataImportPayloadBlock,
		Codec:            "zstd",
	}

	tests := []struct {
		name   string
		mutate func(*PlannedNode)
	}{
		{name: "apiVersion", mutate: func(node *PlannedNode) { node.APIVersion = "v2" }},
		{name: "kind", mutate: func(node *PlannedNode) { node.Kind = "OtherSnapshot" }},
		{name: "name", mutate: func(node *PlannedNode) { node.Name = "pvc-2" }},
		{name: "checksum", mutate: func(node *PlannedNode) { node.NodeChecksum = strings.Repeat("b", sha256HexLength) }},
		{name: "volume mode", mutate: func(node *PlannedNode) { node.VolumeMode = archive.VolumeModeFilesystem }},
		{name: "storage class", mutate: func(node *PlannedNode) { node.StorageClassName = "sc-slow" }},
		{name: "size bytes", mutate: func(node *PlannedNode) { node.SizeBytes++ }},
		{name: "payload kind", mutate: func(node *PlannedNode) { node.PayloadKind = dataImportPayloadFilesystem }},
		{name: "codec", mutate: func(node *PlannedNode) { node.Codec = "none" }},
	}

	baseIdentity := dataImportIdentity(base)
	importer := &clusterVolumeImporter{}
	base.DataImportIdentity = baseIdentity
	baseName := importer.DataImportName(base)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := base
			test.mutate(&changed)
			changed.DataImportIdentity = dataImportIdentity(changed)

			if changed.DataImportIdentity == baseIdentity {
				t.Errorf("identity did not change after changing %s", test.name)
			}

			if importer.DataImportName(changed) == baseName {
				t.Errorf("identity-qualified name did not change after changing %s", test.name)
			}
		})
	}
}

func TestBuildPlan_EquivalentQuantitiesShareIdentity(t *testing.T) {
	tests := []struct {
		name string
		size string
	}{
		{name: "binary quantity", size: "1Gi"},
		{name: "decimal bytes", size: "1073741824"},
	}

	identities := make([]string, 0, len(tests))

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			writeArchiveNode(t, dir, archiveNode{
				apiVersion: "snapshot.storage.k8s.io/v1",
				kind:       "VolumeSnapshot",
				name:       "pvc-1",
				blockData:  []byte("payload"),
				volumes: []archive.VolumeInfo{{
					Target: archive.VolumeObjectRef{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Name:       "pvc-1",
					},
					Artifact: archive.VolumeObjectRef{
						APIVersion: "snapshot.storage.k8s.io/v1",
						Kind:       "VolumeSnapshotContent",
						Name:       "content-1",
					},
					StorageClassName: "sc-fast",
					Size:             test.size,
					VolumeMode:       archive.VolumeModeBlock,
				}},
			})

			plan, err := BuildPlan(dir)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}

			identities = append(identities, plan[0].DataImportIdentity)
		})
	}

	if identities[0] != identities[1] {
		t.Errorf("equivalent quantities produced different identities: %q != %q", identities[0], identities[1])
	}
}

func TestDataImportName_LongLeafStaysKubernetesSafe(t *testing.T) {
	leaf := volumeSnapshotLeaf(strings.Repeat("a", dataImportNameMaxLength))
	name := (&clusterVolumeImporter{}).DataImportName(leaf)

	if len(name) > dataImportNameMaxLength {
		t.Errorf("DataImport name length = %d, want <= %d", len(name), dataImportNameMaxLength)
	}

	if !strings.HasSuffix(name, "-"+dataImportShortID(leaf)) {
		t.Errorf("DataImport name %q does not carry identity suffix %q", name, dataImportShortID(leaf))
	}

	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		t.Errorf("DataImport name %q is not DNS-safe: %v", name, errs)
	}

	if errs := validation.IsValidLabelValue(dataImportShortID(leaf)); len(errs) > 0 {
		t.Errorf("DataImport short ID %q is not label-safe: %v", dataImportShortID(leaf), errs)
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

func TestBuildPlan_ClassifiesFilesystemCodecFromPAX(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		codec        string
		originalPath string
		storedPath   string
	}{
		{name: "none ignores zstd-looking source suffix", codec: "none", originalPath: "report.zst", storedPath: "report.zst"},
		{name: "zstd ignores gzip-looking source suffix", codec: "zstd", originalPath: "report.gz", storedPath: "report.gz.zst"},
		{name: "gzip ignores lz4-looking source suffix", codec: "gzip", originalPath: "report.lz4", storedPath: "report.lz4.gz"},
		{name: "lz4 ignores gzip-looking source suffix", codec: "lz4", originalPath: "report.gz", storedPath: "report.gz.lz4"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			metadata, err := archive.NewFSMetadata(tc.codec, tc.originalPath, 4)
			if err != nil {
				t.Fatalf("NewFSMetadata: %v", err)
			}

			dir := t.TempDir()
			writeArchiveNode(t, dir, archiveNode{
				apiVersion: "snapshot.storage.k8s.io/v1",
				kind:       "VolumeSnapshot",
				name:       "pvc-1",
				tarData: writePlanFSTar(t, &gotar.Header{
					Format:     gotar.FormatPAX,
					Name:       tc.storedPath,
					Mode:       0o600,
					Size:       4,
					Typeflag:   gotar.TypeReg,
					PAXRecords: metadata.PAXRecords(),
				}),
			})

			plan, err := BuildPlan(dir)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}

			node := plan[0]
			if node.PayloadKind != dataImportPayloadFilesystem || node.Codec != tc.codec {
				t.Errorf("payload metadata = {kind:%q codec:%q}, want {filesystem %s}",
					node.PayloadKind, node.Codec, tc.codec)
			}

			if node.DataImportIdentity == "" {
				t.Error("filesystem node has no DataImport identity")
			}
		})
	}
}

func TestBuildPlan_RejectsFilesystemMetadataBeforeIdentity(t *testing.T) {
	t.Parallel()

	valid, err := archive.NewFSMetadata("zstd", "file.txt", 4)
	if err != nil {
		t.Fatalf("NewFSMetadata: %v", err)
	}

	tests := []struct {
		name    string
		header  *gotar.Header
		records map[string]string
	}{
		{
			name:   "missing PAX cannot fall back to suffix",
			header: &gotar.Header{Name: "file.txt.zst", Mode: 0o600, Size: 4, Typeflag: gotar.TypeReg},
		},
		{
			name:    "stored path inconsistent with PAX",
			header:  &gotar.Header{Name: "file.txt.gz", Mode: 0o600, Size: 4, Typeflag: gotar.TypeReg},
			records: valid.PAXRecords(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.header.Format = gotar.FormatPAX
			tc.header.PAXRecords = tc.records

			dir := t.TempDir()
			writeArchiveNode(t, dir, archiveNode{
				apiVersion: "snapshot.storage.k8s.io/v1",
				kind:       "VolumeSnapshot",
				name:       "pvc-1",
				tarData:    writePlanFSTar(t, tc.header),
			})

			_, err := BuildPlan(dir)
			if !errors.Is(err, archive.ErrInvalidFSMetadata) {
				t.Fatalf("BuildPlan error = %v, want ErrInvalidFSMetadata", err)
			}
		})
	}
}

func TestBuildPlanParentReplacementCannotEscapePinnedArchive(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T) (root, trigger, target, outside string)
	}{
		{
			name: "archive root",
			setup: func(t *testing.T) (string, string, string, string) {
				t.Helper()
				root := buildTwoLevelArchive(t)

				return root, filepath.Join(root, archive.SnapshotYAMLName), root, buildTwoLevelArchive(t)
			},
		},
		{
			name: "node directory",
			setup: func(t *testing.T) (string, string, string, string) {
				t.Helper()
				root := buildTwoLevelArchive(t)
				node := childDir(root, "VolumeSnapshot", "pvc-1")
				outside := childDir(buildTwoLevelArchive(t), "VolumeSnapshot", "pvc-1")

				return root, filepath.Join(node, archive.SnapshotYAMLName), node, outside
			},
		},
		{
			name: "manifests directory",
			setup: func(t *testing.T) (string, string, string, string) {
				t.Helper()
				root := buildTwoLevelArchive(t)
				manifests := filepath.Join(root, archive.ManifestsDirName)
				entries, err := os.ReadDir(manifests)
				if err != nil || len(entries) == 0 {
					t.Fatalf("read manifests: entries=%d err=%v", len(entries), err)
				}

				outside := filepath.Join(buildTwoLevelArchive(t), archive.ManifestsDirName)

				return root, filepath.Join(manifests, entries[0].Name()), manifests, outside
			},
		},
		{
			name: "snapshots directory",
			setup: func(t *testing.T) (string, string, string, string) {
				t.Helper()
				root := buildTwoLevelArchive(t)
				snapshots := filepath.Join(root, archive.SnapshotsDirName)

				return root, childDir(root, "VolumeSnapshot", "pvc-1"), snapshots,
					filepath.Join(buildTwoLevelArchive(t), archive.SnapshotsDirName)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root, trigger, target, outside := tc.setup(t)
			replaced := false

			_, err := buildPlan(root, func(path string) {
				if replaced || path != trigger {
					return
				}

				replaced = true
				replacePathWithSymlink(t, target, outside)
			})
			if !replaced {
				t.Fatalf("boundary hook for %s was not reached", trigger)
			}

			if !errors.Is(err, archive.ErrNonRegularArchiveArtifact) {
				t.Fatalf("BuildPlan error = %v, want ErrNonRegularArchiveArtifact", err)
			}
		})
	}
}

func TestBuildPlanFinalReplacementCannotReadOutsideBytes(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T) (root, target, outside string)
	}{
		{
			name: "snapshot yaml",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				root := buildTwoLevelArchive(t)
				outside := filepath.Join(t.TempDir(), "outside.yaml")
				if err := os.WriteFile(outside, []byte("apiVersion: escaped/v1\nkind: Escaped\nname: escaped\n"), 0o600); err != nil {
					t.Fatalf("write outside snapshot: %v", err)
				}

				return root, filepath.Join(root, archive.SnapshotYAMLName), outside
			},
		},
		{
			name: "manifest",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				root := buildTwoLevelArchive(t)
				manifests := filepath.Join(root, archive.ManifestsDirName)
				entries, err := os.ReadDir(manifests)
				if err != nil || len(entries) == 0 {
					t.Fatalf("read manifests: entries=%d err=%v", len(entries), err)
				}

				outside := filepath.Join(t.TempDir(), "outside.yaml")
				if err := os.WriteFile(outside, []byte("apiVersion: v1\nkind: Secret\nmetadata:\n  name: escaped\n"), 0o600); err != nil {
					t.Fatalf("write outside manifest: %v", err)
				}

				return root, filepath.Join(manifests, entries[0].Name()), outside
			},
		},
		{
			name: "block payload",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				root := buildTwoLevelArchive(t)
				target := filepath.Join(childDir(root, "VolumeSnapshot", "pvc-1"), archive.DataBlockName(""))
				outside := filepath.Join(t.TempDir(), "outside.bin")
				if err := os.WriteFile(outside, []byte("escaped block bytes"), 0o600); err != nil {
					t.Fatalf("write outside block: %v", err)
				}

				return root, target, outside
			},
		},
		{
			name: "filesystem payload",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				root := t.TempDir()
				writeArchiveNode(t, root, archiveNode{
					apiVersion: snapshotAPIVersion,
					kind:       snapshotKind,
					name:       "root",
					tarData:    []byte("original tar bytes"),
				})

				target := filepath.Join(root, archive.FsTarName)
				outside := filepath.Join(t.TempDir(), "outside.tar")
				if err := os.WriteFile(outside, []byte("escaped tar bytes"), 0o600); err != nil {
					t.Fatalf("write outside tar: %v", err)
				}

				return root, target, outside
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root, target, outside := tc.setup(t)
			replaced := false

			_, err := buildPlan(root, func(path string) {
				if replaced || path != target {
					return
				}

				replaced = true
				replacePathWithSymlink(t, target, outside)
			})
			if !replaced {
				t.Fatalf("boundary hook for %s was not reached", target)
			}

			if !errors.Is(err, archive.ErrNonRegularArchiveArtifact) {
				t.Fatalf("BuildPlan error = %v, want ErrNonRegularArchiveArtifact", err)
			}
		})
	}
}

const snapimportMountHelperScenario = "D8_SNAPSHOT_UPLOAD_MOUNT_HELPER"

func TestBuildPlanAndVerifyNodeRejectMountedDescendants(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux bind-mount integration test")
	}

	for _, scenario := range []string{
		"build-plan-directory",
		"build-plan-regular-file",
		"verify-node-directory",
		"verify-node-regular-file",
	} {
		t.Run(scenario, func(t *testing.T) {
			runSnapimportMountHelper(t, "^TestLinuxMountedPlanEscapeHelper$", scenario)
		})
	}
}

func TestLinuxMountedPlanEscapeHelper(t *testing.T) {
	scenario := os.Getenv(snapimportMountHelperScenario)
	if scenario == "" {
		return
	}

	root := buildTwoLevelArchive(t)
	sourcePath, targetPath := matchingOutsideMountFixture(t, root, strings.HasSuffix(scenario, "regular-file"))

	mount := func() error {
		return bindMountForTest(sourcePath, targetPath)
	}

	var err error

	switch scenario {
	case "build-plan-directory", "build-plan-regular-file":
		mounted := false
		var mountErr error

		_, err = buildPlan(root, func(path string) {
			if mounted || mountErr != nil || path != targetPath {
				return
			}

			mountErr = mount()
			mounted = mountErr == nil
		})
		if mountErr != nil {
			fmt.Printf("mount namespace unavailable: %v\n", mountErr)

			return
		}

		if !mounted {
			t.Fatalf("boundary hook for %s was not reached", targetPath)
		}
	case "verify-node-directory", "verify-node-regular-file":
		if mountErr := mount(); mountErr != nil {
			fmt.Printf("mount namespace unavailable: %v\n", mountErr)

			return
		}

		err = archive.VerifyNode(root)
	default:
		t.Fatalf("unknown scenario %q", scenario)
	}

	if !errors.Is(err, archive.ErrNonRegularArchiveArtifact) {
		t.Fatalf("%s error = %v, want ErrNonRegularArchiveArtifact", scenario, err)
	}
}

func matchingOutsideMountFixture(t *testing.T, root string, regularFile bool) (string, string) {
	t.Helper()

	if regularFile {
		target := filepath.Join(root, archive.SnapshotYAMLName)
		data, err := os.ReadFile(target)
		if err != nil {
			t.Fatalf("read mounted file target: %v", err)
		}

		source := filepath.Join(t.TempDir(), archive.SnapshotYAMLName)
		if err := os.WriteFile(source, data, 0o600); err != nil {
			t.Fatalf("write mounted file source: %v", err)
		}

		return source, target
	}

	target := filepath.Join(root, archive.ManifestsDirName)
	source := filepath.Join(t.TempDir(), archive.ManifestsDirName)
	if err := os.Mkdir(source, 0o755); err != nil {
		t.Fatalf("mkdir mounted directory source: %v", err)
	}

	entries, err := os.ReadDir(target)
	if err != nil {
		t.Fatalf("read mounted directory target: %v", err)
	}

	for _, entry := range entries {
		data, readErr := os.ReadFile(filepath.Join(target, entry.Name()))
		if readErr != nil {
			t.Fatalf("read mounted directory entry: %v", readErr)
		}

		if writeErr := os.WriteFile(filepath.Join(source, entry.Name()), data, 0o600); writeErr != nil {
			t.Fatalf("write mounted directory entry: %v", writeErr)
		}
	}

	return source, target
}

func bindMountForTest(source, target string) error {
	output, err := exec.Command("mount", "--bind", source, target).CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount --bind: %w: %s", err, output)
	}

	return nil
}

func runSnapimportMountHelper(t *testing.T, testName, scenario string) {
	t.Helper()

	if runtime.GOOS != "linux" {
		t.Skip("Linux bind-mount integration test")
	}

	unshare, err := exec.LookPath("unshare")
	if err != nil {
		t.Skipf("unshare is unavailable: %v", err)
	}

	cmd := exec.Command(unshare,
		"--user", "--map-root-user", "--mount", "--fork",
		os.Args[0], "-test.v", "-test.run="+testName,
	)
	cmd.Env = append(os.Environ(), snapimportMountHelperScenario+"="+scenario)

	output, err := cmd.CombinedOutput()
	if err != nil {
		text := string(output)
		if strings.Contains(text, "unshare failed") || strings.Contains(text, "Operation not permitted") {
			t.Skipf("isolated mount namespace is unavailable:\n%s", text)
		}

		t.Fatalf("mount helper failed: %v\n%s", err, text)
	}

	if strings.Contains(string(output), "mount namespace unavailable:") {
		t.Skipf("isolated bind mounts are unavailable:\n%s", output)
	}
}

func replacePathWithSymlink(t *testing.T, path, target string) {
	t.Helper()

	original := path + ".pinned-original"
	if err := os.Rename(path, original); err != nil {
		t.Fatalf("rename %s: %v", path, err)
	}

	if err := os.Symlink(target, path); err != nil {
		t.Fatalf("symlink %s: %v", path, err)
	}

	t.Cleanup(func() {
		_ = os.Remove(path)
		_ = os.Rename(original, path)
	})
}

func writePlanFSTar(t *testing.T, header *gotar.Header) []byte {
	t.Helper()

	var payload bytes.Buffer

	writer := gotar.NewWriter(&payload)
	if err := writer.WriteHeader(header); err != nil {
		t.Fatalf("write tar header: %v", err)
	}

	if _, err := writer.Write(make([]byte, header.Size)); err != nil {
		t.Fatalf("write tar payload: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}

	return payload.Bytes()
}
