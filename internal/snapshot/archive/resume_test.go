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

package archive_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

// makeCompleteNode creates a node directory under parent with one manifest file
// and a valid snapshot.yaml (checksum computed from the manifest).
// The directory is named NodeDirName(kind, dirName) where dirName = id.DirName if
// set, else the name parameter.
func makeCompleteNode(t *testing.T, parent, kind, name string, id archive.NodeIdentity) string {
	t.Helper()

	dirPart := name
	if id.DirName != "" {
		dirPart = id.DirName
	}

	nodeDir := filepath.Join(parent, archive.NodeDirName(kind, dirPart))

	if err := os.MkdirAll(filepath.Join(nodeDir, archive.ManifestsDirName), 0o755); err != nil {
		t.Fatalf("mkdir manifests: %v", err)
	}

	manifestPath := filepath.Join(nodeDir, archive.ManifestsDirName, archive.ManifestFileName("ConfigMap", "app", ""))

	if err := os.WriteFile(manifestPath, []byte("apiVersion: v1\nkind: ConfigMap\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	checksum, err := archive.ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("compute checksum: %v", err)
	}

	sy := archive.SnapshotYAML{
		APIVersion: id.APIVersion,
		Kind:       id.Kind,
		Name:       id.Name,
		Namespace:  id.Namespace,
		SourceRef:  id.SourceRef,
		Checksum:   checksum,
	}

	if err := archive.WriteSnapshotYAML(nodeDir, sy); err != nil {
		t.Fatalf("write snapshot.yaml: %v", err)
	}

	return nodeDir
}

func TestScanNode_NoPrimaryDir(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{Kind: "VirtualDiskSnapshot", Name: "disk-1"}

	plan, err := archive.ScanNode(parent, id)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.State != archive.NodeStatePending {
		t.Errorf("state = %v, want NodeStatePending", plan.State)
	}

	// When DirName is empty the directory falls back to the CR name.
	want := filepath.Join(parent, archive.NodeDirName("VirtualDiskSnapshot", "disk-1"))

	if plan.TargetDir != want {
		t.Errorf("TargetDir = %q, want %q", plan.TargetDir, want)
	}
}

func TestScanNode_NoPrimaryDir_DirName(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{
		Kind:    "VirtualDiskSnapshot",
		Name:    "snap-cr-abc",
		DirName: "source-disk",
	}

	plan, err := archive.ScanNode(parent, id)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.State != archive.NodeStatePending {
		t.Errorf("state = %v, want NodeStatePending", plan.State)
	}

	// Directory must derive from DirName, not from the CR name.
	want := filepath.Join(parent, archive.NodeDirName("VirtualDiskSnapshot", "source-disk"))

	if plan.TargetDir != want {
		t.Errorf("TargetDir = %q, want %q", plan.TargetDir, want)
	}

	if plan.TargetDir == filepath.Join(parent, archive.NodeDirName("VirtualDiskSnapshot", "snap-cr-abc")) {
		t.Error("TargetDir must not derive from CR name when DirName is set")
	}
}

func TestScanNode_CompleteNodeIdentityMatch(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "VirtualDiskSnapshot",
		Name:       "disk-1",
		Namespace:  "default",
		SourceRef:  "vd/disk-1",
	}

	makeCompleteNode(t, parent, id.Kind, id.Name, id)

	plan, err := archive.ScanNode(parent, id)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.State != archive.NodeStateDone {
		t.Errorf("state = %v, want NodeStateDone", plan.State)
	}

	// DirName not set → falls back to Name.
	want := filepath.Join(parent, archive.NodeDirName(id.Kind, id.Name))

	if plan.TargetDir != want {
		t.Errorf("TargetDir = %q, want %q", plan.TargetDir, want)
	}
}

// TestScanNode_CompleteNodeIdentityMatch_DirName verifies that when DirName differs
// from Name the on-disk path uses DirName while identity matching uses Name+SourceRef.
func TestScanNode_CompleteNodeIdentityMatch_DirName(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "VirtualDiskSnapshot",
		Name:       "snap-cr-abc", // CR name — stored in snapshot.yaml, used for identity
		DirName:    "source-disk", // on-disk dir component, derived from the source object
		Namespace:  "default",
		SourceRef:  `{"apiVersion":"storage.deckhouse.io/v1alpha1","kind":"VirtualDisk","name":"source-disk"}`,
	}

	makeCompleteNode(t, parent, id.Kind, id.Name, id)

	plan, err := archive.ScanNode(parent, id)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.State != archive.NodeStateDone {
		t.Errorf("state = %v, want NodeStateDone", plan.State)
	}

	// Directory derives from DirName ("source-disk"), not from the CR name.
	want := filepath.Join(parent, archive.NodeDirName(id.Kind, "source-disk"))

	if plan.TargetDir != want {
		t.Errorf("TargetDir = %q, want %q", plan.TargetDir, want)
	}

	// Sanity: the CR-name-based path must NOT be the result.
	crNameDir := filepath.Join(parent, archive.NodeDirName(id.Kind, "snap-cr-abc"))
	if plan.TargetDir == crNameDir {
		t.Error("TargetDir must derive from DirName, not from the CR name")
	}
}

func TestScanNode_CompleteNodeIdentityMismatch(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()

	// existing node stored with identity A
	idA := archive.NodeIdentity{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "VirtualDiskSnapshot",
		Name:       "disk-1",
		Namespace:  "default",
		SourceRef:  "vd/disk-a",
	}

	makeCompleteNode(t, parent, idA.Kind, idA.Name, idA)

	// planned node has identity B (different SourceRef)
	idB := archive.NodeIdentity{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "VirtualDiskSnapshot",
		Name:       "disk-1",
		Namespace:  "default",
		SourceRef:  "vd/disk-b",
	}

	plan, err := archive.ScanNode(parent, idB)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The primary dir is complete for identity A; the new node must land elsewhere.
	if plan.State != archive.NodeStatePending {
		t.Errorf("state = %v, want NodeStatePending", plan.State)
	}

	primaryDir := filepath.Join(parent, archive.NodeDirName(idB.Kind, idB.Name))

	if plan.TargetDir == primaryDir {
		t.Error("TargetDir must not be the primary directory on identity mismatch")
	}
}

// TestScanNode_CollisionUseDirName verifies that when DirName is set the collision
// path also derives from DirName and uses it as the directory-name component.
func TestScanNode_CollisionUseDirName(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()

	// Existing complete node stored under the DirName-based path for identity A.
	idA := archive.NodeIdentity{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "VirtualDiskSnapshot",
		Name:       "snap-cr-a",
		DirName:    "source-disk",
		Namespace:  "default",
		SourceRef:  "vd/disk-a",
	}

	makeCompleteNode(t, parent, idA.Kind, idA.Name, idA)

	// New planned node has the same DirName (same source object name) but a
	// different CR name and SourceRef → identity mismatch → collision redirect.
	idB := archive.NodeIdentity{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "VirtualDiskSnapshot",
		Name:       "snap-cr-b",
		DirName:    "source-disk",
		Namespace:  "default",
		SourceRef:  "vd/disk-b",
	}

	plan, err := archive.ScanNode(parent, idB)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.State != archive.NodeStatePending {
		t.Errorf("state = %v, want NodeStatePending", plan.State)
	}

	// Collision path must be under the DirName-based prefix, not the CR name.
	dirNameBase := archive.NodeDirName("VirtualDiskSnapshot", "source-disk")

	if !filepath.IsAbs(plan.TargetDir) {
		t.Errorf("TargetDir %q is not absolute", plan.TargetDir)
	}

	base := filepath.Base(plan.TargetDir)
	if len(base) < len(dirNameBase) || base[:len(dirNameBase)] != dirNameBase {
		t.Errorf("collision TargetDir base %q does not start with %q", base, dirNameBase)
	}
}

func TestScanNode_BlockPartialWithTmp(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{Kind: "VirtualDiskSnapshot", Name: "disk-block"}
	nodeDir := filepath.Join(parent, archive.NodeDirName(id.Kind, id.Name))

	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatalf("mkdir chunkDir: %v", err)
	}

	// two valid chunks
	for _, name := range []string{archive.ChunkFileName(0, ".zst"), archive.ChunkFileName(2, ".zst")} {
		if err := os.WriteFile(filepath.Join(chunkDir, name), []byte("data"), 0o644); err != nil {
			t.Fatalf("write chunk: %v", err)
		}
	}

	// one stale .tmp that must be removed
	tmpPath := filepath.Join(chunkDir, archive.ChunkFileName(1, ".zst")+".tmp")

	if err := os.WriteFile(tmpPath, []byte("partial"), 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}

	plan, err := archive.ScanNode(parent, id)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.State != archive.NodeStateBlockPartial {
		t.Errorf("state = %v, want NodeStateBlockPartial", plan.State)
	}

	if plan.TargetDir != nodeDir {
		t.Errorf("TargetDir = %q, want %q", plan.TargetDir, nodeDir)
	}

	// stale .tmp must be gone
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("stale .tmp file should have been removed")
	}

	// block worker re-derives present chunks by os.Stat in downloadChunk; no
	// PresentChunkIndices field on NodeResumePlan.
}

// TestScanNode_BlockPartialAllPartFiles is the regression test for the
// durable sub-chunk resume design: a chunk directory holding ONLY a durable
// ".part" raw-partial file (no chunk has finalized yet) must still classify
// as NodeStateBlockPartial, not NodeStatePending/ManifestsOnly, so the
// pipeline resumes the node instead of restarting it from scratch. It must
// also NOT be swept by removeTmpFiles, which only targets "*.tmp" — the
// whole reason the durable partial uses a non-.tmp suffix.
func TestScanNode_BlockPartialAllPartFiles(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{Kind: "VirtualDiskSnapshot", Name: "disk-block-inflight"}
	nodeDir := filepath.Join(parent, archive.NodeDirName(id.Kind, id.Name))

	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatalf("mkdir chunkDir: %v", err)
	}

	// Only a durable partial for chunk 0 — no chunk has finalized yet.
	partPath := filepath.Join(chunkDir, archive.ChunkFileName(0, ".zst")+".part")

	if err := os.WriteFile(partPath, []byte("partial raw bytes"), 0o644); err != nil {
		t.Fatalf("write part: %v", err)
	}

	plan, err := archive.ScanNode(parent, id)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.State != archive.NodeStateBlockPartial {
		t.Errorf("state = %v, want NodeStateBlockPartial", plan.State)
	}

	if plan.TargetDir != nodeDir {
		t.Errorf("TargetDir = %q, want %q", plan.TargetDir, nodeDir)
	}

	// The durable partial must survive the resume scan's stale-tmp cleanup.
	if _, err := os.Stat(partPath); err != nil {
		t.Errorf("durable partial %q should survive ScanNode's removeTmpFiles pass, got error: %v", partPath, err)
	}
}

func TestScanNode_FSPartial(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{Kind: "VirtualDiskSnapshot", Name: "disk-fs"}
	nodeDir := filepath.Join(parent, archive.NodeDirName(id.Kind, id.Name))

	// Multi-volume data/ directory triggers FSPartial.
	dataDir := filepath.Join(nodeDir, archive.DataDirName)

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir dataDir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dataDir, "pvc-a.tar"), []byte("z"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	// stale .tmp inside data/ must be removed
	tmpPath := filepath.Join(dataDir, "pvc-b.tar.tmp")

	if err := os.WriteFile(tmpPath, []byte("partial"), 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}

	plan, err := archive.ScanNode(parent, id)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.State != archive.NodeStateFSPartial {
		t.Errorf("state = %v, want NodeStateFSPartial", plan.State)
	}

	if plan.TargetDir != nodeDir {
		t.Errorf("TargetDir = %q, want %q", plan.TargetDir, nodeDir)
	}

	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("stale .tmp file should have been removed")
	}
}

func TestScanNode_FSTarStagingPartial(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{Kind: "VirtualDiskSnapshot", Name: "disk-fstar"}
	nodeDir := filepath.Join(parent, archive.NodeDirName(id.Kind, id.Name))

	// Flat FS tar staging dir (data.tar.d/) triggers FSPartial.
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir stagingDir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(stagingDir, "root.txt"), []byte("raw"), 0o644); err != nil {
		t.Fatalf("write staged file: %v", err)
	}

	plan, err := archive.ScanNode(parent, id)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.State != archive.NodeStateFSPartial {
		t.Errorf("state = %v, want NodeStateFSPartial", plan.State)
	}

	if plan.TargetDir != nodeDir {
		t.Errorf("TargetDir = %q, want %q", plan.TargetDir, nodeDir)
	}
}

func TestScanNode_ManifestsOnly(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{Kind: "VirtualMachineSnapshot", Name: "vm-snap"}
	nodeDir := filepath.Join(parent, archive.NodeDirName(id.Kind, id.Name))

	manifDir := filepath.Join(nodeDir, archive.ManifestsDirName)

	if err := os.MkdirAll(manifDir, 0o755); err != nil {
		t.Fatalf("mkdir manifests: %v", err)
	}

	if err := os.WriteFile(filepath.Join(manifDir, "virtualmachine_vm.yaml"), []byte("apiVersion: v1\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	plan, err := archive.ScanNode(parent, id)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.State != archive.NodeStateManifestsOnly {
		t.Errorf("state = %v, want NodeStateManifestsOnly", plan.State)
	}

	if plan.TargetDir != nodeDir {
		t.Errorf("TargetDir = %q, want %q", plan.TargetDir, nodeDir)
	}
}

func TestScanNode_RemovesTmpRecursively(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{Kind: "VirtualDiskSnapshot", Name: "disk-tmp"}
	nodeDir := filepath.Join(parent, archive.NodeDirName(id.Kind, id.Name))

	manifDir := filepath.Join(nodeDir, archive.ManifestsDirName)

	if err := os.MkdirAll(manifDir, 0o755); err != nil {
		t.Fatalf("mkdir manifests: %v", err)
	}

	stale1 := filepath.Join(manifDir, "configmap_cfg.yaml.tmp")
	stale2 := filepath.Join(nodeDir, "snapshot.yaml.tmp")

	for _, p := range []string{stale1, stale2} {
		if err := os.WriteFile(p, []byte("stale"), 0o644); err != nil {
			t.Fatalf("write tmp: %v", err)
		}
	}

	if _, err := archive.ScanNode(parent, id); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, p := range []string{stale1, stale2} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("stale tmp %q should have been removed", p)
		}
	}
}
