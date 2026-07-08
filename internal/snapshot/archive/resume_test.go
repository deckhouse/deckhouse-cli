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
	"errors"
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

// writeMarker stamps the identity marker the pipeline writes on a node's first
// touch. Partial-dir resume tests seed it so ScanNode/ScanAbsolute can prove the
// dir belongs to the planned node — after partial-node-dir-identity-marker a
// marker-less non-empty partial dir is treated as foreign, not resumed.
func writeMarker(t *testing.T, dir string, id archive.NodeIdentity) {
	t.Helper()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir for marker: %v", err)
	}

	if err := archive.WriteNodeIdentityMarker(dir, id); err != nil {
		t.Fatalf("write identity marker: %v", err)
	}
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

	writeMarker(t, nodeDir, id)

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

	writeMarker(t, nodeDir, id)

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

	writeMarker(t, nodeDir, id)

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

	writeMarker(t, nodeDir, id)

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

// TestScanNode_FSStaging_UserTmpBlobSurvivesSweep pins the
// fs-reserved-suffix-collisions .tmp guard: at codec none a staged user blob
// keeps its verbatim server-provided name, which may end in ".tmp" (e.g.
// "notes.tmp"). Such a blob lives inside the FS tar staging dir (data.tar.d/),
// whose subtree removeTmpFiles now skips entirely — so it survives the resume
// scan and is not re-downloaded on the next run. A genuine stale internal
// ".tmp" OUTSIDE that subtree (a snapshot.yaml.tmp at the node root) must still
// be swept.
func TestScanNode_FSStaging_UserTmpBlobSurvivesSweep(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{Kind: "VirtualDiskSnapshot", Name: "disk-tmp-blob"}
	nodeDir := filepath.Join(parent, archive.NodeDirName(id.Kind, id.Name))

	writeMarker(t, nodeDir, id)

	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir stagingDir: %v", err)
	}

	userBlob := filepath.Join(stagingDir, "notes.tmp")

	if err := os.WriteFile(userBlob, []byte("user data that happens to end in .tmp"), 0o644); err != nil {
		t.Fatalf("write user blob: %v", err)
	}

	nodeTmp := filepath.Join(nodeDir, archive.SnapshotYAMLName+".tmp")

	if err := os.WriteFile(nodeTmp, []byte("half-written"), 0o644); err != nil {
		t.Fatalf("write node tmp: %v", err)
	}

	plan, err := archive.ScanNode(parent, id)

	if err != nil {
		t.Fatalf("ScanNode: %v", err)
	}

	if plan.State != archive.NodeStateFSPartial {
		t.Errorf("state = %v, want NodeStateFSPartial", plan.State)
	}

	if _, err := os.Stat(userBlob); err != nil {
		t.Errorf("staged user blob %q must survive the .tmp sweep, got: %v", userBlob, err)
	}

	if _, err := os.Stat(nodeTmp); !os.IsNotExist(err) {
		t.Error("stale internal node-root .tmp must still be removed by the sweep")
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

	writeMarker(t, nodeDir, id)

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

// TestScanNode_PartialMismatchedMarker_Redirects proves a partial dir whose
// identity marker belongs to a DIFFERENT snapshot is collision-redirected, never
// resumed into — the core cross-snapshot-mixing fix (scenario A/B).
func TestScanNode_PartialMismatchedMarker_Redirects(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()

	idA := archive.NodeIdentity{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "VirtualDiskSnapshot",
		Name:       "disk-1",
		DirName:    "source-disk",
		Namespace:  "default",
		SourceRef:  "vd/disk-a",
	}
	idB := idA
	idB.Name = "disk-2"
	idB.SourceRef = "vd/disk-b"

	// A block-partial dir left by snapshot A (marker A), same on-disk DirName.
	nodeDir := filepath.Join(parent, archive.NodeDirName(idA.Kind, "source-disk"))
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatalf("mkdir chunkDir: %v", err)
	}

	writeMarker(t, nodeDir, idA)

	if err := os.WriteFile(filepath.Join(chunkDir, archive.ChunkFileName(0, ".zst")), []byte("foreign"), 0o644); err != nil {
		t.Fatalf("write chunk: %v", err)
	}

	plan, err := archive.ScanNode(parent, idB)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.State != archive.NodeStatePending {
		t.Errorf("state = %v, want NodeStatePending (redirect)", plan.State)
	}

	if plan.TargetDir == nodeDir {
		t.Error("mismatched partial marker must not resume into the foreign dir")
	}

	// The redirect suffix is stable across re-scans (derived from foreign identity).
	plan2, err := archive.ScanNode(parent, idB)
	if err != nil {
		t.Fatalf("unexpected error on re-scan: %v", err)
	}

	if plan2.TargetDir != plan.TargetDir {
		t.Errorf("collision redirect not stable across re-scan: %q vs %q", plan2.TargetDir, plan.TargetDir)
	}
}

// TestScanNode_PartialNoMarker_Redirects proves a NON-EMPTY partial dir with no
// identity marker (a tree predating this feature, or a foreign dir) is treated
// as unverifiable and collision-redirected rather than silently resumed.
func TestScanNode_PartialNoMarker_Redirects(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{Kind: "VirtualDiskSnapshot", Name: "disk-1"}
	nodeDir := filepath.Join(parent, archive.NodeDirName(id.Kind, id.Name))
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatalf("mkdir chunkDir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(chunkDir, archive.ChunkFileName(0, ".zst")), []byte("unmarked"), 0o644); err != nil {
		t.Fatalf("write chunk: %v", err)
	}

	plan, err := archive.ScanNode(parent, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.State != archive.NodeStatePending {
		t.Errorf("state = %v, want NodeStatePending (redirect)", plan.State)
	}

	if plan.TargetDir == nodeDir {
		t.Error("a marker-less non-empty partial dir must not be resumed into")
	}
}

// TestScanNode_FreshEmptyDir_NoMarker_Resumes proves an empty (or lock-only)
// partial dir with no marker and no snapshot-download artifacts is treated as a
// fresh node, resumed in place so the pipeline can stamp the marker on first
// touch — the case that keeps a first-time root download working.
func TestScanNode_FreshEmptyDir_NoMarker_Resumes(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{Kind: "VirtualDiskSnapshot", Name: "disk-1"}
	nodeDir := filepath.Join(parent, archive.NodeDirName(id.Kind, id.Name))

	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		t.Fatalf("mkdir nodeDir: %v", err)
	}

	// A non-pipeline file (mirrors the download advisory lock) must not count.
	if err := os.WriteFile(filepath.Join(nodeDir, ".d8-snapshot-download.lock"), nil, 0o644); err != nil {
		t.Fatalf("write lock file: %v", err)
	}

	plan, err := archive.ScanNode(parent, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.State != archive.NodeStateManifestsOnly {
		t.Errorf("state = %v, want NodeStateManifestsOnly (fresh, resumable)", plan.State)
	}

	if plan.TargetDir != nodeDir {
		t.Errorf("TargetDir = %q, want %q (resume in place)", plan.TargetDir, nodeDir)
	}
}

// TestScanAbsolute_PartialMatchingMarker_Resumes proves the ScanAbsolute happy
// path: a partial dir whose marker matches the planned identity resumes as today.
func TestScanAbsolute_PartialMatchingMarker_Resumes(t *testing.T) {
	t.Parallel()

	nodeDir := t.TempDir()
	id := archive.NodeIdentity{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "root",
		Namespace:  "default",
	}

	writeMarker(t, nodeDir, id)

	if err := os.MkdirAll(filepath.Join(nodeDir, archive.BlockChunksDirName), 0o755); err != nil {
		t.Fatalf("mkdir chunkDir: %v", err)
	}

	plan, err := archive.ScanAbsolute(nodeDir, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.State != archive.NodeStateBlockPartial {
		t.Errorf("state = %v, want NodeStateBlockPartial", plan.State)
	}

	if plan.TargetDir != nodeDir {
		t.Errorf("TargetDir = %q, want %q", plan.TargetDir, nodeDir)
	}
}

// TestScanAbsolute_PartialMismatchedMarker_Rejects proves ScanAbsolute returns
// ErrIdentityMismatch (rather than resuming) when a partial dir's marker belongs
// to a different snapshot.
func TestScanAbsolute_PartialMismatchedMarker_Rejects(t *testing.T) {
	t.Parallel()

	nodeDir := t.TempDir()

	idA := archive.NodeIdentity{Kind: "Snapshot", Name: "root-a", SourceRef: "a"}
	idB := archive.NodeIdentity{Kind: "Snapshot", Name: "root-b", SourceRef: "b"}

	writeMarker(t, nodeDir, idA)

	if err := os.MkdirAll(filepath.Join(nodeDir, archive.BlockChunksDirName), 0o755); err != nil {
		t.Fatalf("mkdir chunkDir: %v", err)
	}

	_, err := archive.ScanAbsolute(nodeDir, idB)
	if !errors.Is(err, archive.ErrIdentityMismatch) {
		t.Errorf("err = %v, want ErrIdentityMismatch", err)
	}
}

// TestScanAbsolute_PartialNoMarker_Rejects proves ScanAbsolute rejects a
// non-empty partial dir carrying no identity marker.
func TestScanAbsolute_PartialNoMarker_Rejects(t *testing.T) {
	t.Parallel()

	nodeDir := t.TempDir()
	id := archive.NodeIdentity{Kind: "Snapshot", Name: "root"}

	if err := os.MkdirAll(filepath.Join(nodeDir, archive.FsTarStagingDirName), 0o755); err != nil {
		t.Fatalf("mkdir stagingDir: %v", err)
	}

	_, err := archive.ScanAbsolute(nodeDir, id)
	if !errors.Is(err, archive.ErrIdentityMismatch) {
		t.Errorf("err = %v, want ErrIdentityMismatch", err)
	}
}

// TestWriteNodeIdentityMarker_IdempotentFirstWriterWins proves the marker is
// written once and records the FIRST toucher's identity: a second call with a
// different identity is a no-op, so a re-run never overwrites the marker a resume
// must match.
func TestWriteNodeIdentityMarker_IdempotentFirstWriterWins(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	first := archive.NodeIdentity{Kind: "Snapshot", Name: "first", SourceRef: "s1"}
	second := archive.NodeIdentity{Kind: "Snapshot", Name: "second", SourceRef: "s2"}

	if err := archive.WriteNodeIdentityMarker(dir, first); err != nil {
		t.Fatalf("first write: %v", err)
	}

	if err := archive.WriteNodeIdentityMarker(dir, second); err != nil {
		t.Fatalf("second write: %v", err)
	}

	marker, found, err := archive.ReadNodeIdentityMarker(dir)
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}

	if !found {
		t.Fatal("marker must be present after write")
	}

	if marker.Name != "first" || marker.SourceRef != "s1" {
		t.Errorf("marker = %+v, want the first writer's identity", marker)
	}
}

// TestNodeIdentityMarker_DoesNotAffectChecksum proves the identity marker is
// checksum-neutral: adding it to a finalized node leaves ComputeNodeChecksum and
// VerifyNode results unchanged (it is not one of the files the checksum covers).
func TestNodeIdentityMarker_DoesNotAffectChecksum(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "VirtualDiskSnapshot",
		Name:       "disk-1",
		Namespace:  "default",
	}

	nodeDir := makeCompleteNode(t, parent, id.Kind, id.Name, id)

	before, err := archive.ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}

	if err := archive.WriteNodeIdentityMarker(nodeDir, id); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	after, err := archive.ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}

	if before.Hex != after.Hex {
		t.Errorf("checksum changed after writing marker: %q -> %q", before.Hex, after.Hex)
	}

	if err := archive.VerifyNode(nodeDir); err != nil {
		t.Errorf("VerifyNode must still pass with a marker present: %v", err)
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
