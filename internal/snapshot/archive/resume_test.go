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
func makeCompleteNode(t *testing.T, parent, kind, name string, id archive.NodeIdentity) string {
	t.Helper()

	nodeDir := filepath.Join(parent, archive.NodeDirName(kind, name))

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

	want := filepath.Join(parent, archive.NodeDirName("VirtualDiskSnapshot", "disk-1"))

	if plan.TargetDir != want {
		t.Errorf("TargetDir = %q, want %q", plan.TargetDir, want)
	}

	if len(plan.PresentChunkIndices) != 0 {
		t.Errorf("PresentChunkIndices should be empty, got %v", plan.PresentChunkIndices)
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

	want := filepath.Join(parent, archive.NodeDirName(id.Kind, id.Name))

	if plan.TargetDir != want {
		t.Errorf("TargetDir = %q, want %q", plan.TargetDir, want)
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
	for _, name := range []string{archive.ChunkFileName(0), archive.ChunkFileName(2)} {
		if err := os.WriteFile(filepath.Join(chunkDir, name), []byte("data"), 0o644); err != nil {
			t.Fatalf("write chunk: %v", err)
		}
	}

	// one stale .tmp that must be removed
	tmpPath := filepath.Join(chunkDir, archive.ChunkFileName(1)+".tmp")

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

	// present chunks: indices 0 and 2 (sorted)
	if len(plan.PresentChunkIndices) != 2 || plan.PresentChunkIndices[0] != 0 || plan.PresentChunkIndices[1] != 2 {
		t.Errorf("PresentChunkIndices = %v, want [0 2]", plan.PresentChunkIndices)
	}
}

func TestScanNode_FSPartial(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{Kind: "VirtualDiskSnapshot", Name: "disk-fs"}
	nodeDir := filepath.Join(parent, archive.NodeDirName(id.Kind, id.Name))

	dataDir := filepath.Join(nodeDir, archive.DataDirName)

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir dataDir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dataDir, "file1.zst"), []byte("z"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	// stale .tmp inside data/ must be removed
	tmpPath := filepath.Join(dataDir, "file2.zst.tmp")

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
