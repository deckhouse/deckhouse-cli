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

package archive

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// makeNodeDir creates the standard skeleton for a node directory in t.TempDir().
// It returns the nodeDir path.
func makeNodeDir(t *testing.T) string {
	t.Helper()

	nodeDir := t.TempDir()

	if err := os.MkdirAll(filepath.Join(nodeDir, ManifestsDirName), 0o755); err != nil {
		t.Fatalf("mkdir manifests/: %v", err)
	}

	return nodeDir
}

// writeFile writes content to path (creating intermediate dirs as needed).
func writeFile(t *testing.T, path, content string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestComputeNodeChecksum_Deterministic verifies that the same node content
// produces the same checksum on repeated calls.
func TestComputeNodeChecksum_Deterministic(t *testing.T) {
	nodeDir := makeNodeDir(t)
	writeFile(t, filepath.Join(nodeDir, ManifestsDirName, "configmap_app.yaml"), "kind: ConfigMap\nname: app\n")
	writeFile(t, filepath.Join(nodeDir, DataBlockName(".zst")), "fake-block-data")

	c1, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("first compute: %v", err)
	}

	c2, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("second compute: %v", err)
	}

	if c1.Hex != c2.Hex {
		t.Errorf("non-deterministic: got %q then %q", c1.Hex, c2.Hex)
	}

	if c1.Algorithm != "sha256" {
		t.Errorf("algorithm: got %q, want sha256", c1.Algorithm)
	}

	if len(c1.Hex) != 64 {
		t.Errorf("hex length: got %d, want 64", len(c1.Hex))
	}
}

// TestComputeNodeChecksum_ChangedManifest verifies that modifying a manifest
// changes the checksum.
func TestComputeNodeChecksum_ChangedManifest(t *testing.T) {
	nodeDir := makeNodeDir(t)
	mf := filepath.Join(nodeDir, ManifestsDirName, "configmap_app.yaml")
	writeFile(t, mf, "kind: ConfigMap\nname: app\n")

	before, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("before: %v", err)
	}

	writeFile(t, mf, "kind: ConfigMap\nname: app\ndata: {changed: true}\n")

	after, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("after: %v", err)
	}

	if before.Hex == after.Hex {
		t.Error("checksum did not change after mutating manifest")
	}
}

// TestComputeNodeChecksum_ExcludesSnapshotYAML verifies that snapshot.yaml is
// not included in the digest.
func TestComputeNodeChecksum_ExcludesSnapshotYAML(t *testing.T) {
	nodeDir := makeNodeDir(t)
	writeFile(t, filepath.Join(nodeDir, ManifestsDirName, "configmap_x.yaml"), "kind: ConfigMap\n")

	before, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("before: %v", err)
	}

	// Writing snapshot.yaml must not change the checksum.
	writeFile(t, filepath.Join(nodeDir, SnapshotYAMLName), "checksum: {hex: abc}")

	after, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("after: %v", err)
	}

	if before.Hex != after.Hex {
		t.Error("snapshot.yaml was unexpectedly included in the checksum")
	}
}

// TestComputeNodeChecksum_ExcludesSnapshotsDir verifies that the snapshots/ child
// directory is not included in the digest.
func TestComputeNodeChecksum_ExcludesSnapshotsDir(t *testing.T) {
	nodeDir := makeNodeDir(t)
	writeFile(t, filepath.Join(nodeDir, ManifestsDirName, "configmap_x.yaml"), "kind: ConfigMap\n")

	before, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("before: %v", err)
	}

	// Creating a child node directory under snapshots/ must not change the checksum.
	writeFile(t, filepath.Join(nodeDir, SnapshotsDirName, "child_node", SnapshotYAMLName), "child")

	after, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("after: %v", err)
	}

	if before.Hex != after.Hex {
		t.Error("snapshots/ directory was unexpectedly included in the checksum")
	}
}

// TestComputeNodeChecksum_FsVolume verifies that the flat data.tar is covered by the checksum.
func TestComputeNodeChecksum_FsVolume(t *testing.T) {
	nodeDir := makeNodeDir(t)
	writeFile(t, filepath.Join(nodeDir, FsTarName), "tar-content-v1")

	before, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("before: %v", err)
	}

	writeFile(t, filepath.Join(nodeDir, FsTarName), "tar-content-v2")

	after, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("after: %v", err)
	}

	if before.Hex == after.Hex {
		t.Error("checksum did not change after mutating fs volume tar")
	}
}

// TestComputeNodeChecksum_FsVolume_StagingExcluded verifies that the flat FS staging
// directory (data.tar.d/) and its contents are NOT included in the checksum.
func TestComputeNodeChecksum_FsVolume_StagingExcluded(t *testing.T) {
	nodeDir := makeNodeDir(t)
	writeFile(t, filepath.Join(nodeDir, FsTarName), "tar-content")

	base, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("base: %v", err)
	}

	// Writing a file inside the staging dir must NOT change the checksum.
	writeFile(t, filepath.Join(nodeDir, FsTarStagingDirName, "rawfile.txt"), "raw")

	after, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("after: %v", err)
	}

	if base.Hex != after.Hex {
		t.Error("staging dir data.tar.d/ was unexpectedly included in the checksum")
	}
}

// TestComputeNodeChecksum_FsVolume_NestedFileChunkStagingExcluded verifies that
// a per-file chunk directory nested inside the flat FS staging dir
// (data.tar.d/<file>.d/, used by the chunked large-file resume path) and its
// contents are NOT included in the checksum either. collectNodeFiles never
// walks nodeDir itself for the single-volume layout — it only looks at
// manifests/, the flat data.bin*/data.tar files, and (separately) the
// multi-volume data/ dir — so the flat data.tar.d/ tree, at any nesting depth,
// is excluded by construction; this test pins that behavior for the new nested
// case explicitly.
func TestComputeNodeChecksum_FsVolume_NestedFileChunkStagingExcluded(t *testing.T) {
	nodeDir := makeNodeDir(t)
	writeFile(t, filepath.Join(nodeDir, FsTarName), "tar-content")

	base, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("base: %v", err)
	}

	// Writing a chunk file inside a nested "<file><ext>.d/" chunk directory
	// must NOT change the checksum.
	nestedChunkDir := FsFileChunksDirName("payload.bin", ".zst")
	writeFile(t, filepath.Join(nodeDir, FsTarStagingDirName, nestedChunkDir, ChunkFileName(0, ".zst")), "chunk-raw")

	after, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("after: %v", err)
	}

	if base.Hex != after.Hex {
		t.Error("nested per-file chunk dir under data.tar.d/ was unexpectedly included in the checksum")
	}
}

// TestComputeNodeChecksum_ChunkMetaExcluded verifies that the chunks.meta
// geometry sidecar (chunk-size-mismatch-resume-corruption-guard) never
// contributes to a node checksum, in every place it can appear: the
// single-volume flat block chunk dir (data.bin.d/), a per-file chunk dir
// nested inside the flat FS staging dir (data.tar.d/<file><ext>.d/), and the
// multi-volume block chunk dir (data/<pvc>.bin.d/). The first two are
// excluded because collectNodeFiles never walks nodeDir itself for the
// single-volume layout; the third is excluded by the existing ".d"-suffix
// skip in the data/ walk.
func TestComputeNodeChecksum_ChunkMetaExcluded(t *testing.T) {
	t.Run("flat block chunk dir", func(t *testing.T) {
		nodeDir := makeNodeDir(t)
		writeFile(t, filepath.Join(nodeDir, DataBlockName(".zst")), "block-content")

		base, err := ComputeNodeChecksum(nodeDir)
		if err != nil {
			t.Fatalf("base: %v", err)
		}

		chunkDir := filepath.Join(nodeDir, BlockChunksDirName)
		if err := EnsureDir(chunkDir); err != nil {
			t.Fatalf("EnsureDir: %v", err)
		}

		if err := WriteChunkMeta(chunkDir, ChunkMeta{ChunkSize: 100, TotalSize: 1000}); err != nil {
			t.Fatalf("WriteChunkMeta: %v", err)
		}

		after, err := ComputeNodeChecksum(nodeDir)
		if err != nil {
			t.Fatalf("after: %v", err)
		}

		if base.Hex != after.Hex {
			t.Error("chunks.meta under data.bin.d/ was unexpectedly included in the checksum")
		}
	})

	t.Run("nested per-file FS chunk dir", func(t *testing.T) {
		nodeDir := makeNodeDir(t)
		writeFile(t, filepath.Join(nodeDir, FsTarName), "tar-content")

		base, err := ComputeNodeChecksum(nodeDir)
		if err != nil {
			t.Fatalf("base: %v", err)
		}

		nestedChunkDir := filepath.Join(nodeDir, FsTarStagingDirName, FsFileChunksDirName("payload.bin", ".zst"))
		if err := EnsureDir(nestedChunkDir); err != nil {
			t.Fatalf("EnsureDir: %v", err)
		}

		if err := WriteChunkMeta(nestedChunkDir, ChunkMeta{ChunkSize: 100, TotalSize: 1000}); err != nil {
			t.Fatalf("WriteChunkMeta: %v", err)
		}

		after, err := ComputeNodeChecksum(nodeDir)
		if err != nil {
			t.Fatalf("after: %v", err)
		}

		if base.Hex != after.Hex {
			t.Error("chunks.meta under a nested data.tar.d/ chunk dir was unexpectedly included in the checksum")
		}
	})

	t.Run("multi-volume block chunk dir", func(t *testing.T) {
		nodeDir := makeNodeDir(t)
		writeFile(t, filepath.Join(nodeDir, DataDirName, "pvc-a.bin.zst"), "block-content-a")

		base, err := ComputeNodeChecksum(nodeDir)
		if err != nil {
			t.Fatalf("base: %v", err)
		}

		chunkDir := filepath.Join(nodeDir, DataDirName, "pvc-a.bin.d")
		if err := EnsureDir(chunkDir); err != nil {
			t.Fatalf("EnsureDir: %v", err)
		}

		if err := WriteChunkMeta(chunkDir, ChunkMeta{ChunkSize: 100, TotalSize: 1000}); err != nil {
			t.Fatalf("WriteChunkMeta: %v", err)
		}

		after, err := ComputeNodeChecksum(nodeDir)
		if err != nil {
			t.Fatalf("after: %v", err)
		}

		if base.Hex != after.Hex {
			t.Error("chunks.meta under data/<pvc>.bin.d/ was unexpectedly included in the checksum")
		}
	})
}

// TestComputeNodeChecksum_RejectsInvalidBlockPayload verifies that
// ComputeNodeChecksum (via ClassifyBlockPayload, the classifier shared with
// snapimport.BuildPlan) fails deterministically instead of silently picking
// one file when the flat node directory carries an ambiguous or invalid
// data.bin* shape.
func TestComputeNodeChecksum_RejectsInvalidBlockPayload(t *testing.T) {
	t.Run("multiple block files", func(t *testing.T) {
		nodeDir := makeNodeDir(t)
		writeFile(t, filepath.Join(nodeDir, DataBlockName(".zst")), "a")
		writeFile(t, filepath.Join(nodeDir, DataBlockName(".gz")), "b")

		_, err := ComputeNodeChecksum(nodeDir)
		if !errors.Is(err, ErrInvalidBlockPayload) {
			t.Errorf("expected ErrInvalidBlockPayload, got: %v", err)
		}
	})

	t.Run("unknown suffix", func(t *testing.T) {
		nodeDir := makeNodeDir(t)
		writeFile(t, filepath.Join(nodeDir, DataBlockBase+".foo"), "a")

		_, err := ComputeNodeChecksum(nodeDir)
		if !errors.Is(err, ErrInvalidBlockPayload) {
			t.Errorf("expected ErrInvalidBlockPayload, got: %v", err)
		}
	})

	t.Run("block payload coexists with data.tar", func(t *testing.T) {
		nodeDir := makeNodeDir(t)
		writeFile(t, filepath.Join(nodeDir, DataBlockName(".zst")), "a")
		writeFile(t, filepath.Join(nodeDir, FsTarName), "b")

		_, err := ComputeNodeChecksum(nodeDir)
		if !errors.Is(err, ErrInvalidBlockPayload) {
			t.Errorf("expected ErrInvalidBlockPayload, got: %v", err)
		}
	})
}

// TestShortChecksum verifies that ShortChecksum returns the first 8 hex chars.
func TestShortChecksum(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"abcdef0123456789", "abcdef01"},
		{"abcdef01", "abcdef01"},
		{"abcd", "abcd"},
		{"", ""},
	}

	for _, tc := range cases {
		got := ShortChecksum(tc.in)
		if got != tc.want {
			t.Errorf("ShortChecksum(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestWriteReadSnapshotYAML verifies round-trip serialisation of SnapshotYAML.
func TestWriteReadSnapshotYAML(t *testing.T) {
	nodeDir := t.TempDir()

	sy := SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "test-snap",
		Namespace:  "default",
		UID:        "snap-uid-1",
		Checksum: NodeChecksum{
			Algorithm: "sha256",
			Hex:       "deadbeef00112233445566778899aabbccddeeff00112233445566778899aabb",
			Short:     "deadbeef",
		},
	}

	if err := WriteSnapshotYAML(nodeDir, sy); err != nil {
		t.Fatalf("WriteSnapshotYAML: %v", err)
	}

	got, err := ReadSnapshotYAML(nodeDir)
	if err != nil {
		t.Fatalf("ReadSnapshotYAML: %v", err)
	}

	if got.APIVersion != sy.APIVersion {
		t.Errorf("apiVersion: got %q, want %q", got.APIVersion, sy.APIVersion)
	}

	if got.Kind != sy.Kind {
		t.Errorf("kind: got %q, want %q", got.Kind, sy.Kind)
	}

	if got.Name != sy.Name {
		t.Errorf("name: got %q, want %q", got.Name, sy.Name)
	}

	if got.Checksum.Hex != sy.Checksum.Hex {
		t.Errorf("checksum.hex: got %q, want %q", got.Checksum.Hex, sy.Checksum.Hex)
	}

	if got.Checksum.Short != sy.Checksum.Short {
		t.Errorf("checksum.short: got %q, want %q", got.Checksum.Short, sy.Checksum.Short)
	}
}

// TestVerifyNode_OK verifies that VerifyNode returns nil when snapshot.yaml
// matches the current node content.
func TestVerifyNode_OK(t *testing.T) {
	nodeDir := makeNodeDir(t)
	writeFile(t, filepath.Join(nodeDir, ManifestsDirName, "configmap_app.yaml"), "kind: ConfigMap\n")

	cs, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}

	sy := SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "test",
		Checksum:   cs,
	}

	if err := WriteSnapshotYAML(nodeDir, sy); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := VerifyNode(nodeDir); err != nil {
		t.Errorf("VerifyNode: unexpected error: %v", err)
	}
}

// TestVerifyNode_Mismatch verifies that VerifyNode returns ErrChecksumMismatch
// when a manifest is modified after snapshot.yaml was written.
func TestVerifyNode_Mismatch(t *testing.T) {
	nodeDir := makeNodeDir(t)
	mf := filepath.Join(nodeDir, ManifestsDirName, "configmap_app.yaml")
	writeFile(t, mf, "kind: ConfigMap\noriginal\n")

	cs, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}

	sy := SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "test",
		Checksum:   cs,
	}

	if err := WriteSnapshotYAML(nodeDir, sy); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Mutate the manifest after writing snapshot.yaml.
	writeFile(t, mf, "kind: ConfigMap\nmodified\n")

	err = VerifyNode(nodeDir)
	if err == nil {
		t.Fatal("expected ErrChecksumMismatch, got nil")
	}

	if !errors.Is(err, ErrChecksumMismatch) {
		t.Errorf("expected ErrChecksumMismatch, got: %v", err)
	}
}

// TestVerifyNode_Missing verifies that VerifyNode returns ErrSnapshotYAMLMissing
// when snapshot.yaml does not exist.
func TestVerifyNode_Missing(t *testing.T) {
	nodeDir := makeNodeDir(t)

	err := VerifyNode(nodeDir)
	if err == nil {
		t.Fatal("expected ErrSnapshotYAMLMissing, got nil")
	}

	if !errors.Is(err, ErrSnapshotYAMLMissing) {
		t.Errorf("expected ErrSnapshotYAMLMissing, got: %v", err)
	}
}

// TestComputeNodeChecksum_MultiVolumeLayout verifies that the multi-volume layout
// (data/<pvc>.bin.zst for block volumes and data/<pvc>.tar for FS volumes) is
// fully covered by ComputeNodeChecksum.  Staging directories are excluded.
func TestComputeNodeChecksum_MultiVolumeLayout(t *testing.T) {
	nodeDir := makeNodeDir(t)

	// Manifest.
	writeFile(t, filepath.Join(nodeDir, ManifestsDirName, "virtualdisksnapshot_snap.yaml"), "kind: VirtualDiskSnapshot\n")

	// Block-volume PVC-a in multi-volume layout: data/pvc-a.bin.zst.
	writeFile(t, filepath.Join(nodeDir, DataDirName, "pvc-a.bin.zst"), "block-content-a")

	// FS-volume PVC-b in multi-volume layout: data/pvc-b.tar.
	writeFile(t, filepath.Join(nodeDir, DataDirName, "pvc-b.tar"), "tar-content-b")

	c1, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("first compute: %v", err)
	}

	if c1.Algorithm != "sha256" {
		t.Errorf("algorithm: got %q, want sha256", c1.Algorithm)
	}

	if len(c1.Hex) != 64 {
		t.Errorf("hex length: got %d, want 64", len(c1.Hex))
	}

	// Second call must produce the identical digest (determinism / order-independence).
	c2, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("second compute: %v", err)
	}

	if c1.Hex != c2.Hex {
		t.Errorf("non-deterministic: %q vs %q", c1.Hex, c2.Hex)
	}

	// Mutating the block-volume file must change the checksum.
	writeFile(t, filepath.Join(nodeDir, DataDirName, "pvc-a.bin.zst"), "block-content-a-modified")

	c3, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("third compute: %v", err)
	}

	if c1.Hex == c3.Hex {
		t.Error("checksum did not change after mutating data/<pvc>.bin.zst")
	}

	// Restore and mutate the FS-volume tar instead.
	writeFile(t, filepath.Join(nodeDir, DataDirName, "pvc-a.bin.zst"), "block-content-a")
	writeFile(t, filepath.Join(nodeDir, DataDirName, "pvc-b.tar"), "tar-content-b-modified")

	c4, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("fourth compute: %v", err)
	}

	if c1.Hex == c4.Hex {
		t.Error("checksum did not change after mutating data/<pvc>.tar")
	}

	// Staging directory contents must NOT affect the checksum.
	writeFile(t, filepath.Join(nodeDir, DataDirName, "pvc-b.tar.d", "rawfile.txt"), "raw")

	c5, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("fifth compute: %v", err)
	}

	if c4.Hex != c5.Hex {
		t.Error("staging directory data/<pvc>.tar.d/ was unexpectedly included in the checksum")
	}
}

// writeValidBlockNode writes a valid block-data node dir (manifest + data.bin[.<ext>] plus a
// snapshot.yaml with a computed checksum and one well-formed Block VolumeInfo whose volumeMode
// is volumeMode) and returns its path.
func writeValidBlockNode(t *testing.T, ext, volumeMode string) string {
	t.Helper()

	dir := makeNodeDir(t)
	writeFile(t, filepath.Join(dir, ManifestsDirName, "configmap_app.yaml"), "kind: ConfigMap\n")
	writeFile(t, filepath.Join(dir, DataBlockName(ext)), "block-bytes")

	sum, err := ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum: %v", err)
	}

	sy := SnapshotYAML{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "pvc-1",
		Checksum:   sum,
		Volumes: []VolumeInfo{{
			Target:           VolumeObjectRef{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: "pvc-1"},
			Artifact:         VolumeObjectRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshotContent", Name: "c1"},
			VolumeMode:       volumeMode,
			StorageClassName: "sc",
			Size:             "1Gi",
		}},
	}

	if err := WriteSnapshotYAML(dir, sy); err != nil {
		t.Fatalf("WriteSnapshotYAML: %v", err)
	}

	return dir
}

// TestValidateNodeMetadata_ValidBlockNode confirms a well-formed block node passes.
func TestValidateNodeMetadata_ValidBlockNode(t *testing.T) {
	dir := writeValidBlockNode(t, ".zst", VolumeModeBlock)

	if err := ValidateNodeMetadata(dir); err != nil {
		t.Errorf("ValidateNodeMetadata: %v", err)
	}
}

// TestValidateNodeMetadata_MissingSnapshotYAML confirms an absent snapshot.yaml is reported
// as ErrSnapshotYAMLMissing.
func TestValidateNodeMetadata_MissingSnapshotYAML(t *testing.T) {
	dir := makeNodeDir(t)

	if err := ValidateNodeMetadata(dir); !errors.Is(err, ErrSnapshotYAMLMissing) {
		t.Errorf("expected ErrSnapshotYAMLMissing, got: %v", err)
	}
}

// TestValidateNodeMetadata_VolumeModeDisagreesWithPayload confirms a block payload whose
// recorded volumeMode is Filesystem is rejected — the payload kind is derived from disk, not
// trusted from the metadata.
func TestValidateNodeMetadata_VolumeModeDisagreesWithPayload(t *testing.T) {
	dir := writeValidBlockNode(t, ".zst", VolumeModeFilesystem)

	if err := ValidateNodeMetadata(dir); !errors.Is(err, ErrInvalidSnapshotYAML) {
		t.Errorf("expected ErrInvalidSnapshotYAML, got: %v", err)
	}
}

// TestValidateNodeMetadata_InvalidBlockPayload confirms an ambiguous on-disk block payload
// (two recognized block files) surfaces ErrInvalidBlockPayload through ValidateNodeMetadata.
func TestValidateNodeMetadata_InvalidBlockPayload(t *testing.T) {
	dir := writeValidBlockNode(t, ".zst", VolumeModeBlock)
	writeFile(t, filepath.Join(dir, DataBlockName(".gz")), "second")

	if err := ValidateNodeMetadata(dir); !errors.Is(err, ErrInvalidBlockPayload) {
		t.Errorf("expected ErrInvalidBlockPayload, got: %v", err)
	}
}
