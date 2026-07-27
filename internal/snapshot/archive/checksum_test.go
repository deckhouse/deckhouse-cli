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
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

func TestComputeNodeChecksumRejectsNonRegularArchiveArtifacts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, nodeDir string) string
	}{
		{
			name: "manifests directory symlink",
			mutate: func(t *testing.T, nodeDir string) string {
				t.Helper()

				path := filepath.Join(nodeDir, ManifestsDirName)
				if err := os.Remove(path); err != nil {
					t.Fatalf("remove manifests: %v", err)
				}

				outside := filepath.Join(t.TempDir(), ManifestsDirName)
				if err := os.Mkdir(outside, 0o755); err != nil {
					t.Fatalf("mkdir outside manifests: %v", err)
				}

				if err := os.Symlink(outside, path); err != nil {
					t.Fatalf("symlink manifests: %v", err)
				}

				return path
			},
		},
		{
			name: "manifest file symlink",
			mutate: func(t *testing.T, nodeDir string) string {
				t.Helper()

				outside := filepath.Join(t.TempDir(), "outside.yaml")
				writeFile(t, outside, "kind: ConfigMap\n")

				path := filepath.Join(nodeDir, ManifestsDirName, "configmap_outside.yaml")
				if err := os.Symlink(outside, path); err != nil {
					t.Fatalf("symlink manifest: %v", err)
				}

				return path
			},
		},
		{
			name: "legacy data directory symlink",
			mutate: func(t *testing.T, nodeDir string) string {
				t.Helper()

				outside := filepath.Join(t.TempDir(), DataDirName)
				if err := os.Mkdir(outside, 0o755); err != nil {
					t.Fatalf("mkdir outside data: %v", err)
				}

				path := filepath.Join(nodeDir, DataDirName)
				if err := os.Symlink(outside, path); err != nil {
					t.Fatalf("symlink data: %v", err)
				}

				return path
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodeDir := makeNodeDir(t)
			path := tc.mutate(t, nodeDir)

			_, err := ComputeNodeChecksum(nodeDir)
			if !errors.Is(err, ErrNonRegularArchiveArtifact) {
				t.Fatalf("ComputeNodeChecksum error = %v, want ErrNonRegularArchiveArtifact", err)
			}

			if !strings.Contains(err.Error(), path) {
				t.Errorf("error %q does not contain offending path %q", err, path)
			}
		})
	}
}

func TestComputeNodeChecksumLegacyParentReplacementCannotHashOutside(t *testing.T) {
	container := t.TempDir()
	nodeDir := filepath.Join(container, "node")
	if err := os.MkdirAll(filepath.Join(nodeDir, ManifestsDirName), 0o755); err != nil {
		t.Fatalf("mkdir manifests: %v", err)
	}

	dataDir := filepath.Join(nodeDir, DataDirName)
	if err := os.Mkdir(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}

	dataPath := filepath.Join(dataDir, "pvc.bin")
	writeFile(t, dataPath, "inside")

	outside := filepath.Join(t.TempDir(), DataDirName)
	if err := os.Mkdir(outside, 0o755); err != nil {
		t.Fatalf("mkdir outside data: %v", err)
	}
	writeFile(t, filepath.Join(outside, "pvc.bin"), "escaped")

	replaced := false
	source, err := OpenRootedSourceWithHook(nodeDir, func(path string) {
		if replaced || path != dataPath {
			return
		}

		replaced = true
		original := dataDir + ".pinned-original"
		if renameErr := os.Rename(dataDir, original); renameErr != nil {
			t.Fatalf("rename data directory: %v", renameErr)
		}

		if symlinkErr := os.Symlink(outside, dataDir); symlinkErr != nil {
			t.Fatalf("symlink data directory: %v", symlinkErr)
		}

		t.Cleanup(func() {
			_ = os.Remove(dataDir)
			_ = os.Rename(original, dataDir)
		})
	})
	if err != nil {
		t.Fatalf("OpenRootedSourceWithHook: %v", err)
	}
	defer func() { _ = source.Close() }()

	_, err = computeNodeChecksum(source)
	if !replaced {
		t.Fatalf("boundary hook for %s was not reached", dataPath)
	}

	if !errors.Is(err, ErrNonRegularArchiveArtifact) {
		t.Fatalf("computeNodeChecksum error = %v, want ErrNonRegularArchiveArtifact", err)
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

func TestLegacySnapshotCompatibilityPolicyAcrossVerifiers(t *testing.T) {
	nodeDir := makeLegacyNodeDir(t)
	options := SnapshotYAMLReadOptions{AllowUnauthenticatedLegacy: true}

	if err := VerifyNode(nodeDir); !errors.Is(err, ErrLegacySnapshotFormat) {
		t.Fatalf("VerifyNode error = %v, want ErrLegacySnapshotFormat", err)
	}

	if err := VerifyNodeWithOptions(nodeDir, options); err != nil {
		t.Fatalf("VerifyNodeWithOptions: %v", err)
	}

	defaultView, err := OpenVerifiedArchive(nodeDir)
	if err != nil {
		t.Fatalf("OpenVerifiedArchive: %v", err)
	}

	if _, err := defaultView.VerifyNode(context.Background(), nodeDir); !errors.Is(err, ErrLegacySnapshotFormat) {
		_ = defaultView.Close()
		t.Fatalf("default VerifiedArchive.VerifyNode error = %v, want ErrLegacySnapshotFormat", err)
	}

	if err := defaultView.Close(); err != nil {
		t.Fatalf("close default verified archive: %v", err)
	}

	legacyView, err := OpenVerifiedArchiveWithOptions(nodeDir, options)
	if err != nil {
		t.Fatalf("OpenVerifiedArchiveWithOptions: %v", err)
	}

	if _, err := legacyView.VerifyNode(context.Background(), nodeDir); err != nil {
		_ = legacyView.Close()
		t.Fatalf("legacy VerifiedArchive.VerifyNode: %v", err)
	}

	if err := legacyView.Close(); err != nil {
		t.Fatalf("close legacy verified archive: %v", err)
	}

	destination, err := OpenRootedDestination(nodeDir, nil)
	if err != nil {
		t.Fatalf("OpenRootedDestination: %v", err)
	}
	defer func() { _ = destination.Close() }()

	if _, err := destination.ReadSnapshotYAML("."); !errors.Is(err, ErrLegacySnapshotFormat) {
		t.Fatalf("RootedDestination.ReadSnapshotYAML error = %v, want ErrLegacySnapshotFormat", err)
	}

	if _, err := destination.ReadSnapshotYAMLWithOptions(".", options); err != nil {
		t.Fatalf("RootedDestination.ReadSnapshotYAMLWithOptions: %v", err)
	}

	if err := destination.VerifyNode("."); !errors.Is(err, ErrLegacySnapshotFormat) {
		t.Fatalf("RootedDestination.VerifyNode error = %v, want ErrLegacySnapshotFormat", err)
	}

	if err := destination.VerifyNodeWithOptions(".", options); err != nil {
		t.Fatalf("RootedDestination.VerifyNodeWithOptions: %v", err)
	}
}

func TestResumeRejectsLegacySnapshotMetadata(t *testing.T) {
	nodeDir := makeLegacyNodeDir(t)
	id := NodeIdentity{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "legacy",
	}

	_, err := ScanAbsolute(nodeDir, id)
	if !errors.Is(err, ErrLegacySnapshotFormat) {
		t.Fatalf("ScanAbsolute error = %v, want ErrLegacySnapshotFormat", err)
	}

	destination, err := OpenRootedDestination(nodeDir, nil)
	if err != nil {
		t.Fatalf("OpenRootedDestination: %v", err)
	}
	defer func() { _ = destination.Close() }()

	_, err = ScanAbsoluteRootedContext(context.Background(), destination, ".", id)
	if !errors.Is(err, ErrLegacySnapshotFormat) {
		t.Fatalf("ScanAbsoluteRootedContext error = %v, want ErrLegacySnapshotFormat", err)
	}
}

func makeLegacyNodeDir(t *testing.T) string {
	t.Helper()

	nodeDir := makeNodeDir(t)
	checksum, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum: %v", err)
	}

	childrenChecksum := EmptyChildrenChecksum()
	data := fmt.Sprintf(`apiVersion: state-snapshotter.deckhouse.io/v1alpha1
kind: Snapshot
name: legacy
checksum:
  algorithm: %s
  hex: %s
  short: %s
childrenChecksum:
  algorithm: %s
  hex: %s
  short: %s
`, checksum.Algorithm, checksum.Hex, checksum.Short,
		childrenChecksum.Algorithm, childrenChecksum.Hex, childrenChecksum.Short)
	if err := os.WriteFile(filepath.Join(nodeDir, SnapshotYAMLName), []byte(data), 0o600); err != nil {
		t.Fatalf("write legacy snapshot.yaml: %v", err)
	}

	return nodeDir
}

func TestVerifyNodeRejectsSemanticSnapshotYAMLTampering(t *testing.T) {
	tests := []struct {
		name string
		old  string
		new  string
	}{
		{name: "node identity", old: "archive-node-original", new: "archive-node-tampered"},
		{name: "source object reference", old: "source-original", new: "source-tampered"},
		{name: "storage class", old: "storage-original", new: "storage-tampered"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodeDir := makeNodeDir(t)
			writeFile(t, filepath.Join(nodeDir, DataBlockName("")), "block-data")

			checksum, err := ComputeNodeChecksum(nodeDir)
			if err != nil {
				t.Fatalf("compute checksum: %v", err)
			}

			snapshot := SnapshotYAML{
				APIVersion: "snapshot.example.io/v1",
				Kind:       "DiskSnapshot",
				Name:       "archive-node-original",
				SourceObjectRef: &SourceObjectRef{
					APIVersion: "storage.example.io/v1",
					Kind:       "Disk",
					Name:       "source-original",
				},
				Checksum: checksum,
				Volumes: []VolumeInfo{{
					Target: VolumeObjectRef{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Name:       "source-pvc",
					},
					Artifact: VolumeObjectRef{
						APIVersion: "snapshot.storage.k8s.io/v1",
						Kind:       "VolumeSnapshotContent",
						Name:       "source-content",
					},
					VolumeMode:       VolumeModeBlock,
					StorageClassName: "storage-original",
					Size:             "1Gi",
				}},
			}

			if err := WriteSnapshotYAML(nodeDir, snapshot); err != nil {
				t.Fatalf("write snapshot.yaml: %v", err)
			}

			path := filepath.Join(nodeDir, SnapshotYAMLName)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read snapshot.yaml: %v", err)
			}

			tampered := bytes.Replace(data, []byte(tt.old), []byte(tt.new), 1)
			if bytes.Equal(tampered, data) {
				t.Fatalf("snapshot.yaml does not contain mutation source %q", tt.old)
			}

			if err := os.WriteFile(path, tampered, 0o644); err != nil {
				t.Fatalf("tamper snapshot.yaml: %v", err)
			}

			err = VerifyNode(nodeDir)
			if !errors.Is(err, ErrSnapshotMetadataChecksumMismatch) {
				t.Fatalf("VerifyNode error = %v, want ErrSnapshotMetadataChecksumMismatch", err)
			}
		})
	}
}

func TestVerifiedArchiveHandleSurvivesReplacementAndDetectsNamespaceChange(t *testing.T) {
	nodeDir := makeNodeDir(t)
	manifestPath := filepath.Join(nodeDir, ManifestsDirName, "configmap_app.yaml")
	verifiedBytes := "kind: ConfigMap\nmetadata:\n  name: verified\n"
	writeFile(t, manifestPath, verifiedBytes)

	checksum, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("compute checksum: %v", err)
	}

	if err := WriteSnapshotYAML(nodeDir, SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "test",
		Checksum:   checksum,
	}); err != nil {
		t.Fatalf("write snapshot metadata: %v", err)
	}

	view, err := OpenVerifiedArchive(nodeDir)
	if err != nil {
		t.Fatalf("open verified archive: %v", err)
	}
	defer func() { _ = view.Close() }()

	node, err := view.VerifyNode(context.Background(), nodeDir)
	if err != nil {
		t.Fatalf("verify node: %v", err)
	}

	expected, ok := node.File(filepath.Join(ManifestsDirName, "configmap_app.yaml"))
	if !ok {
		t.Fatal("verified manifest is absent")
	}

	handle, err := view.OpenVerifiedFile(context.Background(), expected)
	if err != nil {
		t.Fatalf("open verified file: %v", err)
	}
	defer func() { _ = handle.Close() }()

	if err := os.Rename(manifestPath, manifestPath+".verified"); err != nil {
		t.Fatalf("move verified manifest: %v", err)
	}

	writeFile(t, manifestPath, "kind: Secret\nmetadata:\n  name: replacement\n")

	data, err := io.ReadAll(handle)
	if err != nil {
		t.Fatalf("read pinned handle: %v", err)
	}

	if string(data) != verifiedBytes {
		t.Fatalf("pinned bytes = %q, want %q", data, verifiedBytes)
	}

	if err := handle.Verify(context.Background()); !errors.Is(err, ErrVerifiedArchiveChanged) {
		t.Fatalf("Verify error = %v, want ErrVerifiedArchiveChanged", err)
	}
}

func TestVerifiedHandleRejectsMutateUseRestoreBeforeExposingBytes(t *testing.T) {
	tests := []struct {
		name       string
		writerPath func(t *testing.T, payloadPath string) string
		read       func(handle *VerifiedHandle, buffer []byte) (int, error)
	}{
		{
			name: "same inode through archive path with ReadAt",
			writerPath: func(t *testing.T, payloadPath string) string {
				t.Helper()

				return payloadPath
			},
			read: func(handle *VerifiedHandle, buffer []byte) (int, error) {
				return handle.ReadAt(buffer, authChunkSize+17)
			},
		},
		{
			name: "external hardlink with Seek and Read",
			writerPath: func(t *testing.T, payloadPath string) string {
				t.Helper()

				linkPath := filepath.Join(t.TempDir(), "external-hardlink")
				if err := os.Link(payloadPath, linkPath); err != nil {
					t.Fatalf("create external hardlink: %v", err)
				}

				return linkPath
			},
			read: func(handle *VerifiedHandle, buffer []byte) (int, error) {
				if _, err := handle.Seek(authChunkSize+17, io.SeekStart); err != nil {
					return 0, err
				}

				return handle.Read(buffer)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodeDir := makeNodeDir(t)
			payloadPath := filepath.Join(nodeDir, DataBlockName(""))
			payload := []byte(strings.Repeat("authenticated-payload-", authChunkSize/10+100))

			if err := os.WriteFile(payloadPath, payload, 0o600); err != nil {
				t.Fatalf("write payload: %v", err)
			}

			checksum, err := ComputeNodeChecksum(nodeDir)
			if err != nil {
				t.Fatalf("compute checksum: %v", err)
			}

			if err := WriteSnapshotYAML(nodeDir, SnapshotYAML{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshot",
				Name:       "pvc",
				Checksum:   checksum,
				Volumes: []VolumeInfo{{
					Target: VolumeObjectRef{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Name:       "pvc",
					},
					Artifact: VolumeObjectRef{
						APIVersion: "snapshot.storage.k8s.io/v1",
						Kind:       "VolumeSnapshotContent",
						Name:       "pvc-content",
					},
					VolumeMode:       VolumeModeBlock,
					StorageClassName: "test",
					Size:             "2Mi",
				}},
			}); err != nil {
				t.Fatalf("write snapshot metadata: %v", err)
			}

			view, err := OpenVerifiedArchive(nodeDir)
			if err != nil {
				t.Fatalf("open verified archive: %v", err)
			}

			node, err := view.VerifyNode(context.Background(), nodeDir)
			if err != nil {
				t.Fatalf("verify node: %v", err)
			}

			expected, ok := node.File(DataBlockName(""))
			if !ok {
				t.Fatal("verified payload is absent")
			}

			handle, err := view.OpenVerifiedFile(context.Background(), expected)
			if err != nil {
				t.Fatalf("open verified payload: %v", err)
			}

			info, err := os.Stat(payloadPath)
			if err != nil {
				t.Fatalf("inspect payload: %v", err)
			}

			writerPath := tc.writerPath(t, payloadPath)
			writer, err := os.OpenFile(writerPath, os.O_WRONLY, 0)
			if err != nil {
				t.Fatalf("open mutation writer: %v", err)
			}

			const mutationOffset = authChunkSize + 17

			original := append([]byte(nil), payload[mutationOffset:mutationOffset+32]...)
			changed := bytes.Repeat([]byte{0xA5}, len(original))
			if _, err := writer.WriteAt(changed, mutationOffset); err != nil {
				t.Fatalf("mutate payload: %v", err)
			}

			buffer := make([]byte, len(original))
			count, readErr := tc.read(handle, buffer)

			if _, err := writer.WriteAt(original, mutationOffset); err != nil {
				t.Fatalf("restore payload: %v", err)
			}

			if err := writer.Close(); err != nil {
				t.Fatalf("close mutation writer: %v", err)
			}

			if err := os.Chtimes(payloadPath, info.ModTime(), info.ModTime()); err != nil {
				t.Fatalf("restore payload timestamps: %v", err)
			}

			if count != 0 {
				t.Fatalf("read returned %d unauthenticated bytes, want 0", count)
			}

			if !errors.Is(readErr, ErrVerifiedArchiveChanged) {
				t.Fatalf("read error = %v, want ErrVerifiedArchiveChanged", readErr)
			}

			if err := handle.Verify(context.Background()); !errors.Is(err, ErrVerifiedArchiveChanged) {
				t.Fatalf("Verify after restore = %v, want sticky ErrVerifiedArchiveChanged", err)
			}

			indexPath := handle.indexPath
			if err := handle.Close(); err != nil {
				t.Fatalf("close verified payload: %v", err)
			}

			if _, err := os.Stat(indexPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("authentication index remains after handle close: %v", err)
			}

			authDir := view.authDir
			if err := view.Close(); err != nil {
				t.Fatalf("close verified archive: %v", err)
			}

			if _, err := os.Stat(authDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("private authentication directory remains after close: %v", err)
			}
		})
	}
}

func TestVerifiedHandleReusesImmutableCacheAndRejectsBoundaryMutation(t *testing.T) {
	nodeDir := makeNodeDir(t)
	payloadPath := filepath.Join(nodeDir, DataBlockName(""))
	payload := bytes.Repeat([]byte("authenticated-boundary-data"), authChunkSize/8+1)

	if err := os.WriteFile(payloadPath, payload, 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	checksum, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("compute checksum: %v", err)
	}

	if err := WriteSnapshotYAML(nodeDir, SnapshotYAML{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "pvc",
		Checksum:   checksum,
		Volumes: []VolumeInfo{{
			Target: VolumeObjectRef{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc",
			},
			Artifact: VolumeObjectRef{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshotContent",
				Name:       "pvc-content",
			},
			VolumeMode:       VolumeModeBlock,
			StorageClassName: "test",
			Size:             "2Mi",
		}},
	}); err != nil {
		t.Fatalf("write snapshot metadata: %v", err)
	}

	view, err := OpenVerifiedArchive(nodeDir)
	if err != nil {
		t.Fatalf("open verified archive: %v", err)
	}
	defer func() { _ = view.Close() }()

	node, err := view.VerifyNode(context.Background(), nodeDir)
	if err != nil {
		t.Fatalf("verify node: %v", err)
	}

	expected, ok := node.File(DataBlockName(""))
	if !ok {
		t.Fatal("verified payload is absent")
	}

	handle, err := view.OpenVerifiedFile(context.Background(), expected)
	if err != nil {
		t.Fatalf("open verified payload: %v", err)
	}
	defer func() { _ = handle.Close() }()

	hardlinkPath := filepath.Join(t.TempDir(), "payload-hardlink")
	if err := os.Link(payloadPath, hardlinkPath); err != nil {
		t.Fatalf("create payload hardlink: %v", err)
	}

	writer, err := os.OpenFile(hardlinkPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open hardlink writer: %v", err)
	}
	defer func() { _ = writer.Close() }()

	const cachedOffset = authChunkSize - 64

	originalCached := append([]byte(nil), payload[cachedOffset:cachedOffset+32]...)
	first := make([]byte, len(originalCached))
	if _, err := handle.ReadAt(first, cachedOffset); err != nil {
		t.Fatalf("prime authenticated cache: %v", err)
	}

	changedCached := bytes.Repeat([]byte{0xA5}, len(originalCached))
	if _, err := writer.WriteAt(changedCached, cachedOffset); err != nil {
		t.Fatalf("mutate cached range: %v", err)
	}

	reused := make([]byte, len(originalCached))
	if _, err := handle.ReadAt(reused, cachedOffset); err != nil {
		t.Fatalf("read cached range after mutation: %v", err)
	}

	if !bytes.Equal(reused, originalCached) {
		t.Fatalf("cached bytes = %x, want verified original %x", reused, originalCached)
	}

	if _, err := writer.WriteAt(originalCached, cachedOffset); err != nil {
		t.Fatalf("restore cached range: %v", err)
	}

	originalBoundary := payload[authChunkSize]
	if _, err := writer.WriteAt([]byte{originalBoundary ^ 0xFF}, authChunkSize); err != nil {
		t.Fatalf("mutate next authentication chunk: %v", err)
	}

	crossing := make([]byte, 2)
	count, readErr := handle.ReadAt(crossing, authChunkSize-1)
	if count != 1 {
		t.Fatalf("boundary read returned %d bytes, want only the verified prefix byte", count)
	}

	if crossing[0] != payload[authChunkSize-1] {
		t.Fatalf("boundary prefix = %#x, want %#x", crossing[0], payload[authChunkSize-1])
	}

	if !errors.Is(readErr, ErrVerifiedArchiveChanged) {
		t.Fatalf("boundary read error = %v, want ErrVerifiedArchiveChanged", readErr)
	}

	if _, err := writer.WriteAt([]byte{originalBoundary}, authChunkSize); err != nil {
		t.Fatalf("restore next authentication chunk: %v", err)
	}

	if err := handle.Verify(context.Background()); !errors.Is(err, ErrVerifiedArchiveChanged) {
		t.Fatalf("Verify after boundary restore = %v, want sticky ErrVerifiedArchiveChanged", err)
	}

	stats := handle.AuthenticatedReadStats()
	if stats.SourceBytes != 2*stats.ChunkSize || stats.HashedBytes != stats.SourceBytes {
		t.Fatalf("authenticated work = %+v, want exactly two full chunk reads and hashes", stats)
	}

	if stats.ChunkLoads != 2 || stats.CacheHits < 2 {
		t.Fatalf("authenticated cache stats = %+v, want two loads and at least two hits", stats)
	}
}

func TestPrePostOnlyRehashDoesNotDetectMutateUseRestore(t *testing.T) {
	payloadPath := filepath.Join(t.TempDir(), "payload")
	original := bytes.Repeat([]byte("pre-post-only-authentication-gap"), 4096)
	if err := os.WriteFile(payloadPath, original, 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	hardlinkPath := filepath.Join(t.TempDir(), "external-hardlink")
	if err := os.Link(payloadPath, hardlinkPath); err != nil {
		t.Fatalf("create external hardlink: %v", err)
	}

	reader, err := os.Open(payloadPath)
	if err != nil {
		t.Fatalf("open payload reader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	info, err := reader.Stat()
	if err != nil {
		t.Fatalf("inspect payload: %v", err)
	}

	before, err := hashReaderAtContext(context.Background(), reader, info.Size())
	if err != nil {
		t.Fatalf("pre-consumption hash: %v", err)
	}

	writer, err := os.OpenFile(hardlinkPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open hardlink writer: %v", err)
	}

	const offset = 4096

	changed := bytes.Repeat([]byte{0xA5}, 32)
	if _, err := writer.WriteAt(changed, offset); err != nil {
		t.Fatalf("mutate payload: %v", err)
	}

	consumed := make([]byte, len(changed))
	if _, err := reader.ReadAt(consumed, offset); err != nil {
		t.Fatalf("consume mutated payload: %v", err)
	}

	if _, err := writer.WriteAt(original[offset:offset+len(changed)], offset); err != nil {
		t.Fatalf("restore payload: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close hardlink writer: %v", err)
	}

	if err := os.Chtimes(payloadPath, info.ModTime(), info.ModTime()); err != nil {
		t.Fatalf("restore payload timestamp: %v", err)
	}

	after, err := hashReaderAtContext(context.Background(), reader, info.Size())
	if err != nil {
		t.Fatalf("post-consumption hash: %v", err)
	}

	infoAfter, err := reader.Stat()
	if err != nil {
		t.Fatalf("inspect restored payload: %v", err)
	}

	if before != after || !sameVerifiedInfo(info, infoAfter) {
		t.Fatal("pre/post-only baseline unexpectedly detected the restored mutation")
	}

	if !bytes.Equal(consumed, changed) {
		t.Fatalf("baseline consumed %x, want mutated bytes %x", consumed, changed)
	}
}

func TestVerifiedHandleCancellationIsStickyAndExposesNoBytes(t *testing.T) {
	nodeDir := makeNodeDir(t)
	payloadPath := filepath.Join(nodeDir, DataBlockName(""))
	writeFile(t, payloadPath, strings.Repeat("cancel-authenticated-read", 1024))

	checksum, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("compute checksum: %v", err)
	}

	if err := WriteSnapshotYAML(nodeDir, SnapshotYAML{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "pvc",
		Checksum:   checksum,
		Volumes: []VolumeInfo{{
			Target: VolumeObjectRef{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc",
			},
			Artifact: VolumeObjectRef{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshotContent",
				Name:       "pvc-content",
			},
			VolumeMode:       VolumeModeBlock,
			StorageClassName: "test",
			Size:             "1Mi",
		}},
	}); err != nil {
		t.Fatalf("write snapshot metadata: %v", err)
	}

	view, err := OpenVerifiedArchive(nodeDir)
	if err != nil {
		t.Fatalf("open verified archive: %v", err)
	}
	defer func() { _ = view.Close() }()

	node, err := view.VerifyNode(context.Background(), nodeDir)
	if err != nil {
		t.Fatalf("verify node: %v", err)
	}

	expected, ok := node.File(DataBlockName(""))
	if !ok {
		t.Fatal("verified payload is absent")
	}

	ctx, cancel := context.WithCancel(context.Background())
	handle, err := view.OpenVerifiedFile(ctx, expected)
	if err != nil {
		t.Fatalf("open verified payload: %v", err)
	}
	defer func() { _ = handle.Close() }()

	cancel()

	buffer := make([]byte, 32)
	count, readErr := handle.Read(buffer)
	if count != 0 {
		t.Fatalf("read returned %d bytes after cancellation, want 0", count)
	}

	if !errors.Is(readErr, context.Canceled) {
		t.Fatalf("read error = %v, want context.Canceled", readErr)
	}

	if err := handle.Verify(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Verify after canceled read = %v, want sticky context.Canceled", err)
	}
}

func TestVerifiedHandleLargePayloadMemoryAndIndexAreBounded(t *testing.T) {
	const payloadSize = 128 * 1024 * 1024

	nodeDir := makeNodeDir(t)
	payloadPath := filepath.Join(nodeDir, DataBlockName(""))

	payload, err := os.Create(payloadPath)
	if err != nil {
		t.Fatalf("create sparse payload: %v", err)
	}

	if err := payload.Truncate(payloadSize); err != nil {
		t.Fatalf("truncate sparse payload: %v", err)
	}

	if err := payload.Close(); err != nil {
		t.Fatalf("close sparse payload: %v", err)
	}

	checksum, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("compute checksum: %v", err)
	}

	if err := WriteSnapshotYAML(nodeDir, SnapshotYAML{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "pvc",
		Checksum:   checksum,
		Volumes: []VolumeInfo{{
			Target: VolumeObjectRef{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc",
			},
			Artifact: VolumeObjectRef{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshotContent",
				Name:       "pvc-content",
			},
			VolumeMode:       VolumeModeBlock,
			StorageClassName: "test",
			Size:             "128Mi",
		}},
	}); err != nil {
		t.Fatalf("write snapshot metadata: %v", err)
	}

	view, err := OpenVerifiedArchive(nodeDir)
	if err != nil {
		t.Fatalf("open verified archive: %v", err)
	}
	defer func() { _ = view.Close() }()

	node, err := view.VerifyNode(context.Background(), nodeDir)
	if err != nil {
		t.Fatalf("verify node: %v", err)
	}

	expected, ok := node.File(DataBlockName(""))
	if !ok {
		t.Fatal("verified payload is absent")
	}

	runtime.GC()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	handle, err := view.OpenVerifiedFile(context.Background(), expected)
	if err != nil {
		t.Fatalf("open verified payload: %v", err)
	}
	defer func() { _ = handle.Close() }()

	runtime.GC()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	const maxHeapGrowth = 16 * 1024 * 1024

	if after.HeapAlloc > before.HeapAlloc+maxHeapGrowth {
		t.Fatalf("heap grew by %d bytes for a %d-byte payload, limit is %d",
			after.HeapAlloc-before.HeapAlloc, payloadSize, maxHeapGrowth)
	}

	indexInfo, err := os.Stat(handle.indexPath)
	if err != nil {
		t.Fatalf("inspect authentication index: %v", err)
	}

	recordCount, err := authIndexRecordCount(payloadSize)
	if err != nil {
		t.Fatalf("calculate authentication record count: %v", err)
	}

	expectedIndexSize := recordCount * authIndexRecordSize
	if indexInfo.Size() != expectedIndexSize {
		t.Fatalf("authentication index size = %d, want %d", indexInfo.Size(), expectedIndexSize)
	}

	if len(handle.cache) != authChunkSize {
		t.Fatalf("authentication cache size = %d, want fixed %d", len(handle.cache), authChunkSize)
	}
}

func TestVerifiedArchiveVerificationHonorsCancellation(t *testing.T) {
	nodeDir := makeNodeDir(t)
	writeFile(t, filepath.Join(nodeDir, ManifestsDirName, "configmap_app.yaml"), strings.Repeat("x", 1024*1024))

	checksum, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("compute checksum: %v", err)
	}

	if err := WriteSnapshotYAML(nodeDir, SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "test",
		Checksum:   checksum,
	}); err != nil {
		t.Fatalf("write snapshot metadata: %v", err)
	}

	view, err := OpenVerifiedArchive(nodeDir)
	if err != nil {
		t.Fatalf("open verified archive: %v", err)
	}
	defer func() { _ = view.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := view.VerifyNode(ctx, nodeDir); !errors.Is(err, context.Canceled) {
		t.Fatalf("VerifyNode error = %v, want context.Canceled", err)
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

// validChildChecksum returns a syntactically valid NodeChecksum seeded from content, for use
// as a ChildCommitment's NodeChecksum/ChildrenChecksum in unit tests that never touch disk.
func validChildChecksum(content string) NodeChecksum {
	hex := fmt.Sprintf("%064x", sha256.Sum256([]byte(content)))

	return NodeChecksum{Algorithm: ChecksumAlgorithmSHA256, Hex: hex, Short: ShortChecksum(hex)}
}

// TestComputeChildrenChecksum_DeterministicAndOrderIndependent proves ComputeChildrenChecksum
// canonicalizes its input: the same child set in any input order must hash identically, and
// changing any single committed field (digest or identity) must change the result (AC-1).
func TestComputeChildrenChecksum_DeterministicAndOrderIndependent(t *testing.T) {
	a := ChildCommitment{
		APIVersion: "v1", Kind: "Snapshot", Name: "a", Namespace: "ns",
		NodeChecksum: validChildChecksum("a-node"), ChildrenChecksum: validChildChecksum("a-children"),
	}
	b := ChildCommitment{
		APIVersion: "v1", Kind: "Snapshot", Name: "b", Namespace: "ns",
		NodeChecksum: validChildChecksum("b-node"), ChildrenChecksum: validChildChecksum("b-children"),
	}

	forward, err := ComputeChildrenChecksum([]ChildCommitment{a, b})
	if err != nil {
		t.Fatalf("ComputeChildrenChecksum(a,b): %v", err)
	}

	reversed, err := ComputeChildrenChecksum([]ChildCommitment{b, a})
	if err != nil {
		t.Fatalf("ComputeChildrenChecksum(b,a): %v", err)
	}

	if forward.Hex != reversed.Hex {
		t.Errorf("checksum depends on input order: %q vs %q", forward.Hex, reversed.Hex)
	}

	tamperedDigest := a
	tamperedDigest.NodeChecksum = validChildChecksum("a-node-tampered")

	changed, err := ComputeChildrenChecksum([]ChildCommitment{tamperedDigest, b})
	if err != nil {
		t.Fatalf("ComputeChildrenChecksum(tampered,b): %v", err)
	}

	if changed.Hex == forward.Hex {
		t.Error("checksum did not change when a committed child digest changed")
	}

	empty, err := ComputeChildrenChecksum(nil)
	if err != nil {
		t.Fatalf("ComputeChildrenChecksum(nil): %v", err)
	}

	if empty.Hex != EmptyChildrenChecksum().Hex {
		t.Errorf("ComputeChildrenChecksum(nil) = %q, want EmptyChildrenChecksum %q", empty.Hex, EmptyChildrenChecksum().Hex)
	}

	if forward.Hex == empty.Hex {
		t.Error("non-empty child set must not collide with the empty commitment")
	}
}

// TestComputeChildrenChecksum_RejectsDuplicateIdentity proves two commitments sharing the full
// (apiVersion, kind, namespace, name, uid) identity tuple are rejected — a hybrid tree cannot
// smuggle two conflicting records claiming to be the same child (AC-1 canonicalization).
func TestComputeChildrenChecksum_RejectsDuplicateIdentity(t *testing.T) {
	dup := ChildCommitment{
		APIVersion: "v1", Kind: "Snapshot", Name: "dup", Namespace: "ns",
		NodeChecksum: validChildChecksum("first"), ChildrenChecksum: EmptyChildrenChecksum(),
	}
	other := dup
	other.NodeChecksum = validChildChecksum("second")

	if _, err := ComputeChildrenChecksum([]ChildCommitment{dup, other}); !errors.Is(err, ErrInvalidSnapshotYAML) {
		t.Errorf("ComputeChildrenChecksum with duplicate identity: got %v, want ErrInvalidSnapshotYAML", err)
	}
}

// TestComputeChildrenChecksum_RejectsIncompleteIdentity proves a commitment missing any of
// apiVersion/kind/name is rejected rather than silently hashed with an ambiguous identity.
func TestComputeChildrenChecksum_RejectsIncompleteIdentity(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(c *ChildCommitment)
		wantErr error
	}{
		{name: "missing apiVersion", mutate: func(c *ChildCommitment) { c.APIVersion = "" }, wantErr: ErrInvalidSnapshotYAML},
		{name: "missing kind", mutate: func(c *ChildCommitment) { c.Kind = "" }, wantErr: ErrInvalidSnapshotYAML},
		{name: "missing name", mutate: func(c *ChildCommitment) { c.Name = "" }, wantErr: ErrInvalidSnapshotYAML},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ChildCommitment{
				APIVersion: "v1", Kind: "Snapshot", Name: "child", Namespace: "ns",
				NodeChecksum: validChildChecksum("node"), ChildrenChecksum: EmptyChildrenChecksum(),
			}
			tt.mutate(&c)

			if _, err := ComputeChildrenChecksum([]ChildCommitment{c}); !errors.Is(err, tt.wantErr) {
				t.Errorf("got %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// TestComputeChildrenChecksum_RejectsMalformedChecksum proves a commitment whose NodeChecksum
// or ChildrenChecksum fails validateChecksum (wrong algorithm, wrong hex length, inconsistent
// short form) is rejected rather than silently hashed into the parent's commitment.
func TestComputeChildrenChecksum_RejectsMalformedChecksum(t *testing.T) {
	base := func() ChildCommitment {
		return ChildCommitment{
			APIVersion: "v1", Kind: "Snapshot", Name: "child", Namespace: "ns",
			NodeChecksum: validChildChecksum("node"), ChildrenChecksum: EmptyChildrenChecksum(),
		}
	}

	tests := []struct {
		name   string
		mutate func(c *ChildCommitment)
	}{
		{name: "bad node checksum algorithm", mutate: func(c *ChildCommitment) { c.NodeChecksum.Algorithm = "md5" }},
		{name: "bad node checksum hex length", mutate: func(c *ChildCommitment) { c.NodeChecksum.Hex = "abc" }},
		{name: "bad children checksum algorithm", mutate: func(c *ChildCommitment) { c.ChildrenChecksum.Algorithm = "md5" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := base()
			tt.mutate(&c)

			if _, err := ComputeChildrenChecksum([]ChildCommitment{c}); !errors.Is(err, ErrInvalidSnapshotYAML) {
				t.Errorf("got %v, want ErrInvalidSnapshotYAML", err)
			}
		})
	}
}

// finalizeNodeForChildrenChecksumTest writes a minimal, fully-finalized node directory (own
// NodeChecksum and ChildrenChecksum both authenticated) under parent/snapshots/<name>, mirroring
// the bottom-up finalize contract every production writer follows.
func finalizeNodeForChildrenChecksumTest(t *testing.T, parentSnapshotsDir, name string) string {
	t.Helper()

	dir := filepath.Join(parentSnapshotsDir, name)
	if err := os.MkdirAll(filepath.Join(dir, ManifestsDirName), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}

	checksum, err := ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("compute node checksum for %s: %v", dir, err)
	}

	childrenChecksum := EmptyChildrenChecksum()

	if err := WriteSnapshotYAML(dir, SnapshotYAML{
		APIVersion: "v1", Kind: "Snapshot", Name: name,
		Checksum: checksum, ChildrenChecksum: &childrenChecksum,
	}); err != nil {
		t.Fatalf("write snapshot.yaml for %s: %v", dir, err)
	}

	return dir
}

// TestComputeNodeChildrenChecksum_RejectsDirectChildCountBeyondBound exercises the production
// scan/commitment path (ComputeNodeChildrenChecksum, the function volume.finalizeNodeContext
// and archive verification both call) with more direct children than maxDirectChildren, proving
// it fails deterministically without reading every child's metadata (the bound is enforced by
// the bounded directory read itself, before any per-child snapshot.yaml is opened) — the
// explicit resource bound required by AC-1.
func TestComputeNodeChildrenChecksum_RejectsDirectChildCountBeyondBound(t *testing.T) {
	nodeDir := t.TempDir()
	snapshotsDir := filepath.Join(nodeDir, SnapshotsDirName)

	if err := os.MkdirAll(snapshotsDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", snapshotsDir, err)
	}

	// One over the bound: enough to prove the bound is enforced, without materializing a
	// second child's worth of extra directories unnecessarily.
	for i := range maxDirectChildren + 1 {
		if err := os.Mkdir(filepath.Join(snapshotsDir, fmt.Sprintf("snapshot_child-%05d", i)), 0o755); err != nil {
			t.Fatalf("mkdir child %d: %v", i, err)
		}
	}

	if _, err := ComputeNodeChildrenChecksum(nodeDir); !errors.Is(err, ErrTooManyDirectChildren) {
		t.Fatalf("ComputeNodeChildrenChecksum with %d children: got %v, want ErrTooManyDirectChildren",
			maxDirectChildren+1, err)
	}
}

// TestComputeNodeChildrenChecksum_RejectsMetadataBudgetBeyondBound exercises the production
// scan/commitment path with a single direct child whose snapshot.yaml exceeds
// maxChildrenMetadataBytes, proving the aggregate metadata budget is enforced independently of
// the direct-child count bound (AC-1's second explicit resource bound).
func TestComputeNodeChildrenChecksum_RejectsMetadataBudgetBeyondBound(t *testing.T) {
	nodeDir := t.TempDir()
	snapshotsDir := filepath.Join(nodeDir, SnapshotsDirName)
	childDir := filepath.Join(snapshotsDir, "snapshot_bloated-child")

	if err := os.MkdirAll(filepath.Join(childDir, ManifestsDirName), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", childDir, err)
	}

	checksum, err := ComputeNodeChecksum(childDir)
	if err != nil {
		t.Fatalf("compute node checksum for %s: %v", childDir, err)
	}

	childrenChecksum := EmptyChildrenChecksum()
	// A namespace field padded well beyond maxChildrenMetadataBytes: the aggregate metadata
	// budget is charged against the raw snapshot.yaml file size, so a single oversized child
	// is enough to exceed it regardless of maxDirectChildren.
	oversizedNamespace := strings.Repeat("x", int(maxChildrenMetadataBytes)+1)

	if err := WriteSnapshotYAML(childDir, SnapshotYAML{
		APIVersion: "v1", Kind: "Snapshot", Name: "bloated-child", Namespace: oversizedNamespace,
		Checksum: checksum, ChildrenChecksum: &childrenChecksum,
	}); err != nil {
		t.Fatalf("write snapshot.yaml for %s: %v", childDir, err)
	}

	if _, err := ComputeNodeChildrenChecksum(nodeDir); !errors.Is(err, ErrChildrenMetadataBudgetExceeded) {
		t.Fatalf("ComputeNodeChildrenChecksum with oversized child metadata: got %v, want ErrChildrenMetadataBudgetExceeded", err)
	}
}

// TestComputeNodeChildrenChecksum_ReflectsActualDirectChildrenOnDisk proves the production
// commitment path (ComputeNodeChildrenChecksum) changes when a direct child is added, removed,
// or replaced by a different identity at the same path — the exact "hybrid tree" mutations
// AC-2 requires be detectable, exercised here at the commitment layer itself.
func TestComputeNodeChildrenChecksum_ReflectsActualDirectChildrenOnDisk(t *testing.T) {
	nodeDir := t.TempDir()
	snapshotsDir := filepath.Join(nodeDir, SnapshotsDirName)

	if err := os.MkdirAll(snapshotsDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", snapshotsDir, err)
	}

	empty, err := ComputeNodeChildrenChecksum(nodeDir)
	if err != nil {
		t.Fatalf("ComputeNodeChildrenChecksum (no children): %v", err)
	}

	if empty.Hex != EmptyChildrenChecksum().Hex {
		t.Errorf("no-children commitment = %q, want the canonical empty commitment %q", empty.Hex, EmptyChildrenChecksum().Hex)
	}

	finalizeNodeForChildrenChecksumTest(t, snapshotsDir, "child-a")

	withA, err := ComputeNodeChildrenChecksum(nodeDir)
	if err != nil {
		t.Fatalf("ComputeNodeChildrenChecksum (added child-a): %v", err)
	}

	if withA.Hex == empty.Hex {
		t.Error("adding a direct child did not change the commitment")
	}

	finalizeNodeForChildrenChecksumTest(t, snapshotsDir, "child-b")

	withAB, err := ComputeNodeChildrenChecksum(nodeDir)
	if err != nil {
		t.Fatalf("ComputeNodeChildrenChecksum (added child-b): %v", err)
	}

	if withAB.Hex == withA.Hex {
		t.Error("adding a second direct child did not change the commitment")
	}

	if err := os.RemoveAll(filepath.Join(snapshotsDir, "child-a")); err != nil {
		t.Fatalf("remove child-a: %v", err)
	}

	afterRemoval, err := ComputeNodeChildrenChecksum(nodeDir)
	if err != nil {
		t.Fatalf("ComputeNodeChildrenChecksum (removed child-a): %v", err)
	}

	if afterRemoval.Hex == withAB.Hex {
		t.Error("removing a direct child did not change the commitment")
	}

	if afterRemoval.Hex == empty.Hex {
		t.Error("commitment with child-b still present must not equal the empty commitment")
	}

	if afterRemoval.Hex == withA.Hex {
		t.Error("commitment over {child-b} must not equal the earlier, unrelated commitment over {child-a}")
	}
}

func TestVerifyNodeChildrenChecksumMismatchParity(t *testing.T) {
	root := t.TempDir()
	snapshotsDir := filepath.Join(root, SnapshotsDirName)
	childDir := finalizeNodeForChildrenChecksumTest(t, snapshotsDir, "child")

	if err := os.MkdirAll(filepath.Join(root, ManifestsDirName), 0o755); err != nil {
		t.Fatalf("mkdir root manifests: %v", err)
	}

	rootChecksum, err := ComputeNodeChecksum(root)
	if err != nil {
		t.Fatalf("compute root checksum: %v", err)
	}

	childrenChecksum, err := ComputeNodeChildrenChecksum(root)
	if err != nil {
		t.Fatalf("compute root children checksum: %v", err)
	}

	if err := WriteSnapshotYAML(root, SnapshotYAML{
		APIVersion:       "v1",
		Kind:             "Snapshot",
		Name:             "root",
		Checksum:         rootChecksum,
		ChildrenChecksum: &childrenChecksum,
	}); err != nil {
		t.Fatalf("write root snapshot.yaml: %v", err)
	}

	if err := os.WriteFile(filepath.Join(childDir, ManifestsDirName, "changed.yaml"), []byte("changed"), 0o600); err != nil {
		t.Fatalf("change child manifest: %v", err)
	}

	childChecksum, err := ComputeNodeChecksum(childDir)
	if err != nil {
		t.Fatalf("recompute child checksum: %v", err)
	}

	childMetadata, err := ReadSnapshotYAML(childDir)
	if err != nil {
		t.Fatalf("read child snapshot.yaml: %v", err)
	}
	childMetadata.Checksum = childChecksum

	if err := WriteSnapshotYAML(childDir, childMetadata); err != nil {
		t.Fatalf("republish self-consistent child: %v", err)
	}

	if err := VerifyNode(root); !errors.Is(err, ErrChildrenChecksumMismatch) {
		t.Fatalf("VerifyNode error = %v, want ErrChildrenChecksumMismatch", err)
	}

	view, err := OpenVerifiedArchive(root)
	if err != nil {
		t.Fatalf("open verified archive: %v", err)
	}
	defer func() { _ = view.Close() }()

	if _, err := view.VerifyNode(context.Background(), root); !errors.Is(err, ErrChildrenChecksumMismatch) {
		t.Fatalf("VerifiedArchive.VerifyNode error = %v, want ErrChildrenChecksumMismatch", err)
	}

	destination, err := OpenRootedDestination(root, nil)
	if err != nil {
		t.Fatalf("open rooted destination: %v", err)
	}
	defer func() { _ = destination.Close() }()

	if err := destination.VerifyNode(root); !errors.Is(err, ErrChildrenChecksumMismatch) {
		t.Fatalf("RootedDestination.VerifyNode error = %v, want ErrChildrenChecksumMismatch", err)
	}
}
