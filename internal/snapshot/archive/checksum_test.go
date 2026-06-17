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
	writeFile(t, filepath.Join(nodeDir, DataBlockName), "fake-block-data")

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

// TestComputeNodeChecksum_FsVolume verifies that data/ files are covered by the checksum.
func TestComputeNodeChecksum_FsVolume(t *testing.T) {
	nodeDir := makeNodeDir(t)
	writeFile(t, filepath.Join(nodeDir, DataDirName, "file.txt.zst"), "compressed-content-1")

	before, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("before: %v", err)
	}

	writeFile(t, filepath.Join(nodeDir, DataDirName, "file.txt.zst"), "compressed-content-2")

	after, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("after: %v", err)
	}

	if before.Hex == after.Hex {
		t.Error("checksum did not change after mutating fs volume file")
	}
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
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "test-snap",
		Namespace:  "default",
		SourceRef:  "app/vm-1",
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
		APIVersion: "storage.deckhouse.io/v1alpha1",
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
		APIVersion: "storage.deckhouse.io/v1alpha1",
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
