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
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

func TestReadSnapshotYAMLRejectsSymlinkBeforeReadingTarget(t *testing.T) {
	nodeDir := t.TempDir()
	outsidePath := filepath.Join(t.TempDir(), "outside.yaml")
	if err := os.WriteFile(outsidePath, []byte("apiVersion: escaped/v1\nkind: Escaped\nname: escaped\n"), 0o600); err != nil {
		t.Fatalf("write outside snapshot: %v", err)
	}

	snapshotPath := filepath.Join(nodeDir, archive.SnapshotYAMLName)
	if err := os.Symlink(outsidePath, snapshotPath); err != nil {
		t.Fatalf("symlink snapshot.yaml: %v", err)
	}

	_, err := archive.ReadSnapshotYAML(nodeDir)
	if !errors.Is(err, archive.ErrNonRegularArchiveArtifact) {
		t.Fatalf("ReadSnapshotYAML error = %v, want ErrNonRegularArchiveArtifact", err)
	}

	if !strings.Contains(err.Error(), snapshotPath) {
		t.Errorf("error %q does not contain offending path %q", err, snapshotPath)
	}
}

func TestOpenRegularFileRejectsSpecialFilesWithoutBlocking(t *testing.T) {
	tests := []struct {
		name  string
		build func(t *testing.T) string
	}{
		{
			name: "directory",
			build: func(t *testing.T) string {
				t.Helper()

				return t.TempDir()
			},
		},
		{
			name: "symlink",
			build: func(t *testing.T) string {
				t.Helper()

				target := filepath.Join(t.TempDir(), "target")
				if err := os.WriteFile(target, []byte("outside"), 0o600); err != nil {
					t.Fatalf("write target: %v", err)
				}

				path := filepath.Join(t.TempDir(), "artifact")
				if err := os.Symlink(target, path); err != nil {
					t.Fatalf("symlink artifact: %v", err)
				}

				return path
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.build(t)

			file, err := archive.OpenRegularFile(path)
			if file != nil {
				_ = file.Close()
			}

			if !errors.Is(err, archive.ErrNonRegularArchiveArtifact) {
				t.Fatalf("OpenRegularFile error = %v, want ErrNonRegularArchiveArtifact", err)
			}

			if !strings.Contains(err.Error(), path) {
				t.Errorf("error %q does not contain offending path %q", err, path)
			}
		})
	}
}

func TestOpenRegularFileAcceptsOrdinaryHardLink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("hard-linked bytes"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}

	link := filepath.Join(dir, "artifact")
	if err := os.Link(target, link); err != nil {
		t.Skipf("hard links are unavailable: %v", err)
	}

	file, err := archive.OpenRegularFile(link)
	if err != nil {
		t.Fatalf("OpenRegularFile: %v", err)
	}
	defer func() { _ = file.Close() }()

	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("read hard link: %v", err)
	}

	if string(data) != "hard-linked bytes" {
		t.Fatalf("hard-link content = %q, want hard-linked bytes", data)
	}
}

// validChecksum returns a well-formed NodeChecksum (sha256, 64 lowercase hex, consistent short).
func validChecksum() archive.NodeChecksum {
	hex := strings.Repeat("a", 64)

	return archive.NodeChecksum{
		Algorithm: archive.ChecksumAlgorithmSHA256,
		Hex:       hex,
		Short:     archive.ShortChecksum(hex),
	}
}

// validVolume returns a complete data VolumeInfo whose volumeMode is mode.
func validVolume(mode string) archive.VolumeInfo {
	return archive.VolumeInfo{
		Target:           archive.VolumeObjectRef{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: "pvc-1"},
		Artifact:         archive.VolumeObjectRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshotContent", Name: "content-1"},
		VolumeMode:       mode,
		StorageClassName: "sc",
		Size:             "1Gi",
	}
}

// TestValidateSnapshotYAML exercises the strict metadata invariants ValidateSnapshotYAML
// enforces over the fields the integrity checksum does NOT cover.
func TestValidateSnapshotYAML(t *testing.T) {
	t.Parallel()

	base := func() archive.SnapshotYAML {
		return archive.SnapshotYAML{APIVersion: "g/v1", Kind: "Snapshot", Name: "root", Checksum: validChecksum()}
	}

	tests := []struct {
		name     string
		mutate   func(sy *archive.SnapshotYAML)
		hasBlock bool
		hasFS    bool
		wantErr  bool
	}{
		{name: "valid: non-data structural node", mutate: func(*archive.SnapshotYAML) {}},
		{
			name: "valid: block data node",
			mutate: func(sy *archive.SnapshotYAML) {
				sy.Volumes = []archive.VolumeInfo{validVolume(archive.VolumeModeBlock)}
			},
			hasBlock: true,
		},
		{
			name: "valid: filesystem data node",
			mutate: func(sy *archive.SnapshotYAML) {
				sy.Volumes = []archive.VolumeInfo{validVolume(archive.VolumeModeFilesystem)}
			},
			hasFS: true,
		},
		{
			name: "valid: complete sourceObjectRef",
			mutate: func(sy *archive.SnapshotYAML) {
				sy.SourceObjectRef = &archive.SourceObjectRef{APIVersion: "demo/v1", Kind: "Disk", Name: "d1"}
			},
		},
		{name: "error: missing apiVersion", mutate: func(sy *archive.SnapshotYAML) { sy.APIVersion = "" }, wantErr: true},
		{name: "error: missing kind", mutate: func(sy *archive.SnapshotYAML) { sy.Kind = "" }, wantErr: true},
		{name: "error: missing name", mutate: func(sy *archive.SnapshotYAML) { sy.Name = "" }, wantErr: true},
		{name: "error: bad checksum algorithm", mutate: func(sy *archive.SnapshotYAML) { sy.Checksum.Algorithm = "md5" }, wantErr: true},
		{
			name:    "error: short hex",
			mutate:  func(sy *archive.SnapshotYAML) { sy.Checksum.Hex = "abcd"; sy.Checksum.Short = "abcd" },
			wantErr: true,
		},
		{
			name: "error: uppercase hex",
			mutate: func(sy *archive.SnapshotYAML) {
				sy.Checksum.Hex = strings.Repeat("A", 64)
				sy.Checksum.Short = strings.Repeat("A", 8)
			},
			wantErr: true,
		},
		{name: "error: inconsistent short", mutate: func(sy *archive.SnapshotYAML) { sy.Checksum.Short = "deadbeef" }, wantErr: true},
		{
			name: "error: partial sourceObjectRef",
			mutate: func(sy *archive.SnapshotYAML) {
				sy.SourceObjectRef = &archive.SourceObjectRef{APIVersion: "demo/v1", Name: "d1"}
			},
			wantErr: true,
		},
		{
			name: "error: two volumes",
			mutate: func(sy *archive.SnapshotYAML) {
				sy.Volumes = []archive.VolumeInfo{validVolume(archive.VolumeModeBlock), validVolume(archive.VolumeModeBlock)}
			},
			hasBlock: true,
			wantErr:  true,
		},
		{
			name: "error: non-data node carries a volume",
			mutate: func(sy *archive.SnapshotYAML) {
				sy.Volumes = []archive.VolumeInfo{validVolume(archive.VolumeModeBlock)}
			},
			wantErr: true,
		},
		{name: "error: data node with zero volumes", mutate: func(*archive.SnapshotYAML) {}, hasBlock: true, wantErr: true},
		{
			name: "error: data volume missing target identity",
			mutate: func(sy *archive.SnapshotYAML) {
				v := validVolume(archive.VolumeModeBlock)
				v.Target.Name = ""
				sy.Volumes = []archive.VolumeInfo{v}
			},
			hasBlock: true,
			wantErr:  true,
		},
		{
			name: "error: data volume missing artifact identity",
			mutate: func(sy *archive.SnapshotYAML) {
				v := validVolume(archive.VolumeModeBlock)
				v.Artifact.APIVersion = ""
				sy.Volumes = []archive.VolumeInfo{v}
			},
			hasBlock: true,
			wantErr:  true,
		},
		{
			name: "error: data volume missing storageClassName",
			mutate: func(sy *archive.SnapshotYAML) {
				v := validVolume(archive.VolumeModeBlock)
				v.StorageClassName = ""
				sy.Volumes = []archive.VolumeInfo{v}
			},
			hasBlock: true,
			wantErr:  true,
		},
		{
			name: "error: data volume unparseable size",
			mutate: func(sy *archive.SnapshotYAML) {
				v := validVolume(archive.VolumeModeBlock)
				v.Size = "not-a-quantity"
				sy.Volumes = []archive.VolumeInfo{v}
			},
			hasBlock: true,
			wantErr:  true,
		},
		{
			name: "error: data volume zero size",
			mutate: func(sy *archive.SnapshotYAML) {
				v := validVolume(archive.VolumeModeBlock)
				v.Size = "0"
				sy.Volumes = []archive.VolumeInfo{v}
			},
			hasBlock: true,
			wantErr:  true,
		},
		{
			name: "error: block payload with Filesystem volumeMode",
			mutate: func(sy *archive.SnapshotYAML) {
				sy.Volumes = []archive.VolumeInfo{validVolume(archive.VolumeModeFilesystem)}
			},
			hasBlock: true,
			wantErr:  true,
		},
		{
			name: "error: filesystem payload with Block volumeMode",
			mutate: func(sy *archive.SnapshotYAML) {
				sy.Volumes = []archive.VolumeInfo{validVolume(archive.VolumeModeBlock)}
			},
			hasFS:   true,
			wantErr: true,
		},
		{
			name:     "error: block payload with empty volumeMode",
			mutate:   func(sy *archive.SnapshotYAML) { sy.Volumes = []archive.VolumeInfo{validVolume("")} },
			hasBlock: true,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sy := base()
			tc.mutate(&sy)

			err := archive.ValidateSnapshotYAML(sy, tc.hasBlock, tc.hasFS)

			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}

				if !errors.Is(err, archive.ErrInvalidSnapshotYAML) {
					t.Errorf("expected ErrInvalidSnapshotYAML, got: %v", err)
				}

				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// makeSnapshotNodeDir creates a temp dir with a manifests/ subdir and one manifest file.
func makeSnapshotNodeDir(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()

	if err := os.MkdirAll(filepath.Join(dir, archive.ManifestsDirName), 0o755); err != nil {
		t.Fatalf("mkdir manifests: %v", err)
	}

	// One manifest so the checksum is non-trivial.
	manifest := filepath.Join(dir, archive.ManifestsDirName, "configmap_test.yaml")
	if err := os.WriteFile(manifest, []byte("apiVersion: v1\nkind: ConfigMap\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	return dir
}

func TestSnapshotYAML_RoundTrip_WithoutVolume(t *testing.T) {
	t.Parallel()

	dir := makeSnapshotNodeDir(t)

	checksum, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum: %v", err)
	}

	want := archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-1",
		Namespace:  "ns-a",
		UID:        "vm/my-vm",
		Checksum:   checksum,
	}

	if err := archive.WriteSnapshotYAML(dir, want); err != nil {
		t.Fatalf("WriteSnapshotYAML: %v", err)
	}

	got, err := archive.ReadSnapshotYAML(dir)
	if err != nil {
		t.Fatalf("ReadSnapshotYAML: %v", err)
	}

	if got.APIVersion != want.APIVersion {
		t.Errorf("APIVersion: got %q, want %q", got.APIVersion, want.APIVersion)
	}

	if got.Kind != want.Kind {
		t.Errorf("Kind: got %q, want %q", got.Kind, want.Kind)
	}

	if got.Name != want.Name {
		t.Errorf("Name: got %q, want %q", got.Name, want.Name)
	}

	if got.Namespace != want.Namespace {
		t.Errorf("Namespace: got %q, want %q", got.Namespace, want.Namespace)
	}

	if got.UID != want.UID {
		t.Errorf("UID: got %q, want %q", got.UID, want.UID)
	}

	if got.Checksum.Hex != want.Checksum.Hex {
		t.Errorf("Checksum.Hex: got %q, want %q", got.Checksum.Hex, want.Checksum.Hex)
	}

	if len(got.Volumes) != 0 {
		t.Errorf("Volumes must be empty for a snapshot node without OwnDataRefs, got %+v", got.Volumes)
	}
}

func TestSnapshotYAML_RoundTrip_WithVolume(t *testing.T) {
	t.Parallel()

	dir := makeSnapshotNodeDir(t)

	checksum, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum: %v", err)
	}

	wantVol := archive.VolumeInfo{
		Target: archive.VolumeObjectRef{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
			Name:       "my-pvc",
			Namespace:  "ns-a",
			UID:        "abc-123",
		},
		Artifact: archive.VolumeObjectRef{
			APIVersion: "snapshot.storage.k8s.io/v1",
			Kind:       "VolumeSnapshotContent",
			Name:       "snapcontent-xyz",
		},
	}

	want := archive.SnapshotYAML{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "d8-ss-aabbccdd",
		Namespace:  "ns-a",
		Checksum:   checksum,
		Volumes:    []archive.VolumeInfo{wantVol},
	}

	if err := archive.WriteSnapshotYAML(dir, want); err != nil {
		t.Fatalf("WriteSnapshotYAML: %v", err)
	}

	got, err := archive.ReadSnapshotYAML(dir)
	if err != nil {
		t.Fatalf("ReadSnapshotYAML: %v", err)
	}

	if len(got.Volumes) != 1 {
		t.Fatalf("Volumes length: got %d, want 1", len(got.Volumes))
	}

	gotVol := got.Volumes[0]

	if gotVol.Target.Name != wantVol.Target.Name {
		t.Errorf("Volumes[0].Target.Name: got %q, want %q", gotVol.Target.Name, wantVol.Target.Name)
	}

	if gotVol.Target.UID != wantVol.Target.UID {
		t.Errorf("Volumes[0].Target.UID: got %q, want %q", gotVol.Target.UID, wantVol.Target.UID)
	}

	if gotVol.Artifact.Name != wantVol.Artifact.Name {
		t.Errorf("Volumes[0].Artifact.Name: got %q, want %q", gotVol.Artifact.Name, wantVol.Artifact.Name)
	}

	if gotVol.Artifact.APIVersion != wantVol.Artifact.APIVersion {
		t.Errorf("Volumes[0].Artifact.APIVersion: got %q, want %q", gotVol.Artifact.APIVersion, wantVol.Artifact.APIVersion)
	}
}

// TestSnapshotYAML_RoundTrip_MultiVolume verifies that N>1 volumes are correctly
// serialised and deserialised.
func TestSnapshotYAML_RoundTrip_MultiVolume(t *testing.T) {
	t.Parallel()

	dir := makeSnapshotNodeDir(t)

	checksum, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum: %v", err)
	}

	vols := []archive.VolumeInfo{
		{
			Target:   archive.VolumeObjectRef{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: "pvc-a", UID: "uid-a"},
			Artifact: archive.VolumeObjectRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshotContent", Name: "vsc-a"},
		},
		{
			Target:   archive.VolumeObjectRef{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: "pvc-b", UID: "uid-b"},
			Artifact: archive.VolumeObjectRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshotContent", Name: "vsc-b"},
		},
	}

	want := archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "VirtualDiskSnapshot",
		Name:       "multi-snap",
		Checksum:   checksum,
		Volumes:    vols,
	}

	if err := archive.WriteSnapshotYAML(dir, want); err != nil {
		t.Fatalf("WriteSnapshotYAML: %v", err)
	}

	got, err := archive.ReadSnapshotYAML(dir)
	if err != nil {
		t.Fatalf("ReadSnapshotYAML: %v", err)
	}

	if len(got.Volumes) != len(vols) {
		t.Fatalf("Volumes length: got %d, want %d", len(got.Volumes), len(vols))
	}

	for i, wv := range vols {
		gv := got.Volumes[i]

		if gv.Target.Name != wv.Target.Name {
			t.Errorf("Volumes[%d].Target.Name: got %q, want %q", i, gv.Target.Name, wv.Target.Name)
		}

		if gv.Artifact.Name != wv.Artifact.Name {
			t.Errorf("Volumes[%d].Artifact.Name: got %q, want %q", i, gv.Artifact.Name, wv.Artifact.Name)
		}
	}
}

func TestSnapshotYAML_OmitemptyVolume(t *testing.T) {
	t.Parallel()

	dir := makeSnapshotNodeDir(t)

	checksum, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum: %v", err)
	}

	// Snapshot node: Volume is nil → must be omitted from YAML output.
	sy := archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-omit",
		Checksum:   checksum,
	}

	if err := archive.WriteSnapshotYAML(dir, sy); err != nil {
		t.Fatalf("WriteSnapshotYAML: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, archive.SnapshotYAMLName))
	if err != nil {
		t.Fatalf("ReadFile snapshot.yaml: %v", err)
	}

	if strings.Contains(string(raw), "volumes:") {
		t.Errorf("snapshot.yaml must not contain 'volumes:' key when Volumes is nil; got:\n%s", raw)
	}
}

func TestSnapshotYAML_RoundTrip_WithSourceName(t *testing.T) {
	t.Parallel()

	dir := makeSnapshotNodeDir(t)

	checksum, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum: %v", err)
	}

	want := archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-2",
		Namespace:  "ns-b",
		UID:        `{"apiVersion":"v1","kind":"PersistentVolumeClaim","namespace":"ns-b","name":"my-pvc","uid":"uid-xyz"}`,
		SourceName: "my-pvc",
		Checksum:   checksum,
	}

	if err := archive.WriteSnapshotYAML(dir, want); err != nil {
		t.Fatalf("WriteSnapshotYAML: %v", err)
	}

	got, err := archive.ReadSnapshotYAML(dir)
	if err != nil {
		t.Fatalf("ReadSnapshotYAML: %v", err)
	}

	if got.SourceName != want.SourceName {
		t.Errorf("SourceName: got %q, want %q", got.SourceName, want.SourceName)
	}
}

func TestSnapshotYAML_OmitemptySourceName(t *testing.T) {
	t.Parallel()

	dir := makeSnapshotNodeDir(t)

	checksum, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum: %v", err)
	}

	// SourceName empty → must be absent from YAML output.
	sy := archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-omit-sn",
		Checksum:   checksum,
	}

	if err := archive.WriteSnapshotYAML(dir, sy); err != nil {
		t.Fatalf("WriteSnapshotYAML: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, archive.SnapshotYAMLName))
	if err != nil {
		t.Fatalf("ReadFile snapshot.yaml: %v", err)
	}

	if strings.Contains(string(raw), "sourceName:") {
		t.Errorf("snapshot.yaml must not contain 'sourceName:' key when SourceName is empty; got:\n%s", raw)
	}
}

// TestSnapshotYAML_ChecksumUnaffectedBySourceNameField is a regression test that
// confirms adding the SourceName field to snapshot.yaml does NOT change the node
// checksum (because snapshot.yaml is excluded from ComputeNodeChecksum).
func TestSnapshotYAML_ChecksumUnaffectedBySourceNameField(t *testing.T) {
	t.Parallel()

	dir := makeSnapshotNodeDir(t)

	checksum1, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum before write: %v", err)
	}

	sy := archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-sn-regression",
		SourceName: "some-vm",
		Checksum:   checksum1,
	}

	if err := archive.WriteSnapshotYAML(dir, sy); err != nil {
		t.Fatalf("WriteSnapshotYAML: %v", err)
	}

	checksum2, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum after write: %v", err)
	}

	if checksum1.Hex != checksum2.Hex {
		t.Errorf("checksum changed after writing snapshot.yaml with SourceName (snapshot.yaml must be excluded):\nbefore %q\nafter  %q",
			checksum1.Hex, checksum2.Hex)
	}

	if err := archive.VerifyNode(dir); err != nil {
		t.Errorf("VerifyNode must pass after adding SourceName field: %v", err)
	}
}

// TestSnapshotYAML_RoundTrip_WithSourceObjectRef verifies that SourceObjectRef is correctly
// serialised and deserialised.
func TestSnapshotYAML_RoundTrip_WithSourceObjectRef(t *testing.T) {
	t.Parallel()

	dir := makeSnapshotNodeDir(t)

	checksum, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum: %v", err)
	}

	wantSOR := &archive.SourceObjectRef{
		APIVersion: "demo.deckhouse.io/v1alpha1",
		Kind:       "DemoVirtualDisk",
		Name:       "my-disk",
	}

	want := archive.SnapshotYAML{
		APIVersion:      "demo.deckhouse.io/v1alpha1",
		Kind:            "DemoVirtualDiskSnapshot",
		Name:            "snap-1",
		Namespace:       "ns-a",
		SourceObjectRef: wantSOR,
		Checksum:        checksum,
	}

	if err := archive.WriteSnapshotYAML(dir, want); err != nil {
		t.Fatalf("WriteSnapshotYAML: %v", err)
	}

	got, err := archive.ReadSnapshotYAML(dir)
	if err != nil {
		t.Fatalf("ReadSnapshotYAML: %v", err)
	}

	if got.SourceObjectRef == nil {
		t.Fatal("SourceObjectRef: got nil, want non-nil")
	}

	if got.SourceObjectRef.APIVersion != wantSOR.APIVersion {
		t.Errorf("SourceObjectRef.APIVersion: got %q, want %q", got.SourceObjectRef.APIVersion, wantSOR.APIVersion)
	}

	if got.SourceObjectRef.Kind != wantSOR.Kind {
		t.Errorf("SourceObjectRef.Kind: got %q, want %q", got.SourceObjectRef.Kind, wantSOR.Kind)
	}

	if got.SourceObjectRef.Name != wantSOR.Name {
		t.Errorf("SourceObjectRef.Name: got %q, want %q", got.SourceObjectRef.Name, wantSOR.Name)
	}
}

// TestSnapshotYAML_OmitemptySourceObjectRef verifies that when SourceObjectRef is nil
// the field is absent from the serialised YAML output.
func TestSnapshotYAML_OmitemptySourceObjectRef(t *testing.T) {
	t.Parallel()

	dir := makeSnapshotNodeDir(t)

	checksum, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum: %v", err)
	}

	sy := archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-omit-sor",
		Checksum:   checksum,
	}

	if err := archive.WriteSnapshotYAML(dir, sy); err != nil {
		t.Fatalf("WriteSnapshotYAML: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, archive.SnapshotYAMLName))
	if err != nil {
		t.Fatalf("ReadFile snapshot.yaml: %v", err)
	}

	if strings.Contains(string(raw), "sourceObjectRef:") {
		t.Errorf("snapshot.yaml must not contain 'sourceObjectRef:' when nil; got:\n%s", raw)
	}
}

// TestSnapshotYAML_ChecksumUnaffectedBySourceObjectRef is a regression test confirming
// that adding SourceObjectRef to snapshot.yaml does NOT change the node checksum
// (because snapshot.yaml is excluded from ComputeNodeChecksum).
func TestSnapshotYAML_ChecksumUnaffectedBySourceObjectRef(t *testing.T) {
	t.Parallel()

	dir := makeSnapshotNodeDir(t)

	checksum1, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum before write: %v", err)
	}

	sy := archive.SnapshotYAML{
		APIVersion: "demo.deckhouse.io/v1alpha1",
		Kind:       "DemoVirtualDiskSnapshot",
		Name:       "snap-sor-regression",
		SourceObjectRef: &archive.SourceObjectRef{
			APIVersion: "demo.deckhouse.io/v1alpha1",
			Kind:       "DemoVirtualDisk",
			Name:       "my-disk",
		},
		Checksum: checksum1,
	}

	if err := archive.WriteSnapshotYAML(dir, sy); err != nil {
		t.Fatalf("WriteSnapshotYAML: %v", err)
	}

	checksum2, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum after write: %v", err)
	}

	if checksum1.Hex != checksum2.Hex {
		t.Errorf("checksum changed after writing snapshot.yaml with SourceObjectRef (snapshot.yaml must be excluded):\nbefore %q\nafter  %q",
			checksum1.Hex, checksum2.Hex)
	}

	if err := archive.VerifyNode(dir); err != nil {
		t.Errorf("VerifyNode must pass after adding SourceObjectRef field: %v", err)
	}
}

// TestSnapshotYAML_ChecksumUnaffectedByVolumeField is a regression test that
// confirms adding the Volume field to snapshot.yaml does NOT change the node
// checksum (because snapshot.yaml is excluded from ComputeNodeChecksum).
func TestSnapshotYAML_ChecksumUnaffectedByVolumeField(t *testing.T) {
	t.Parallel()

	dir := makeSnapshotNodeDir(t)

	// Compute checksum without any snapshot.yaml.
	checksum1, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum before write: %v", err)
	}

	// Write snapshot.yaml with a Volumes block.
	sy := archive.SnapshotYAML{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "vol-node",
		Checksum:   checksum1,
		Volumes: []archive.VolumeInfo{
			{
				Target: archive.VolumeObjectRef{
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Name:       "pvc-a",
					Namespace:  "ns",
					UID:        "uid-111",
				},
				Artifact: archive.VolumeObjectRef{
					APIVersion: "snapshot.storage.k8s.io/v1",
					Kind:       "VolumeSnapshotContent",
					Name:       "vsc-aaa",
				},
			},
		},
	}

	if err := archive.WriteSnapshotYAML(dir, sy); err != nil {
		t.Fatalf("WriteSnapshotYAML: %v", err)
	}

	// Recompute checksum after snapshot.yaml is present.
	checksum2, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum after write: %v", err)
	}

	if checksum1.Hex != checksum2.Hex {
		t.Errorf("checksum changed after writing snapshot.yaml (snapshot.yaml must be excluded from the digest):\nbefore %q\nafter  %q",
			checksum1.Hex, checksum2.Hex)
	}

	// VerifyNode must also pass.
	if err := archive.VerifyNode(dir); err != nil {
		t.Errorf("VerifyNode must pass after adding Volume field: %v", err)
	}
}
