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
	"strings"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

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
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-1",
		Namespace:  "ns-a",
		SourceRef:  "vm/my-vm",
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

	if got.SourceRef != want.SourceRef {
		t.Errorf("SourceRef: got %q, want %q", got.SourceRef, want.SourceRef)
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
		APIVersion: "storage.deckhouse.io/v1alpha1",
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
		APIVersion: "storage.deckhouse.io/v1alpha1",
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
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-2",
		Namespace:  "ns-b",
		SourceRef:  `{"apiVersion":"v1","kind":"PersistentVolumeClaim","namespace":"ns-b","name":"my-pvc","uid":"uid-xyz"}`,
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
		APIVersion: "storage.deckhouse.io/v1alpha1",
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
		APIVersion: "storage.deckhouse.io/v1alpha1",
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
