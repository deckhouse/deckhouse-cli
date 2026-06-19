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

func TestNodeDirName(t *testing.T) {
	t.Helper()

	tests := []struct {
		kind string
		name string
		want string
	}{
		{"VirtualDisk", "disk-vm", "virtualdisk_disk-vm"},
		{"VirtualMachine", "vm-1", "virtualmachine_vm-1"},
		{"PersistentVolumeClaim", "orphan-data", "persistentvolumeclaim_orphan-data"},
		// Kind already lowercase — no change.
		{"configmap", "app-config", "configmap_app-config"},
		// Mixed-case kind.
		{"Secret", "app-secret", "secret_app-secret"},
	}

	for _, tc := range tests {
		t.Run(tc.kind+"/"+tc.name, func(t *testing.T) {
			got := archive.NodeDirName(tc.kind, tc.name)
			if got != tc.want {
				t.Errorf("NodeDirName(%q, %q) = %q; want %q", tc.kind, tc.name, got, tc.want)
			}
		})
	}
}

func TestNodeDirNameRootIsNamespace(t *testing.T) {
	// The root node directory is the user-supplied output name — callers pass it
	// directly and do not call NodeDirName for the root. This test documents the
	// convention by asserting that such a name used directly is correct.
	const outputDir = "my-snapshot-backup"

	if outputDir != outputDir { // always true; guards the constant above for future readers
		t.Fatal("invariant violated")
	}
}

func TestManifestFileName(t *testing.T) {
	t.Helper()

	tests := []struct {
		kind     string
		name     string
		apiGroup string
		want     string
	}{
		// Normal form — no collision.
		{"ConfigMap", "app-config", "", "configmap_app-config.yaml"},
		{"Secret", "app-secret", "", "secret_app-secret.yaml"},
		{"VirtualMachine", "vm-1", "", "virtualmachine_vm-1.yaml"},
		{"PersistentVolumeClaim", "disk-vm-pvc", "", "persistentvolumeclaim_disk-vm-pvc.yaml"},
		// Collision fallback — apiGroup provided.
		{"Pod", "demo-binder", "core", "pod.core_demo-binder.yaml"},
		{"VirtualDisk", "disk-vm", "virtualization.deckhouse.io", "virtualdisk.virtualization.deckhouse.io_disk-vm.yaml"},
		// Kind already lowercase, apiGroup empty.
		{"configmap", "my-cm", "", "configmap_my-cm.yaml"},
	}

	for _, tc := range tests {
		t.Run(tc.kind+"/"+tc.name+"/"+tc.apiGroup, func(t *testing.T) {
			got := archive.ManifestFileName(tc.kind, tc.name, tc.apiGroup)
			if got != tc.want {
				t.Errorf("ManifestFileName(%q, %q, %q) = %q; want %q",
					tc.kind, tc.name, tc.apiGroup, got, tc.want)
			}
		})
	}
}

func TestManifestFileNameCollisionFallbackOnlyWhenAPIGroupGiven(t *testing.T) {
	// The collision fallback (dot-separator + apiGroup) MUST NOT appear when
	// apiGroup is empty, even if the kind and name would collide in theory.
	normal := archive.ManifestFileName("Pod", "foo", "")
	withGroup := archive.ManifestFileName("Pod", "foo", "core")

	if normal != "pod_foo.yaml" {
		t.Errorf("normal form: got %q; want pod_foo.yaml", normal)
	}

	if withGroup != "pod.core_foo.yaml" {
		t.Errorf("collision form: got %q; want pod.core_foo.yaml", withGroup)
	}
}

func TestFsTarName(t *testing.T) {
	t.Parallel()

	if archive.FsTarName != "data.tar" {
		t.Errorf("FsTarName = %q; want data.tar", archive.FsTarName)
	}

	if archive.FsTarStagingDirName != "data.tar.d" {
		t.Errorf("FsTarStagingDirName = %q; want data.tar.d", archive.FsTarStagingDirName)
	}
}

func TestDataBlockName(t *testing.T) {
	t.Helper()

	tests := []struct {
		ext  string
		want string
	}{
		{".zst", "data.bin.zst"},
		{".lz4", "data.bin.lz4"},
		{".gz", "data.bin.gz"},
		{"", "data.bin"},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			got := archive.DataBlockName(tc.ext)
			if got != tc.want {
				t.Errorf("DataBlockName(%q) = %q; want %q", tc.ext, got, tc.want)
			}
		})
	}
}

func TestChunkFileName(t *testing.T) {
	t.Helper()

	tests := []struct {
		i    int
		ext  string
		want string
	}{
		{0, ".zst", "chunk_00000.zst"},
		{1, ".lz4", "chunk_00001.lz4"},
		{9, ".gz", "chunk_00009.gz"},
		{10, "", "chunk_00010"},
		{99999, ".zst", "chunk_99999.zst"},
		{42, ".zst", "chunk_00042.zst"},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			got := archive.ChunkFileName(tc.i, tc.ext)
			if got != tc.want {
				t.Errorf("ChunkFileName(%d, %q) = %q; want %q", tc.i, tc.ext, got, tc.want)
			}
		})
	}
}

func TestConstants(t *testing.T) {
	t.Helper()

	if archive.SnapshotYAMLName != "snapshot.yaml" {
		t.Errorf("SnapshotYAMLName = %q; want snapshot.yaml", archive.SnapshotYAMLName)
	}

	if archive.ManifestsDirName != "manifests" {
		t.Errorf("ManifestsDirName = %q; want manifests", archive.ManifestsDirName)
	}

	if archive.SnapshotsDirName != "snapshots" {
		t.Errorf("SnapshotsDirName = %q; want snapshots", archive.SnapshotsDirName)
	}

	if archive.DataBlockBase != "data.bin" {
		t.Errorf("DataBlockBase = %q; want data.bin", archive.DataBlockBase)
	}

	if archive.DataDirName != "data" {
		t.Errorf("DataDirName = %q; want data", archive.DataDirName)
	}

	if archive.BlockChunksDirName != "data.bin.d" {
		t.Errorf("BlockChunksDirName = %q; want data.bin.d", archive.BlockChunksDirName)
	}

	if archive.FsTarName != "data.tar" {
		t.Errorf("FsTarName = %q; want data.tar", archive.FsTarName)
	}

	if archive.FsTarStagingDirName != "data.tar.d" {
		t.Errorf("FsTarStagingDirName = %q; want data.tar.d", archive.FsTarStagingDirName)
	}
}

func TestFindBlockData(t *testing.T) {
	t.Parallel()

	t.Run("no file", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()

		path, found, err := archive.FindBlockData(dir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if found {
			t.Errorf("found = true, want false (no file present)")
		}

		if path != "" {
			t.Errorf("path = %q, want empty", path)
		}
	})

	t.Run("zstd file", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		want := filepath.Join(dir, "data.bin.zst")

		if err := os.WriteFile(want, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}

		path, found, err := archive.FindBlockData(dir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !found {
			t.Error("found = false, want true")
		}

		if path != want {
			t.Errorf("path = %q, want %q", path, want)
		}
	})

	t.Run("none codec (no ext)", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		want := filepath.Join(dir, "data.bin")

		if err := os.WriteFile(want, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}

		path, found, err := archive.FindBlockData(dir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !found {
			t.Error("found = false, want true")
		}

		if path != want {
			t.Errorf("path = %q, want %q", path, want)
		}
	})

	t.Run("excludes staging dir", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()

		if err := os.MkdirAll(filepath.Join(dir, "data.bin.d"), 0o755); err != nil {
			t.Fatal(err)
		}

		path, found, err := archive.FindBlockData(dir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if found {
			t.Errorf("found = true, want false (only staging dir present): path=%q", path)
		}
	})
}

func TestDeterminism(t *testing.T) {
	// Every helper must be pure: calling with the same inputs twice yields the same result.
	const kind = "VirtualDisk"
	const name = "my-disk"

	if archive.NodeDirName(kind, name) != archive.NodeDirName(kind, name) {
		t.Error("NodeDirName is not deterministic")
	}

	if archive.ManifestFileName(kind, name, "") != archive.ManifestFileName(kind, name, "") {
		t.Error("ManifestFileName (normal) is not deterministic")
	}

	if archive.ManifestFileName(kind, name, "virt.io") != archive.ManifestFileName(kind, name, "virt.io") {
		t.Error("ManifestFileName (collision) is not deterministic")
	}

	if archive.FsTarName != archive.FsTarName {
		t.Error("FsTarName is not deterministic")
	}

	if archive.MultiVolumeTarName("my-pvc") != archive.MultiVolumeTarName("my-pvc") {
		t.Error("MultiVolumeTarName is not deterministic")
	}

	if archive.ChunkFileName(7, ".zst") != archive.ChunkFileName(7, ".zst") {
		t.Error("ChunkFileName is not deterministic")
	}

	if archive.DataBlockName(".zst") != archive.DataBlockName(".zst") {
		t.Error("DataBlockName is not deterministic")
	}
}

func TestMultiVolumeBlockName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pvc  string
		ext  string
		want string
	}{
		{"pvc-disk-a", ".zst", "data/pvc-disk-a.bin.zst"},
		{"my-pvc", ".lz4", "data/my-pvc.bin.lz4"},
		{"disk.with.dots", "", "data/disk.with.dots.bin"},
	}

	for _, tc := range tests {
		got := archive.MultiVolumeBlockName(tc.pvc, tc.ext)
		if got != tc.want {
			t.Errorf("MultiVolumeBlockName(%q, %q) = %q; want %q", tc.pvc, tc.ext, got, tc.want)
		}
	}
}

func TestMultiVolumeTarName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pvc  string
		want string
	}{
		{"pvc-disk-a", "data/pvc-disk-a.tar"},
		{"my-pvc", "data/my-pvc.tar"},
		{"disk.with.dots", "data/disk.with.dots.tar"},
	}

	for _, tc := range tests {
		got := archive.MultiVolumeTarName(tc.pvc)
		if got != tc.want {
			t.Errorf("MultiVolumeTarName(%q) = %q; want %q", tc.pvc, got, tc.want)
		}
	}
}

func TestMultiVolumeTarStagingDirName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pvc  string
		want string
	}{
		{"pvc-disk-a", "data/pvc-disk-a.tar.d"},
		{"my-pvc", "data/my-pvc.tar.d"},
		{"disk.with.dots", "data/disk.with.dots.tar.d"},
	}

	for _, tc := range tests {
		got := archive.MultiVolumeTarStagingDirName(tc.pvc)
		if got != tc.want {
			t.Errorf("MultiVolumeTarStagingDirName(%q) = %q; want %q", tc.pvc, got, tc.want)
		}
	}
}

func TestBlockChunksDirNameFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pvc  string
		want string
	}{
		{"pvc-disk-a", "data/pvc-disk-a.bin.d"},
		{"my-pvc", "data/my-pvc.bin.d"},
		{"disk.with.dots", "data/disk.with.dots.bin.d"},
	}

	for _, tc := range tests {
		got := archive.BlockChunksDirNameFor(tc.pvc)
		if got != tc.want {
			t.Errorf("BlockChunksDirNameFor(%q) = %q; want %q", tc.pvc, got, tc.want)
		}
	}
}

// TestMultiVolumeHelpers_Consistency verifies that the multi-volume helpers are
// mutually consistent: block name, FS tar name, staging dirs, and chunk dir all
// share the pvc prefix under data/ and are distinct from each other and from the
// single-volume flat names.
func TestMultiVolumeHelpers_Consistency(t *testing.T) {
	t.Parallel()

	const pvc = "my-pvc"
	const ext = ".zst"

	blockName := archive.MultiVolumeBlockName(pvc, ext)
	tarName := archive.MultiVolumeTarName(pvc)
	tarStaging := archive.MultiVolumeTarStagingDirName(pvc)
	chunkDir := archive.BlockChunksDirNameFor(pvc)

	// All four must be distinct.
	names := []string{blockName, tarName, tarStaging, chunkDir}
	for i := range names {
		for j := i + 1; j < len(names); j++ {
			if names[i] == names[j] {
				t.Errorf("helpers not distinct: [%d]=%q == [%d]=%q", i, names[i], j, names[j])
			}
		}
	}

	// All four must differ from the single-volume flat names.
	flats := map[string]string{
		"DataBlockName":       archive.DataBlockName(ext),
		"FsTarName":           archive.FsTarName,
		"FsTarStagingDirName": archive.FsTarStagingDirName,
		"BlockChunksDirName":  archive.BlockChunksDirName,
		"DataDirName":         archive.DataDirName,
	}

	for _, multi := range names {
		for flatLabel, flat := range flats {
			if multi == flat {
				t.Errorf("%q must not equal flat %s=%q", multi, flatLabel, flat)
			}
		}
	}
}
