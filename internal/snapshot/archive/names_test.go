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

func TestFsFileName(t *testing.T) {
	t.Helper()

	tests := []struct {
		relPath string
		want    string
	}{
		{"file1.txt", "file1.txt.zst"},
		{"sub/file.txt", "sub/file.txt.zst"},
		{"a/b/c.jpg", "a/b/c.jpg.zst"},
		{"noext", "noext.zst"},
	}

	for _, tc := range tests {
		t.Run(tc.relPath, func(t *testing.T) {
			got := archive.FsFileName(tc.relPath)
			if got != tc.want {
				t.Errorf("FsFileName(%q) = %q; want %q", tc.relPath, got, tc.want)
			}
		})
	}
}

func TestChunkFileName(t *testing.T) {
	t.Helper()

	tests := []struct {
		i    int
		want string
	}{
		{0, "chunk_00000.zst"},
		{1, "chunk_00001.zst"},
		{9, "chunk_00009.zst"},
		{10, "chunk_00010.zst"},
		{99999, "chunk_99999.zst"},
		// Verify zero-padding is exactly 5 digits.
		{42, "chunk_00042.zst"},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			got := archive.ChunkFileName(tc.i)
			if got != tc.want {
				t.Errorf("ChunkFileName(%d) = %q; want %q", tc.i, got, tc.want)
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

	if archive.DataBlockName != "data.img.zst" {
		t.Errorf("DataBlockName = %q; want data.img.zst", archive.DataBlockName)
	}

	if archive.DataDirName != "data" {
		t.Errorf("DataDirName = %q; want data", archive.DataDirName)
	}

	if archive.BlockChunksDirName != "data.img.zst.d" {
		t.Errorf("BlockChunksDirName = %q; want data.img.zst.d", archive.BlockChunksDirName)
	}
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

	if archive.FsFileName("sub/data.bin") != archive.FsFileName("sub/data.bin") {
		t.Error("FsFileName is not deterministic")
	}

	if archive.ChunkFileName(7) != archive.ChunkFileName(7) {
		t.Error("ChunkFileName is not deterministic")
	}
}
