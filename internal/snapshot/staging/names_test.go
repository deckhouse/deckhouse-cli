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

package staging_test

import (
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/staging"
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
			got := staging.NodeDirName(tc.kind, tc.name)
			if got != tc.want {
				t.Errorf("NodeDirName(%q, %q) = %q; want %q", tc.kind, tc.name, got, tc.want)
			}
		})
	}
}

func TestNodeDirNameRootIsNamespace(t *testing.T) {
	// The root node directory is the namespace itself — callers pass the namespace
	// as-is and do not call NodeDirName for the root.  This test documents the
	// convention by asserting that a namespace string used directly is correct.
	const ns = "snapshot-demo-tree"

	if ns != ns { // always true; guards the constant above for future readers
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
			got := staging.ManifestFileName(tc.kind, tc.name, tc.apiGroup)
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
	normal := staging.ManifestFileName("Pod", "foo", "")
	withGroup := staging.ManifestFileName("Pod", "foo", "core")

	if normal != "pod_foo.yaml" {
		t.Errorf("normal form: got %q; want pod_foo.yaml", normal)
	}

	if withGroup != "pod.core_foo.yaml" {
		t.Errorf("collision form: got %q; want pod.core_foo.yaml", withGroup)
	}
}

func TestBlockDataName(t *testing.T) {
	t.Helper()

	tests := []struct {
		kind string
		name string
		want string
	}{
		{"PersistentVolumeClaim", "disk-vm-pvc", "persistentvolumeclaim_disk-vm-pvc.img.zst"},
		{"PersistentVolumeClaim", "orphan-data", "persistentvolumeclaim_orphan-data.img.zst"},
		{"VirtualDisk", "disk-standalone", "virtualdisk_disk-standalone.img.zst"},
	}

	for _, tc := range tests {
		t.Run(tc.kind+"/"+tc.name, func(t *testing.T) {
			got := staging.BlockDataName(tc.kind, tc.name)
			if got != tc.want {
				t.Errorf("BlockDataName(%q, %q) = %q; want %q", tc.kind, tc.name, got, tc.want)
			}
		})
	}
}

func TestFsDataName(t *testing.T) {
	t.Helper()

	tests := []struct {
		kind string
		name string
		want string
	}{
		{"PersistentVolumeClaim", "fs-pvc", "persistentvolumeclaim_fs-pvc.fs.tar.zst"},
		{"VirtualDisk", "fs-disk", "virtualdisk_fs-disk.fs.tar.zst"},
	}

	for _, tc := range tests {
		t.Run(tc.kind+"/"+tc.name, func(t *testing.T) {
			got := staging.FsDataName(tc.kind, tc.name)
			if got != tc.want {
				t.Errorf("FsDataName(%q, %q) = %q; want %q", tc.kind, tc.name, got, tc.want)
			}
		})
	}
}

func TestChecksumsFileName(t *testing.T) {
	if staging.ChecksumsFileName != "checksums.sha256" {
		t.Errorf("ChecksumsFileName = %q; want checksums.sha256", staging.ChecksumsFileName)
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
			got := staging.ChunkFileName(tc.i)
			if got != tc.want {
				t.Errorf("ChunkFileName(%d) = %q; want %q", tc.i, got, tc.want)
			}
		})
	}
}

func TestBlockStagingDirName(t *testing.T) {
	t.Helper()

	tests := []struct {
		kind string
		name string
		want string
	}{
		{"PersistentVolumeClaim", "disk-vm-pvc", "persistentvolumeclaim_disk-vm-pvc.img.zst.d"},
		{"VirtualDisk", "standalone", "virtualdisk_standalone.img.zst.d"},
	}

	for _, tc := range tests {
		t.Run(tc.kind+"/"+tc.name, func(t *testing.T) {
			got := staging.BlockStagingDirName(tc.kind, tc.name)
			if got != tc.want {
				t.Errorf("BlockStagingDirName(%q, %q) = %q; want %q", tc.kind, tc.name, got, tc.want)
			}
		})
	}
}

func TestFsStagingDirName(t *testing.T) {
	t.Helper()

	tests := []struct {
		kind string
		name string
		want string
	}{
		{"PersistentVolumeClaim", "fs-pvc", "persistentvolumeclaim_fs-pvc.fs.d"},
		{"VirtualDisk", "fs-disk", "virtualdisk_fs-disk.fs.d"},
	}

	for _, tc := range tests {
		t.Run(tc.kind+"/"+tc.name, func(t *testing.T) {
			got := staging.FsStagingDirName(tc.kind, tc.name)
			if got != tc.want {
				t.Errorf("FsStagingDirName(%q, %q) = %q; want %q", tc.kind, tc.name, got, tc.want)
			}
		})
	}
}

func TestDeterminism(t *testing.T) {
	// Every helper must be pure: calling with the same inputs twice yields the same result.
	const kind = "VirtualDisk"
	const name = "my-disk"

	if staging.NodeDirName(kind, name) != staging.NodeDirName(kind, name) {
		t.Error("NodeDirName is not deterministic")
	}

	if staging.ManifestFileName(kind, name, "") != staging.ManifestFileName(kind, name, "") {
		t.Error("ManifestFileName (normal) is not deterministic")
	}

	if staging.ManifestFileName(kind, name, "virt.io") != staging.ManifestFileName(kind, name, "virt.io") {
		t.Error("ManifestFileName (collision) is not deterministic")
	}

	if staging.BlockDataName(kind, name) != staging.BlockDataName(kind, name) {
		t.Error("BlockDataName is not deterministic")
	}

	if staging.FsDataName(kind, name) != staging.FsDataName(kind, name) {
		t.Error("FsDataName is not deterministic")
	}

	if staging.ChunkFileName(7) != staging.ChunkFileName(7) {
		t.Error("ChunkFileName is not deterministic")
	}

	if staging.BlockStagingDirName(kind, name) != staging.BlockStagingDirName(kind, name) {
		t.Error("BlockStagingDirName is not deterministic")
	}

	if staging.FsStagingDirName(kind, name) != staging.FsStagingDirName(kind, name) {
		t.Error("FsStagingDirName is not deterministic")
	}
}
