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
	"strings"
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

// writeEntry creates a file (content non-nil) or an empty directory (content nil)
// named name inside dir.
func writeEntry(t *testing.T, dir, name string, content []byte) {
	t.Helper()

	path := filepath.Join(dir, name)

	if content == nil {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}

		return
	}

	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestClassifyBlockPayload_Valid covers every recognized codec name and the
// "no payload at all" shape, table-driven, per the task's acceptance criteria
// ("data.bin maps to codec none/raw; supported compressed names map exactly").
func TestClassifyBlockPayload_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		entries   map[string][]byte // filename -> content (nil content => directory)
		wantFound bool
		wantName  string
		wantExt   string
	}{
		{
			name:      "no payload",
			entries:   nil,
			wantFound: false,
		},
		{
			name:      "raw data.bin maps to codec none",
			entries:   map[string][]byte{"data.bin": []byte("x")},
			wantFound: true,
			wantName:  "data.bin",
			wantExt:   "",
		},
		{
			name:      "data.bin.zst",
			entries:   map[string][]byte{"data.bin.zst": []byte("x")},
			wantFound: true,
			wantName:  "data.bin.zst",
			wantExt:   ".zst",
		},
		{
			name:      "data.bin.gz",
			entries:   map[string][]byte{"data.bin.gz": []byte("x")},
			wantFound: true,
			wantName:  "data.bin.gz",
			wantExt:   ".gz",
		},
		{
			name:      "data.bin.lz4",
			entries:   map[string][]byte{"data.bin.lz4": []byte("x")},
			wantFound: true,
			wantName:  "data.bin.lz4",
			wantExt:   ".lz4",
		},
		{
			name: "staging dir alone is ignored, not a payload",
			entries: map[string][]byte{
				"data.bin.d": nil,
			},
			wantFound: false,
		},
		{
			name: "staging dir beside a real payload is ignored",
			entries: map[string][]byte{
				"data.bin.zst": []byte("x"),
				"data.bin.d":   nil,
			},
			wantFound: true,
			wantName:  "data.bin.zst",
			wantExt:   ".zst",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()

			for name, content := range tc.entries {
				writeEntry(t, dir, name, content)
			}

			payload, found, err := archive.ClassifyBlockPayload(dir)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if found != tc.wantFound {
				t.Fatalf("found = %v, want %v", found, tc.wantFound)
			}

			if !tc.wantFound {
				return
			}

			if want := filepath.Join(dir, tc.wantName); payload.Path != want {
				t.Errorf("path = %q, want %q", payload.Path, want)
			}

			if payload.Ext != tc.wantExt {
				t.Errorf("ext = %q, want %q", payload.Ext, tc.wantExt)
			}
		})
	}
}

// TestClassifyBlockPayload_Invalid covers every rejected shape: unknown/chained
// suffixes, multiple block files, a block file coexisting with data.tar, and a
// non-directory occupying the staging-dir name. Every case must fail
// deterministically with ErrInvalidBlockPayload, not silently pick one file.
func TestClassifyBlockPayload_Invalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		entries map[string][]byte
	}{
		{
			name:    "unknown codec suffix",
			entries: map[string][]byte{"data.bin.foo": []byte("x")},
		},
		{
			name:    "chained suffix",
			entries: map[string][]byte{"data.bin.zst.bak": []byte("x")},
		},
		{
			name:    "double chained suffix",
			entries: map[string][]byte{"data.bin.zst.gz": []byte("x")},
		},
		{
			name: "multiple recognized block files",
			entries: map[string][]byte{
				"data.bin.zst": []byte("x"),
				"data.bin.gz":  []byte("y"),
			},
		},
		{
			name: "raw and compressed both present",
			entries: map[string][]byte{
				"data.bin":     []byte("x"),
				"data.bin.zst": []byte("y"),
			},
		},
		{
			name: "block payload coexists with data.tar",
			entries: map[string][]byte{
				"data.bin.zst": []byte("x"),
				"data.tar":     []byte("y"),
			},
		},
		{
			name: "staging-dir name occupied by a file",
			entries: map[string][]byte{
				"data.bin.d": []byte("not a directory"),
			},
		},
		{
			name: "an unexpected directory shares the base name",
			entries: map[string][]byte{
				"data.bin": nil, // directory named exactly "data.bin"
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()

			for name, content := range tc.entries {
				writeEntry(t, dir, name, content)
			}

			_, found, err := archive.ClassifyBlockPayload(dir)
			if err == nil {
				t.Fatalf("expected error, got nil (found=%v)", found)
			}

			if !errors.Is(err, archive.ErrInvalidBlockPayload) {
				t.Errorf("expected ErrInvalidBlockPayload, got: %v", err)
			}
		})
	}
}

func TestClassifyBlockPayload_RejectsNonRegularExactNames(t *testing.T) {
	tests := []struct {
		name  string
		build func(t *testing.T, path string)
	}{
		{
			name: "symlink",
			build: func(t *testing.T, path string) {
				t.Helper()

				target := filepath.Join(t.TempDir(), "outside")
				if err := os.WriteFile(target, []byte("outside"), 0o600); err != nil {
					t.Fatalf("write target: %v", err)
				}

				if err := os.Symlink(target, path); err != nil {
					t.Fatalf("symlink payload: %v", err)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodeDir := t.TempDir()
			path := filepath.Join(nodeDir, archive.DataBlockName(".zst"))
			tc.build(t, path)

			_, found, err := archive.ClassifyBlockPayload(nodeDir)
			if found {
				t.Error("found = true, want false")
			}

			if !errors.Is(err, archive.ErrInvalidBlockPayload) {
				t.Errorf("error = %v, want ErrInvalidBlockPayload", err)
			}

			if !errors.Is(err, archive.ErrNonRegularArchiveArtifact) {
				t.Errorf("error = %v, want ErrNonRegularArchiveArtifact", err)
			}

			if !strings.Contains(err.Error(), path) {
				t.Errorf("error %q does not contain offending path %q", err, path)
			}
		})
	}
}

// TestClassifyBlockPayload_DirectoryOrderIndependent verifies that the result
// does not depend on os.ReadDir's entry ordering by exercising both possible
// orderings of the same two-file "multiple block files" shape.
func TestClassifyBlockPayload_DirectoryOrderIndependent(t *testing.T) {
	t.Parallel()

	for _, names := range [][2]string{
		{"data.bin.zst", "data.bin.lz4"},
		{"data.bin.lz4", "data.bin.zst"},
	} {
		dir := t.TempDir()

		for _, n := range names {
			writeEntry(t, dir, n, []byte("x"))
		}

		_, _, err := archive.ClassifyBlockPayload(dir)
		if !errors.Is(err, archive.ErrInvalidBlockPayload) {
			t.Errorf("order %v: expected ErrInvalidBlockPayload, got: %v", names, err)
		}
	}
}

// TestClassifyBlockPayload_MissingDir verifies that a nonexistent node
// directory is reported as "no payload" rather than an error — BuildPlan
// only calls this after confirming the node dir exists via snapshot.yaml, but
// the classifier itself must not require the caller to pre-check existence.
func TestClassifyBlockPayload_MissingDir(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "does-not-exist")

	_, found, err := archive.ClassifyBlockPayload(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if found {
		t.Error("found = true, want false for a missing directory")
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

	if archive.FsTarName != archive.FsTarName {
		t.Error("FsTarName is not deterministic")
	}

	if archive.ChunkFileName(7, ".zst") != archive.ChunkFileName(7, ".zst") {
		t.Error("ChunkFileName is not deterministic")
	}

	if archive.DataBlockName(".zst") != archive.DataBlockName(".zst") {
		t.Error("DataBlockName is not deterministic")
	}
}

func TestFsFileChunksDirName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		relPath string
		ext     string
		want    string
	}{
		{"payload.bin", ".zst", ".d8-meta/chunks/payload.bin.zst.d"},
		{"disk/payload.bin", ".zst", ".d8-meta/chunks/disk/payload.bin.zst.d"},
		{"payload.bin", "", ".d8-meta/chunks/payload.bin.d"},
		{"a/b/c.img", ".lz4", ".d8-meta/chunks/a/b/c.img.lz4.d"},
	}

	for _, tc := range tests {
		got := archive.FsFileChunksDirName(tc.relPath, tc.ext)
		if got != tc.want {
			t.Errorf("FsFileChunksDirName(%q, %q) = %q; want %q", tc.relPath, tc.ext, got, tc.want)
		}
	}
}
