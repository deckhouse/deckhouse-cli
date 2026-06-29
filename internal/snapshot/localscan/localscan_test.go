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

package localscan_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/localscan"
)

// writeNodeYAML writes a SnapshotYAML to dir/snapshot.yaml, failing the test on error.
func writeNodeYAML(t *testing.T, dir string, sy archive.SnapshotYAML) {
	t.Helper()

	if err := archive.WriteSnapshotYAML(dir, sy); err != nil {
		t.Fatalf("WriteSnapshotYAML in %s: %v", dir, err)
	}
}

// makeChildDir creates a child node directory under parent/snapshots/<name>
// and writes the given SnapshotYAML. Returns the child directory path.
func makeChildDir(t *testing.T, parent, name string, sy archive.SnapshotYAML) string {
	t.Helper()

	childDir := filepath.Join(parent, archive.SnapshotsDirName, name)

	if err := os.MkdirAll(childDir, 0o755); err != nil {
		t.Fatalf("MkdirAll %s: %v", childDir, err)
	}

	writeNodeYAML(t, childDir, sy)

	return childDir
}

func TestScan_RootNoChildren(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeNodeYAML(t, root, archive.SnapshotYAML{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "root-snap",
		Namespace:  "default",
	})

	node, err := localscan.Scan(root)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if node.Kind != "Snapshot" {
		t.Errorf("Kind: got %q, want %q", node.Kind, "Snapshot")
	}

	if node.Name != "root-snap" {
		t.Errorf("Name: got %q, want %q", node.Name, "root-snap")
	}

	if node.Namespace != "default" {
		t.Errorf("Namespace: got %q, want %q", node.Namespace, "default")
	}

	if node.Path != "." {
		t.Errorf("Path: got %q, want %q", node.Path, ".")
	}

	if len(node.Children) != 0 {
		t.Errorf("Children: got %d, want 0", len(node.Children))
	}

	if len(node.Volumes) != 0 {
		t.Errorf("Volumes: got %d, want 0", len(node.Volumes))
	}
}

func TestScan_RootWithDirectChildren(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeNodeYAML(t, root, archive.SnapshotYAML{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "root-snap",
		Namespace:  "ns-a",
	})

	childADir := makeChildDir(t, root, "demovirtualdisksnapshot_disk-a", archive.SnapshotYAML{
		APIVersion: "demo.deckhouse.io/v1alpha1",
		Kind:       "DemoVirtualDiskSnapshot",
		Name:       "nss-child-a",
		Namespace:  "ns-a",
	})

	_ = makeChildDir(t, root, "demovirtualdisksnapshot_disk-b", archive.SnapshotYAML{
		APIVersion: "demo.deckhouse.io/v1alpha1",
		Kind:       "DemoVirtualDiskSnapshot",
		Name:       "nss-child-b",
		Namespace:  "ns-a",
	})

	node, err := localscan.Scan(root)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if node.Name != "root-snap" {
		t.Errorf("root Name: got %q, want %q", node.Name, "root-snap")
	}

	if len(node.Children) != 2 {
		t.Fatalf("Children count: got %d, want 2", len(node.Children))
	}

	// Locate child-a by path.
	wantChildAPath, err := filepath.Rel(root, childADir)
	if err != nil {
		t.Fatalf("Rel: %v", err)
	}

	var gotChildA *localscan.Node

	for _, c := range node.Children {
		if c.Path == wantChildAPath {
			gotChildA = c

			break
		}
	}

	if gotChildA == nil {
		t.Fatalf("child-a not found by path %q", wantChildAPath)
	}

	if gotChildA.Kind != "DemoVirtualDiskSnapshot" {
		t.Errorf("child-a Kind: got %q, want %q", gotChildA.Kind, "DemoVirtualDiskSnapshot")
	}

	if gotChildA.Name != "nss-child-a" {
		t.Errorf("child-a Name: got %q, want %q", gotChildA.Name, "nss-child-a")
	}

	if len(gotChildA.Children) != 0 {
		t.Errorf("child-a should have no children, got %d", len(gotChildA.Children))
	}
}

func TestScan_NonDirectory(t *testing.T) {
	t.Parallel()

	f, err := os.CreateTemp(t.TempDir(), "not-a-dir-*.txt")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}

	_ = f.Close()

	_, scanErr := localscan.Scan(f.Name())
	if scanErr == nil {
		t.Fatal("Scan on a file: expected error, got nil")
	}
}

func TestScan_PathNotExist(t *testing.T) {
	t.Parallel()

	_, err := localscan.Scan(filepath.Join(t.TempDir(), "does-not-exist"))
	if err == nil {
		t.Fatal("Scan on non-existent path: expected error, got nil")
	}
}

func TestScan_MissingSnapshotYAML(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	_, err := localscan.Scan(root)
	if err == nil {
		t.Fatal("Scan on root without snapshot.yaml: expected error, got nil")
	}
}

func TestScan_NestedTree(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeNodeYAML(t, root, archive.SnapshotYAML{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "root-snap",
	})

	parentDir := makeChildDir(t, root, "snapshot_parent", archive.SnapshotYAML{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "parent-snap",
	})

	grandchildDir := makeChildDir(t, parentDir, "volumesnapshot_pvc-a", archive.SnapshotYAML{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "nss-vs-pvc-a",
		Namespace:  "ns-a",
	})

	node, err := localscan.Scan(root)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if len(node.Children) != 1 {
		t.Fatalf("root children: got %d, want 1", len(node.Children))
	}

	parent := node.Children[0]

	if parent.Name != "parent-snap" {
		t.Errorf("parent Name: got %q, want %q", parent.Name, "parent-snap")
	}

	if len(parent.Children) != 1 {
		t.Fatalf("parent children: got %d, want 1", len(parent.Children))
	}

	grandchild := parent.Children[0]

	if grandchild.Kind != "VolumeSnapshot" {
		t.Errorf("grandchild Kind: got %q, want %q", grandchild.Kind, "VolumeSnapshot")
	}

	if grandchild.Name != "nss-vs-pvc-a" {
		t.Errorf("grandchild Name: got %q, want %q", grandchild.Name, "nss-vs-pvc-a")
	}

	wantGrandchildPath, err := filepath.Rel(root, grandchildDir)
	if err != nil {
		t.Fatalf("Rel: %v", err)
	}

	if grandchild.Path != wantGrandchildPath {
		t.Errorf("grandchild Path: got %q, want %q", grandchild.Path, wantGrandchildPath)
	}
}

func TestScan_VolumesPopulated(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	wantVol := archive.VolumeInfo{
		Target: archive.VolumeObjectRef{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
			Name:       "my-pvc",
			Namespace:  "ns-a",
			UID:        "uid-111",
		},
		Artifact: archive.VolumeObjectRef{
			APIVersion: "snapshot.storage.k8s.io/v1",
			Kind:       "VolumeSnapshotContent",
			Name:       "snapcontent-xyz",
		},
		VolumeMode: "Block",
		Size:       "10Gi",
	}

	writeNodeYAML(t, root, archive.SnapshotYAML{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "nss-vs-pvc-a",
		Namespace:  "ns-a",
		Volumes:    []archive.VolumeInfo{wantVol},
	})

	node, err := localscan.Scan(root)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if len(node.Volumes) != 1 {
		t.Fatalf("Volumes length: got %d, want 1", len(node.Volumes))
	}

	got := node.Volumes[0]

	if got.Target.Name != wantVol.Target.Name {
		t.Errorf("Volumes[0].Target.Name: got %q, want %q", got.Target.Name, wantVol.Target.Name)
	}

	if got.Target.UID != wantVol.Target.UID {
		t.Errorf("Volumes[0].Target.UID: got %q, want %q", got.Target.UID, wantVol.Target.UID)
	}

	if got.VolumeMode != wantVol.VolumeMode {
		t.Errorf("Volumes[0].VolumeMode: got %q, want %q", got.VolumeMode, wantVol.VolumeMode)
	}

	if got.Size != wantVol.Size {
		t.Errorf("Volumes[0].Size: got %q, want %q", got.Size, wantVol.Size)
	}
}

func TestScan_EmptySnapshotsDir(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeNodeYAML(t, root, archive.SnapshotYAML{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap",
	})

	// Create an empty snapshots/ dir (no child node dirs inside).
	snapshotsDir := filepath.Join(root, archive.SnapshotsDirName)

	if err := os.MkdirAll(snapshotsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	node, err := localscan.Scan(root)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if len(node.Children) != 0 {
		t.Errorf("Children: got %d, want 0 for empty snapshots/ dir", len(node.Children))
	}
}

func TestScan_NonDirEntryInSnapshotsSubdir(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeNodeYAML(t, root, archive.SnapshotYAML{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap",
	})

	snapshotsDir := filepath.Join(root, archive.SnapshotsDirName)

	if err := os.MkdirAll(snapshotsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	// Write a plain file (not a dir) inside snapshots/. The scanner should skip it.
	if err := os.WriteFile(filepath.Join(snapshotsDir, "README.txt"), []byte("notes"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	makeChildDir(t, root, "snapshot_child", archive.SnapshotYAML{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "child-snap",
	})

	node, err := localscan.Scan(root)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if len(node.Children) != 1 {
		t.Errorf("Children: got %d, want 1 (non-dir entry must be skipped)", len(node.Children))
	}
}
