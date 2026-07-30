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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sigsyaml "sigs.k8s.io/yaml"

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
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
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

func TestScanRequiresExplicitLegacyCompatibility(t *testing.T) {
	root := t.TempDir()
	writeNodeYAML(t, root, archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "original",
	})

	path := filepath.Join(root, archive.SnapshotYAMLName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read current snapshot.yaml: %v", err)
	}

	var fields map[string]interface{}
	if err := sigsyaml.Unmarshal(data, &fields); err != nil {
		t.Fatalf("unmarshal current snapshot.yaml: %v", err)
	}

	delete(fields, "formatVersion")
	delete(fields, "metadataChecksum")
	fields["name"] = "tampered"

	data, err = sigsyaml.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal downgraded snapshot.yaml: %v", err)
	}

	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write downgraded snapshot.yaml: %v", err)
	}

	if _, err := localscan.Scan(root); !errors.Is(err, archive.ErrLegacySnapshotFormat) {
		t.Fatalf("Scan error = %v, want ErrLegacySnapshotFormat", err)
	}

	node, err := localscan.ScanWithOptions(root, archive.SnapshotYAMLReadOptions{
		AllowUnauthenticatedLegacy: true,
	})
	if err != nil {
		t.Fatalf("ScanWithOptions: %v", err)
	}

	if node.Name != "tampered" {
		t.Fatalf("legacy node name = %q, want tampered", node.Name)
	}
}

func TestScanVerifiedChecksEveryNodeAndPreservesLimits(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(t *testing.T) string
		limits  localscan.ScanLimits
		wantErr error
	}{
		{
			name: "valid tree",
			prepare: func(t *testing.T) string {
				t.Helper()

				root := t.TempDir()
				child := filepath.Join(root, archive.SnapshotsDirName, "snapshot_child")
				if err := os.MkdirAll(filepath.Join(child, archive.ManifestsDirName), 0o755); err != nil {
					t.Fatalf("create child manifests: %v", err)
				}

				if err := os.WriteFile(
					filepath.Join(child, archive.ManifestsDirName, "configmap_child.yaml"),
					[]byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: child\n"),
					0o600,
				); err != nil {
					t.Fatalf("write child manifest: %v", err)
				}

				finalizeVerifiedNode(t, child, archive.SnapshotYAML{
					APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
					Kind:       "Snapshot",
					Name:       "child",
				})

				// Finalize root last, once its only direct child is itself already
				// finalized on disk, so root's ChildrenChecksum authenticates the real
				// child set (see finalizeVerifiedNode's bottom-up contract).
				finalizeVerifiedNode(t, root, archive.SnapshotYAML{
					APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
					Kind:       "Snapshot",
					Name:       "root",
				})

				return root
			},
			limits: localscan.DefaultScanLimits(),
		},
		{
			name: "corrupt child manifest",
			prepare: func(t *testing.T) string {
				t.Helper()

				root := t.TempDir()
				child := filepath.Join(root, archive.SnapshotsDirName, "snapshot_child")
				if err := os.MkdirAll(filepath.Join(child, archive.ManifestsDirName), 0o755); err != nil {
					t.Fatalf("create child manifests: %v", err)
				}

				manifest := filepath.Join(child, archive.ManifestsDirName, "configmap_child.yaml")
				if err := os.WriteFile(manifest, []byte("original"), 0o600); err != nil {
					t.Fatalf("write child manifest: %v", err)
				}

				finalizeVerifiedNode(t, child, archive.SnapshotYAML{
					APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
					Kind:       "Snapshot",
					Name:       "child",
				})

				// Finalize root only once the (still-uncorrupted) child is finalized,
				// so root's ChildrenChecksum is valid before the manifest is tampered
				// with below to trigger the child's own NodeChecksum mismatch.
				finalizeVerifiedNode(t, root, archive.SnapshotYAML{
					APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
					Kind:       "Snapshot",
					Name:       "root",
				})

				if err := os.WriteFile(manifest, []byte("corrupt"), 0o600); err != nil {
					t.Fatalf("corrupt child manifest: %v", err)
				}

				return root
			},
			limits:  localscan.DefaultScanLimits(),
			wantErr: archive.ErrChecksumMismatch,
		},
		{
			name: "invalid structural metadata",
			prepare: func(t *testing.T) string {
				t.Helper()

				root := t.TempDir()
				finalizeVerifiedNode(t, root, archive.SnapshotYAML{
					Kind: "Snapshot",
					Name: "root",
				})

				return root
			},
			limits:  localscan.DefaultScanLimits(),
			wantErr: archive.ErrInvalidSnapshotYAML,
		},
		{
			name: "node budget",
			prepare: func(t *testing.T) string {
				t.Helper()

				root := t.TempDir()
				child := filepath.Join(root, archive.SnapshotsDirName, "snapshot_child")
				if err := os.MkdirAll(child, 0o755); err != nil {
					t.Fatalf("create child: %v", err)
				}

				finalizeVerifiedNode(t, child, archive.SnapshotYAML{
					APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
					Kind:       "Snapshot",
					Name:       "child",
				})

				finalizeVerifiedNode(t, root, archive.SnapshotYAML{
					APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
					Kind:       "Snapshot",
					Name:       "root",
				})

				return root
			},
			limits: localscan.ScanLimits{
				MaxDepth: 64,
				MaxNodes: 1,
			},
			wantErr: localscan.ErrScanBudget,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := test.prepare(t)

			node, err := localscan.ScanVerifiedWithLimitsAndOptions(
				root,
				test.limits,
				archive.SnapshotYAMLReadOptions{},
			)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("ScanVerifiedWithLimitsAndOptions() error = %v, want %v", err, test.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("ScanVerifiedWithLimitsAndOptions(): %v", err)
			}

			if len(node.Children) != 1 {
				t.Fatalf("verified children = %d, want 1", len(node.Children))
			}
		})
	}
}

// finalizeVerifiedNode computes and writes both NodeChecksum and ChildrenChecksum for dir,
// matching the pipeline's bottom-up publication contract (see pipeline.run): callers MUST
// finalize every direct child (recursively) before finalizing dir, or the ChildrenChecksum
// computed here will not match the child set a later verification pass observes on disk.
func finalizeVerifiedNode(t *testing.T, dir string, sy archive.SnapshotYAML) {
	t.Helper()

	if err := os.MkdirAll(filepath.Join(dir, archive.ManifestsDirName), 0o755); err != nil {
		t.Fatalf("create manifests directory for %s: %v", dir, err)
	}

	childrenChecksum, err := archive.ComputeNodeChildrenChecksum(dir)
	if err != nil {
		t.Fatalf("compute children checksum for %s: %v", dir, err)
	}

	sy.ChildrenChecksum = &childrenChecksum

	checksum, err := archive.ComputeNodeChecksum(dir)
	if err != nil {
		t.Fatalf("compute checksum for %s: %v", dir, err)
	}

	sy.Checksum = checksum
	writeNodeYAML(t, dir, sy)
}

// TestScanVerified_RejectsHybridTree proves ScanVerifiedWithLimitsAndOptions — the production
// entry point behind `d8 snapshot local get`/`describe` — rejects every hybrid-tree shape
// AC-2 lists (added, removed, replaced, or duplicated children) both at the root's direct
// children and one level deeper (multi-level), and never a false positive on a valid tree.
func TestScanVerified_RejectsHybridTree(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(t *testing.T) string
		wantErr error
	}{
		{
			name: "added child not in the commitment",
			prepare: func(t *testing.T) string {
				t.Helper()

				root := t.TempDir()
				childA := filepath.Join(root, archive.SnapshotsDirName, "snapshot_child-a")
				finalizeVerifiedNode(t, childA, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "child-a"})
				// Root's commitment is computed and stored here, over {child-a} only.
				finalizeVerifiedNode(t, root, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "root"})

				// child-b is added to disk AFTER root's commitment was written and never
				// authenticated by it: a textbook hybrid-tree addition.
				childB := filepath.Join(root, archive.SnapshotsDirName, "snapshot_child-b")
				finalizeVerifiedNode(t, childB, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "child-b"})

				return root
			},
			wantErr: archive.ErrChildrenChecksumMismatch,
		},
		{
			name: "removed child still referenced by the commitment",
			prepare: func(t *testing.T) string {
				t.Helper()

				root := t.TempDir()
				childA := filepath.Join(root, archive.SnapshotsDirName, "snapshot_child-a")
				finalizeVerifiedNode(t, childA, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "child-a"})
				childB := filepath.Join(root, archive.SnapshotsDirName, "snapshot_child-b")
				finalizeVerifiedNode(t, childB, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "child-b"})
				// Root commits to {child-a, child-b}.
				finalizeVerifiedNode(t, root, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "root"})

				if err := os.RemoveAll(childB); err != nil {
					t.Fatalf("remove child-b: %v", err)
				}

				return root
			},
			wantErr: archive.ErrChildrenChecksumMismatch,
		},
		{
			name: "replaced child identity without re-finalizing the parent",
			prepare: func(t *testing.T) string {
				t.Helper()

				root := t.TempDir()
				child := filepath.Join(root, archive.SnapshotsDirName, "snapshot_child")
				finalizeVerifiedNode(t, child, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "original"})
				// Root's commitment authenticates the "original" identity at this path.
				finalizeVerifiedNode(t, root, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "root"})

				// The child subtree at the SAME path is now swapped for a different, internally
				// self-consistent child (its own NodeChecksum/ChildrenChecksum are valid), but
				// root's stale commitment still names the old identity/digest.
				finalizeVerifiedNode(t, child, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "replaced"})

				return root
			},
			wantErr: archive.ErrChildrenChecksumMismatch,
		},
		{
			name: "duplicate direct child identity",
			prepare: func(t *testing.T) string {
				t.Helper()

				root := t.TempDir()
				dupSY := archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "dup", Namespace: "ns"}
				childA := filepath.Join(root, archive.SnapshotsDirName, "snapshot_child-a")
				finalizeVerifiedNode(t, childA, dupSY)
				childB := filepath.Join(root, archive.SnapshotsDirName, "snapshot_child-b")
				finalizeVerifiedNode(t, childB, dupSY)

				// Root can never legitimately finalize over these two children (their shared
				// identity makes ComputeNodeChildrenChecksum itself fail), so its own
				// commitment is fabricated from an unrelated, single-child snapshot below —
				// any stored value is equally unable to authenticate this on-disk duplicate.
				other := t.TempDir()
				finalizeVerifiedNode(t, filepath.Join(other, archive.SnapshotsDirName, "snapshot_solo"),
					archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "solo"})
				borrowedChecksum, err := archive.ComputeNodeChildrenChecksum(other)
				if err != nil {
					t.Fatalf("compute unrelated children checksum: %v", err)
				}

				if err := os.MkdirAll(filepath.Join(root, archive.ManifestsDirName), 0o755); err != nil {
					t.Fatalf("create root manifests: %v", err)
				}

				rootChecksum, err := archive.ComputeNodeChecksum(root)
				if err != nil {
					t.Fatalf("compute root checksum: %v", err)
				}

				writeNodeYAML(t, root, archive.SnapshotYAML{
					Kind: "Snapshot", APIVersion: "v1", Name: "root",
					Checksum: rootChecksum, ChildrenChecksum: &borrowedChecksum,
				})

				return root
			},
			wantErr: archive.ErrInvalidSnapshotYAML,
		},
		{
			name: "multi-level: grandchild swap invalidates the child's own commitment",
			prepare: func(t *testing.T) string {
				t.Helper()

				root := t.TempDir()
				child := filepath.Join(root, archive.SnapshotsDirName, "snapshot_child")
				grandchild := filepath.Join(child, archive.SnapshotsDirName, "snapshot_grandchild")

				finalizeVerifiedNode(t, grandchild, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "original-gc"})
				// child's commitment authenticates {original-gc}.
				finalizeVerifiedNode(t, child, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "child"})
				// root's commitment authenticates {child} (computed AFTER child was finalized).
				finalizeVerifiedNode(t, root, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "root"})

				// The grandchild is swapped without re-finalizing its parent (child): child's
				// stored commitment now diverges from the actual on-disk grandchild.
				finalizeVerifiedNode(t, grandchild, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "replaced-gc"})

				return root
			},
			wantErr: archive.ErrChildrenChecksumMismatch,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := test.prepare(t)

			if _, err := localscan.ScanVerifiedWithLimitsAndOptions(
				root, localscan.DefaultScanLimits(), archive.SnapshotYAMLReadOptions{},
			); !errors.Is(err, test.wantErr) {
				t.Fatalf("ScanVerifiedWithLimitsAndOptions() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestScan_RootWithDirectChildren(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeNodeYAML(t, root, archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
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

func TestScanWithLimits_RejectsTraversalBudget(t *testing.T) {
	tests := []struct {
		name   string
		limits localscan.ScanLimits
		build  func(t *testing.T) string
		want   string
	}{
		{
			name: "maxDepth",
			limits: localscan.ScanLimits{
				MaxDepth: 1,
				MaxNodes: 10,
			},
			build: func(t *testing.T) string {
				t.Helper()

				root := t.TempDir()
				writeNodeYAML(t, root, archive.SnapshotYAML{Kind: "Snapshot", Name: "root"})
				child := makeChildDir(t, root, "snapshot_child", archive.SnapshotYAML{
					Kind: "Snapshot",
					Name: "child",
				})
				makeChildDir(t, child, "snapshot_grandchild", archive.SnapshotYAML{
					Kind: "Snapshot",
					Name: "grandchild",
				})

				return root
			},
			want: "maxDepth",
		},
		{
			name: "maxNodes",
			limits: localscan.ScanLimits{
				MaxDepth: 10,
				MaxNodes: 2,
			},
			build: func(t *testing.T) string {
				t.Helper()

				root := t.TempDir()
				writeNodeYAML(t, root, archive.SnapshotYAML{Kind: "Snapshot", Name: "root"})
				makeChildDir(t, root, "snapshot_child_a", archive.SnapshotYAML{
					Kind: "Snapshot",
					Name: "child-a",
				})
				makeChildDir(t, root, "snapshot_child_b", archive.SnapshotYAML{
					Kind: "Snapshot",
					Name: "child-b",
				})

				return root
			},
			want: "maxNodes",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := test.build(t)

			_, err := localscan.ScanWithLimits(root, test.limits)
			if !errors.Is(err, localscan.ErrScanBudget) {
				t.Fatalf("ScanWithLimits() error = %v, want ErrScanBudget", err)
			}

			if !strings.Contains(err.Error(), test.want) {
				t.Errorf("ScanWithLimits() error = %q, want budget name %q", err, test.want)
			}
		})
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
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "root-snap",
	})

	parentDir := makeChildDir(t, root, "snapshot_parent", archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
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

func TestNode_VolumeCount_RecursiveSum(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	blockVol := archive.VolumeInfo{
		Target: archive.VolumeObjectRef{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
			Name:       "disk-a-pvc",
			Namespace:  "ns-a",
		},
		VolumeMode: "Block",
	}

	orphanVol := archive.VolumeInfo{
		Target: archive.VolumeObjectRef{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
			Name:       "demo-pvc",
			Namespace:  "ns-a",
		},
		VolumeMode: "Filesystem",
	}

	// Root aggregator owns no data (empty Volumes) — matches decision #4:
	// data lives in the owning domain/leaf node, never the aggregator.
	writeNodeYAML(t, root, archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "root-snap",
		Namespace:  "ns-a",
	})

	// vm-snap owns no data either (only child disk-snapshot nodes).
	vmDir := makeChildDir(t, root, "demovirtualmachinesnapshot_vm-a", archive.SnapshotYAML{
		APIVersion: "demo.deckhouse.io/v1alpha1",
		Kind:       "DemoVirtualMachineSnapshot",
		Name:       "nss-child-vm-a",
		Namespace:  "ns-a",
	})

	// disk-a is a domain node that owns one captured volume.
	makeChildDir(t, vmDir, "demovirtualdisksnapshot_disk-a", archive.SnapshotYAML{
		APIVersion: "demo.deckhouse.io/v1alpha1",
		Kind:       "DemoVirtualDiskSnapshot",
		Name:       "nss-child-disk-a",
		Namespace:  "ns-a",
		Volumes:    []archive.VolumeInfo{blockVol},
	})

	// An orphan-PVC leaf directly under the root owns its own volume.
	makeChildDir(t, root, "volumesnapshot_demo-pvc", archive.SnapshotYAML{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "nss-vs-demo-pvc",
		Namespace:  "ns-a",
		Volumes:    []archive.VolumeInfo{orphanVol},
	})

	node, err := localscan.Scan(root)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if len(node.Volumes) != 0 {
		t.Fatalf("root Volumes: got %d, want 0 (aggregator owns no data)", len(node.Volumes))
	}

	// This is the assertion that fails against the old `len(root.Volumes)`
	// behavior (which would report 0 here instead of 2).
	if got, want := node.VolumeCount(), 2; got != want {
		t.Errorf("VolumeCount: got %d, want %d", got, want)
	}
}

func TestNode_VolumeCount_ZeroVolumeTree(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeNodeYAML(t, root, archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "root-snap",
	})

	makeChildDir(t, root, "snapshot_child", archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "child-snap",
	})

	node, err := localscan.Scan(root)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if got, want := node.VolumeCount(), 0; got != want {
		t.Errorf("VolumeCount: got %d, want %d", got, want)
	}
}

func TestScan_EmptySnapshotsDir(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeNodeYAML(t, root, archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
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
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
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
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
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

// writeOrphanCollisionDir creates a child directory under parent/snapshots/ that carries an
// identity marker but no snapshot.yaml — the on-disk shape archive.CollisionNodeDir leaves
// behind for an interrupted-and-resumed download (see archive/resume.go). Scan must skip it
// rather than fail.
func writeOrphanCollisionDir(t *testing.T, parent, name string) {
	t.Helper()

	dir := filepath.Join(parent, archive.SnapshotsDirName, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll orphan %s: %v", dir, err)
	}

	if err := archive.WriteNodeIdentityMarker(dir, archive.NodeIdentity{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       name,
	}); err != nil {
		t.Fatalf("write identity marker in %s: %v", dir, err)
	}
}

// TestScan_SkipsChildDirWithoutSnapshotYAML proves Scan tolerates an orphaned collision/resume
// directory (see writeOrphanCollisionDir): it is skipped rather than failing the whole scan.
func TestScan_SkipsChildDirWithoutSnapshotYAML(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeNodeYAML(t, root, archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "root-snap",
	})

	makeChildDir(t, root, "snapshot_child", archive.SnapshotYAML{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "child-snap",
	})

	writeOrphanCollisionDir(t, root, "snapshot_orphan__deadbeef")

	node, err := localscan.Scan(root)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if len(node.Children) != 1 {
		t.Fatalf("Children: got %d, want 1 (orphan directory without snapshot.yaml must be skipped)",
			len(node.Children))
	}

	if node.Children[0].Name != "child-snap" {
		t.Errorf("Children[0].Name: got %q, want %q", node.Children[0].Name, "child-snap")
	}
}

// TestScanVerified_SkipsChildDirWithoutSnapshotYAML proves the verified scan path skips an
// orphaned collision/resume directory WITHOUT attempting to verify its integrity: since the
// orphan carries no snapshot.yaml, VerifyNode/ReadSnapshotYAML would fail immediately if the
// verified path recursed into it, so a successful scan here proves it did not.
func TestScanVerified_SkipsChildDirWithoutSnapshotYAML(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	child := filepath.Join(root, archive.SnapshotsDirName, "snapshot_child")
	finalizeVerifiedNode(t, child, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "child"})
	finalizeVerifiedNode(t, root, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "root"})

	writeOrphanCollisionDir(t, root, "snapshot_orphan__deadbeef")

	node, err := localscan.ScanVerified(root)
	if err != nil {
		t.Fatalf("ScanVerified: %v", err)
	}

	if len(node.Children) != 1 {
		t.Fatalf("Children: got %d, want 1 (orphan directory must be skipped, not verified)", len(node.Children))
	}
}

// TestScan_NonRegularSnapshotYAMLInChildStaysError proves the orphaned-directory tolerance
// does NOT widen to cover a non-regular snapshot.yaml (a symlink or a directory) in a child
// node directory: Scan must still fail hard.
func TestScan_NonRegularSnapshotYAMLInChildStaysError(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, dir string)
	}{
		{
			name: "symlink",
			setup: func(t *testing.T, dir string) {
				t.Helper()

				path := filepath.Join(dir, archive.SnapshotYAMLName)
				if err := os.WriteFile(path, []byte("kind: Snapshot\n"), 0o600); err != nil {
					t.Fatalf("write snapshot.yaml: %v", err)
				}

				outside := filepath.Join(t.TempDir(), "elsewhere.yaml")
				if err := os.Rename(path, outside); err != nil {
					t.Fatalf("move snapshot.yaml outside: %v", err)
				}

				if err := os.Symlink(outside, path); err != nil {
					t.Fatalf("symlink snapshot.yaml: %v", err)
				}
			},
		},
		{
			name: "directory",
			setup: func(t *testing.T, dir string) {
				t.Helper()

				if err := os.MkdirAll(filepath.Join(dir, archive.SnapshotYAMLName), 0o755); err != nil {
					t.Fatalf("mkdir snapshot.yaml: %v", err)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeNodeYAML(t, root, archive.SnapshotYAML{
				APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
				Kind:       "Snapshot",
				Name:       "root-snap",
			})

			bogus := filepath.Join(root, archive.SnapshotsDirName, "snapshot_bogus")
			if err := os.MkdirAll(bogus, 0o755); err != nil {
				t.Fatalf("mkdir bogus child: %v", err)
			}

			tc.setup(t, bogus)

			if _, err := localscan.Scan(root); err == nil {
				t.Fatal("Scan() error = nil, want error for non-regular snapshot.yaml")
			}
		})
	}
}

// TestScanVerifiedWithOptionsReportingSkips_ReportsPaths proves the reporting scan variant
// returns every skipped orphaned collision/resume directory, truncating the reported Paths
// sample while keeping an untruncated Total.
func TestScanVerifiedWithOptionsReportingSkips_ReportsPaths(t *testing.T) {
	root := t.TempDir()
	child := filepath.Join(root, archive.SnapshotsDirName, "snapshot_child")
	finalizeVerifiedNode(t, child, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "child"})
	finalizeVerifiedNode(t, root, archive.SnapshotYAML{Kind: "Snapshot", APIVersion: "v1", Name: "root"})

	const orphanCount = 40 // exceeds the package's internal reporting cap, proving truncation

	for i := 0; i < orphanCount; i++ {
		writeOrphanCollisionDir(t, root, fmt.Sprintf("snapshot_orphan-%02d", i))
	}

	node, skipped, err := localscan.ScanVerifiedWithOptionsReportingSkips(root, archive.SnapshotYAMLReadOptions{})
	if err != nil {
		t.Fatalf("ScanVerifiedWithOptionsReportingSkips: %v", err)
	}

	if len(node.Children) != 1 {
		t.Fatalf("Children: got %d, want 1", len(node.Children))
	}

	if skipped.Total != orphanCount {
		t.Errorf("skipped.Total = %d, want %d", skipped.Total, orphanCount)
	}

	if len(skipped.Paths) >= orphanCount {
		t.Errorf("len(skipped.Paths) = %d, want a truncated sample smaller than Total (%d)",
			len(skipped.Paths), orphanCount)
	}
}
