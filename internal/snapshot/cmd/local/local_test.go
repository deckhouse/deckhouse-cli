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

package local_test

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/cmd/local"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/localscan"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/treeview"
)

// discardLog returns a logger that discards all output, used so tests do not
// pollute the terminal.
func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// writeNodeYAML writes sy to dir/snapshot.yaml, failing t on error.
func writeNodeYAML(t *testing.T, dir string, sy archive.SnapshotYAML) {
	t.Helper()

	if err := archive.WriteSnapshotYAML(dir, sy); err != nil {
		t.Fatalf("WriteSnapshotYAML %s: %v", dir, err)
	}
}

// makeChildDir creates <parent>/snapshots/<name>/, writes sy, and returns the path.
func makeChildDir(t *testing.T, parent, name string, sy archive.SnapshotYAML) string {
	t.Helper()

	dir := filepath.Join(parent, archive.SnapshotsDirName, name)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll %s: %v", dir, err)
	}

	writeNodeYAML(t, dir, sy)

	return dir
}

// nsOrDash returns ns unchanged unless it is empty, in which case it returns "-".
// This mirrors the display convention used by 'local get'.
func nsOrDash(ns string) string {
	if ns == "" {
		return "-"
	}

	return ns
}

// formatSummaryLine formats the one-line summary that 'local get' is expected
// to emit for the root node of the scanned archive.
// Format: "<name>  <kind>  <namespace-or-dash>  <N> children  <M> volume(s)"
func formatSummaryLine(n *localscan.Node) string {
	nChildren := len(n.Children)
	nVolumes := len(n.Volumes)
	volWord := "volumes"

	if nVolumes == 1 {
		volWord = "volume"
	}

	return fmt.Sprintf("%s  %s  %s  %d children  %d %s",
		n.Name,
		n.Kind,
		nsOrDash(n.Namespace),
		nChildren,
		nVolumes,
		volWord,
	)
}

// scanToTreeViewNode maps a *localscan.Node tree into a treeview.Node tree.
// This mirrors the mapping that 'local describe' will perform: each node's
// label is "<Kind>/<Name>" and volumes become leaf strings.
func scanToTreeViewNode(n *localscan.Node) treeview.Node {
	label := n.Kind + "/" + n.Name

	children := make([]treeview.Node, 0, len(n.Children))
	for _, c := range n.Children {
		children = append(children, scanToTreeViewNode(c))
	}

	vols := make([]string, 0, len(n.Volumes))
	for _, v := range n.Volumes {
		vols = append(vols, v.Target.Name)
	}

	return treeview.Node{
		Label:    label,
		Children: children,
		Volumes:  vols,
	}
}

// TestLocalGroup_NoErrorOnNoArgs verifies that running the parent command with
// no arguments succeeds (it prints help and returns nil).
func TestLocalGroup_NoErrorOnNoArgs(t *testing.T) {
	t.Parallel()

	cmd := local.NewCommand(discardLog())

	var out bytes.Buffer

	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute with no args: unexpected error: %v", err)
	}
}

// TestLocalGroup_HelpMentionsOffline asserts that the parent command's help
// text communicates that no kubeconfig is required.
func TestLocalGroup_HelpMentionsOffline(t *testing.T) {
	t.Parallel()

	cmd := local.NewCommand(discardLog())

	var out bytes.Buffer

	cmd.SetOut(&out)
	cmd.SetErr(&out)

	if err := cmd.Help(); err != nil {
		t.Fatalf("Help(): %v", err)
	}

	got := out.String()

	for _, want := range []string{"no kubeconfig", "locally"} {
		if !strings.Contains(got, want) {
			t.Errorf("help text missing %q\nfull output:\n%s", want, got)
		}
	}
}

// TestLocalGroup_NoPersistentKubeFlags asserts that the parent command registers
// no persistent flags for kubeconfig, context, or namespace. The local group is
// fully offline and must never require cluster credentials.
func TestLocalGroup_NoPersistentKubeFlags(t *testing.T) {
	t.Parallel()

	cmd := local.NewCommand(discardLog())
	pflags := cmd.PersistentFlags()

	for _, forbidden := range []string{"kubeconfig", "context", "namespace", "kube-context"} {
		if f := pflags.Lookup(forbidden); f != nil {
			t.Errorf("unexpected persistent flag --%s registered on the local group", forbidden)
		}
	}
}

// TestLocalGetCommand exercises the expected behavior of 'd8 snapshot local get':
//   - The parent group itself rejects 'get' as an unknown subcommand until
//     get.go is wired in with AddCommand (cobra returns an error).
//   - localscan.Scan on a fabricated archive produces the node fields that
//     'local get' will format into its one-line summary.
//
// When get.go is wired in, this test should be extended to call the actual
// command and assert cmd.OutOrStdout() against the formatted summary line.
func TestLocalGetCommand(t *testing.T) {
	t.Parallel()

	t.Run("parent rejects unknown subcommand", func(t *testing.T) {
		t.Parallel()

		cmd := local.NewCommand(discardLog())

		var out bytes.Buffer

		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs([]string{"get"})

		// Until get.go is wired in, 'get' is an unknown subcommand.
		// Once wired with cobra.ExactArgs(1), calling 'get' with no
		// dir argument will also error — both cases return a non-nil error.
		if err := cmd.Execute(); err == nil {
			t.Error("Execute with unknown 'get': expected error, got nil")
		}
	})

	t.Run("localscan data matches expected summary fields", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		writeNodeYAML(t, root, archive.SnapshotYAML{
			APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
			Kind:       "Snapshot",
			Name:       "vm-snap",
			Namespace:  "prod",
		})

		makeChildDir(t, root, "demovirtualdisksnapshot_disk-a", archive.SnapshotYAML{
			APIVersion: "demo.deckhouse.io/v1alpha1",
			Kind:       "DemoVirtualDiskSnapshot",
			Name:       "nss-child-a",
			Namespace:  "prod",
		})

		makeChildDir(t, root, "demovirtualdisksnapshot_disk-b", archive.SnapshotYAML{
			APIVersion: "demo.deckhouse.io/v1alpha1",
			Kind:       "DemoVirtualDiskSnapshot",
			Name:       "nss-child-b",
			Namespace:  "prod",
			Volumes: []archive.VolumeInfo{{
				Target: archive.VolumeObjectRef{
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Name:       "disk-b-pvc",
					Namespace:  "prod",
				},
			}},
		})

		node, err := localscan.Scan(root)
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}

		summary := formatSummaryLine(node)

		for _, want := range []string{"vm-snap", "Snapshot", "prod", "2 children"} {
			if !strings.Contains(summary, want) {
				t.Errorf("summary line missing %q; got: %q", want, summary)
			}
		}
	})

	t.Run("cluster-scoped namespace renders as dash", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		writeNodeYAML(t, root, archive.SnapshotYAML{
			APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
			Kind:       "Snapshot",
			Name:       "cluster-snap",
			// Namespace intentionally empty (cluster-scoped).
		})

		node, err := localscan.Scan(root)
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}

		summary := formatSummaryLine(node)

		if !strings.Contains(summary, "-") {
			t.Errorf("summary line for cluster-scoped snap must contain '-' for namespace; got: %q", summary)
		}
	})
}

// TestLocalDescribeCommand exercises the expected behavior of 'd8 snapshot local describe':
//   - The parent group rejects 'describe' as an unknown subcommand until
//     describe.go is wired in (cobra returns an error).
//   - localscan.Scan + treeview.Render on a fabricated archive produces the
//     ASCII tree that 'local describe' will render.
//
// When describe.go is wired in, this test should be extended to call the actual
// command and assert cmd.OutOrStdout() against the rendered tree.
func TestLocalDescribeCommand(t *testing.T) {
	t.Parallel()

	t.Run("parent rejects unknown subcommand", func(t *testing.T) {
		t.Parallel()

		cmd := local.NewCommand(discardLog())

		var out bytes.Buffer

		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs([]string{"describe"})

		// Until describe.go is wired in, 'describe' is an unknown subcommand.
		// Once wired with cobra.ExactArgs(1), calling 'describe' with no
		// dir argument will also error.
		if err := cmd.Execute(); err == nil {
			t.Error("Execute with unknown 'describe': expected error, got nil")
		}
	})

	t.Run("treeview rendering of scanned archive", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		writeNodeYAML(t, root, archive.SnapshotYAML{
			APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
			Kind:       "Snapshot",
			Name:       "root-snap",
			Namespace:  "default",
		})

		makeChildDir(t, root, "demovirtualdisksnapshot_disk-a", archive.SnapshotYAML{
			APIVersion: "demo.deckhouse.io/v1alpha1",
			Kind:       "DemoVirtualDiskSnapshot",
			Name:       "nss-child-a",
			Namespace:  "default",
		})

		leafDir := filepath.Join(root, archive.SnapshotsDirName, "volumesnapshot_pvc-b")

		if err := os.MkdirAll(leafDir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}

		writeNodeYAML(t, leafDir, archive.SnapshotYAML{
			APIVersion: "snapshot.storage.k8s.io/v1",
			Kind:       "VolumeSnapshot",
			Name:       "nss-vs-pvc-b",
			Namespace:  "default",
			Volumes: []archive.VolumeInfo{{
				Target: archive.VolumeObjectRef{
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Name:       "pvc-b",
					Namespace:  "default",
				},
			}},
		})

		node, err := localscan.Scan(root)
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}

		tvRoot := scanToTreeViewNode(node)

		var out bytes.Buffer

		if err := treeview.Render(&out, tvRoot); err != nil {
			t.Fatalf("treeview.Render: %v", err)
		}

		rendered := out.String()

		for _, want := range []string{
			"Snapshot/root-snap",
			"DemoVirtualDiskSnapshot/nss-child-a",
			"VolumeSnapshot/nss-vs-pvc-b",
			"pvc-b",
		} {
			if !strings.Contains(rendered, want) {
				t.Errorf("rendered tree missing %q\nfull output:\n%s", want, rendered)
			}
		}
	})

	t.Run("single root with volume rendered", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		writeNodeYAML(t, root, archive.SnapshotYAML{
			APIVersion: "snapshot.storage.k8s.io/v1",
			Kind:       "VolumeSnapshot",
			Name:       "nss-vs-pvc-a",
			Namespace:  "ns-a",
			Volumes: []archive.VolumeInfo{{
				Target: archive.VolumeObjectRef{
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Name:       "my-pvc",
					Namespace:  "ns-a",
				},
				VolumeMode: "Block",
				Size:       "10Gi",
			}},
		})

		node, err := localscan.Scan(root)
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}

		if len(node.Volumes) != 1 {
			t.Fatalf("expected 1 volume, got %d", len(node.Volumes))
		}

		tvRoot := scanToTreeViewNode(node)

		var out bytes.Buffer

		if err := treeview.Render(&out, tvRoot); err != nil {
			t.Fatalf("treeview.Render: %v", err)
		}

		rendered := out.String()

		for _, want := range []string{"VolumeSnapshot/nss-vs-pvc-a", "my-pvc"} {
			if !strings.Contains(rendered, want) {
				t.Errorf("rendered tree missing %q\nfull output:\n%s", want, rendered)
			}
		}
	})
}

// TestLocalScan_VolumeCountInSummary verifies that the one-line summary
// correctly reflects multi-volume nodes when formatting the summary line.
func TestLocalScan_VolumeCountInSummary(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeNodeYAML(t, root, archive.SnapshotYAML{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "multi-vol",
		Namespace:  "ns-x",
		Volumes: []archive.VolumeInfo{
			{Target: archive.VolumeObjectRef{Name: "pvc-1"}},
			{Target: archive.VolumeObjectRef{Name: "pvc-2"}},
		},
	})

	node, err := localscan.Scan(root)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	summary := formatSummaryLine(node)

	if !strings.Contains(summary, "2 volumes") {
		t.Errorf("expected '2 volumes' in summary; got: %q", summary)
	}

	if !strings.Contains(summary, "0 children") {
		t.Errorf("expected '0 children' in summary; got: %q", summary)
	}
}
