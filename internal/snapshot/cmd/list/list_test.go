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

package list

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

// snapshotObj builds an unstructured Snapshot for tests. An empty ready/content
// is omitted; age==0 leaves creationTimestamp unset.
func snapshotObj(namespace, name, ready, content string, children int, age time.Duration) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "Snapshot",
		"metadata": map[string]interface{}{
			"namespace": namespace,
			"name":      name,
		},
	}}

	if age > 0 {
		obj.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-age)))
	}

	status := map[string]interface{}{}

	if ready != "" {
		status["conditions"] = []interface{}{
			map[string]interface{}{"type": "Ready", "status": ready},
		}
	}

	if content != "" {
		status["boundSnapshotContentName"] = content
	}

	if children > 0 {
		refs := make([]interface{}, 0, children)
		for i := 0; i < children; i++ {
			refs = append(refs, map[string]interface{}{
				"apiVersion": "storage.deckhouse.io/v1alpha1",
				"kind":       "Snapshot",
				"name":       fmt.Sprintf("child-%d", i),
			})
		}

		status["childrenSnapshotRefs"] = refs
	}

	if len(status) > 0 {
		obj.Object["status"] = status
	}

	return obj
}

func newFakeDynamic(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	gvrToListKind := map[schema.GroupVersionResource]string{
		snapshotGVR: "SnapshotList",
	}

	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), gvrToListKind, objs...)
}

func TestReadyStatus(t *testing.T) {
	cases := []struct {
		name string
		obj  map[string]interface{}
		want string
	}{
		{
			name: "ready true",
			obj: map[string]interface{}{"status": map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "True"},
			}}},
			want: "True",
		},
		{
			name: "ready false",
			obj: map[string]interface{}{"status": map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "False"},
			}}},
			want: "False",
		},
		{
			name: "ready unknown",
			obj: map[string]interface{}{"status": map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "Unknown"},
			}}},
			want: "Unknown",
		},
		{
			name: "other condition only",
			obj: map[string]interface{}{"status": map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "VolumesReady", "status": "True"},
			}}},
			want: notAvailable,
		},
		{
			name: "no conditions",
			obj:  map[string]interface{}{"status": map[string]interface{}{}},
			want: notAvailable,
		},
		{
			name: "no status",
			obj:  map[string]interface{}{},
			want: notAvailable,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := readyStatus(&unstructured.Unstructured{Object: tc.obj})
			if got != tc.want {
				t.Fatalf("readyStatus = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildSnapshotRows(t *testing.T) {
	items := []unstructured.Unstructured{
		*snapshotObj("ns1", "ready-snap", "True", "content-a", 2, 5*time.Minute),
		*snapshotObj("ns1", "bare-snap", "", "", 0, 0),
	}

	rows := buildSnapshotRows(items)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	if rows[0].Name != "ready-snap" || rows[0].Ready != "True" ||
		rows[0].SnapshotContent != "content-a" || rows[0].Children != 2 {
		t.Fatalf("unexpected first row: %+v", rows[0])
	}

	if rows[0].Age != "5m" {
		t.Fatalf("expected age 5m, got %q", rows[0].Age)
	}

	// Bare snapshot: empty content and no conditions fall back to "-".
	if rows[1].Ready != notAvailable || rows[1].SnapshotContent != notAvailable ||
		rows[1].Children != 0 || rows[1].Age != notAvailable {
		t.Fatalf("unexpected bare row: %+v", rows[1])
	}
}

func TestPrintSnapshotTableSingleNamespace(t *testing.T) {
	var buf bytes.Buffer

	rows := buildSnapshotRows([]unstructured.Unstructured{
		*snapshotObj("ns1", "snap-a", "True", "content-a", 1, time.Hour),
	})

	if err := printSnapshotTable(&buf, rows, false); err != nil {
		t.Fatalf("printSnapshotTable: %v", err)
	}

	out := buf.String()

	if !strings.Contains(out, "NAME") || !strings.Contains(out, "READY") ||
		!strings.Contains(out, "SNAPSHOTCONTENT") || !strings.Contains(out, "CHILDREN") ||
		!strings.Contains(out, "AGE") {
		t.Fatalf("missing expected header columns:\n%s", out)
	}

	if strings.Contains(out, "NAMESPACE") {
		t.Fatalf("single-namespace table must not include NAMESPACE column:\n%s", out)
	}

	if !strings.Contains(out, "snap-a") || !strings.Contains(out, "content-a") {
		t.Fatalf("missing row data:\n%s", out)
	}
}

func TestPrintSnapshotTableAllNamespaces(t *testing.T) {
	var buf bytes.Buffer

	rows := buildSnapshotRows([]unstructured.Unstructured{
		*snapshotObj("ns1", "snap-a", "True", "content-a", 0, time.Hour),
		*snapshotObj("ns2", "snap-b", "False", "", 0, time.Hour),
	})

	if err := printSnapshotTable(&buf, rows, true); err != nil {
		t.Fatalf("printSnapshotTable: %v", err)
	}

	out := buf.String()

	if !strings.HasPrefix(strings.TrimSpace(out), "NAMESPACE") {
		t.Fatalf("all-namespaces table must lead with NAMESPACE column:\n%s", out)
	}

	if !strings.Contains(out, "ns1") || !strings.Contains(out, "ns2") {
		t.Fatalf("missing namespace values:\n%s", out)
	}
}

func TestPrintSnapshotTableEmpty(t *testing.T) {
	var buf bytes.Buffer

	if err := printSnapshotTable(&buf, nil, false); err != nil {
		t.Fatalf("printSnapshotTable: %v", err)
	}

	if got := strings.TrimSpace(buf.String()); got != "No snapshots found." {
		t.Fatalf("empty list output = %q, want %q", got, "No snapshots found.")
	}
}

func TestRenderJSON(t *testing.T) {
	list := &unstructured.UnstructuredList{Object: map[string]interface{}{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "SnapshotList",
	}}
	list.Items = []unstructured.Unstructured{
		*snapshotObj("ns1", "snap-a", "True", "content-a", 0, time.Hour),
	}

	var buf bytes.Buffer
	if err := render(&buf, list, false, "json"); err != nil {
		t.Fatalf("render json: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, `"items"`) || !strings.Contains(out, "snap-a") ||
		!strings.Contains(out, "content-a") {
		t.Fatalf("json output missing expected content:\n%s", out)
	}
}

func TestRenderYAML(t *testing.T) {
	list := &unstructured.UnstructuredList{Object: map[string]interface{}{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "SnapshotList",
	}}
	list.Items = []unstructured.Unstructured{
		*snapshotObj("ns1", "snap-a", "True", "content-a", 0, time.Hour),
	}

	var buf bytes.Buffer
	if err := render(&buf, list, false, "yaml"); err != nil {
		t.Fatalf("render yaml: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "items:") || !strings.Contains(out, "snap-a") {
		t.Fatalf("yaml output missing expected content:\n%s", out)
	}
}

func TestRenderUnsupportedFormat(t *testing.T) {
	list := &unstructured.UnstructuredList{}

	err := render(io.Discard, list, false, "wide")
	if !errors.Is(err, errUnsupportedFormat) {
		t.Fatalf("expected errUnsupportedFormat, got %v", err)
	}
}

func TestListSnapshots(t *testing.T) {
	dyn := newFakeDynamic(
		snapshotObj("ns1", "snap-a", "True", "content-a", 0, time.Hour),
		snapshotObj("ns1", "snap-b", "False", "", 0, time.Hour),
		snapshotObj("ns2", "snap-c", "True", "content-c", 0, time.Hour),
	)

	t.Run("single namespace", func(t *testing.T) {
		list, err := listSnapshots(context.Background(), dyn, "ns1", false)
		if err != nil {
			t.Fatalf("listSnapshots: %v", err)
		}

		if len(list.Items) != 2 {
			t.Fatalf("expected 2 snapshots in ns1, got %d", len(list.Items))
		}
	})

	t.Run("all namespaces", func(t *testing.T) {
		list, err := listSnapshots(context.Background(), dyn, "", true)
		if err != nil {
			t.Fatalf("listSnapshots: %v", err)
		}

		if len(list.Items) != 3 {
			t.Fatalf("expected 3 snapshots across all namespaces, got %d", len(list.Items))
		}
	})
}

func TestRunMutuallyExclusiveScope(t *testing.T) {
	cmd := NewCommand(slog.Default())
	cmd.SetArgs([]string{"--all-namespaces", "--namespace", "ns1"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when -A and -n are combined")
	}

	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected mutually-exclusive error, got %v", err)
	}
}

func TestHumanAge(t *testing.T) {
	if got := humanAge(time.Time{}); got != notAvailable {
		t.Fatalf("zero time age = %q, want %q", got, notAvailable)
	}

	if got := humanAge(time.Now().Add(-5 * time.Minute)); got != "5m" {
		t.Fatalf("age = %q, want %q", got, "5m")
	}
}

// writeLocalNode creates a node directory with a snapshot.yaml, mirroring the
// download tree layout used on disk.
func writeLocalNode(t *testing.T, dir string, sy archive.SnapshotYAML) {
	t.Helper()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}

	if err := archive.WriteSnapshotYAML(dir, sy); err != nil {
		t.Fatalf("write snapshot.yaml in %s: %v", dir, err)
	}
}

// buildLocalTree lays out a small download tree under a temp dir and returns its root:
//
//	root (Snapshot/vol-tree, ns=snap-e2e)
//	  snapshots/demovirtualdisksnapshot_disk   (1 volume, leaf)
//	  snapshots/demovirtualmachinesnapshot_vm   (aggregator)
//	    snapshots/volumesnapshot_leaf            (1 volume, leaf)
func buildLocalTree(t *testing.T) string {
	t.Helper()

	root := t.TempDir()

	writeLocalNode(t, root, archive.SnapshotYAML{
		APIVersion: "storage.deckhouse.io/v1alpha1", Kind: "Snapshot", Name: "vol-tree", Namespace: "snap-e2e",
	})
	writeLocalNode(t, filepath.Join(root, archive.SnapshotsDirName, "demovirtualdisksnapshot_disk"), archive.SnapshotYAML{
		APIVersion: "demo.deckhouse.io/v1alpha1", Kind: "DemoVirtualDiskSnapshot", Name: "disk", Namespace: "snap-e2e",
		Volumes: []archive.VolumeInfo{{}},
	})
	writeLocalNode(t, filepath.Join(root, archive.SnapshotsDirName, "demovirtualmachinesnapshot_vm"), archive.SnapshotYAML{
		APIVersion: "demo.deckhouse.io/v1alpha1", Kind: "DemoVirtualMachineSnapshot", Name: "vm", Namespace: "snap-e2e",
	})
	writeLocalNode(t, filepath.Join(root, archive.SnapshotsDirName, "demovirtualmachinesnapshot_vm", archive.SnapshotsDirName, "volumesnapshot_leaf"), archive.SnapshotYAML{
		APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "leaf", Namespace: "snap-e2e",
		Volumes: []archive.VolumeInfo{{}},
	})

	return root
}

func TestDiscoverLocalSnapshots(t *testing.T) {
	root := buildLocalTree(t)

	rows, err := discoverLocalSnapshots(root)
	if err != nil {
		t.Fatalf("discoverLocalSnapshots: %v", err)
	}

	if len(rows) != 4 {
		t.Fatalf("expected 4 nodes, got %d: %+v", len(rows), rows)
	}

	byName := map[string]localSnapshotRow{}
	for _, r := range rows {
		byName[r.Name] = r
	}

	rootRow, ok := byName["vol-tree"]
	if !ok {
		t.Fatalf("root node not found in rows: %+v", rows)
	}

	if rootRow.Kind != "Snapshot" || rootRow.Children != 2 || rootRow.Path != "." {
		t.Fatalf("unexpected root row: %+v", rootRow)
	}

	if vm := byName["vm"]; vm.Children != 1 {
		t.Fatalf("vm node should have 1 child, got %+v", vm)
	}

	if disk := byName["disk"]; disk.Volumes != 1 {
		t.Fatalf("disk node should have 1 volume, got %+v", disk)
	}

	// The first row is the root (depth-first, lexical order).
	if rows[0].Name != "vol-tree" {
		t.Fatalf("expected root first, got %q", rows[0].Name)
	}
}

func TestDiscoverLocalSnapshotsEmpty(t *testing.T) {
	rows, err := discoverLocalSnapshots(t.TempDir())
	if err != nil {
		t.Fatalf("discoverLocalSnapshots: %v", err)
	}

	if len(rows) != 0 {
		t.Fatalf("expected no rows for empty dir, got %d", len(rows))
	}
}

func TestDiscoverLocalSnapshotsNotADir(t *testing.T) {
	file := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	if _, err := discoverLocalSnapshots(file); err == nil {
		t.Fatal("expected error for a non-directory path")
	}
}

func TestRunLocalTableViaCommand(t *testing.T) {
	root := buildLocalTree(t)

	var buf bytes.Buffer
	cmd := NewCommand(slog.Default())
	cmd.SetArgs([]string{root})
	cmd.SetOut(&buf)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute local list: %v", err)
	}

	out := buf.String()

	for _, want := range []string{"NAME", "KIND", "PATH", "vol-tree", "VolumeSnapshot", "leaf"} {
		if !strings.Contains(out, want) {
			t.Fatalf("table output missing %q:\n%s", want, out)
		}
	}
}

func TestRunLocalRejectsScopeFlags(t *testing.T) {
	root := buildLocalTree(t)

	for _, scope := range [][]string{{"-A", root}, {"-n", "snap-e2e", root}} {
		cmd := NewCommand(slog.Default())
		cmd.SetArgs(scope)
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)

		err := cmd.Execute()
		if err == nil {
			t.Fatalf("expected error for scope flags %v with a local dir", scope)
		}

		if !strings.Contains(err.Error(), "not applicable") {
			t.Fatalf("unexpected error for %v: %v", scope, err)
		}
	}
}

func TestRenderLocalJSON(t *testing.T) {
	rows, err := discoverLocalSnapshots(buildLocalTree(t))
	if err != nil {
		t.Fatalf("discoverLocalSnapshots: %v", err)
	}

	var buf bytes.Buffer
	if err := renderLocal(&buf, rows, "json"); err != nil {
		t.Fatalf("renderLocal json: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "vol-tree") || !strings.Contains(out, `"kind"`) || !strings.Contains(out, `"path"`) {
		t.Fatalf("json output missing expected content:\n%s", out)
	}
}

func TestPrintLocalTableEmpty(t *testing.T) {
	var buf bytes.Buffer
	if err := printLocalTable(&buf, nil); err != nil {
		t.Fatalf("printLocalTable: %v", err)
	}

	if got := strings.TrimSpace(buf.String()); got != "No snapshots found." {
		t.Fatalf("empty local table = %q, want %q", got, "No snapshots found.")
	}
}

// TestLocalClusterScopedNamespace verifies a cluster-scoped node keeps an empty
// Namespace in the row (so -o json omits it), while the table substitutes "-".
func TestLocalClusterScopedNamespace(t *testing.T) {
	root := t.TempDir()
	writeLocalNode(t, root, archive.SnapshotYAML{
		APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshotContent", Name: "vsc-1",
	})

	rows, err := discoverLocalSnapshots(root)
	if err != nil {
		t.Fatalf("discoverLocalSnapshots: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	if rows[0].Namespace != "" {
		t.Fatalf("cluster-scoped node should keep empty Namespace, got %q", rows[0].Namespace)
	}

	var tbuf bytes.Buffer
	if err := printLocalTable(&tbuf, rows); err != nil {
		t.Fatalf("printLocalTable: %v", err)
	}

	lines := strings.Split(strings.TrimRight(tbuf.String(), "\n"), "\n")
	fields := strings.Fields(lines[len(lines)-1])
	if len(fields) < 3 || fields[2] != notAvailable {
		t.Fatalf("expected NAMESPACE column %q, got fields %v", notAvailable, fields)
	}

	var jbuf bytes.Buffer
	if err := renderLocal(&jbuf, rows, "json"); err != nil {
		t.Fatalf("renderLocal json: %v", err)
	}

	if strings.Contains(jbuf.String(), "namespace") {
		t.Fatalf("json should omit empty namespace:\n%s", jbuf.String())
	}
}
