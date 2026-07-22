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

package describe

import (
	"bytes"
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
)

const (
	testNS      = "default"
	testRootAPI = snapshotapi.StorageGroup + "/" + snapshotapi.Version
	testVSAPI   = "snapshot.storage.k8s.io/v1"
)

// describeScheme returns an empty scheme so the fake client stores and returns every
// snapshot-tree object verbatim as unstructured. Registering the typed Snapshot API would
// make the fake client round-trip registered kinds through their Go structs, silently
// dropping the namespaced status.sourceRef/status.data fields the tree builder reads.
func describeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	return runtime.NewScheme()
}

// describeClient creates a controller-runtime fake client seeded with unstructured
// snapshot-tree objects. The tree is resolved purely from each object's own namespaced
// status (status.data / status.childrenSnapshotRefs) — no cluster-scoped SnapshotContent.
func describeClient(t *testing.T, objs ...*unstructured.Unstructured) client.Client {
	t.Helper()

	builder := fake.NewClientBuilder().WithScheme(describeScheme(t))
	for _, u := range objs {
		builder = builder.WithObjects(u)
	}

	return builder.Build()
}

// dataMap builds a status.data map for a PVC-backed captured volume (Variant A, ≤1 per node).
func dataMap(pvcName, uid string) map[string]interface{} {
	return map[string]interface{}{
		"sourceRef": map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "PersistentVolumeClaim",
			"namespace":  testNS,
			"name":       pvcName,
			"uid":        uid,
		},
		"artifactRef": map[string]interface{}{
			"apiVersion": testVSAPI,
			"kind":       "VolumeSnapshotContent",
			"name":       "vsc-" + pvcName,
		},
	}
}

// withReadyCondition adds a status.conditions entry of type "Ready" to obj (built by
// makeSnap), mutating it in place and returning it for chaining. reason/message are
// omitted from the condition map when empty, matching how a real Ready=True condition
// carries no reason.
func withReadyCondition(t *testing.T, obj *unstructured.Unstructured, status, reason, message string) *unstructured.Unstructured {
	t.Helper()

	cond := map[string]interface{}{"type": "Ready", "status": status}

	if reason != "" {
		cond["reason"] = reason
	}

	if message != "" {
		cond["message"] = message
	}

	existing, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	existing = append(existing, cond)

	if err := unstructured.SetNestedSlice(obj.Object, existing, "status", "conditions"); err != nil {
		t.Fatalf("SetNestedSlice status.conditions: %v", err)
	}

	return obj
}

// childRefMap builds one status.childrenSnapshotRefs element.
func childRefMap(apiVersion, kind, name string) map[string]interface{} {
	return map[string]interface{}{"apiVersion": apiVersion, "kind": kind, "name": name}
}

// makeSnap builds an unstructured snapshot-tree object from its namespaced status:
// an optional status.data (captured volume) and optional status.childrenSnapshotRefs.
func makeSnap(apiVersion, kind, name, uid string, data map[string]interface{}, children ...map[string]interface{}) *unstructured.Unstructured {
	status := map[string]interface{}{}

	if data != nil {
		status["data"] = data
	}

	if len(children) > 0 {
		raw := make([]interface{}, len(children))
		for i, c := range children {
			raw[i] = c
		}

		status["childrenSnapshotRefs"] = raw
	}

	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": testNS,
				"uid":       uid,
			},
			"status": status,
		},
	}
}

// TestRun_RootOnly verifies that a snapshot with no children and no volume data
// renders as just its root label with no additional entries.
func TestRun_RootOnly(t *testing.T) {
	t.Helper()

	snap := makeSnap(testRootAPI, "Snapshot", "my-snap", "uid-my-snap", nil)

	c := describeClient(t, snap)

	var buf bytes.Buffer

	err := Run(context.Background(), c, testNS, "my-snap", &buf)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := "Snapshot/my-snap\n"
	got := buf.String()

	if got != want {
		t.Errorf("output:\ngot  %q\nwant %q", got, want)
	}
}

// TestRun_OneChild_WithVolume verifies that a root with one child snapshot node that owns
// a single volume (status.data) renders the volume label nested under the child.
func TestRun_OneChild_WithVolume(t *testing.T) {
	t.Helper()

	root := makeSnap(testRootAPI, "Snapshot", "root", "uid-root", nil,
		childRefMap(testRootAPI, "Snapshot", "child"))
	child := makeSnap(testRootAPI, "Snapshot", "child", "uid-child", dataMap("pvc-a", "uid-pvc-a"))

	c := describeClient(t, root, child)

	var buf bytes.Buffer

	err := Run(context.Background(), c, testNS, "root", &buf)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := "Snapshot/root\n" +
		"└── Snapshot/child\n" +
		"    └── pvc-a\n"
	got := buf.String()

	if got != want {
		t.Errorf("output:\ngot  %q\nwant %q", got, want)
	}
}

// TestRun_DeepTree verifies a 3-level hierarchy:
// Snapshot/root → DemoVirtualMachineSnapshot/vm-snap → DemoVirtualDiskSnapshot/disk-snap → volume.
func TestRun_DeepTree(t *testing.T) {
	t.Helper()

	const demoAPI = "demo.deckhouse.io/v1alpha1"

	root := makeSnap(testRootAPI, "Snapshot", "root", "uid-root", nil,
		childRefMap(demoAPI, "DemoVirtualMachineSnapshot", "vm-snap"))
	vmSnap := makeSnap(demoAPI, "DemoVirtualMachineSnapshot", "vm-snap", "uid-vm", nil,
		childRefMap(demoAPI, "DemoVirtualDiskSnapshot", "disk-snap"))
	diskSnap := makeSnap(demoAPI, "DemoVirtualDiskSnapshot", "disk-snap", "uid-disk", dataMap("pvc-disk", "uid-pvc-disk"))

	c := describeClient(t, root, vmSnap, diskSnap)

	var buf bytes.Buffer

	err := Run(context.Background(), c, testNS, "root", &buf)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := "Snapshot/root\n" +
		"└── DemoVirtualMachineSnapshot/vm-snap\n" +
		"    └── DemoVirtualDiskSnapshot/disk-snap\n" +
		"        └── pvc-disk\n"
	got := buf.String()

	if got != want {
		t.Errorf("output:\ngot  %q\nwant %q", got, want)
	}
}

// TestRun_OrphanLeaf verifies that an aggregator root with a VolumeSnapshot visibility-leaf
// renders the captured PVC name under the VS leaf node.
func TestRun_OrphanLeaf(t *testing.T) {
	t.Helper()

	root := makeSnap(testRootAPI, "Snapshot", "root", "uid-root", nil,
		childRefMap(testVSAPI, "VolumeSnapshot", "nss-vs-pvc"))
	vs := makeSnap(testVSAPI, "VolumeSnapshot", "nss-vs-pvc", "uid-vs", dataMap("my-pvc", "uid-my-pvc"))

	c := describeClient(t, root, vs)

	var buf bytes.Buffer

	err := Run(context.Background(), c, testNS, "root", &buf)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := "Snapshot/root\n" +
		"└── VolumeSnapshot/nss-vs-pvc\n" +
		"    └── my-pvc\n"
	got := buf.String()

	if got != want {
		t.Errorf("output:\ngot  %q\nwant %q", got, want)
	}
}

// TestRun_MixedChildren verifies that a root with one domain snapshot child and one
// VolumeSnapshot orphan leaf renders the domain child first (├──) and the leaf last
// (└──), with the captured PVC name indented under the leaf.
func TestRun_MixedChildren(t *testing.T) {
	t.Helper()

	root := makeSnap(testRootAPI, "Snapshot", "root", "uid-root", nil,
		childRefMap(testRootAPI, "Snapshot", "snap-child"),
		childRefMap(testVSAPI, "VolumeSnapshot", "nss-vs-leaf"))
	snapChild := makeSnap(testRootAPI, "Snapshot", "snap-child", "uid-snap-child", nil)
	vs := makeSnap(testVSAPI, "VolumeSnapshot", "nss-vs-leaf", "uid-vs-leaf", dataMap("vol-pvc", "uid-vol-pvc"))

	c := describeClient(t, root, snapChild, vs)

	var buf bytes.Buffer

	err := Run(context.Background(), c, testNS, "root", &buf)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := "Snapshot/root\n" +
		"├── Snapshot/snap-child\n" +
		"└── VolumeSnapshot/nss-vs-leaf\n" +
		"    └── vol-pvc\n"
	got := buf.String()

	if got != want {
		t.Errorf("output:\ngot  %q\nwant %q", got, want)
	}
}

// ownData builds a *source.NodeData whose captured source PVC has the given name, the
// only field volumeLabels reads.
func ownData(pvcName string) *source.NodeData {
	return &source.NodeData{SourceRef: source.SourceRefIdentity{Name: pvcName}}
}

// TestRun_DegradedRoot verifies that a root snapshot with Ready=False/reason=
// ChildSnapshotDeleted renders a visible degradation indicator (including the condition's
// message) on the root's label, per backlog #15.
func TestRun_DegradedRoot(t *testing.T) {
	t.Helper()

	root := makeSnap(testRootAPI, "Snapshot", "root", "uid-root", nil)
	withReadyCondition(t, root, "False", "ChildSnapshotDeleted", "child snapshot was deleted")

	c := describeClient(t, root)

	var buf bytes.Buffer

	err := Run(context.Background(), c, testNS, "root", &buf)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := "Snapshot/root  (DEGRADED: child snapshot was deleted)\n"
	got := buf.String()

	if got != want {
		t.Errorf("output:\ngot  %q\nwant %q", got, want)
	}
}

// TestRun_ReadyRootUnchanged verifies that a non-degraded root (Ready=True) renders
// exactly as before this task — no degradation indicator appended.
func TestRun_ReadyRootUnchanged(t *testing.T) {
	t.Helper()

	root := makeSnap(testRootAPI, "Snapshot", "root", "uid-root", nil)
	withReadyCondition(t, root, "True", "", "")

	c := describeClient(t, root)

	var buf bytes.Buffer

	err := Run(context.Background(), c, testNS, "root", &buf)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := "Snapshot/root\n"
	got := buf.String()

	if got != want {
		t.Errorf("output:\ngot  %q\nwant %q", got, want)
	}
}

// TestDegradedSuffix covers degradedSuffix directly: only Ready=False with a reason in
// source.DegradedReadyReasons produces an annotation; every other case returns "".
func TestDegradedSuffix(t *testing.T) {
	t.Helper()

	cases := []struct {
		name  string
		ready source.NodeReadyStatus
		want  string
	}{
		{
			name:  "zero_value",
			ready: source.NodeReadyStatus{},
			want:  "",
		},
		{
			name:  "ready_true",
			ready: source.NodeReadyStatus{Status: "True"},
			want:  "",
		},
		{
			name:  "false_non_degraded_reason",
			ready: source.NodeReadyStatus{Status: "False", Reason: "DataCapturePending"},
			want:  "",
		},
		{
			name:  "false_degraded_reason",
			ready: source.NodeReadyStatus{Status: "False", Reason: "ChildSnapshotDeleted", Message: "child snapshot was deleted"},
			want:  "  (DEGRADED: child snapshot was deleted)",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := degradedSuffix(tc.ready)
			if got != tc.want {
				t.Errorf("degradedSuffix(%+v) = %q, want %q", tc.ready, got, tc.want)
			}
		})
	}
}

// TestToTreeViewNode covers the source.Node → treeview.Node mapping produced by
// toTreeViewNode: label format, the single volume label from status.data, and child
// recursion — all without any cluster client.
func TestToTreeViewNode(t *testing.T) {
	t.Helper()

	childNode := &source.Node{Kind: "Snapshot", Name: "child", Data: ownData("pvc-a")}
	nestedDisk := &source.Node{Kind: "DemoVirtualDiskSnapshot", Name: "disk", Data: ownData("pvc-a")}
	vmNode := &source.Node{
		Kind:     "DemoVirtualMachineSnapshot",
		Name:     "vm",
		Children: []*source.Node{nestedDisk},
	}

	cases := []struct {
		name         string
		node         *source.Node
		wantLabel    string
		wantVolumes  []string
		wantChildren int
	}{
		{
			name:         "root_no_data",
			node:         &source.Node{Kind: "Snapshot", Name: "root"},
			wantLabel:    "Snapshot/root",
			wantVolumes:  nil,
			wantChildren: 0,
		},
		{
			name:         "own_data",
			node:         &source.Node{Kind: "Snapshot", Name: "disk", Data: ownData("pvc-a")},
			wantLabel:    "Snapshot/disk",
			wantVolumes:  []string{"pvc-a"},
			wantChildren: 0,
		},
		{
			name:         "orphan_leaf_data",
			node:         &source.Node{Kind: "VolumeSnapshot", Name: "vs-1", Data: ownData("pvc-x")},
			wantLabel:    "VolumeSnapshot/vs-1",
			wantVolumes:  []string{"pvc-x"},
			wantChildren: 0,
		},
		{
			name:         "one_child_no_data",
			node:         &source.Node{Kind: "Snapshot", Name: "parent", Children: []*source.Node{childNode}},
			wantLabel:    "Snapshot/parent",
			wantVolumes:  nil,
			wantChildren: 1,
		},
		{
			name:         "nested_children_recurse",
			node:         vmNode,
			wantLabel:    "DemoVirtualMachineSnapshot/vm",
			wantVolumes:  nil,
			wantChildren: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := toTreeViewNode(tc.node)

			if got.Label != tc.wantLabel {
				t.Errorf("Label: got %q, want %q", got.Label, tc.wantLabel)
			}

			if len(got.Volumes) != len(tc.wantVolumes) {
				t.Fatalf("Volumes len: got %d, want %d (values: %v)", len(got.Volumes), len(tc.wantVolumes), got.Volumes)
			}

			for i, v := range tc.wantVolumes {
				if got.Volumes[i] != v {
					t.Errorf("Volumes[%d]: got %q, want %q", i, got.Volumes[i], v)
				}
			}

			if len(got.Children) != tc.wantChildren {
				t.Errorf("Children len: got %d, want %d", len(got.Children), tc.wantChildren)
			}
		})
	}
}

// TestToTreeViewNode_ChildLabels verifies that child node labels and volume labels are
// correctly populated when toTreeViewNode recurses into children.
func TestToTreeViewNode_ChildLabels(t *testing.T) {
	t.Helper()

	child := &source.Node{Kind: "DemoVirtualDiskSnapshot", Name: "disk", Data: ownData("pvc-child")}
	parent := &source.Node{
		Kind:     "DemoVirtualMachineSnapshot",
		Name:     "vm",
		Children: []*source.Node{child},
	}

	got := toTreeViewNode(parent)

	if got.Label != "DemoVirtualMachineSnapshot/vm" {
		t.Errorf("parent label: got %q, want DemoVirtualMachineSnapshot/vm", got.Label)
	}

	if len(got.Children) != 1 {
		t.Fatalf("parent children: got %d, want 1", len(got.Children))
	}

	childTV := got.Children[0]

	if childTV.Label != "DemoVirtualDiskSnapshot/disk" {
		t.Errorf("child label: got %q, want DemoVirtualDiskSnapshot/disk", childTV.Label)
	}

	if len(childTV.Volumes) != 1 {
		t.Fatalf("child volumes len: got %d, want 1", len(childTV.Volumes))
	}

	if childTV.Volumes[0] != "pvc-child" {
		t.Errorf("child volume: got %q, want pvc-child", childTV.Volumes[0])
	}
}

// TestVolumeLabels verifies the volumeLabels helper: no data yields no labels, and a node
// that captured its own volume yields exactly one label (the captured PVC name).
func TestVolumeLabels(t *testing.T) {
	t.Helper()

	cases := []struct {
		name string
		node *source.Node
		want []string
	}{
		{
			name: "no_data",
			node: &source.Node{},
			want: nil,
		},
		{
			name: "own_data",
			node: &source.Node{Data: ownData("pvc-a")},
			want: []string{"pvc-a"},
		},
		{
			name: "orphan_leaf_data",
			node: &source.Node{Data: ownData("pvc-c")},
			want: []string{"pvc-c"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := volumeLabels(tc.node)

			if len(got) != len(tc.want) {
				t.Fatalf("len: got %d, want %d (values: %v)", len(got), len(tc.want), got)
			}

			for i, w := range tc.want {
				if got[i] != w {
					t.Errorf("[%d]: got %q, want %q", i, got[i], w)
				}
			}
		})
	}
}
