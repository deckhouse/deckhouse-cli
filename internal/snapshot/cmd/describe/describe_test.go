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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// describeScheme builds a runtime scheme with the snapshot API types registered.
func describeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := snapshotapi.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	return scheme
}

// describeClient creates a controller-runtime fake client seeded with typed and
// unstructured objects, using a scheme with snapshot API types registered.
func describeClient(t *testing.T, typed []client.Object, uns []*unstructured.Unstructured) client.Client {
	t.Helper()

	builder := fake.NewClientBuilder().
		WithScheme(describeScheme(t)).
		WithObjects(typed...)

	for _, u := range uns {
		builder = builder.WithObjects(u)
	}

	return builder.Build()
}

// testSnapshot creates a typed Snapshot CR for the fake client.
func testSnapshot(name, contentName string, children []snapshotapi.SnapshotChildRef) *snapshotapi.Snapshot {
	return &snapshotapi.Snapshot{
		TypeMeta: metav1.TypeMeta{
			APIVersion: testRootAPI,
			Kind:       "Snapshot",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNS,
		},
		Status: snapshotapi.SnapshotStatus{
			BoundSnapshotContentName: contentName,
			ChildrenSnapshotRefs:     children,
		},
	}
}

// testContent creates a cluster-scoped SnapshotContent for the fake client.
// dataRef is nil when the node carries no volume data.
func testContent(name string, dataRef *snapshotapi.SnapshotDataBinding) *snapshotapi.SnapshotContent {
	return &snapshotapi.SnapshotContent{
		TypeMeta: metav1.TypeMeta{
			APIVersion: testRootAPI,
			Kind:       "SnapshotContent",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Status: snapshotapi.SnapshotContentStatus{
			DataRef: dataRef,
		},
	}
}

// testBinding builds a minimal SnapshotDataBinding for the given PVC name.
func testBinding(pvcName string) snapshotapi.SnapshotDataBinding {
	return snapshotapi.SnapshotDataBinding{
		TargetUID: "uid-" + pvcName,
		Target: snapshotapi.SnapshotSubjectRef{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
			Name:       pvcName,
			Namespace:  testNS,
		},
		Artifact: snapshotapi.SnapshotDataArtifactRef{
			APIVersion: testVSAPI,
			Kind:       "VolumeSnapshotContent",
			Name:       "vsc-" + pvcName,
		},
	}
}

// testVolumeSnapshot creates an unstructured VolumeSnapshot with
// status.boundSnapshotContentName set.
func testVolumeSnapshot(vsName, boundContentName string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": testVSAPI,
			"kind":       "VolumeSnapshot",
			"metadata": map[string]interface{}{
				"name":      vsName,
				"namespace": testNS,
			},
			"status": map[string]interface{}{
				"boundSnapshotContentName": boundContentName,
			},
		},
	}
}

// testDomainSnap creates an unstructured domain snapshot object with the given
// apiVersion/kind. Pass a non-nil children slice to populate
// status.childrenSnapshotRefs.
func testDomainSnap(apiVersion, kind, name, contentName string, children []interface{}) *unstructured.Unstructured {
	status := map[string]interface{}{
		"boundSnapshotContentName": contentName,
	}

	if len(children) > 0 {
		status["childrenSnapshotRefs"] = children
	}

	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": testNS,
			},
			"status": status,
		},
	}
}

// TestRun_RootOnly verifies that a snapshot with no children and no volume data
// renders as just its root label with no additional entries.
func TestRun_RootOnly(t *testing.T) {
	t.Helper()

	snap := testSnapshot("my-snap", "sc-root", nil)
	sc := testContent("sc-root", nil)

	c := describeClient(t, []client.Object{snap, sc}, nil)

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
// a single volume renders the volume label nested under the child.
func TestRun_OneChild_WithVolume(t *testing.T) {
	t.Helper()

	root := testSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: testRootAPI, Kind: "Snapshot", Name: "child"},
	})
	child := testSnapshot("child", "sc-child", nil)
	scRoot := testContent("sc-root", nil)

	b := testBinding("pvc-a")
	scChild := testContent("sc-child", &b)

	c := describeClient(t, []client.Object{root, child, scRoot, scChild}, nil)

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

	root := testSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: demoAPI, Kind: "DemoVirtualMachineSnapshot", Name: "vm-snap"},
	})
	scRoot := testContent("sc-root", nil)

	diskChild := map[string]interface{}{
		"apiVersion": demoAPI,
		"kind":       "DemoVirtualDiskSnapshot",
		"name":       "disk-snap",
	}
	vmSnap := testDomainSnap(demoAPI, "DemoVirtualMachineSnapshot", "vm-snap", "sc-vm", []interface{}{diskChild})
	scVM := testContent("sc-vm", nil)

	diskSnap := testDomainSnap(demoAPI, "DemoVirtualDiskSnapshot", "disk-snap", "sc-disk", nil)
	db := testBinding("pvc-disk")
	scDisk := testContent("sc-disk", &db)

	c := describeClient(t,
		[]client.Object{root, scRoot, scVM, scDisk},
		[]*unstructured.Unstructured{vmSnap, diskSnap},
	)

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

	root := testSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: testVSAPI, Kind: "VolumeSnapshot", Name: "nss-vs-pvc"},
	})
	scRoot := testContent("sc-root", nil)

	vs := testVolumeSnapshot("nss-vs-pvc", "sc-leaf-child")
	lb := testBinding("my-pvc")
	scLeaf := testContent("sc-leaf-child", &lb)

	c := describeClient(t,
		[]client.Object{root, scRoot, scLeaf},
		[]*unstructured.Unstructured{vs},
	)

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

	root := testSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: testRootAPI, Kind: "Snapshot", Name: "snap-child"},
		{APIVersion: testVSAPI, Kind: "VolumeSnapshot", Name: "nss-vs-leaf"},
	})
	snapChild := testSnapshot("snap-child", "sc-snap-child", nil)
	scRoot := testContent("sc-root", nil)
	scSnapChild := testContent("sc-snap-child", nil)

	vs := testVolumeSnapshot("nss-vs-leaf", "sc-vs-leaf-child")
	vb := testBinding("vol-pvc")
	scLeafChild := testContent("sc-vs-leaf-child", &vb)

	c := describeClient(t,
		[]client.Object{root, snapChild, scRoot, scSnapChild, scLeafChild},
		[]*unstructured.Unstructured{vs},
	)

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

// TestToTreeViewNode covers the source.Node → treeview.Node mapping produced by
// toTreeViewNode: label format, volume labels from OwnDataRefs/Binding, and child
// recursion — all without any cluster client.
func TestToTreeViewNode(t *testing.T) {
	t.Helper()

	pvcA := snapshotapi.SnapshotDataBinding{
		Target: snapshotapi.SnapshotSubjectRef{Name: "pvc-a"},
	}
	pvcB := snapshotapi.SnapshotDataBinding{
		Target: snapshotapi.SnapshotSubjectRef{Name: "pvc-b"},
	}
	bindX := &snapshotapi.SnapshotDataBinding{
		Target: snapshotapi.SnapshotSubjectRef{Name: "pvc-x"},
	}

	childNode := &source.Node{
		Kind:        "Snapshot",
		Name:        "child",
		OwnDataRefs: []snapshotapi.SnapshotDataBinding{pvcA},
	}
	nestedDisk := &source.Node{
		Kind:        "DemoVirtualDiskSnapshot",
		Name:        "disk",
		OwnDataRefs: []snapshotapi.SnapshotDataBinding{pvcA},
	}
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
			name:         "one_own_data_ref",
			node:         &source.Node{Kind: "Snapshot", Name: "disk", OwnDataRefs: []snapshotapi.SnapshotDataBinding{pvcA}},
			wantLabel:    "Snapshot/disk",
			wantVolumes:  []string{"pvc-a"},
			wantChildren: 0,
		},
		{
			name:         "two_own_data_refs",
			node:         &source.Node{Kind: "Snapshot", Name: "multi", OwnDataRefs: []snapshotapi.SnapshotDataBinding{pvcA, pvcB}},
			wantLabel:    "Snapshot/multi",
			wantVolumes:  []string{"pvc-a", "pvc-b"},
			wantChildren: 0,
		},
		{
			name:         "orphan_leaf_binding",
			node:         &source.Node{Kind: "VolumeSnapshot", Name: "vs-1", Binding: bindX},
			wantLabel:    "VolumeSnapshot/vs-1",
			wantVolumes:  []string{"pvc-x"},
			wantChildren: 0,
		},
		{
			// Binding takes priority: when Binding != nil, OwnDataRefs are ignored.
			name:         "binding_overrides_own_data_refs",
			node:         &source.Node{Kind: "VolumeSnapshot", Name: "vs-2", Binding: bindX, OwnDataRefs: []snapshotapi.SnapshotDataBinding{pvcA}},
			wantLabel:    "VolumeSnapshot/vs-2",
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

	pvc := snapshotapi.SnapshotDataBinding{
		Target: snapshotapi.SnapshotSubjectRef{Name: "pvc-child"},
	}
	child := &source.Node{
		Kind:        "DemoVirtualDiskSnapshot",
		Name:        "disk",
		OwnDataRefs: []snapshotapi.SnapshotDataBinding{pvc},
	}
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

// TestVolumeLabels verifies the volumeLabels helper across all three input paths:
// nil (no data), Binding set (orphan leaf), and OwnDataRefs set (non-aggregator node).
func TestVolumeLabels(t *testing.T) {
	t.Helper()

	pvcA := snapshotapi.SnapshotDataBinding{Target: snapshotapi.SnapshotSubjectRef{Name: "pvc-a"}}
	pvcB := snapshotapi.SnapshotDataBinding{Target: snapshotapi.SnapshotSubjectRef{Name: "pvc-b"}}
	bindC := &snapshotapi.SnapshotDataBinding{Target: snapshotapi.SnapshotSubjectRef{Name: "pvc-c"}}

	cases := []struct {
		name string
		node *source.Node
		want []string
	}{
		{
			name: "no_binding_no_own_refs",
			node: &source.Node{},
			want: nil,
		},
		{
			name: "binding_only",
			node: &source.Node{Binding: bindC},
			want: []string{"pvc-c"},
		},
		{
			name: "one_own_data_ref",
			node: &source.Node{OwnDataRefs: []snapshotapi.SnapshotDataBinding{pvcA}},
			want: []string{"pvc-a"},
		},
		{
			name: "two_own_data_refs_order_preserved",
			node: &source.Node{OwnDataRefs: []snapshotapi.SnapshotDataBinding{pvcA, pvcB}},
			want: []string{"pvc-a", "pvc-b"},
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
