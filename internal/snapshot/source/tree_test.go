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

package source

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
)

const (
	testNS  = "default"
	demoAPI = "demo.deckhouse.io/v1alpha1"
)

// makeScheme builds a scheme with the snapshot API types registered.
func makeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := snapshotapi.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	return scheme
}

// makeSnapshot creates a typed Snapshot CR for the fake client.
func makeSnapshot(name string, contentName string, children []snapshotapi.SnapshotChildRef) *snapshotapi.Snapshot {
	return &snapshotapi.Snapshot{
		TypeMeta: metav1.TypeMeta{
			APIVersion: rootAPIVersion,
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

// makeContent creates a SnapshotContent CR (cluster-scoped, no namespace).
func makeContent(name, mcpName string, dataRefs []snapshotapi.SnapshotDataBinding) *snapshotapi.SnapshotContent {
	return &snapshotapi.SnapshotContent{
		TypeMeta: metav1.TypeMeta{
			APIVersion: rootAPIVersion,
			Kind:       "SnapshotContent",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Status: snapshotapi.SnapshotContentStatus{
			ManifestCheckpointName: mcpName,
			DataRefs:               dataRefs,
		},
	}
}

// makeUnstructuredSnap builds an unstructured snapshot object (for non-Snapshot kinds
// such as DemoVirtualMachineSnapshot that are not registered in the scheme).
func makeUnstructuredSnap(apiVersion, kind, namespace, name, contentName string, childRefs []interface{}) *unstructured.Unstructured {
	obj := map[string]interface{}{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": namespace,
		},
		"status": map[string]interface{}{
			"boundSnapshotContentName": contentName,
		},
	}

	if len(childRefs) > 0 {
		statusMap := obj["status"].(map[string]interface{})
		statusMap["childrenSnapshotRefs"] = childRefs
	}

	return &unstructured.Unstructured{Object: obj}
}

// childRef builds a map suitable for the childrenSnapshotRefs unstructured slice.
func childRef(apiVersion, kind, name string) interface{} {
	return map[string]interface{}{
		"apiVersion": apiVersion,
		"kind":       kind,
		"name":       name,
	}
}

// dataBinding builds a minimal SnapshotDataBinding.
func dataBinding(targetUID string) snapshotapi.SnapshotDataBinding {
	return snapshotapi.SnapshotDataBinding{
		TargetUID: targetUID,
		Target: snapshotapi.SnapshotSubjectRef{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
			Name:       "pvc-" + targetUID,
			Namespace:  testNS,
		},
		Artifact: snapshotapi.SnapshotDataArtifactRef{
			APIVersion: "snapshot.storage.k8s.io/v1",
			Kind:       "VolumeSnapshotContent",
			Name:       "vsc-" + targetUID,
		},
	}
}

func buildFakeClient(scheme *runtime.Scheme, typed []client.Object, unstructured []*unstructured.Unstructured) client.Client {
	builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(typed...)
	for _, u := range unstructured {
		builder = builder.WithObjects(u)
	}

	return builder.Build()
}

// TestBuildTree_DiskNode_OwnDataRefs verifies that a domain disk snapshot node with
// one dataRef and no visibility-leaf children stores the data in OwnDataRefs and
// produces no leaf volume children.
func TestBuildTree_DiskNode_OwnDataRefs(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	// root -> child1 (disk snapshot, one dataRef, no visibility-leaf children)
	root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: rootAPIVersion, Kind: "Snapshot", Name: "child1"},
	})
	child1 := makeSnapshot("child1", "sc-child1", nil)

	scRoot := makeContent("sc-root", "mcp-root", nil)
	scChild1 := makeContent("sc-child1", "mcp-child1", []snapshotapi.SnapshotDataBinding{dataBinding("uid-1")})

	c := buildFakeClient(scheme, []client.Object{root, child1, scRoot, scChild1}, nil)
	tree, err := BuildTree(context.Background(), c, testNS, "root")

	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if tree.Name != "root" {
		t.Errorf("root name: got %q, want %q", tree.Name, "root")
	}

	if tree.Parent != nil {
		t.Errorf("root parent should be nil")
	}

	if len(tree.OwnDataRefs) != 0 {
		t.Errorf("root OwnDataRefs len: got %d, want 0", len(tree.OwnDataRefs))
	}

	// root has one domain snapshot child (child1); root has no dataRefs.
	if len(tree.Children) != 1 {
		t.Fatalf("root children len: got %d, want 1", len(tree.Children))
	}

	c1 := tree.Children[0]

	if c1.Name != "child1" {
		t.Errorf("child1 name: got %q", c1.Name)
	}

	if c1.Parent != tree {
		t.Errorf("child1 parent should be root")
	}

	// child1 is a non-aggregator disk node: one dataRef in OwnDataRefs, zero children.
	if len(c1.OwnDataRefs) != 1 {
		t.Fatalf("child1 OwnDataRefs len: got %d, want 1", len(c1.OwnDataRefs))
	}

	if c1.OwnDataRefs[0].TargetUID != "uid-1" {
		t.Errorf("child1 OwnDataRefs[0].TargetUID: got %q", c1.OwnDataRefs[0].TargetUID)
	}

	if len(c1.Children) != 0 {
		t.Errorf("child1 (disk node) must have no children, got %d", len(c1.Children))
	}

	if c1.Binding != nil {
		t.Errorf("child1 Binding must be nil for non-aggregator snapshot node")
	}
}

// TestBuildTree_TwoDataRefs_InOwnDataRefs verifies that a non-aggregator node with
// two dataRefs stores both in OwnDataRefs and produces no leaf children.
func TestBuildTree_TwoDataRefs_InOwnDataRefs(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	root := makeSnapshot("root", "sc-root", nil)
	scRoot := makeContent("sc-root", "mcp-root", []snapshotapi.SnapshotDataBinding{
		dataBinding("uid-1"),
		dataBinding("uid-2"),
	})

	c := buildFakeClient(scheme, []client.Object{root, scRoot}, nil)

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if len(tree.OwnDataRefs) != 2 {
		t.Fatalf("OwnDataRefs: got %d, want 2", len(tree.OwnDataRefs))
	}

	if len(tree.Children) != 0 {
		t.Errorf("non-aggregator must have no children, got %d", len(tree.Children))
	}

	if tree.OwnDataRefs[0].TargetUID != "uid-1" {
		t.Errorf("OwnDataRefs[0] TargetUID: got %q, want uid-1", tree.OwnDataRefs[0].TargetUID)
	}

	if tree.OwnDataRefs[1].TargetUID != "uid-2" {
		t.Errorf("OwnDataRefs[1] TargetUID: got %q, want uid-2", tree.OwnDataRefs[1].TargetUID)
	}
}

// TestBuildTree_Aggregator_VisibilityLeafProducesOrphanLeaves verifies that when a node
// has a VolumeSnapshot visibility-leaf child ref, all of its content.DataRefs are
// materialised as orphan leaf volume nodes (Binding set, Name=pvc name, OwnDataRefs nil).
// The visibility-leaf VS objects are NOT fetched from the API server.
func TestBuildTree_Aggregator_VisibilityLeafProducesOrphanLeaves(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	// root has one visibility-leaf child ref (a CSI VolumeSnapshot) and one dataRef.
	// The VolumeSnapshot object itself is NOT in the fake client — it must not be fetched.
	root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: volumeSnapshotAPIVersion, Kind: "VolumeSnapshot", Name: "vs-orphan-pvc"},
	})
	scRoot := makeContent("sc-root", "mcp-root", []snapshotapi.SnapshotDataBinding{dataBinding("uid-pvc")})

	c := buildFakeClient(scheme, []client.Object{root, scRoot}, nil)

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	// Aggregator: OwnDataRefs is nil; one orphan leaf child.
	if tree.OwnDataRefs != nil {
		t.Errorf("aggregator OwnDataRefs must be nil, got %v", tree.OwnDataRefs)
	}

	if len(tree.Children) != 1 {
		t.Fatalf("aggregator must have 1 orphan leaf child, got %d", len(tree.Children))
	}

	leaf := tree.Children[0]

	if leaf.Kind != "VolumeSnapshot" {
		t.Errorf("leaf Kind: got %q, want VolumeSnapshot", leaf.Kind)
	}

	if leaf.APIVersion != volumeSnapshotAPIVersion {
		t.Errorf("leaf APIVersion: got %q, want %q", leaf.APIVersion, volumeSnapshotAPIVersion)
	}

	// Leaf node is named after the PVC (dataRef.Target.Name), NOT the VS object.
	if leaf.Name != "pvc-uid-pvc" {
		t.Errorf("leaf Name: got %q, want pvc-uid-pvc", leaf.Name)
	}

	if leaf.SourceName != "pvc-uid-pvc" {
		t.Errorf("leaf SourceName: got %q, want pvc-uid-pvc", leaf.SourceName)
	}

	if leaf.SourceRef != "uid-pvc" {
		t.Errorf("leaf SourceRef: got %q, want uid-pvc", leaf.SourceRef)
	}

	if leaf.Binding == nil {
		t.Fatal("leaf Binding must not be nil")
	}

	if leaf.Binding.TargetUID != "uid-pvc" {
		t.Errorf("leaf Binding.TargetUID: got %q, want uid-pvc", leaf.Binding.TargetUID)
	}

	if leaf.OwnDataRefs != nil {
		t.Errorf("leaf OwnDataRefs must be nil")
	}

	if len(leaf.Children) != 0 {
		t.Errorf("leaf must have no children, got %d", len(leaf.Children))
	}

	if leaf.Parent != tree {
		t.Errorf("leaf Parent must be root")
	}
}

// TestBuildTree_Aggregator_MultipleOrphanLeaves verifies the aggregator path with
// two dataRefs: both become orphan leaf nodes in dataRefs order.
func TestBuildTree_Aggregator_MultipleOrphanLeaves(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: volumeSnapshotAPIVersion, Kind: "VolumeSnapshot", Name: "vs-leaf-1"},
	})
	scRoot := makeContent("sc-root", "mcp-root", []snapshotapi.SnapshotDataBinding{
		dataBinding("uid-1"),
		dataBinding("uid-2"),
	})

	c := buildFakeClient(scheme, []client.Object{root, scRoot}, nil)

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if tree.OwnDataRefs != nil {
		t.Errorf("aggregator OwnDataRefs must be nil")
	}

	if len(tree.Children) != 2 {
		t.Fatalf("aggregator children: got %d, want 2", len(tree.Children))
	}

	for i, leaf := range tree.Children {
		if leaf.Kind != "VolumeSnapshot" {
			t.Errorf("child[%d] kind: got %q, want VolumeSnapshot", i, leaf.Kind)
		}

		if leaf.Binding == nil {
			t.Fatalf("child[%d] Binding is nil", i)
		}

		if leaf.OwnDataRefs != nil {
			t.Errorf("child[%d] OwnDataRefs must be nil", i)
		}
	}

	if tree.Children[0].Binding.TargetUID != "uid-1" {
		t.Errorf("leaf[0] TargetUID: got %q, want uid-1", tree.Children[0].Binding.TargetUID)
	}

	if tree.Children[1].Binding.TargetUID != "uid-2" {
		t.Errorf("leaf[1] TargetUID: got %q, want uid-2", tree.Children[1].Binding.TargetUID)
	}
}

// TestBuildTree_DeepTree verifies a root → vm-snap (unstructured) → disk-snap (unstructured) tree.
// The disk node has one dataRef with no visibility-leaf children → OwnDataRefs set, no children.
func TestBuildTree_DeepTree(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	// root Snapshot (typed) → DemoVirtualMachineSnapshot "vm-snap" (unstructured)
	root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: demoAPI, Kind: "DemoVirtualMachineSnapshot", Name: "vm-snap"},
	})
	scRoot := makeContent("sc-root", "mcp-root", nil)

	// DemoVirtualMachineSnapshot (unstructured) → DemoVirtualDiskSnapshot "disk-snap"
	vmSnap := makeUnstructuredSnap(demoAPI, "DemoVirtualMachineSnapshot", testNS, "vm-snap", "sc-vm",
		[]interface{}{childRef(demoAPI, "DemoVirtualDiskSnapshot", "disk-snap")})
	scVM := makeContent("sc-vm", "mcp-vm", nil)

	// DemoVirtualDiskSnapshot (unstructured, leaf with one volume, no visibility-leaf children)
	diskSnap := makeUnstructuredSnap(demoAPI, "DemoVirtualDiskSnapshot", testNS, "disk-snap", "sc-disk", nil)
	scDisk := makeContent("sc-disk", "mcp-disk", []snapshotapi.SnapshotDataBinding{dataBinding("uid-disk")})

	c := buildFakeClient(scheme,
		[]client.Object{root, scRoot, scVM, scDisk},
		[]*unstructured.Unstructured{vmSnap, diskSnap},
	)

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	// root has one snapshot child (vm-snap); root has no dataRefs.
	if len(tree.Children) != 1 {
		t.Fatalf("root children: %d, want 1", len(tree.Children))
	}

	vm := tree.Children[0]

	if vm.Kind != "DemoVirtualMachineSnapshot" {
		t.Errorf("vm kind: got %q", vm.Kind)
	}

	// vm-snap has one snapshot child (disk-snap); vm has no dataRefs.
	if len(vm.Children) != 1 {
		t.Fatalf("vm children: %d, want 1", len(vm.Children))
	}

	disk := vm.Children[0]

	if disk.Kind != "DemoVirtualDiskSnapshot" {
		t.Errorf("disk kind: got %q", disk.Kind)
	}

	// disk is a non-aggregator node: one dataRef in OwnDataRefs, no children.
	if len(disk.OwnDataRefs) != 1 {
		t.Fatalf("disk OwnDataRefs: %d, want 1", len(disk.OwnDataRefs))
	}

	if disk.OwnDataRefs[0].TargetUID != "uid-disk" {
		t.Errorf("disk OwnDataRefs[0].TargetUID: got %q", disk.OwnDataRefs[0].TargetUID)
	}

	if len(disk.Children) != 0 {
		t.Errorf("disk (non-aggregator) must have no children, got %d", len(disk.Children))
	}

	if disk.Parent != vm {
		t.Errorf("disk parent should be vm")
	}
}

// TestBuildTree_ZeroDataRefs_NoVolumeNodes verifies that a node with no dataRefs
// produces no volume child nodes and OwnDataRefs is nil.
func TestBuildTree_ZeroDataRefs_NoVolumeNodes(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	root := makeSnapshot("root", "sc-root", nil)
	scRoot := makeContent("sc-root", "mcp-root", nil) // no dataRefs

	c := buildFakeClient(scheme, []client.Object{root, scRoot}, nil)

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if len(tree.Children) != 0 {
		t.Errorf("expected no children, got %d", len(tree.Children))
	}

	if tree.OwnDataRefs != nil {
		t.Errorf("expected nil OwnDataRefs for node with no dataRefs, got %v", tree.OwnDataRefs)
	}
}

// TestBuildTree_DomainChildBeforeOrphanLeaf verifies that domain snapshot children
// appear before orphan leaf volume children in an aggregator node.
func TestBuildTree_DomainChildBeforeOrphanLeaf(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	// root has one domain snap child AND one visibility-leaf child; root has one dataRef.
	root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: rootAPIVersion, Kind: "Snapshot", Name: "snap-child"},
		{APIVersion: volumeSnapshotAPIVersion, Kind: "VolumeSnapshot", Name: "vs-orphan"},
	})
	snapChild := makeSnapshot("snap-child", "sc-snap-child", nil)
	scRoot := makeContent("sc-root", "mcp-root", []snapshotapi.SnapshotDataBinding{dataBinding("uid-vol")})
	scSnapChild := makeContent("sc-snap-child", "mcp-snap-child", nil)

	c := buildFakeClient(scheme, []client.Object{root, snapChild, scRoot, scSnapChild}, nil)

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	// root is an aggregator: one domain child + one orphan leaf child = 2 total.
	if len(tree.Children) != 2 {
		t.Fatalf("root children: got %d, want 2", len(tree.Children))
	}

	first := tree.Children[0]
	second := tree.Children[1]

	if first.Kind != "Snapshot" {
		t.Errorf("first child should be the domain snapshot child, got kind %q", first.Kind)
	}

	if first.Name != "snap-child" {
		t.Errorf("first child name: got %q, want snap-child", first.Name)
	}

	if second.Kind != "VolumeSnapshot" {
		t.Errorf("second child should be the orphan leaf, got kind %q", second.Kind)
	}

	if second.Binding == nil {
		t.Error("orphan leaf Binding must not be nil")
	}

	// Aggregator stores no OwnDataRefs.
	if tree.OwnDataRefs != nil {
		t.Errorf("aggregator OwnDataRefs must be nil")
	}
}

// TestBuildTree_OwnDataRefs_IndependentCopy verifies that OwnDataRefs on a non-aggregator
// node is an independent copy of the content.DataRefs slice: mutations to the source
// after BuildTree do not affect the node.
func TestBuildTree_OwnDataRefs_IndependentCopy(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	binding := dataBinding("uid-x")
	root := makeSnapshot("root", "sc-root", nil)
	scRoot := makeContent("sc-root", "mcp-root", []snapshotapi.SnapshotDataBinding{binding})

	c := buildFakeClient(scheme, []client.Object{root, scRoot}, nil)

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if len(tree.OwnDataRefs) != 1 {
		t.Fatalf("OwnDataRefs: got %d, want 1", len(tree.OwnDataRefs))
	}

	originalUID := tree.OwnDataRefs[0].TargetUID

	// Mutate the source binding.
	binding.TargetUID = "mutated"

	if tree.OwnDataRefs[0].TargetUID != originalUID {
		t.Errorf("OwnDataRefs[0].TargetUID was mutated to %q; expected independent copy %q",
			tree.OwnDataRefs[0].TargetUID, originalUID)
	}
}

// TestBuildTree_Aggregator_BindingIndependentCopy verifies that the Binding pointer on an
// orphan leaf node is an independent copy: mutations to the content DataRefs after
// BuildTree do not affect Node.Binding.
func TestBuildTree_Aggregator_BindingIndependentCopy(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	binding := dataBinding("uid-x")
	root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: volumeSnapshotAPIVersion, Kind: "VolumeSnapshot", Name: "vs-leaf"},
	})
	scRoot := makeContent("sc-root", "mcp-root", []snapshotapi.SnapshotDataBinding{binding})

	c := buildFakeClient(scheme, []client.Object{root, scRoot}, nil)

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if len(tree.Children) != 1 {
		t.Fatalf("children: got %d, want 1", len(tree.Children))
	}

	leaf := tree.Children[0]
	if leaf.Binding == nil {
		t.Fatal("Binding is nil")
	}

	originalUID := leaf.Binding.TargetUID

	// Mutate the source binding.
	binding.TargetUID = "mutated"

	if leaf.Binding.TargetUID != originalUID {
		t.Errorf("Binding.TargetUID was mutated to %q; expected independent copy %q",
			leaf.Binding.TargetUID, originalUID)
	}
}

// TestBuildTree_CycleError verifies that a cycle in childrenSnapshotRefs returns ErrCycle.
func TestBuildTree_CycleError(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	// root → child1 → root (cycle)
	root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: rootAPIVersion, Kind: "Snapshot", Name: "child1"},
	})
	child1 := makeSnapshot("child1", "sc-child1", []snapshotapi.SnapshotChildRef{
		{APIVersion: rootAPIVersion, Kind: "Snapshot", Name: "root"},
	})
	scRoot := makeContent("sc-root", "mcp-root", nil)
	scChild1 := makeContent("sc-child1", "mcp-child1", nil)

	c := buildFakeClient(scheme, []client.Object{root, child1, scRoot, scChild1}, nil)
	_, err := BuildTree(context.Background(), c, testNS, "root")

	if err == nil {
		t.Fatal("expected ErrCycle, got nil")
	}

	if !errors.Is(err, ErrCycle) {
		t.Errorf("expected ErrCycle, got: %v", err)
	}
}

// TestBuildTree_UnboundNode verifies that a node with empty boundSnapshotContentName returns an error.
func TestBuildTree_UnboundNode(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	root := makeSnapshot("root", "", nil) // no bound content
	c := buildFakeClient(scheme, []client.Object{root}, nil)
	_, err := BuildTree(context.Background(), c, testNS, "root")

	if err == nil {
		t.Fatal("expected error for unbound node, got nil")
	}
}

// makeUnstructuredSnapWithSourceRef builds an unstructured snapshot object that carries
// the state-snapshotter.deckhouse.io/source-ref annotation.
func makeUnstructuredSnapWithSourceRef(apiVersion, kind, namespace, name, contentName, sourceRef string) *unstructured.Unstructured {
	obj := makeUnstructuredSnap(apiVersion, kind, namespace, name, contentName, nil)
	obj.SetAnnotations(map[string]string{
		snapshotapi.AnnotationSourceRef: sourceRef,
	})

	return obj
}

// TestBuildTree_SourceRefAnnotation verifies that the source-ref annotation is captured
// when present and that its absence results in an empty SourceRef (no error).
func TestBuildTree_SourceRefAnnotation(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: demoAPI, Kind: "DemoVirtualDiskSnapshot", Name: "disk-with-ref"},
		{APIVersion: demoAPI, Kind: "DemoVirtualDiskSnapshot", Name: "disk-without-ref"},
	})
	scRoot := makeContent("sc-root", "mcp-root", nil)

	sourceRefJSON := `{"apiVersion":"v1","kind":"PersistentVolumeClaim","namespace":"default","name":"some-pvc","uid":"uid-abc"}`
	diskWithRef := makeUnstructuredSnapWithSourceRef(demoAPI, "DemoVirtualDiskSnapshot", testNS, "disk-with-ref", "sc-disk-ref", sourceRefJSON)
	scDiskRef := makeContent("sc-disk-ref", "mcp-disk-ref", nil)

	diskWithoutRef := makeUnstructuredSnap(demoAPI, "DemoVirtualDiskSnapshot", testNS, "disk-without-ref", "sc-disk-noref", nil)
	scDiskNoRef := makeContent("sc-disk-noref", "mcp-disk-noref", nil)

	c := buildFakeClient(scheme,
		[]client.Object{root, scRoot, scDiskRef, scDiskNoRef},
		[]*unstructured.Unstructured{diskWithRef, diskWithoutRef},
	)

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if tree.SourceRef != "" {
		t.Errorf("root SourceRef: got %q, want empty", tree.SourceRef)
	}

	// root has two domain snapshot children; root has no dataRefs so no leaf children.
	if len(tree.Children) != 2 {
		t.Fatalf("children: %d, want 2", len(tree.Children))
	}

	var withRef, withoutRef *Node

	for _, ch := range tree.Children {
		if ch.Name == "disk-with-ref" {
			withRef = ch
			continue
		}
		withoutRef = ch
	}

	if withRef == nil || withoutRef == nil {
		t.Fatal("could not find expected children")
	}

	// Raw annotation value is preserved in SourceRef.
	if withRef.SourceRef != sourceRefJSON {
		t.Errorf("disk-with-ref SourceRef: got %q, want %q", withRef.SourceRef, sourceRefJSON)
	}

	// SourceName is parsed from the annotation.
	if withRef.SourceName != "some-pvc" {
		t.Errorf("disk-with-ref SourceName: got %q, want some-pvc", withRef.SourceName)
	}

	if withoutRef.SourceRef != "" {
		t.Errorf("disk-without-ref SourceRef: got %q, want empty", withoutRef.SourceRef)
	}

	if withoutRef.SourceName != "" {
		t.Errorf("disk-without-ref SourceName: got %q, want empty", withoutRef.SourceName)
	}
}

// TestBuildTree_ChildNamespace verifies that children are fetched in the root namespace.
func TestBuildTree_ChildNamespace(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: rootAPIVersion, Kind: "Snapshot", Name: "child1"},
	})
	child1 := makeSnapshot("child1", "sc-child1", nil)
	scRoot := makeContent("sc-root", "mcp-root", nil)
	scChild1 := makeContent("sc-child1", "mcp-child1", nil)

	c := buildFakeClient(scheme, []client.Object{root, child1, scRoot, scChild1}, nil)

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if tree.Namespace != testNS {
		t.Errorf("root namespace: got %q, want %q", tree.Namespace, testNS)
	}

	if len(tree.Children) != 1 {
		t.Fatalf("children: %d", len(tree.Children))
	}

	if tree.Children[0].Namespace != testNS {
		t.Errorf("child namespace: got %q, want %q", tree.Children[0].Namespace, testNS)
	}
}
