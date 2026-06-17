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
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/naming"
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

// TestBuildTree_SimpleTree verifies that a root Snapshot with a typed child resolves
// correctly, and that the child's single dataRef produces a VolumeSnapshot leaf.
func TestBuildTree_SimpleTree(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	// root -> child1 -> (no snapshot children, one dataRef)
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

	if tree.Kind != "Snapshot" {
		t.Errorf("root kind: got %q, want %q", tree.Kind, "Snapshot")
	}

	if tree.ManifestCheckpointName != "mcp-root" {
		t.Errorf("root mcp: got %q, want %q", tree.ManifestCheckpointName, "mcp-root")
	}

	if tree.Parent != nil {
		t.Errorf("root parent should be nil")
	}

	if len(tree.DataRefs) != 0 {
		t.Errorf("root dataRefs len: got %d, want 0", len(tree.DataRefs))
	}

	// root has one snapshot child (child1); root has no dataRefs so no volume children.
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

	if c1.ManifestCheckpointName != "mcp-child1" {
		t.Errorf("child1 mcp: got %q", c1.ManifestCheckpointName)
	}

	if len(c1.DataRefs) != 1 {
		t.Fatalf("child1 dataRefs len: got %d, want 1", len(c1.DataRefs))
	}

	if c1.DataRefs[0].TargetUID != "uid-1" {
		t.Errorf("child1 dataRefs[0].TargetUID: got %q", c1.DataRefs[0].TargetUID)
	}

	// child1 has no snapshot children but one dataRef -> one VolumeSnapshot child.
	if len(c1.Children) != 1 {
		t.Fatalf("child1 children len: got %d, want 1 (volume node)", len(c1.Children))
	}

	vol := c1.Children[0]

	if vol.Kind != "VolumeSnapshot" {
		t.Errorf("volume node kind: got %q, want VolumeSnapshot", vol.Kind)
	}

	wantName := naming.ShadowName("vsc-uid-1")
	if vol.Name != wantName {
		t.Errorf("volume node name: got %q, want %q", vol.Name, wantName)
	}

	if vol.SourceRef != "uid-1" {
		t.Errorf("volume node SourceRef: got %q, want uid-1", vol.SourceRef)
	}

	if vol.Parent != c1 {
		t.Errorf("volume node parent should be child1")
	}

	if vol.Binding == nil {
		t.Fatal("volume node Binding is nil")
	}

	if vol.Binding.TargetUID != "uid-1" {
		t.Errorf("volume node Binding.TargetUID: got %q, want uid-1", vol.Binding.TargetUID)
	}

	if len(vol.Children) != 0 {
		t.Errorf("volume node is a leaf: got %d children, want 0", len(vol.Children))
	}

	if vol.DataRefs != nil {
		t.Errorf("volume node DataRefs should be nil")
	}
}

// TestBuildTree_DeepTree verifies a root → vm-snap (unstructured) → disk-snap (unstructured) tree.
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

	// DemoVirtualDiskSnapshot (unstructured, leaf with one volume)
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

	if vm.ManifestCheckpointName != "mcp-vm" {
		t.Errorf("vm mcp: got %q", vm.ManifestCheckpointName)
	}

	// vm-snap has one snapshot child (disk-snap); vm has no dataRefs.
	if len(vm.Children) != 1 {
		t.Fatalf("vm children: %d, want 1", len(vm.Children))
	}

	disk := vm.Children[0]

	if disk.Kind != "DemoVirtualDiskSnapshot" {
		t.Errorf("disk kind: got %q", disk.Kind)
	}

	if disk.ManifestCheckpointName != "mcp-disk" {
		t.Errorf("disk mcp: got %q", disk.ManifestCheckpointName)
	}

	if len(disk.DataRefs) != 1 {
		t.Fatalf("disk dataRefs: %d, want 1", len(disk.DataRefs))
	}

	if disk.DataRefs[0].TargetUID != "uid-disk" {
		t.Errorf("disk targetUID: got %q", disk.DataRefs[0].TargetUID)
	}

	if disk.Parent != vm {
		t.Errorf("disk parent should be vm")
	}

	// disk has no snapshot children; it has one dataRef -> one VolumeSnapshot child.
	if len(disk.Children) != 1 {
		t.Fatalf("disk children: %d, want 1 (volume node)", len(disk.Children))
	}

	vol := disk.Children[0]

	if vol.Kind != "VolumeSnapshot" {
		t.Errorf("volume node kind: got %q, want VolumeSnapshot", vol.Kind)
	}

	if vol.Parent != disk {
		t.Errorf("volume node parent should be disk")
	}

	if vol.Binding == nil || vol.Binding.TargetUID != "uid-disk" {
		t.Errorf("volume node Binding: got %v", vol.Binding)
	}
}

// TestBuildTree_TwoDataRefs_ProducesTwoVolumeNodes verifies that a SnapshotContent with
// two dataRefs produces two VolumeSnapshot child nodes with correct fields.
func TestBuildTree_TwoDataRefs_ProducesTwoVolumeNodes(t *testing.T) {
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

	// root has no snapshot children; two dataRefs -> two VolumeSnapshot children.
	if len(tree.Children) != 2 {
		t.Fatalf("children: got %d, want 2", len(tree.Children))
	}

	for i, vol := range tree.Children {
		if vol.Kind != "VolumeSnapshot" {
			t.Errorf("child[%d] kind: got %q, want VolumeSnapshot", i, vol.Kind)
		}

		if vol.APIVersion != volumeSnapshotAPIVersion {
			t.Errorf("child[%d] apiVersion: got %q, want %q", i, vol.APIVersion, volumeSnapshotAPIVersion)
		}

		if vol.Namespace != testNS {
			t.Errorf("child[%d] namespace: got %q, want %q", i, vol.Namespace, testNS)
		}

		if vol.Parent != tree {
			t.Errorf("child[%d] parent should be root", i)
		}

		if vol.Binding == nil {
			t.Fatalf("child[%d] Binding is nil", i)
		}

		if vol.DataRefs != nil {
			t.Errorf("child[%d] DataRefs should be nil (volume node is a leaf)", i)
		}

		if len(vol.Children) != 0 {
			t.Errorf("child[%d] Children should be empty (volume node is a leaf)", i)
		}
	}

	// Check ordering matches dataRefs order and names/SourceRef are correct.
	vol0 := tree.Children[0]
	if vol0.SourceRef != "uid-1" {
		t.Errorf("child[0] SourceRef: got %q, want uid-1", vol0.SourceRef)
	}

	wantName0 := naming.ShadowName("vsc-uid-1")
	if vol0.Name != wantName0 {
		t.Errorf("child[0] Name: got %q, want %q", vol0.Name, wantName0)
	}

	if vol0.Binding.TargetUID != "uid-1" {
		t.Errorf("child[0] Binding.TargetUID: got %q, want uid-1", vol0.Binding.TargetUID)
	}

	vol1 := tree.Children[1]
	if vol1.SourceRef != "uid-2" {
		t.Errorf("child[1] SourceRef: got %q, want uid-2", vol1.SourceRef)
	}

	wantName1 := naming.ShadowName("vsc-uid-2")
	if vol1.Name != wantName1 {
		t.Errorf("child[1] Name: got %q, want %q", vol1.Name, wantName1)
	}

	if vol1.Binding.TargetUID != "uid-2" {
		t.Errorf("child[1] Binding.TargetUID: got %q, want uid-2", vol1.Binding.TargetUID)
	}

	// ManifestCheckpointName on a volume node is the parent's checkpoint.
	if vol0.ManifestCheckpointName != "mcp-root" {
		t.Errorf("child[0] ManifestCheckpointName: got %q, want mcp-root", vol0.ManifestCheckpointName)
	}
}

// TestBuildTree_ZeroDataRefs_NoVolumeNodes verifies that a node with no dataRefs
// produces no volume child nodes.
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
}

// TestBuildTree_SnapshotChildrenBeforeVolumeChildren verifies that snapshot children
// (from childrenSnapshotRefs) appear before volume children (from dataRefs).
func TestBuildTree_SnapshotChildrenBeforeVolumeChildren(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	// root has one snapshot child AND one dataRef.
	root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: rootAPIVersion, Kind: "Snapshot", Name: "snap-child"},
	})
	snapChild := makeSnapshot("snap-child", "sc-snap-child", nil)
	scRoot := makeContent("sc-root", "mcp-root", []snapshotapi.SnapshotDataBinding{dataBinding("uid-vol")})
	scSnapChild := makeContent("sc-snap-child", "mcp-snap-child", nil)

	c := buildFakeClient(scheme, []client.Object{root, snapChild, scRoot, scSnapChild}, nil)

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	// root: one snapshot child + one volume child = 2 total.
	if len(tree.Children) != 2 {
		t.Fatalf("root children: got %d, want 2", len(tree.Children))
	}

	first := tree.Children[0]
	second := tree.Children[1]

	if first.Kind != "Snapshot" {
		t.Errorf("first child should be the snapshot child, got kind %q", first.Kind)
	}

	if first.Name != "snap-child" {
		t.Errorf("first child name: got %q, want snap-child", first.Name)
	}

	if second.Kind != "VolumeSnapshot" {
		t.Errorf("second child should be the volume node, got kind %q", second.Kind)
	}
}

// TestBuildTree_LoopCopyIndependence verifies that the Binding pointer on a volume node
// is an independent copy: it does not alias the source SnapshotContent's DataRefs slice,
// so mutations to the source after BuildTree do not affect Node.Binding.
func TestBuildTree_LoopCopyIndependence(t *testing.T) {
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

	if len(tree.Children) != 1 {
		t.Fatalf("children: got %d, want 1", len(tree.Children))
	}

	vol := tree.Children[0]
	if vol.Binding == nil {
		t.Fatal("Binding is nil")
	}

	// Record the original TargetUID from the tree.
	originalUID := vol.Binding.TargetUID

	// Mutate the source binding (simulating a caller modifying the content slice).
	binding.TargetUID = "mutated"

	// The node's Binding must be unaffected because it holds its own copy.
	if vol.Binding.TargetUID != originalUID {
		t.Errorf("Binding.TargetUID was mutated to %q; expected independent copy %q",
			vol.Binding.TargetUID, originalUID)
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

	diskWithRef := makeUnstructuredSnapWithSourceRef(demoAPI, "DemoVirtualDiskSnapshot", testNS, "disk-with-ref", "sc-disk-ref", "pvc/some-pvc")
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

	// root has two snapshot children; root has no dataRefs so no volume children.
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

	if withRef.SourceRef != "pvc/some-pvc" {
		t.Errorf("disk-with-ref SourceRef: got %q, want %q", withRef.SourceRef, "pvc/some-pvc")
	}

	if withoutRef.SourceRef != "" {
		t.Errorf("disk-without-ref SourceRef: got %q, want empty", withoutRef.SourceRef)
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
