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
// dataRef is nil when the node owns no volume data (Variant A: cardinality ≤1).
func makeContent(name, mcpName string, dataRef *snapshotapi.SnapshotDataBinding) *snapshotapi.SnapshotContent {
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
			DataRef:                dataRef,
		},
	}
}

// dataBindingPtr returns a pointer to a minimal SnapshotDataBinding.
func dataBindingPtr(targetUID string) *snapshotapi.SnapshotDataBinding {
	b := dataBinding(targetUID)
	return &b
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
	scChild1 := makeContent("sc-child1", "mcp-child1", dataBindingPtr("uid-1"))

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

// TestBuildTree_Aggregator_VisibilityLeafProducesOrphanLeaves verifies that when a node
// has a VolumeSnapshot visibility-leaf child ref, the tree builder resolves the leaf via
// VolumeSnapshot.status.boundSnapshotContentName → child SnapshotContent → status.dataRef.
// Uses REAL producer keys (snapshot.storage.k8s.io/v1, boundSnapshotContentName, dataRef singular).
func TestBuildTree_Aggregator_VisibilityLeafProducesOrphanLeaves(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	// Aggregator root: has one VolumeSnapshot visibility-leaf child ref; no own dataRef.
	root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: volumeSnapshotAPIVersion, Kind: "VolumeSnapshot", Name: "nss-vs-orphan"},
	})
	scRoot := makeContent("sc-root", "mcp-root", nil) // aggregator keeps dataRef=nil

	// The VolumeSnapshot object carries status.boundSnapshotContentName -> child content.
	vs := makeUnstructuredVolumeSnapshot(testNS, "nss-vs-orphan", "sc-orphan-child")

	// Child SnapshotContent owns the dataRef (the PVC binding).
	scChild := makeContent("sc-orphan-child", "mcp-orphan-child", dataBindingPtr("uid-pvc"))

	c := buildFakeClient(scheme, []client.Object{root, scRoot, scChild}, []*unstructured.Unstructured{vs})

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

	// Leaf node Name is the VS CR name (for ManifestScopeRef connector), NOT the PVC name.
	if leaf.Name != "nss-vs-orphan" {
		t.Errorf("leaf Name: got %q, want nss-vs-orphan", leaf.Name)
	}

	// SourceName is the captured PVC name (dataRef.Target.Name) — used for directory naming.
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

	// ManifestScopeRef must be the leaf's own ref (VS ref), NOT the parent aggregator.
	scopeRef := leaf.ManifestScopeRef()
	if scopeRef.APIVersion != volumeSnapshotAPIVersion {
		t.Errorf("ManifestScopeRef.APIVersion: got %q, want %q", scopeRef.APIVersion, volumeSnapshotAPIVersion)
	}

	if scopeRef.Kind != "VolumeSnapshot" {
		t.Errorf("ManifestScopeRef.Kind: got %q, want VolumeSnapshot", scopeRef.Kind)
	}

	if scopeRef.Name != "nss-vs-orphan" {
		t.Errorf("ManifestScopeRef.Name: got %q, want nss-vs-orphan (VS CR name, not PVC name)", scopeRef.Name)
	}

	if scopeRef.Namespace != testNS {
		t.Errorf("ManifestScopeRef.Namespace: got %q, want %q", scopeRef.Namespace, testNS)
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
	scDisk := makeContent("sc-disk", "mcp-disk", dataBindingPtr("uid-disk"))

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

// TestBuildTree_ZeroDataRefs_NoVolumeNodes verifies that a node with no dataRef
// produces no volume child nodes and OwnDataRefs is nil.
func TestBuildTree_ZeroDataRefs_NoVolumeNodes(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	root := makeSnapshot("root", "sc-root", nil)
	scRoot := makeContent("sc-root", "mcp-root", nil) // no dataRef

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

// TestBuildTree_DataRefSingularContract verifies that the CLI correctly reads the REAL
// producer key "dataRef" (singular pointer, Variant A) from SnapshotContent.status and
// maps it into node.OwnDataRefs, including the extended fields (volumeMode/storageClassName/size).
// A node whose content has no dataRef must yield OwnDataRefs == nil.
func TestBuildTree_DataRefSingularContract(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	cases := []struct {
		name            string
		dataRef         *snapshotapi.SnapshotDataBinding
		wantOwnLen      int
		wantTargetUID   string
		wantVolumeMode  string
		wantStorageCls  string
		wantSize        string
		wantAccessModes []string
	}{
		{
			name: "one_dataRef_with_extended_fields",
			dataRef: &snapshotapi.SnapshotDataBinding{
				TargetUID: "uid-disk-sc",
				Target: snapshotapi.SnapshotSubjectRef{
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Name:       "pvc-disk",
					Namespace:  testNS,
				},
				Artifact: snapshotapi.SnapshotDataArtifactRef{
					APIVersion: "snapshot.storage.k8s.io/v1",
					Kind:       "VolumeSnapshotContent",
					Name:       "vsc-disk",
				},
				VolumeMode:       "Block",
				StorageClassName: "csi-ceph-rbd",
				Size:             "20Gi",
				AccessModes:      []string{"ReadWriteOnce"},
			},
			wantOwnLen:      1,
			wantTargetUID:   "uid-disk-sc",
			wantVolumeMode:  "Block",
			wantStorageCls:  "csi-ceph-rbd",
			wantSize:        "20Gi",
			wantAccessModes: []string{"ReadWriteOnce"},
		},
		{
			name:       "absent_dataRef_yields_nil_OwnDataRefs",
			dataRef:    nil,
			wantOwnLen: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := makeSnapshot("root", "sc-root", nil)
			scRoot := makeContent("sc-root", "mcp-root", tc.dataRef)

			c := buildFakeClient(scheme, []client.Object{root, scRoot}, nil)

			tree, err := BuildTree(context.Background(), c, testNS, "root")
			if err != nil {
				t.Fatalf("BuildTree: %v", err)
			}

			if len(tree.OwnDataRefs) != tc.wantOwnLen {
				t.Fatalf("OwnDataRefs len: got %d, want %d", len(tree.OwnDataRefs), tc.wantOwnLen)
			}

			if tc.wantOwnLen == 0 {
				return
			}

			got := tree.OwnDataRefs[0]

			if got.TargetUID != tc.wantTargetUID {
				t.Errorf("TargetUID: got %q, want %q", got.TargetUID, tc.wantTargetUID)
			}

			if got.VolumeMode != tc.wantVolumeMode {
				t.Errorf("VolumeMode: got %q, want %q", got.VolumeMode, tc.wantVolumeMode)
			}

			if got.StorageClassName != tc.wantStorageCls {
				t.Errorf("StorageClassName: got %q, want %q", got.StorageClassName, tc.wantStorageCls)
			}

			if got.Size != tc.wantSize {
				t.Errorf("Size: got %q, want %q", got.Size, tc.wantSize)
			}

			if len(got.AccessModes) != len(tc.wantAccessModes) {
				t.Fatalf("AccessModes len: got %d, want %d", len(got.AccessModes), len(tc.wantAccessModes))
			}

			for i, am := range tc.wantAccessModes {
				if got.AccessModes[i] != am {
					t.Errorf("AccessModes[%d]: got %q, want %q", i, got.AccessModes[i], am)
				}
			}

			if len(tree.Children) != 0 {
				t.Errorf("non-aggregator must have no children, got %d", len(tree.Children))
			}
		})
	}
}

// TestBuildTree_DomainChildBeforeOrphanLeaf verifies that domain snapshot children
// appear before orphan leaf volume children in an aggregator node.
func TestBuildTree_DomainChildBeforeOrphanLeaf(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	// root has one domain snap child AND one visibility-leaf child.
	// Under Variant A the aggregator content keeps dataRef=nil.
	root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: rootAPIVersion, Kind: "Snapshot", Name: "snap-child"},
		{APIVersion: volumeSnapshotAPIVersion, Kind: "VolumeSnapshot", Name: "nss-vs-orphan"},
	})
	snapChild := makeSnapshot("snap-child", "sc-snap-child", nil)
	scRoot := makeContent("sc-root", "mcp-root", nil) // aggregator has no own dataRef
	scSnapChild := makeContent("sc-snap-child", "mcp-snap-child", nil)

	// VS leaf resolves to its own child content which carries the binding.
	vs := makeUnstructuredVolumeSnapshot(testNS, "nss-vs-orphan", "sc-leaf-child")
	scLeafChild := makeContent("sc-leaf-child", "mcp-leaf-child", dataBindingPtr("uid-vol"))

	c := buildFakeClient(scheme,
		[]client.Object{root, snapChild, scRoot, scSnapChild, scLeafChild},
		[]*unstructured.Unstructured{vs},
	)

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

	// Leaf Name is VS CR name; SourceName is PVC name.
	if second.Name != "nss-vs-orphan" {
		t.Errorf("orphan leaf Name: got %q, want nss-vs-orphan", second.Name)
	}

	if second.SourceName != "pvc-uid-vol" {
		t.Errorf("orphan leaf SourceName: got %q, want pvc-uid-vol", second.SourceName)
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
	scRoot := makeContent("sc-root", "mcp-root", &binding)

	c := buildFakeClient(scheme, []client.Object{root, scRoot}, nil)

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if len(tree.OwnDataRefs) != 1 {
		t.Fatalf("OwnDataRefs: got %d, want 1", len(tree.OwnDataRefs))
	}

	originalUID := tree.OwnDataRefs[0].TargetUID

	// Mutate the source binding after BuildTree returns.
	binding.TargetUID = "mutated"

	if tree.OwnDataRefs[0].TargetUID != originalUID {
		t.Errorf("OwnDataRefs[0].TargetUID was mutated to %q; expected independent copy %q",
			tree.OwnDataRefs[0].TargetUID, originalUID)
	}
}

// TestBuildTree_Aggregator_BindingIndependentCopy verifies that the Binding pointer on an
// orphan leaf node is an independent copy: mutations to the child content's DataRef after
// BuildTree do not affect Node.Binding.
func TestBuildTree_Aggregator_BindingIndependentCopy(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	binding := dataBinding("uid-x")
	root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: volumeSnapshotAPIVersion, Kind: "VolumeSnapshot", Name: "nss-vs-leaf"},
	})
	scRoot := makeContent("sc-root", "mcp-root", nil) // aggregator: nil dataRef

	vs := makeUnstructuredVolumeSnapshot(testNS, "nss-vs-leaf", "sc-leaf-copy")
	scLeaf := makeContent("sc-leaf-copy", "mcp-leaf-copy", &binding)

	c := buildFakeClient(scheme, []client.Object{root, scRoot, scLeaf}, []*unstructured.Unstructured{vs})

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

	// Mutate the source binding value after BuildTree returns.
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

// makeUnstructuredVolumeSnapshot builds a CSI VolumeSnapshot unstructured object with
// status.boundSnapshotContentName set. Used to exercise the VS visibility-leaf path.
func makeUnstructuredVolumeSnapshot(namespace, vsName, boundContentName string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": volumeSnapshotAPIVersion,
			"kind":       "VolumeSnapshot",
			"metadata": map[string]interface{}{
				"name":      vsName,
				"namespace": namespace,
			},
			"status": map[string]interface{}{
				"boundSnapshotContentName": boundContentName,
			},
		},
	}

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

// TestBuildTree_VolumeSnapshotLeaf_ViaBoundContent is the canonical test for the VS
// visibility-leaf resolution path using REAL producer keys:
//   - VolumeSnapshot carries status.boundSnapshotContentName (Deckhouse extended-VS field)
//   - Child SnapshotContent carries status.dataRef (singular pointer, Variant A)
//
// Asserts:
//   - leaf.Name = VS CR name (for ManifestScopeRef connector call)
//   - leaf.SourceName = PVC name from dataRef.target.name (for directory naming)
//   - leaf.Binding = child content's dataRef (with extended fields)
//   - ManifestScopeRef returns the leaf's own ref (not parent's)
func TestBuildTree_VolumeSnapshotLeaf_ViaBoundContent(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	cases := []struct {
		name         string
		vsName       string
		boundContent string
		pvcName      string
		targetUID    string
		volumeMode   string
		storageClass string
		size         string
		accessModes  []string
	}{
		{
			name:         "block_volume_with_all_fields",
			vsName:       "nss-vs-abc123",
			boundContent: "sc-child-block",
			pvcName:      "pvc-my-disk",
			targetUID:    "uid-my-disk",
			volumeMode:   "Block",
			storageClass: "csi-ceph-rbd",
			size:         "10Gi",
			accessModes:  []string{"ReadWriteOnce"},
		},
		{
			name:         "filesystem_volume_minimal",
			vsName:       "nss-vs-def456",
			boundContent: "sc-child-fs",
			pvcName:      "pvc-fs-disk",
			targetUID:    "uid-fs-disk",
			volumeMode:   "Filesystem",
			storageClass: "csi-ceph-cephfs",
			size:         "20Gi",
			accessModes:  []string{"ReadWriteMany"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
				{APIVersion: volumeSnapshotAPIVersion, Kind: "VolumeSnapshot", Name: tc.vsName},
			})
			scRoot := makeContent("sc-root", "mcp-root", nil)

			vs := makeUnstructuredVolumeSnapshot(testNS, tc.vsName, tc.boundContent)

			childBinding := &snapshotapi.SnapshotDataBinding{
				TargetUID: tc.targetUID,
				Target: snapshotapi.SnapshotSubjectRef{
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Name:       tc.pvcName,
					Namespace:  testNS,
				},
				Artifact: snapshotapi.SnapshotDataArtifactRef{
					APIVersion: "snapshot.storage.k8s.io/v1",
					Kind:       "VolumeSnapshotContent",
					Name:       "vsc-" + tc.targetUID,
				},
				VolumeMode:       tc.volumeMode,
				StorageClassName: tc.storageClass,
				Size:             tc.size,
				AccessModes:      tc.accessModes,
			}
			scChild := makeContent(tc.boundContent, "mcp-child-"+tc.name, childBinding)

			c := buildFakeClient(scheme, []client.Object{root, scRoot, scChild}, []*unstructured.Unstructured{vs})

			tree, err := BuildTree(context.Background(), c, testNS, "root")
			if err != nil {
				t.Fatalf("BuildTree: %v", err)
			}

			if tree.OwnDataRefs != nil {
				t.Errorf("aggregator OwnDataRefs must be nil")
			}

			if len(tree.Children) != 1 {
				t.Fatalf("children: got %d, want 1", len(tree.Children))
			}

			leaf := tree.Children[0]

			// Name = VS CR name (connector key), SourceName = PVC name (dir naming).
			if leaf.Name != tc.vsName {
				t.Errorf("leaf.Name: got %q, want %q", leaf.Name, tc.vsName)
			}

			if leaf.SourceName != tc.pvcName {
				t.Errorf("leaf.SourceName: got %q, want %q", leaf.SourceName, tc.pvcName)
			}

			if leaf.SourceRef != tc.targetUID {
				t.Errorf("leaf.SourceRef: got %q, want %q", leaf.SourceRef, tc.targetUID)
			}

			if leaf.APIVersion != volumeSnapshotAPIVersion {
				t.Errorf("leaf.APIVersion: got %q, want %q", leaf.APIVersion, volumeSnapshotAPIVersion)
			}

			if leaf.Kind != "VolumeSnapshot" {
				t.Errorf("leaf.Kind: got %q, want VolumeSnapshot", leaf.Kind)
			}

			if leaf.Binding == nil {
				t.Fatal("leaf.Binding must not be nil")
			}

			if leaf.Binding.TargetUID != tc.targetUID {
				t.Errorf("leaf.Binding.TargetUID: got %q, want %q", leaf.Binding.TargetUID, tc.targetUID)
			}

			if leaf.Binding.VolumeMode != tc.volumeMode {
				t.Errorf("leaf.Binding.VolumeMode: got %q, want %q", leaf.Binding.VolumeMode, tc.volumeMode)
			}

			if leaf.Binding.StorageClassName != tc.storageClass {
				t.Errorf("leaf.Binding.StorageClassName: got %q, want %q", leaf.Binding.StorageClassName, tc.storageClass)
			}

			if leaf.Binding.Size != tc.size {
				t.Errorf("leaf.Binding.Size: got %q, want %q", leaf.Binding.Size, tc.size)
			}

			if len(leaf.Binding.AccessModes) != len(tc.accessModes) {
				t.Fatalf("leaf.Binding.AccessModes len: got %d, want %d", len(leaf.Binding.AccessModes), len(tc.accessModes))
			}

			for i, am := range tc.accessModes {
				if leaf.Binding.AccessModes[i] != am {
					t.Errorf("leaf.Binding.AccessModes[%d]: got %q, want %q", i, leaf.Binding.AccessModes[i], am)
				}
			}

			// ManifestScopeRef must be the leaf's own ref (VS connector), NOT the parent.
			scope := leaf.ManifestScopeRef()
			if scope.APIVersion != volumeSnapshotAPIVersion || scope.Kind != "VolumeSnapshot" || scope.Name != tc.vsName || scope.Namespace != testNS {
				t.Errorf("ManifestScopeRef: got {%s %s %s/%s}, want {%s VolumeSnapshot %s/%s}",
					scope.APIVersion, scope.Kind, scope.Namespace, scope.Name,
					volumeSnapshotAPIVersion, testNS, tc.vsName)
			}

			if leaf.OwnDataRefs != nil {
				t.Errorf("leaf OwnDataRefs must be nil")
			}

			if len(leaf.Children) != 0 {
				t.Errorf("leaf must have no children, got %d", len(leaf.Children))
			}

			if leaf.Parent != tree {
				t.Errorf("leaf.Parent must be root")
			}
		})
	}
}

// makeUnstructuredSnapWithSpecSourceRef builds an unstructured domain snapshot CR that
// carries spec.sourceRef = {apiVersion, kind, name}.
func makeUnstructuredSnapWithSpecSourceRef(apiVersion, kind, namespace, name, contentName, srcAV, srcKind, srcName string) *unstructured.Unstructured {
	obj := makeUnstructuredSnap(apiVersion, kind, namespace, name, contentName, nil)

	_ = unstructured.SetNestedField(obj.Object, map[string]interface{}{
		"apiVersion": srcAV,
		"kind":       srcKind,
		"name":       srcName,
	}, "spec", "sourceRef")

	return obj
}

// TestBuildTree_DomainNode_SpecSourceRef verifies that:
//   - a domain snapshot node whose CR carries spec.sourceRef has Node.SpecSourceRef set;
//   - a domain node without spec.sourceRef has Node.SpecSourceRef == nil;
//   - core Snapshot nodes (root and typed children) have Node.SpecSourceRef == nil.
func TestBuildTree_DomainNode_SpecSourceRef(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: demoAPI, Kind: "DemoVirtualDiskSnapshot", Name: "disk-with-ref"},
		{APIVersion: demoAPI, Kind: "DemoVirtualDiskSnapshot", Name: "disk-no-ref"},
		{APIVersion: rootAPIVersion, Kind: "Snapshot", Name: "core-child"},
	})
	scRoot := makeContent("sc-root", "mcp-root", nil)

	diskWithRef := makeUnstructuredSnapWithSpecSourceRef(
		demoAPI, "DemoVirtualDiskSnapshot", testNS, "disk-with-ref", "sc-disk-ref",
		"demo.deckhouse.io/v1alpha1", "DemoVirtualDisk", "my-disk",
	)
	scDiskRef := makeContent("sc-disk-ref", "mcp-disk-ref", nil)

	diskNoRef := makeUnstructuredSnap(demoAPI, "DemoVirtualDiskSnapshot", testNS, "disk-no-ref", "sc-disk-noref", nil)
	scDiskNoRef := makeContent("sc-disk-noref", "mcp-disk-noref", nil)

	coreChild := makeSnapshot("core-child", "sc-core-child", nil)
	scCoreChild := makeContent("sc-core-child", "mcp-core-child", nil)

	c := buildFakeClient(scheme,
		[]client.Object{root, scRoot, scDiskRef, scDiskNoRef, coreChild, scCoreChild},
		[]*unstructured.Unstructured{diskWithRef, diskNoRef},
	)

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if tree.SpecSourceRef != nil {
		t.Errorf("root SpecSourceRef must be nil (core Snapshot node), got %+v", tree.SpecSourceRef)
	}

	var withRef, noRef, core *Node

	for _, ch := range tree.Children {
		switch ch.Name {
		case "disk-with-ref":
			withRef = ch
		case "disk-no-ref":
			noRef = ch
		case "core-child":
			core = ch
		}
	}

	if withRef == nil || noRef == nil || core == nil {
		t.Fatalf("expected all three children; withRef=%v noRef=%v core=%v", withRef, noRef, core)
	}

	// Domain node with spec.sourceRef populated.
	if withRef.SpecSourceRef == nil {
		t.Fatal("disk-with-ref SpecSourceRef must not be nil")
	}

	if withRef.SpecSourceRef.APIVersion != "demo.deckhouse.io/v1alpha1" {
		t.Errorf("SpecSourceRef.APIVersion: got %q, want demo.deckhouse.io/v1alpha1", withRef.SpecSourceRef.APIVersion)
	}

	if withRef.SpecSourceRef.Kind != "DemoVirtualDisk" {
		t.Errorf("SpecSourceRef.Kind: got %q, want DemoVirtualDisk", withRef.SpecSourceRef.Kind)
	}

	if withRef.SpecSourceRef.Name != "my-disk" {
		t.Errorf("SpecSourceRef.Name: got %q, want my-disk", withRef.SpecSourceRef.Name)
	}

	// Domain node without spec.sourceRef — SpecSourceRef must be nil.
	if noRef.SpecSourceRef != nil {
		t.Errorf("disk-no-ref SpecSourceRef must be nil, got %+v", noRef.SpecSourceRef)
	}

	// Core Snapshot child — SpecSourceRef must be nil.
	if core.SpecSourceRef != nil {
		t.Errorf("core-child SpecSourceRef must be nil (core Snapshot node), got %+v", core.SpecSourceRef)
	}
}

// TestBuildTree_VolumeSnapshotLeaf_Unbound verifies that a VolumeSnapshot with an empty
// status.boundSnapshotContentName returns ErrLeafNotBound and does not silently produce
// an empty leaf node.
func TestBuildTree_VolumeSnapshotLeaf_Unbound(t *testing.T) {
	t.Helper()

	scheme := makeScheme(t)

	root := makeSnapshot("root", "sc-root", []snapshotapi.SnapshotChildRef{
		{APIVersion: volumeSnapshotAPIVersion, Kind: "VolumeSnapshot", Name: "nss-vs-unbound"},
	})
	scRoot := makeContent("sc-root", "mcp-root", nil)

	// VS exists but boundSnapshotContentName is empty (not yet bound).
	vs := makeUnstructuredVolumeSnapshot(testNS, "nss-vs-unbound", "")

	c := buildFakeClient(scheme, []client.Object{root, scRoot}, []*unstructured.Unstructured{vs})

	_, err := BuildTree(context.Background(), c, testNS, "root")
	if err == nil {
		t.Fatal("expected error for unbound VolumeSnapshot leaf, got nil")
	}

	if !errors.Is(err, ErrLeafNotBound) {
		t.Errorf("expected ErrLeafNotBound in error chain, got: %v", err)
	}
}
