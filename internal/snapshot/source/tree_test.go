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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testNS  = "default"
	demoAPI = "demo.deckhouse.io/v1alpha1"
)

// makeScheme builds an empty scheme; every fixture is served as unstructured, so no typed
// registration is required (the tree builder reads namespaced status via ParseNodeStatus).
func makeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	return runtime.NewScheme()
}

// namespaceSourceRef is the root capture-Snapshot's status.sourceRef: the cluster-scoped
// Namespace (v1/Namespace), which legitimately carries no namespace field.
func namespaceSourceRef(nsName, uid string) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"name":       nsName,
		"uid":        uid,
	}
}

// pvcSourceRef builds a namespaced status.sourceRef for a captured PVC.
func pvcSourceRef(name, uid string) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"namespace":  testNS,
		"name":       name,
		"uid":        uid,
	}
}

// domainSourceRef builds a namespaced status.sourceRef for a captured domain object.
func domainSourceRef(apiVersion, kind, name, uid string) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": apiVersion,
		"kind":       kind,
		"namespace":  testNS,
		"name":       name,
		"uid":        uid,
	}
}

// nodeDataMap builds a namespaced status.data descriptor for a captured PVC volume.
func nodeDataMap(pvcName, pvcUID string) map[string]interface{} {
	return map[string]interface{}{
		"sourceRef": map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "PersistentVolumeClaim",
			"namespace":  testNS,
			"name":       pvcName,
			"uid":        pvcUID,
		},
		"artifactRef": map[string]interface{}{
			"apiVersion": "snapshot.storage.k8s.io/v1",
			"kind":       "VolumeSnapshotContent",
			"name":       "vsc-" + pvcUID,
		},
	}
}

// snapOpts describes a snapshot node fixture.
type snapOpts struct {
	apiVersion string
	kind       string
	name       string
	uid        string
	sourceRef  map[string]interface{}
	data       map[string]interface{}
	childRefs  []interface{}
}

// makeSnap builds an unstructured snapshot node carrying its namespaced status
// (sourceRef/data/childrenSnapshotRefs) and metadata.uid.
func makeSnap(o snapOpts) *unstructured.Unstructured {
	status := map[string]interface{}{}
	if o.sourceRef != nil {
		status["sourceRef"] = o.sourceRef
	}

	if o.data != nil {
		status["data"] = o.data
	}

	if len(o.childRefs) > 0 {
		status["childrenSnapshotRefs"] = o.childRefs
	}

	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": o.apiVersion,
		"kind":       o.kind,
		"metadata": map[string]interface{}{
			"name":      o.name,
			"namespace": testNS,
			"uid":       o.uid,
		},
		"status": status,
	}}
}

// rootSnap builds the root capture Snapshot fixture with the given child refs.
func rootSnap(name, uid string, childRefs []interface{}) *unstructured.Unstructured {
	return makeSnap(snapOpts{
		apiVersion: rootAPIVersion,
		kind:       "Snapshot",
		name:       name,
		uid:        uid,
		sourceRef:  namespaceSourceRef(testNS, "ns-uid-"+uid),
		childRefs:  childRefs,
	})
}

// childRef builds a childrenSnapshotRefs entry.
func childRef(apiVersion, kind, name string) interface{} {
	return map[string]interface{}{
		"apiVersion": apiVersion,
		"kind":       kind,
		"name":       name,
	}
}

func buildFakeClient(scheme *runtime.Scheme, objs []*unstructured.Unstructured) client.Client {
	builder := fake.NewClientBuilder().WithScheme(scheme)
	for _, u := range objs {
		builder = builder.WithObjects(u)
	}

	return builder.Build()
}

// TestBuildTree_DiskNode_Data verifies that a domain disk snapshot node with its own
// status.data and no visibility-leaf children stores the volume in Data and produces no
// leaf children.
func TestBuildTree_DiskNode_Data(t *testing.T) {
	scheme := makeScheme(t)

	root := rootSnap("root", "root-uid", []interface{}{
		childRef(demoAPI, "DemoVirtualDiskSnapshot", "child1"),
	})
	child1 := makeSnap(snapOpts{
		apiVersion: demoAPI,
		kind:       "DemoVirtualDiskSnapshot",
		name:       "child1",
		uid:        "child1-uid",
		sourceRef:  domainSourceRef(demoAPI, "DemoVirtualDisk", "disk-1", "disk-1-uid"),
		data:       nodeDataMap("pvc-1", "uid-1"),
	})

	c := buildFakeClient(scheme, []*unstructured.Unstructured{root, child1})
	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if tree.Name != "root" {
		t.Errorf("root name: got %q, want root", tree.Name)
	}

	if tree.Parent != nil {
		t.Errorf("root parent should be nil")
	}

	if tree.Data != nil {
		t.Errorf("root Data must be nil, got %+v", tree.Data)
	}

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

	if c1.Data == nil {
		t.Fatal("child1 Data must not be nil")
	}

	if c1.Data.SourceRef.UID != "uid-1" {
		t.Errorf("child1 Data.SourceRef.UID: got %q, want uid-1", c1.Data.SourceRef.UID)
	}

	if c1.DirBaseName() != "disk-1" {
		t.Errorf("child1 DirBaseName: got %q, want disk-1 (from status.sourceRef.name)", c1.DirBaseName())
	}

	if len(c1.Children) != 0 {
		t.Errorf("child1 (disk node) must have no children, got %d", len(c1.Children))
	}
}

// TestBuildTree_Aggregator_VisibilityLeafProducesOrphanLeaves verifies that a
// VolumeSnapshot visibility-leaf child ref is resolved from the VS's own namespaced status.
func TestBuildTree_Aggregator_VisibilityLeafProducesOrphanLeaves(t *testing.T) {
	scheme := makeScheme(t)

	root := rootSnap("root", "root-uid", []interface{}{
		childRef(volumeSnapshotAPIVersion, "VolumeSnapshot", "nss-vs-orphan"),
	})

	vs := makeSnap(snapOpts{
		apiVersion: volumeSnapshotAPIVersion,
		kind:       "VolumeSnapshot",
		name:       "nss-vs-orphan",
		uid:        "vs-uid",
		sourceRef:  pvcSourceRef("pvc-orphan", "uid-pvc"),
		data:       nodeDataMap("pvc-orphan", "uid-pvc"),
	})

	c := buildFakeClient(scheme, []*unstructured.Unstructured{root, vs})

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if tree.Data != nil {
		t.Errorf("aggregator Data must be nil, got %+v", tree.Data)
	}

	if len(tree.Children) != 1 {
		t.Fatalf("aggregator must have 1 orphan leaf child, got %d", len(tree.Children))
	}

	leaf := tree.Children[0]
	if !leaf.IsVolumeLeaf() {
		t.Errorf("leaf must be a volume leaf, got %s/%s", leaf.APIVersion, leaf.Kind)
	}

	// Leaf node Name is the VS CR name (for ManifestScopeRef connector), NOT the PVC name.
	if leaf.Name != "nss-vs-orphan" {
		t.Errorf("leaf Name: got %q, want nss-vs-orphan", leaf.Name)
	}

	// The readable directory base is the captured PVC name (status.sourceRef.name).
	if leaf.DirBaseName() != "pvc-orphan" {
		t.Errorf("leaf DirBaseName: got %q, want pvc-orphan", leaf.DirBaseName())
	}

	if leaf.Data == nil || leaf.Data.SourceRef.UID != "uid-pvc" {
		t.Errorf("leaf Data.SourceRef.UID: got %+v, want uid-pvc", leaf.Data)
	}

	if len(leaf.Children) != 0 {
		t.Errorf("leaf must have no children, got %d", len(leaf.Children))
	}

	if leaf.Parent != tree {
		t.Errorf("leaf Parent must be root")
	}

	// ManifestScopeRef must be the leaf's own ref (VS ref), NOT the parent aggregator.
	scopeRef := leaf.ManifestScopeRef()
	if scopeRef.APIVersion != volumeSnapshotAPIVersion || scopeRef.Kind != "VolumeSnapshot" ||
		scopeRef.Name != "nss-vs-orphan" || scopeRef.Namespace != testNS {
		t.Errorf("ManifestScopeRef: got {%s %s %s/%s}, want {%s VolumeSnapshot %s/nss-vs-orphan}",
			scopeRef.APIVersion, scopeRef.Kind, scopeRef.Namespace, scopeRef.Name,
			volumeSnapshotAPIVersion, testNS)
	}
}

// TestBuildTree_DeepTree verifies a root → vm-snap → disk-snap tree. The disk node has its
// own status.data with no visibility-leaf children → Data set, no children.
func TestBuildTree_DeepTree(t *testing.T) {
	scheme := makeScheme(t)

	root := rootSnap("root", "root-uid", []interface{}{
		childRef(demoAPI, "DemoVirtualMachineSnapshot", "vm-snap"),
	})

	vmSnap := makeSnap(snapOpts{
		apiVersion: demoAPI,
		kind:       "DemoVirtualMachineSnapshot",
		name:       "vm-snap",
		uid:        "vm-uid",
		sourceRef:  domainSourceRef(demoAPI, "DemoVirtualMachine", "vm-1", "vm-1-uid"),
		childRefs:  []interface{}{childRef(demoAPI, "DemoVirtualDiskSnapshot", "disk-snap")},
	})

	diskSnap := makeSnap(snapOpts{
		apiVersion: demoAPI,
		kind:       "DemoVirtualDiskSnapshot",
		name:       "disk-snap",
		uid:        "disk-uid",
		sourceRef:  domainSourceRef(demoAPI, "DemoVirtualDisk", "disk-1", "disk-1-uid"),
		data:       nodeDataMap("pvc-disk", "uid-disk"),
	})

	c := buildFakeClient(scheme, []*unstructured.Unstructured{root, vmSnap, diskSnap})

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if len(tree.Children) != 1 {
		t.Fatalf("root children: %d, want 1", len(tree.Children))
	}

	vm := tree.Children[0]
	if vm.Kind != "DemoVirtualMachineSnapshot" {
		t.Errorf("vm kind: got %q", vm.Kind)
	}

	if len(vm.Children) != 1 {
		t.Fatalf("vm children: %d, want 1", len(vm.Children))
	}

	disk := vm.Children[0]
	if disk.Kind != "DemoVirtualDiskSnapshot" {
		t.Errorf("disk kind: got %q", disk.Kind)
	}

	if disk.Data == nil || disk.Data.SourceRef.UID != "uid-disk" {
		t.Errorf("disk Data.SourceRef.UID: got %+v, want uid-disk", disk.Data)
	}

	if len(disk.Children) != 0 {
		t.Errorf("disk (non-aggregator) must have no children, got %d", len(disk.Children))
	}

	if disk.Parent != vm {
		t.Errorf("disk parent should be vm")
	}
}

// TestBuildTree_NoData_NoVolumeNodes verifies that a node with no status.data has Data == nil
// and produces no volume children.
func TestBuildTree_NoData_NoVolumeNodes(t *testing.T) {
	scheme := makeScheme(t)

	root := rootSnap("root", "root-uid", nil)

	c := buildFakeClient(scheme, []*unstructured.Unstructured{root})

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if len(tree.Children) != 0 {
		t.Errorf("expected no children, got %d", len(tree.Children))
	}

	if tree.Data != nil {
		t.Errorf("expected nil Data for node with no status.data, got %+v", tree.Data)
	}
}

// TestBuildTree_DataExtendedFields verifies that the extended volume metadata in status.data
// (volumeMode/storageClassName/size/accessModes/fsType) is decoded onto Node.Data.
func TestBuildTree_DataExtendedFields(t *testing.T) {
	scheme := makeScheme(t)

	data := nodeDataMap("pvc-disk", "uid-disk-sc")
	data["volumeMode"] = "Block"
	data["storageClassName"] = "csi-ceph-rbd"
	data["size"] = "20Gi"
	data["accessModes"] = []interface{}{"ReadWriteOnce"}

	root := makeSnap(snapOpts{
		apiVersion: rootAPIVersion,
		kind:       "Snapshot",
		name:       "root",
		uid:        "root-uid",
		sourceRef:  namespaceSourceRef(testNS, "ns-uid"),
		data:       data,
	})

	c := buildFakeClient(scheme, []*unstructured.Unstructured{root})

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if tree.Data == nil {
		t.Fatal("Data must not be nil")
	}

	got := tree.Data
	if got.SourceRef.UID != "uid-disk-sc" {
		t.Errorf("SourceRef.UID: got %q, want uid-disk-sc", got.SourceRef.UID)
	}

	if got.VolumeMode != "Block" {
		t.Errorf("VolumeMode: got %q, want Block", got.VolumeMode)
	}

	if got.StorageClassName != "csi-ceph-rbd" {
		t.Errorf("StorageClassName: got %q, want csi-ceph-rbd", got.StorageClassName)
	}

	if got.Size != "20Gi" {
		t.Errorf("Size: got %q, want 20Gi", got.Size)
	}

	if len(got.AccessModes) != 1 || got.AccessModes[0] != "ReadWriteOnce" {
		t.Errorf("AccessModes: got %v, want [ReadWriteOnce]", got.AccessModes)
	}
}

// TestBuildTree_DomainChildBeforeOrphanLeaf verifies that domain snapshot children appear
// before orphan leaf volume children in an aggregator node.
func TestBuildTree_DomainChildBeforeOrphanLeaf(t *testing.T) {
	scheme := makeScheme(t)

	root := rootSnap("root", "root-uid", []interface{}{
		childRef(rootAPIVersion, "Snapshot", "snap-child"),
		childRef(volumeSnapshotAPIVersion, "VolumeSnapshot", "nss-vs-orphan"),
	})
	snapChild := makeSnap(snapOpts{
		apiVersion: rootAPIVersion,
		kind:       "Snapshot",
		name:       "snap-child",
		uid:        "snap-child-uid",
		sourceRef:  namespaceSourceRef(testNS, "ns-uid-2"),
	})
	vs := makeSnap(snapOpts{
		apiVersion: volumeSnapshotAPIVersion,
		kind:       "VolumeSnapshot",
		name:       "nss-vs-orphan",
		uid:        "vs-uid",
		sourceRef:  pvcSourceRef("pvc-vol", "uid-vol"),
		data:       nodeDataMap("pvc-vol", "uid-vol"),
	})

	c := buildFakeClient(scheme, []*unstructured.Unstructured{root, snapChild, vs})

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	if len(tree.Children) != 2 {
		t.Fatalf("root children: got %d, want 2", len(tree.Children))
	}

	first := tree.Children[0]
	second := tree.Children[1]

	if first.Kind != "Snapshot" || first.Name != "snap-child" {
		t.Errorf("first child should be domain snapshot snap-child, got %s/%s", first.Kind, first.Name)
	}

	if !second.IsVolumeLeaf() || second.Name != "nss-vs-orphan" {
		t.Errorf("second child should be orphan leaf nss-vs-orphan, got %s/%s", second.Kind, second.Name)
	}

	if second.DirBaseName() != "pvc-vol" {
		t.Errorf("orphan leaf DirBaseName: got %q, want pvc-vol", second.DirBaseName())
	}

	if second.Data == nil {
		t.Error("orphan leaf Data must not be nil")
	}

	if tree.Data != nil {
		t.Errorf("aggregator Data must be nil")
	}
}

// TestBuildTree_CycleError verifies that a cycle in childrenSnapshotRefs returns ErrCycle.
func TestBuildTree_CycleError(t *testing.T) {
	scheme := makeScheme(t)

	root := rootSnap("root", "root-uid", []interface{}{
		childRef(rootAPIVersion, "Snapshot", "child1"),
	})
	child1 := makeSnap(snapOpts{
		apiVersion: rootAPIVersion,
		kind:       "Snapshot",
		name:       "child1",
		uid:        "child1-uid",
		sourceRef:  namespaceSourceRef(testNS, "ns-uid-2"),
		childRefs:  []interface{}{childRef(rootAPIVersion, "Snapshot", "root")},
	})

	c := buildFakeClient(scheme, []*unstructured.Unstructured{root, child1})
	_, err := BuildTree(context.Background(), c, testNS, "root")
	if err == nil {
		t.Fatal("expected ErrCycle, got nil")
	}

	if !errors.Is(err, ErrCycle) {
		t.Errorf("expected ErrCycle, got: %v", err)
	}
}

// TestBuildTree_IncompleteIdentity verifies that a node missing metadata.uid fails closed.
func TestBuildTree_IncompleteIdentity(t *testing.T) {
	scheme := makeScheme(t)

	root := makeSnap(snapOpts{
		apiVersion: rootAPIVersion,
		kind:       "Snapshot",
		name:       "root",
		uid:        "", // missing uid → ParseNodeStatus fails closed
		sourceRef:  namespaceSourceRef(testNS, "ns-uid"),
	})

	c := buildFakeClient(scheme, []*unstructured.Unstructured{root})
	_, err := BuildTree(context.Background(), c, testNS, "root")
	if err == nil {
		t.Fatal("expected error for node with incomplete identity, got nil")
	}
}

// TestBuildTree_SourceRef verifies that the namespaced status.sourceRef is decoded onto
// Node.SourceRef and drives DirBaseName; a domain node carries its captured object identity.
func TestBuildTree_SourceRef(t *testing.T) {
	scheme := makeScheme(t)

	root := rootSnap("root", "root-uid", []interface{}{
		childRef(demoAPI, "DemoVirtualDiskSnapshot", "disk-with-ref"),
	})
	diskWithRef := makeSnap(snapOpts{
		apiVersion: demoAPI,
		kind:       "DemoVirtualDiskSnapshot",
		name:       "disk-with-ref",
		uid:        "disk-uid",
		sourceRef:  domainSourceRef(demoAPI, "DemoVirtualDisk", "my-disk", "my-disk-uid"),
		data:       nodeDataMap("pvc-x", "uid-x"),
	})

	c := buildFakeClient(scheme, []*unstructured.Unstructured{root, diskWithRef})

	tree, err := BuildTree(context.Background(), c, testNS, "root")
	if err != nil {
		t.Fatalf("BuildTree: %v", err)
	}

	// Root's status.sourceRef is the cluster-scoped Namespace.
	if tree.SourceRef == nil || tree.SourceRef.Kind != "Namespace" {
		t.Errorf("root SourceRef: got %+v, want v1/Namespace", tree.SourceRef)
	}

	if len(tree.Children) != 1 {
		t.Fatalf("children: %d, want 1", len(tree.Children))
	}

	withRef := tree.Children[0]
	if withRef.SourceRef == nil {
		t.Fatal("disk-with-ref SourceRef must not be nil")
	}

	if withRef.SourceRef.Kind != "DemoVirtualDisk" || withRef.SourceRef.Name != "my-disk" {
		t.Errorf("disk-with-ref SourceRef: got %+v, want DemoVirtualDisk/my-disk", withRef.SourceRef)
	}

	if withRef.DirBaseName() != "my-disk" {
		t.Errorf("disk-with-ref DirBaseName: got %q, want my-disk", withRef.DirBaseName())
	}
}

// TestBuildTree_ChildNamespace verifies that children are fetched in the root namespace.
func TestBuildTree_ChildNamespace(t *testing.T) {
	scheme := makeScheme(t)

	root := rootSnap("root", "root-uid", []interface{}{
		childRef(rootAPIVersion, "Snapshot", "child1"),
	})
	child1 := makeSnap(snapOpts{
		apiVersion: rootAPIVersion,
		kind:       "Snapshot",
		name:       "child1",
		uid:        "child1-uid",
		sourceRef:  namespaceSourceRef(testNS, "ns-uid-2"),
	})

	c := buildFakeClient(scheme, []*unstructured.Unstructured{root, child1})

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

// TestBuildTree_VolumeSnapshotLeaf_NotCaptured verifies that a VolumeSnapshot leaf with no
// status.data returns ErrLeafNotBound and does not silently produce an empty leaf node.
func TestBuildTree_VolumeSnapshotLeaf_NotCaptured(t *testing.T) {
	scheme := makeScheme(t)

	root := rootSnap("root", "root-uid", []interface{}{
		childRef(volumeSnapshotAPIVersion, "VolumeSnapshot", "nss-vs-unbound"),
	})

	// VS exists and carries a sourceRef but no status.data (not yet captured).
	vs := makeSnap(snapOpts{
		apiVersion: volumeSnapshotAPIVersion,
		kind:       "VolumeSnapshot",
		name:       "nss-vs-unbound",
		uid:        "vs-uid",
		sourceRef:  pvcSourceRef("pvc-unbound", "uid-unbound"),
	})

	c := buildFakeClient(scheme, []*unstructured.Unstructured{root, vs})

	_, err := BuildTree(context.Background(), c, testNS, "root")
	if err == nil {
		t.Fatal("expected error for not-yet-captured VolumeSnapshot leaf, got nil")
	}

	if !errors.Is(err, ErrLeafNotBound) {
		t.Errorf("expected ErrLeafNotBound in error chain, got: %v", err)
	}
}
