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

package pipeline

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
)

// ---------------------------------------------------------------------------
// Name determinism tests
// ---------------------------------------------------------------------------

func TestShadowHash_Determinism(t *testing.T) {
	h1 := shadowHash("Snapshot--snap-a", "vsc-disk-1")
	h2 := shadowHash("Snapshot--snap-a", "vsc-disk-1")
	h3 := shadowHash("Snapshot--snap-b", "vsc-disk-1") // different nodeID

	if h1 != h2 {
		t.Errorf("shadowHash not deterministic: %q vs %q", h1, h2)
	}

	if h1 == h3 {
		t.Errorf("shadowHash should differ for different nodeIDs")
	}
}

func TestShadowVSCName_Format(t *testing.T) {
	name := shadowVSCName("node-1", "vsc-1")

	if !strings.HasPrefix(name, shadowVSCPrefix) {
		t.Errorf("shadowVSCName %q should have prefix %q", name, shadowVSCPrefix)
	}
}

func TestShadowVSName_Format(t *testing.T) {
	name := shadowVSName("node-1", "vsc-1")

	if !strings.HasPrefix(name, shadowVSPrefix) {
		t.Errorf("shadowVSName %q should have prefix %q", name, shadowVSPrefix)
	}
}

func TestShadowNames_DifferentPrefixes(t *testing.T) {
	vsc := shadowVSCName("node", "vsc")
	vs := shadowVSName("node", "vsc")

	if vsc == vs {
		t.Errorf("shadow VSC name and VS name must differ: both %q", vsc)
	}

	if !strings.HasPrefix(vsc, shadowVSCPrefix) || !strings.HasPrefix(vs, shadowVSPrefix) {
		t.Errorf("unexpected prefixes: vsc=%q, vs=%q", vsc, vs)
	}

	// Hashes derived from the same key must be equal (only prefix differs).
	vscHash := strings.TrimPrefix(vsc, shadowVSCPrefix)
	vsHash := strings.TrimPrefix(vs, shadowVSPrefix)

	if vscHash != vsHash {
		t.Errorf("underlying hashes should be identical: %q vs %q", vscHash, vsHash)
	}
}

// ---------------------------------------------------------------------------
// buildShadowVSC tests
// ---------------------------------------------------------------------------

func TestBuildShadowVSC_RequiredFields(t *testing.T) {
	obj := buildShadowVSC("shadow-vsc-abc", "shadow-vs-abc", "snap-handle-123", "csi.example.com", "my-vsc-class", "snap-ns")

	// GVK
	if got := obj.GetKind(); got != "VolumeSnapshotContent" {
		t.Errorf("kind = %q, want VolumeSnapshotContent", got)
	}

	if got := obj.GetName(); got != "shadow-vsc-abc" {
		t.Errorf("name = %q, want shadow-vsc-abc", got)
	}

	// spec.deletionPolicy must be Retain
	dp, _, _ := unstructured.NestedString(obj.Object, "spec", "deletionPolicy")
	if dp != "Retain" {
		t.Errorf("deletionPolicy = %q, want Retain", dp)
	}

	// spec.driver
	driver, _, _ := unstructured.NestedString(obj.Object, "spec", "driver")
	if driver != "csi.example.com" {
		t.Errorf("driver = %q, want csi.example.com", driver)
	}

	// spec.source.snapshotHandle
	handle, _, _ := unstructured.NestedString(obj.Object, "spec", "source", "snapshotHandle")
	if handle != "snap-handle-123" {
		t.Errorf("snapshotHandle = %q, want snap-handle-123", handle)
	}

	// spec.volumeSnapshotRef.name must point to the shadow VS
	refName, _, _ := unstructured.NestedString(obj.Object, "spec", "volumeSnapshotRef", "name")
	if refName != "shadow-vs-abc" {
		t.Errorf("volumeSnapshotRef.name = %q, want shadow-vs-abc", refName)
	}

	// spec.volumeSnapshotRef.namespace
	refNS, _, _ := unstructured.NestedString(obj.Object, "spec", "volumeSnapshotRef", "namespace")
	if refNS != "snap-ns" {
		t.Errorf("volumeSnapshotRef.namespace = %q, want snap-ns", refNS)
	}

	// volumeSnapshotClassName present
	vscClass, _, _ := unstructured.NestedString(obj.Object, "spec", "volumeSnapshotClassName")
	if vscClass != "my-vsc-class" {
		t.Errorf("volumeSnapshotClassName = %q, want my-vsc-class", vscClass)
	}

	// managed-by label
	label, _, _ := unstructured.NestedString(obj.Object, "metadata", "labels", "app.kubernetes.io/managed-by")
	if label != "d8-snapshot-download" {
		t.Errorf("managed-by label = %q, want d8-snapshot-download", label)
	}
}

func TestBuildShadowVSC_EmptyVSCClassName(t *testing.T) {
	obj := buildShadowVSC("v", "s", "h", "csi.example.com", "", "ns")
	_, found, _ := unstructured.NestedString(obj.Object, "spec", "volumeSnapshotClassName")

	if found {
		t.Error("volumeSnapshotClassName should be absent when empty")
	}
}

// ---------------------------------------------------------------------------
// buildShadowVS tests
// ---------------------------------------------------------------------------

func TestBuildShadowVS_RequiredFields(t *testing.T) {
	obj := buildShadowVS("shadow-vs-abc", "shadow-vsc-abc", "snap-ns", "my-vsc-class", "fast-sc", "Block")

	if got := obj.GetKind(); got != "VolumeSnapshot" {
		t.Errorf("kind = %q, want VolumeSnapshot", got)
	}

	if got := obj.GetName(); got != "shadow-vs-abc" {
		t.Errorf("name = %q, want shadow-vs-abc", got)
	}

	if got := obj.GetNamespace(); got != "snap-ns" {
		t.Errorf("namespace = %q, want snap-ns", got)
	}

	// spec.source.volumeSnapshotContentName
	vscRef, _, _ := unstructured.NestedString(obj.Object, "spec", "source", "volumeSnapshotContentName")
	if vscRef != "shadow-vsc-abc" {
		t.Errorf("source.volumeSnapshotContentName = %q, want shadow-vsc-abc", vscRef)
	}

	// annotations
	scAnnot, _, _ := unstructured.NestedString(obj.Object, "metadata", "annotations", annotationStorageClass)
	if scAnnot != "fast-sc" {
		t.Errorf("storage-class annotation = %q, want fast-sc", scAnnot)
	}

	vmAnnot, _, _ := unstructured.NestedString(obj.Object, "metadata", "annotations", annotationVolumeMode)
	if vmAnnot != "Block" {
		t.Errorf("volume-mode annotation = %q, want Block", vmAnnot)
	}

	// volumeSnapshotClassName
	vscClass, _, _ := unstructured.NestedString(obj.Object, "spec", "volumeSnapshotClassName")
	if vscClass != "my-vsc-class" {
		t.Errorf("volumeSnapshotClassName = %q, want my-vsc-class", vscClass)
	}
}

// ---------------------------------------------------------------------------
// resolveStorageClassForDriver tests
// ---------------------------------------------------------------------------

func makeStorageClass(name, provisioner string) *unstructured.Unstructured {
	obj := new(unstructured.Unstructured)
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "storage.k8s.io", Version: "v1", Kind: "StorageClass"})
	obj.SetName(name)
	_ = unstructured.SetNestedField(obj.Object, provisioner, "provisioner")

	return obj
}

func TestResolveStorageClassForDriver_Found(t *testing.T) {
	sc := makeStorageClass("csi-fast", "csi.example.com")
	client := fake.NewClientBuilder().
		WithScheme(runtime.NewScheme()).
		WithObjects(sc).
		Build()

	got, err := resolveStorageClassForDriver(context.Background(), client, "csi.example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != "csi-fast" {
		t.Errorf("got StorageClass %q, want csi-fast", got)
	}
}

func TestResolveStorageClassForDriver_NotFound(t *testing.T) {
	sc := makeStorageClass("other-sc", "other.csi.com")
	client := fake.NewClientBuilder().
		WithScheme(runtime.NewScheme()).
		WithObjects(sc).
		Build()

	_, err := resolveStorageClassForDriver(context.Background(), client, "csi.example.com")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestResolveStorageClassForDriver_MultipleClasses(t *testing.T) {
	sc1 := makeStorageClass("sc-slow", "slow.csi.com")
	sc2 := makeStorageClass("sc-fast", "csi.example.com")
	client := fake.NewClientBuilder().
		WithScheme(runtime.NewScheme()).
		WithObjects(sc1, sc2).
		Build()

	got, err := resolveStorageClassForDriver(context.Background(), client, "csi.example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != "sc-fast" {
		t.Errorf("got StorageClass %q, want sc-fast", got)
	}
}

// ---------------------------------------------------------------------------
// detectVolumeMode tests
// ---------------------------------------------------------------------------

func makeOrigVSC(sourceVolumeMode string) *unstructured.Unstructured {
	obj := new(unstructured.Unstructured)
	obj.SetGroupVersionKind(shadowVSCGVK)
	obj.SetName("original-vsc")

	if sourceVolumeMode != "" {
		_ = unstructured.SetNestedField(obj.Object, sourceVolumeMode, "spec", "sourceVolumeMode")
	}

	return obj
}

func makePVC(name, ns, volumeMode string) *unstructured.Unstructured {
	obj := new(unstructured.Unstructured)
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "PersistentVolumeClaim"})
	obj.SetName(name)
	obj.SetNamespace(ns)

	if volumeMode != "" {
		_ = unstructured.SetNestedField(obj.Object, volumeMode, "spec", "volumeMode")
	}

	return obj
}

func TestDetectVolumeMode_FromSourceVolumeMode(t *testing.T) {
	vsc := makeOrigVSC("Block")
	client := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).Build()
	dr := source.DataRef{PVCName: "pvc", PVCNamespace: "ns"}

	got := detectVolumeMode(context.Background(), client, vsc, dr)
	if got != "Block" {
		t.Errorf("got %q, want Block", got)
	}
}

func TestDetectVolumeMode_FromPVC(t *testing.T) {
	vsc := makeOrigVSC("") // no sourceVolumeMode
	pvc := makePVC("pvc-1", "demo", "Block")
	client := fake.NewClientBuilder().
		WithScheme(runtime.NewScheme()).
		WithObjects(pvc).
		Build()
	dr := source.DataRef{PVCName: "pvc-1", PVCNamespace: "demo"}

	got := detectVolumeMode(context.Background(), client, vsc, dr)
	if got != "Block" {
		t.Errorf("got %q, want Block", got)
	}
}

func TestDetectVolumeMode_DefaultFilesystem(t *testing.T) {
	vsc := makeOrigVSC("") // no sourceVolumeMode
	client := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).Build()
	dr := source.DataRef{} // no PVC coordinates

	got := detectVolumeMode(context.Background(), client, vsc, dr)
	if got != "Filesystem" {
		t.Errorf("got %q, want Filesystem", got)
	}
}

func TestDetectVolumeMode_PVCNotFound_DefaultsFilesystem(t *testing.T) {
	vsc := makeOrigVSC("") // no sourceVolumeMode
	client := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).Build()
	dr := source.DataRef{PVCName: "missing-pvc", PVCNamespace: "ns"} // PVC doesn't exist

	got := detectVolumeMode(context.Background(), client, vsc, dr)
	if got != "Filesystem" {
		t.Errorf("got %q, want Filesystem", got)
	}
}

func TestDetectVolumeMode_SourceVolumeModeOverridesPVC(t *testing.T) {
	// sourceVolumeMode=Filesystem but PVC says Block — sourceVolumeMode wins
	vsc := makeOrigVSC("Filesystem")
	pvc := makePVC("pvc-1", "demo", "Block")
	client := fake.NewClientBuilder().
		WithScheme(runtime.NewScheme()).
		WithObjects(pvc).
		Build()
	dr := source.DataRef{PVCName: "pvc-1", PVCNamespace: "demo"}

	got := detectVolumeMode(context.Background(), client, vsc, dr)
	if got != "Filesystem" {
		t.Errorf("got %q, want Filesystem (sourceVolumeMode should win)", got)
	}
}
