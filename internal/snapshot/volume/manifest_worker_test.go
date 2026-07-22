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

package volume_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
)

// stubManifestSource is a test-only ManifestSource that returns a pre-seeded list
// regardless of the requested node ref (each test wires a single source).
type stubManifestSource struct {
	objs []unstructured.Unstructured
	err  error
}

func (s *stubManifestSource) FetchNodeManifests(_ context.Context, _ aggapi.NodeRef) ([]unstructured.Unstructured, error) {
	return s.objs, s.err
}

// pvcSource builds a SourceRefIdentity for a captured PersistentVolumeClaim source.
func pvcSource(name, uid string) source.SourceRefIdentity {
	return source.SourceRefIdentity{
		APIVersion: "v1",
		Kind:       "PersistentVolumeClaim",
		Namespace:  "ns",
		Name:       name,
		UID:        uid,
	}
}

// pvcNodeData builds a *source.NodeData for a captured PVC-backed volume (Variant A, ≤1).
func pvcNodeData(name, uid, vsc string) *source.NodeData {
	return &source.NodeData{
		SourceRef:   pvcSource(name, uid),
		ArtifactRef: source.ArtifactRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshotContent", Name: vsc},
	}
}

// volumeLeaf builds a CSI VolumeSnapshot visibility-leaf node carrying the captured PVC in
// its own status.data (IsVolumeLeaf() == true requires the CSI apiVersion + kind).
func volumeLeaf(vsName, pvcName, uid string) *source.Node {
	return &source.Node{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       vsName,
		Data:       pvcNodeData(pvcName, uid, "vsc-"+pvcName),
	}
}

// makeObj builds a minimal unstructured object.
func makeObj(apiVersion, kind, name string) unstructured.Unstructured {
	obj := unstructured.Unstructured{}
	obj.SetAPIVersion(apiVersion)
	obj.SetKind(kind)
	obj.SetName(name)

	return obj
}

// setupNodeDir creates a nodeDir with a manifests/ subdirectory.
func setupNodeDir(t *testing.T) string {
	t.Helper()

	nodeDir := t.TempDir()

	if err := os.MkdirAll(filepath.Join(nodeDir, archive.ManifestsDirName), 0o755); err != nil {
		t.Fatalf("mkdir manifests: %v", err)
	}

	return nodeDir
}

func TestWriteNodeManifests_WritesFiles(t *testing.T) {
	nodeDir := setupNodeDir(t)

	objs := []unstructured.Unstructured{
		makeObj("v1", "ConfigMap", "my-cm"),
		makeObj("apps/v1", "Deployment", "my-deploy"),
	}

	node := &source.Node{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-1",
		Namespace:  "default",
	}

	src := &stubManifestSource{objs: objs}

	if err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node); err != nil {
		t.Fatalf("WriteNodeManifests: %v", err)
	}

	// Expect one file per object.
	expected := []string{
		"configmap_my-cm.yaml",
		"deployment_my-deploy.yaml",
	}

	for _, name := range expected {
		path := filepath.Join(nodeDir, archive.ManifestsDirName, name)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected manifest file %s: %v", name, err)
		}
	}
}

func TestWriteNodeManifests_CollisionFallback(t *testing.T) {
	nodeDir := setupNodeDir(t)

	// Two objects with the same kind+name but different API groups.
	objs := []unstructured.Unstructured{
		makeObj("v1", "Pod", "my-pod"),
		makeObj("apps/v1", "Pod", "my-pod"),
	}

	node := &source.Node{}
	src := &stubManifestSource{objs: objs}

	if err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node); err != nil {
		t.Fatalf("WriteNodeManifests: %v", err)
	}

	manifestsDir := filepath.Join(nodeDir, archive.ManifestsDirName)

	// Normal name for the first-written object (core group → no qualifier).
	normalPath := filepath.Join(manifestsDir, "pod_my-pod.yaml")
	if _, err := os.Stat(normalPath); err != nil {
		t.Errorf("expected normal manifest file pod_my-pod.yaml: %v", err)
	}

	// Qualified name for the second object (different API group).
	qualifiedPath := filepath.Join(manifestsDir, "pod.apps_my-pod.yaml")
	if _, err := os.Stat(qualifiedPath); err != nil {
		t.Errorf("expected qualified manifest file pod.apps_my-pod.yaml: %v", err)
	}
}

func TestWriteNodeManifests_Idempotent(t *testing.T) {
	nodeDir := setupNodeDir(t)

	objs := []unstructured.Unstructured{makeObj("v1", "ConfigMap", "cm1")}
	node := &source.Node{}
	src := &stubManifestSource{objs: objs}

	// First call.
	if err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node); err != nil {
		t.Fatalf("first WriteNodeManifests: %v", err)
	}

	path := filepath.Join(nodeDir, archive.ManifestsDirName, "configmap_cm1.yaml")

	fi1, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after first call: %v", err)
	}

	// Second call (idempotent rewrite).
	if err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node); err != nil {
		t.Fatalf("second WriteNodeManifests: %v", err)
	}

	fi2, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after second call: %v", err)
	}

	// File must still be present (idempotent — content unchanged, file re-written atomically).
	if fi1.Name() != fi2.Name() {
		t.Error("file disappeared after idempotent rewrite")
	}
}

func TestFinalizeNode_WritesSnapshotYAML(t *testing.T) {
	nodeDir := setupNodeDir(t)

	// Write a manifest so the checksum has content to cover.
	obj := makeObj("v1", "ConfigMap", "cm")

	if err := archive.WriteManifest(nodeDir, obj); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	node := &source.Node{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-test",
		Namespace:  "ns1",
		UID:        "uid-snap-test",
		SourceRef:  &source.SourceRefIdentity{APIVersion: "v1", Kind: "Namespace", Name: "vm-1", UID: "uid-vm-1"},
	}

	if err := volume.FinalizeNode(nodeDir, node); err != nil {
		t.Fatalf("FinalizeNode: %v", err)
	}

	// snapshot.yaml must exist.
	syPath := filepath.Join(nodeDir, archive.SnapshotYAMLName)
	if _, err := os.Stat(syPath); err != nil {
		t.Fatalf("snapshot.yaml missing: %v", err)
	}

	// VerifyNode must pass (checksum in snapshot.yaml == recomputed checksum).
	if err := archive.VerifyNode(nodeDir); err != nil {
		t.Errorf("VerifyNode after FinalizeNode: %v", err)
	}

	// Identity fields must be preserved.
	sy, err := archive.ReadSnapshotYAML(nodeDir)
	if err != nil {
		t.Fatalf("ReadSnapshotYAML: %v", err)
	}

	if sy.APIVersion != node.APIVersion {
		t.Errorf("APIVersion: got %q, want %q", sy.APIVersion, node.APIVersion)
	}

	if sy.Kind != node.Kind {
		t.Errorf("Kind: got %q, want %q", sy.Kind, node.Kind)
	}

	if sy.Name != node.Name {
		t.Errorf("Name: got %q, want %q", sy.Name, node.Name)
	}

	if sy.SourceName != node.SourceRef.Name {
		t.Errorf("SourceName: got %q, want %q", sy.SourceName, node.SourceRef.Name)
	}

	if sy.UID != string(node.UID) {
		t.Errorf("UID: got %q, want %q", sy.UID, node.UID)
	}
}

func TestFinalizeNode_Idempotent(t *testing.T) {
	nodeDir := setupNodeDir(t)

	obj := makeObj("v1", "ConfigMap", "cm2")

	if err := archive.WriteManifest(nodeDir, obj); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	node := &source.Node{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-idem",
	}

	if err := volume.FinalizeNode(nodeDir, node); err != nil {
		t.Fatalf("first FinalizeNode: %v", err)
	}

	if err := volume.FinalizeNode(nodeDir, node); err != nil {
		t.Fatalf("second FinalizeNode: %v", err)
	}

	if err := archive.VerifyNode(nodeDir); err != nil {
		t.Errorf("VerifyNode after two FinalizeNode calls: %v", err)
	}
}

// TestFinalizeNode_RemovesIdentityMarker proves finalize-removes-identity-marker:
// a node dir carrying the resume identity marker on first touch has snapshot.yaml
// written AND the marker removed once finalize succeeds, so a finalized node
// never keeps a stray identity.json. A second FinalizeNode call (marker already
// absent) must still succeed — the remove is idempotent (os.ErrNotExist ignored).
func TestFinalizeNode_RemovesIdentityMarker(t *testing.T) {
	t.Parallel()

	nodeDir := setupNodeDir(t)

	if err := archive.WriteManifest(nodeDir, makeObj("v1", "ConfigMap", "cm-marker")); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	node := &source.Node{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-marker",
		Namespace:  "ns",
		UID:        "uid-snap-marker",
		SourceRef:  &source.SourceRefIdentity{APIVersion: "v1", Kind: "Namespace", Name: "vm-1", UID: "uid-vm-1"},
	}

	// Stamp the marker the pipeline writes on first touch.
	if err := archive.WriteNodeIdentityMarker(nodeDir, archive.NodeIdentity{
		APIVersion: node.APIVersion,
		Kind:       node.Kind,
		Name:       node.Name,
		Namespace:  node.Namespace,
		UID:        string(node.UID),
	}); err != nil {
		t.Fatalf("WriteNodeIdentityMarker: %v", err)
	}

	if err := volume.FinalizeNode(nodeDir, node); err != nil {
		t.Fatalf("FinalizeNode: %v", err)
	}

	// snapshot.yaml written and VerifyNode passes.
	if err := archive.VerifyNode(nodeDir); err != nil {
		t.Errorf("VerifyNode after FinalizeNode: %v", err)
	}

	// The identity marker must be gone.
	if _, found, err := archive.ReadNodeIdentityMarker(nodeDir); err != nil {
		t.Fatalf("ReadNodeIdentityMarker: %v", err)
	} else if found {
		t.Error("identity marker must be removed after FinalizeNode")
	}

	// Second call (marker already absent) must succeed.
	if err := volume.FinalizeNode(nodeDir, node); err != nil {
		t.Fatalf("second FinalizeNode (marker absent): %v", err)
	}

	if err := archive.VerifyNode(nodeDir); err != nil {
		t.Errorf("VerifyNode after second FinalizeNode: %v", err)
	}
}

func TestWriteNodeManifests_FetchError(t *testing.T) {
	nodeDir := setupNodeDir(t)

	want := errors.New("backend unavailable")
	src := &stubManifestSource{err: want}

	node := &source.Node{}

	err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, want) {
		t.Errorf("expected wrapped backend error, got: %v", err)
	}
}

// makeObjWithUID builds a minimal unstructured object with a UID.
func makeObjWithUID(apiVersion, kind, name string, uid types.UID) unstructured.Unstructured {
	obj := makeObj(apiVersion, kind, name)
	obj.SetUID(uid)

	return obj
}

// TestWriteNodeManifests_ExcludesLeafChildPVCs verifies that PVCs matching orphan
// leaf volume children (Children[i].IsVolumeLeaf() with status.data) are excluded from the
// aggregator node's manifests/ directory (they belong in each leaf node's own manifests/).
func TestWriteNodeManifests_ExcludesLeafChildPVCs(t *testing.T) {
	t.Parallel()

	nodeDir := setupNodeDir(t)

	// The checkpoint has two PVCs (leaf-child targets) and one ConfigMap.
	objs := []unstructured.Unstructured{
		makeObjWithUID("v1", "PersistentVolumeClaim", "pvc-disk", "uid-disk"),
		makeObjWithUID("v1", "PersistentVolumeClaim", "pvc-extra", "uid-extra"),
		makeObj("v1", "ConfigMap", "owner-cm"),
	}

	leafA := volumeLeaf("vs-disk", "pvc-disk", "uid-disk")
	leafB := volumeLeaf("vs-extra", "pvc-extra", "uid-extra")

	node := &source.Node{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-ex",
		Children:   []*source.Node{leafA, leafB},
	}

	src := &stubManifestSource{objs: objs}

	if err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node); err != nil {
		t.Fatalf("WriteNodeManifests: %v", err)
	}

	manifestsDir := filepath.Join(nodeDir, archive.ManifestsDirName)

	// Only the ConfigMap must be written; the two leaf-child PVCs must be excluded.
	cmPath := filepath.Join(manifestsDir, "configmap_owner-cm.yaml")
	if _, err := os.Stat(cmPath); err != nil {
		t.Errorf("expected configmap_owner-cm.yaml: %v", err)
	}

	for _, name := range []string{"persistentvolumeclaim_pvc-disk.yaml", "persistentvolumeclaim_pvc-extra.yaml"} {
		path := filepath.Join(manifestsDir, name)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("leaf-child PVC %s must not be written to aggregator manifests/; err=%v", name, err)
		}
	}
}

// TestWriteNodeManifests_ExcludesLeafChildByNameFallback verifies that a leaf child
// with no UID in its status.data sourceRef is still excluded by name.
func TestWriteNodeManifests_ExcludesLeafChildByNameFallback(t *testing.T) {
	t.Parallel()

	nodeDir := setupNodeDir(t)

	// PVC object has no UID in the captured manifest (uid field absent / empty).
	pvcNoUID := makeObj("v1", "PersistentVolumeClaim", "pvc-nouid")

	objs := []unstructured.Unstructured{
		pvcNoUID,
		makeObj("v1", "ConfigMap", "cm-keep"),
	}

	leaf := &source.Node{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "pvc-nouid",
		Data: &source.NodeData{
			SourceRef: source.SourceRefIdentity{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: "pvc-nouid"},
		},
	}

	node := &source.Node{
		Children: []*source.Node{leaf},
	}

	src := &stubManifestSource{objs: objs}

	if err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node); err != nil {
		t.Fatalf("WriteNodeManifests: %v", err)
	}

	manifestsDir := filepath.Join(nodeDir, archive.ManifestsDirName)

	if _, err := os.Stat(filepath.Join(manifestsDir, "configmap_cm-keep.yaml")); err != nil {
		t.Errorf("configmap_cm-keep.yaml must be written: %v", err)
	}

	if _, err := os.Stat(filepath.Join(manifestsDir, "persistentvolumeclaim_pvc-nouid.yaml")); !os.IsNotExist(err) {
		t.Errorf("leaf-child PVC with no UID must be excluded by name fallback; err=%v", err)
	}
}

// TestWriteNodeManifests_ExcludesOwnDataPVC verifies that a node's own captured volume PVC
// (status.data) is excluded from manifests/. For non-aggregator data-owning nodes the PVC
// data is captured in the volume payload (data.bin[.<ext>]/data.tar); the manifest must not
// appear alongside the domain object manifest.
func TestWriteNodeManifests_ExcludesOwnDataPVC(t *testing.T) {
	t.Parallel()

	nodeDir := setupNodeDir(t)

	// Checkpoint has a PVC matching the node's own status.data and one ConfigMap.
	objs := []unstructured.Unstructured{
		makeObjWithUID("v1", "PersistentVolumeClaim", "pvc-own", "uid-own"),
		makeObj("v1", "ConfigMap", "snap-cm"),
	}

	node := &source.Node{
		APIVersion: "demo.deckhouse.io/v1alpha1",
		Kind:       "VirtualDiskSnapshot",
		Name:       "vds-1",
		Data:       pvcNodeData("pvc-own", "uid-own", "vsc-own"),
	}

	src := &stubManifestSource{objs: objs}

	if err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node); err != nil {
		t.Fatalf("WriteNodeManifests: %v", err)
	}

	manifestsDir := filepath.Join(nodeDir, archive.ManifestsDirName)

	// The ConfigMap must be written; the own-data PVC must be excluded.
	if _, err := os.Stat(filepath.Join(manifestsDir, "configmap_snap-cm.yaml")); err != nil {
		t.Errorf("expected configmap_snap-cm.yaml to be written: %v", err)
	}

	if _, err := os.Stat(filepath.Join(manifestsDir, "persistentvolumeclaim_pvc-own.yaml")); !os.IsNotExist(err) {
		t.Errorf("own-data PVC must be excluded from node manifests/; err=%v", err)
	}
}

func TestWriteVolumeManifest_WritesMatchingPVC(t *testing.T) {
	t.Parallel()

	nodeDir := setupNodeDir(t)

	pvc := makeObjWithUID("v1", "PersistentVolumeClaim", "pvc-target", "uid-target")
	objs := []unstructured.Unstructured{
		makeObj("v1", "ConfigMap", "other-cm"),
		pvc,
	}

	volNode := &source.Node{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "d8-ss-aabbccdd",
		Data:       pvcNodeData("pvc-target", "uid-target", "vsc-1"),
	}

	src := &stubManifestSource{objs: objs}

	if err := volume.WriteVolumeManifest(context.Background(), src, nodeDir, volNode); err != nil {
		t.Fatalf("WriteVolumeManifest: %v", err)
	}

	manifestsDir := filepath.Join(nodeDir, archive.ManifestsDirName)

	// Only the matching PVC must be written.
	pvcPath := filepath.Join(manifestsDir, "persistentvolumeclaim_pvc-target.yaml")
	if _, err := os.Stat(pvcPath); err != nil {
		t.Errorf("expected persistentvolumeclaim_pvc-target.yaml: %v", err)
	}

	// The ConfigMap must NOT be written.
	cmPath := filepath.Join(manifestsDir, "configmap_other-cm.yaml")
	if _, err := os.Stat(cmPath); !os.IsNotExist(err) {
		t.Errorf("ConfigMap must not be written by WriteVolumeManifest; err=%v", err)
	}
}

func TestWriteVolumeManifest_MatchByNameFallback(t *testing.T) {
	t.Parallel()

	nodeDir := setupNodeDir(t)

	// PVC in checkpoint has no UID; binding has no TargetUID either.
	pvcNoUID := makeObj("v1", "PersistentVolumeClaim", "pvc-byname")
	objs := []unstructured.Unstructured{pvcNoUID}

	volNode := &source.Node{
		Data: &source.NodeData{
			SourceRef: source.SourceRefIdentity{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: "pvc-byname"},
		},
	}

	src := &stubManifestSource{objs: objs}

	if err := volume.WriteVolumeManifest(context.Background(), src, nodeDir, volNode); err != nil {
		t.Fatalf("WriteVolumeManifest by name: %v", err)
	}

	pvcPath := filepath.Join(nodeDir, archive.ManifestsDirName, "persistentvolumeclaim_pvc-byname.yaml")
	if _, err := os.Stat(pvcPath); err != nil {
		t.Errorf("expected persistentvolumeclaim_pvc-byname.yaml: %v", err)
	}
}

func TestWriteVolumeManifest_ErrorWhenPVCMissing(t *testing.T) {
	t.Parallel()

	nodeDir := setupNodeDir(t)

	// Checkpoint has only a ConfigMap, not the target PVC.
	objs := []unstructured.Unstructured{makeObj("v1", "ConfigMap", "stray-cm")}

	volNode := &source.Node{
		Data: pvcNodeData("pvc-missing", "uid-missing", "vsc-missing"),
	}

	src := &stubManifestSource{objs: objs}

	err := volume.WriteVolumeManifest(context.Background(), src, nodeDir, volNode)
	if err == nil {
		t.Fatal("expected error when target PVC is absent from checkpoint, got nil")
	}
}

func TestFinalizeNode_VolumeNodeWritesVolumeBlock(t *testing.T) {
	t.Parallel()

	nodeDir := setupNodeDir(t)

	// One manifest so the checksum is non-trivial.
	if err := archive.WriteManifest(nodeDir, makeObjWithUID("v1", "PersistentVolumeClaim", "my-pvc", "uid-abc")); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	data := &source.NodeData{
		SourceRef: source.SourceRefIdentity{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
			Name:       "my-pvc",
			Namespace:  "ns",
			UID:        "uid-abc",
		},
		ArtifactRef: source.ArtifactRef{
			APIVersion: "snapshot.storage.k8s.io/v1",
			Kind:       "VolumeSnapshotContent",
			Name:       "vsc-xyz",
		},
	}

	node := &source.Node{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "d8-ss-aabbccdd",
		Namespace:  "ns",
		UID:        "uid-vs-node",
		Data:       data,
	}

	if err := volume.FinalizeNode(nodeDir, node); err != nil {
		t.Fatalf("FinalizeNode: %v", err)
	}

	sy, err := archive.ReadSnapshotYAML(nodeDir)
	if err != nil {
		t.Fatalf("ReadSnapshotYAML: %v", err)
	}

	if len(sy.Volumes) != 1 {
		t.Fatalf("Volumes length: got %d, want 1 for a leaf volume node", len(sy.Volumes))
	}

	vol := sy.Volumes[0]

	if vol.Target.Name != "my-pvc" {
		t.Errorf("Volumes[0].Target.Name: got %q, want %q", vol.Target.Name, "my-pvc")
	}

	if vol.Target.UID != "uid-abc" {
		t.Errorf("Volumes[0].Target.UID: got %q, want %q", vol.Target.UID, "uid-abc")
	}

	if vol.Artifact.Name != "vsc-xyz" {
		t.Errorf("Volumes[0].Artifact.Name: got %q, want %q", vol.Artifact.Name, "vsc-xyz")
	}

	if vol.Artifact.Kind != "VolumeSnapshotContent" {
		t.Errorf("Volumes[0].Artifact.Kind: got %q, want %q", vol.Artifact.Kind, "VolumeSnapshotContent")
	}

	// VerifyNode must pass (Volume field does not affect the digest).
	if err := archive.VerifyNode(nodeDir); err != nil {
		t.Errorf("VerifyNode must pass for volume node: %v", err)
	}
}

func TestFinalizeNode_SnapshotNodeOmitsVolumeBlock(t *testing.T) {
	t.Parallel()

	nodeDir := setupNodeDir(t)

	if err := archive.WriteManifest(nodeDir, makeObj("v1", "ConfigMap", "cm")); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	node := &source.Node{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-1",
		Namespace:  "ns",
	}

	if err := volume.FinalizeNode(nodeDir, node); err != nil {
		t.Fatalf("FinalizeNode: %v", err)
	}

	sy, err := archive.ReadSnapshotYAML(nodeDir)
	if err != nil {
		t.Fatalf("ReadSnapshotYAML: %v", err)
	}

	if len(sy.Volumes) != 0 {
		t.Errorf("Volumes must be empty for a snapshot node without status.data, got %+v", sy.Volumes)
	}

	if err := archive.VerifyNode(nodeDir); err != nil {
		t.Errorf("VerifyNode must pass for snapshot node: %v", err)
	}
}

// TestFinalizeNode_VolumeBlockDoesNotAffectVerify is a regression test asserting that
// adding the volume block to snapshot.yaml does not invalidate the node checksum.
func TestFinalizeNode_VolumeBlockDoesNotAffectVerify(t *testing.T) {
	t.Parallel()

	nodeDir := setupNodeDir(t)

	if err := archive.WriteManifest(nodeDir, makeObjWithUID("v1", "PersistentVolumeClaim", "pvc", "uid-1")); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	data := &source.NodeData{
		SourceRef: source.SourceRefIdentity{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
			Name:       "pvc",
			Namespace:  "ns",
			UID:        "uid-1",
		},
		ArtifactRef: source.ArtifactRef{
			APIVersion: "snapshot.storage.k8s.io/v1",
			Kind:       "VolumeSnapshotContent",
			Name:       "vsc-1",
		},
	}

	node := &source.Node{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "d8-ss-reg-test",
		Namespace:  "ns",
		UID:        "uid-vs-reg",
		Data:       data,
	}

	// First finalize — writes Volume block.
	if err := volume.FinalizeNode(nodeDir, node); err != nil {
		t.Fatalf("FinalizeNode: %v", err)
	}

	// VerifyNode must pass without any changes to the content files.
	if err := archive.VerifyNode(nodeDir); err != nil {
		t.Errorf("VerifyNode regression: Volume block must not affect checksum: %v", err)
	}

	// Second finalize is idempotent.
	if err := volume.FinalizeNode(nodeDir, node); err != nil {
		t.Fatalf("second FinalizeNode: %v", err)
	}

	if err := archive.VerifyNode(nodeDir); err != nil {
		t.Errorf("VerifyNode after second FinalizeNode: %v", err)
	}
}

// TestFinalizeNode_SnapshotNodeWithOwnData verifies that a non-aggregator snapshot node
// with its own captured volume (status.data, Variant A ≤1) writes exactly one VolumeInfo
// into Volumes, carrying the captured volume metadata through to the archive.
func TestFinalizeNode_SnapshotNodeWithOwnData(t *testing.T) {
	t.Parallel()

	nodeDir := setupNodeDir(t)

	if err := archive.WriteManifest(nodeDir, makeObj("v1", "ConfigMap", "snap-cm")); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	node := &source.Node{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "VirtualDiskSnapshot",
		Name:       "snap-data",
		Namespace:  "ns",
		UID:        "uid-snap-data",
		Data: &source.NodeData{
			SourceRef: source.SourceRefIdentity{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-a",
				Namespace:  "ns",
				UID:        "uid-pvc-a",
			},
			ArtifactRef: source.ArtifactRef{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshotContent",
				Name:       "vsc-a",
			},
			VolumeMode:       "Block",
			StorageClassName: "linstor-thin-r1",
			Size:             "10Gi",
		},
	}

	if err := volume.FinalizeNode(nodeDir, node); err != nil {
		t.Fatalf("FinalizeNode: %v", err)
	}

	sy, err := archive.ReadSnapshotYAML(nodeDir)
	if err != nil {
		t.Fatalf("ReadSnapshotYAML: %v", err)
	}

	if len(sy.Volumes) != 1 {
		t.Fatalf("Volumes length: got %d, want 1", len(sy.Volumes))
	}

	if sy.Volumes[0].Target.Name != "pvc-a" {
		t.Errorf("Volumes[0].Target.Name: got %q, want pvc-a", sy.Volumes[0].Target.Name)
	}

	if sy.Volumes[0].Artifact.Name != "vsc-a" {
		t.Errorf("Volumes[0].Artifact.Name: got %q, want vsc-a", sy.Volumes[0].Artifact.Name)
	}

	// The captured volume metadata must be carried through to the archive so the import
	// side can rebuild the Mode A DataImport spec.
	if sy.Volumes[0].VolumeMode != "Block" || sy.Volumes[0].StorageClassName != "linstor-thin-r1" || sy.Volumes[0].Size != "10Gi" {
		t.Errorf("Volumes[0] metadata = {mode:%q, sc:%q, size:%q}, want {Block, linstor-thin-r1, 10Gi}",
			sy.Volumes[0].VolumeMode, sy.Volumes[0].StorageClassName, sy.Volumes[0].Size)
	}

	// VerifyNode must pass: Volumes does not affect the digest.
	if err := archive.VerifyNode(nodeDir); err != nil {
		t.Errorf("VerifyNode after FinalizeNode with own data: %v", err)
	}
}

// TestFinalizeNode_NoVolumesOmitted verifies that a purely manifest node (no status.data)
// produces a snapshot.yaml with Volumes omitted entirely.
func TestFinalizeNode_NoVolumesOmitted(t *testing.T) {
	t.Parallel()

	nodeDir := setupNodeDir(t)

	if err := archive.WriteManifest(nodeDir, makeObj("v1", "ConfigMap", "cm")); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	node := &source.Node{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-no-vol",
	}

	if err := volume.FinalizeNode(nodeDir, node); err != nil {
		t.Fatalf("FinalizeNode: %v", err)
	}

	sy, err := archive.ReadSnapshotYAML(nodeDir)
	if err != nil {
		t.Fatalf("ReadSnapshotYAML: %v", err)
	}

	if len(sy.Volumes) != 0 {
		t.Errorf("Volumes must be empty for a no-volume node, got %+v", sy.Volumes)
	}

	if err := archive.VerifyNode(nodeDir); err != nil {
		t.Errorf("VerifyNode: %v", err)
	}
}
