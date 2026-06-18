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

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
)

// stubManifestSource is a test-only ManifestSource that returns a pre-seeded list.
type stubManifestSource struct {
	objs []unstructured.Unstructured
	err  error
}

func (s *stubManifestSource) FetchNodeManifests(_ context.Context, _ string) ([]unstructured.Unstructured, error) {
	return s.objs, s.err
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
		APIVersion:             "storage.deckhouse.io/v1alpha1",
		Kind:                   "Snapshot",
		Name:                   "snap-1",
		Namespace:              "default",
		ManifestCheckpointName: "mc-1",
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

	node := &source.Node{ManifestCheckpointName: "mc-collision"}
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

func TestWriteNodeManifests_EmptyCheckpointName(t *testing.T) {
	nodeDir := setupNodeDir(t)

	node := &source.Node{ManifestCheckpointName: ""}
	src := &stubManifestSource{objs: []unstructured.Unstructured{makeObj("v1", "Secret", "s")}}

	if err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node); err != nil {
		t.Fatalf("WriteNodeManifests with empty checkpoint: %v", err)
	}

	// No files should be written because checkpoint name is empty.
	entries, err := os.ReadDir(filepath.Join(nodeDir, archive.ManifestsDirName))
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 0 {
		t.Errorf("expected no files written for empty checkpoint, got %d", len(entries))
	}
}

func TestWriteNodeManifests_Idempotent(t *testing.T) {
	nodeDir := setupNodeDir(t)

	objs := []unstructured.Unstructured{makeObj("v1", "ConfigMap", "cm1")}
	node := &source.Node{ManifestCheckpointName: "mc-idem"}
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
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-test",
		Namespace:  "ns1",
		SourceRef:  "demo/vm-1",
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

	if sy.SourceRef != node.SourceRef {
		t.Errorf("SourceRef: got %q, want %q", sy.SourceRef, node.SourceRef)
	}
}

func TestFinalizeNode_Idempotent(t *testing.T) {
	nodeDir := setupNodeDir(t)

	obj := makeObj("v1", "ConfigMap", "cm2")

	if err := archive.WriteManifest(nodeDir, obj); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	node := &source.Node{
		APIVersion: "storage.deckhouse.io/v1alpha1",
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

func TestWriteNodeManifests_FetchError(t *testing.T) {
	nodeDir := setupNodeDir(t)

	want := errors.New("backend unavailable")
	src := &stubManifestSource{err: want}

	node := &source.Node{ManifestCheckpointName: "mc-err"}

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

func TestWriteNodeManifests_ExcludesDataRefPVCs(t *testing.T) {
	t.Parallel()

	nodeDir := setupNodeDir(t)

	// The checkpoint has two PVCs (both DataRef targets) and one ConfigMap.
	objs := []unstructured.Unstructured{
		makeObjWithUID("v1", "PersistentVolumeClaim", "pvc-disk", "uid-disk"),
		makeObjWithUID("v1", "PersistentVolumeClaim", "pvc-extra", "uid-extra"),
		makeObj("v1", "ConfigMap", "owner-cm"),
	}

	node := &source.Node{
		APIVersion:             "storage.deckhouse.io/v1alpha1",
		Kind:                   "Snapshot",
		Name:                   "snap-ex",
		ManifestCheckpointName: "mc-ex",
		OwnDataRefs: []snapshotapi.SnapshotDataBinding{
			{TargetUID: "uid-disk", Target: snapshotapi.SnapshotSubjectRef{Name: "pvc-disk"}},
			{TargetUID: "uid-extra", Target: snapshotapi.SnapshotSubjectRef{Name: "pvc-extra"}},
		},
	}

	src := &stubManifestSource{objs: objs}

	if err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node); err != nil {
		t.Fatalf("WriteNodeManifests: %v", err)
	}

	manifestsDir := filepath.Join(nodeDir, archive.ManifestsDirName)

	// Only the ConfigMap must be written; the two DataRef PVCs must be excluded.
	cmPath := filepath.Join(manifestsDir, "configmap_owner-cm.yaml")
	if _, err := os.Stat(cmPath); err != nil {
		t.Errorf("expected configmap_owner-cm.yaml: %v", err)
	}

	for _, name := range []string{"persistentvolumeclaim_pvc-disk.yaml", "persistentvolumeclaim_pvc-extra.yaml"} {
		path := filepath.Join(manifestsDir, name)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("DataRef PVC %s must not be written; err=%v", name, err)
		}
	}
}

func TestWriteNodeManifests_ExcludesByNameFallback(t *testing.T) {
	t.Parallel()

	nodeDir := setupNodeDir(t)

	// PVC object has no UID in the captured manifest (uid field absent / empty).
	pvcNoUID := makeObj("v1", "PersistentVolumeClaim", "pvc-nouid")

	objs := []unstructured.Unstructured{
		pvcNoUID,
		makeObj("v1", "ConfigMap", "cm-keep"),
	}

	node := &source.Node{
		ManifestCheckpointName: "mc-fallback",
		OwnDataRefs: []snapshotapi.SnapshotDataBinding{
			{TargetUID: "", Target: snapshotapi.SnapshotSubjectRef{Name: "pvc-nouid"}},
		},
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
		t.Errorf("DataRef PVC with no UID must still be excluded by name fallback; err=%v", err)
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

	binding := snapshotapi.SnapshotDataBinding{
		TargetUID: "uid-target",
		Target:    snapshotapi.SnapshotSubjectRef{Name: "pvc-target"},
		Artifact:  snapshotapi.SnapshotDataArtifactRef{Name: "vsc-1"},
	}

	volNode := &source.Node{
		APIVersion:             "snapshot.storage.k8s.io/v1",
		Kind:                   "VolumeSnapshot",
		Name:                   "d8-ss-aabbccdd",
		ManifestCheckpointName: "mc-parent",
		Binding:                &binding,
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

	binding := snapshotapi.SnapshotDataBinding{
		TargetUID: "",
		Target:    snapshotapi.SnapshotSubjectRef{Name: "pvc-byname"},
	}

	volNode := &source.Node{
		ManifestCheckpointName: "mc-byname",
		Binding:                &binding,
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

	binding := snapshotapi.SnapshotDataBinding{
		TargetUID: "uid-missing",
		Target:    snapshotapi.SnapshotSubjectRef{Name: "pvc-missing"},
	}

	volNode := &source.Node{
		ManifestCheckpointName: "mc-missing",
		Binding:                &binding,
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

	binding := &snapshotapi.SnapshotDataBinding{
		TargetUID: "uid-abc",
		Target: snapshotapi.SnapshotSubjectRef{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
			Name:       "my-pvc",
			Namespace:  "ns",
			UID:        "uid-abc",
		},
		Artifact: snapshotapi.SnapshotDataArtifactRef{
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
		SourceRef:  "uid-abc",
		Binding:    binding,
	}

	if err := volume.FinalizeNode(nodeDir, node); err != nil {
		t.Fatalf("FinalizeNode: %v", err)
	}

	sy, err := archive.ReadSnapshotYAML(nodeDir)
	if err != nil {
		t.Fatalf("ReadSnapshotYAML: %v", err)
	}

	if sy.Volume == nil {
		t.Fatal("Volume block must be present for a volume node")
	}

	if sy.Volume.Target.Name != "my-pvc" {
		t.Errorf("Volume.Target.Name: got %q, want %q", sy.Volume.Target.Name, "my-pvc")
	}

	if sy.Volume.Target.UID != "uid-abc" {
		t.Errorf("Volume.Target.UID: got %q, want %q", sy.Volume.Target.UID, "uid-abc")
	}

	if sy.Volume.Artifact.Name != "vsc-xyz" {
		t.Errorf("Volume.Artifact.Name: got %q, want %q", sy.Volume.Artifact.Name, "vsc-xyz")
	}

	if sy.Volume.Artifact.Kind != "VolumeSnapshotContent" {
		t.Errorf("Volume.Artifact.Kind: got %q, want %q", sy.Volume.Artifact.Kind, "VolumeSnapshotContent")
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
		APIVersion: "storage.deckhouse.io/v1alpha1",
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

	if sy.Volume != nil {
		t.Errorf("Volume block must be nil for a snapshot node, got %+v", sy.Volume)
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

	binding := &snapshotapi.SnapshotDataBinding{
		TargetUID: "uid-1",
		Target: snapshotapi.SnapshotSubjectRef{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
			Name:       "pvc",
			Namespace:  "ns",
			UID:        "uid-1",
		},
		Artifact: snapshotapi.SnapshotDataArtifactRef{
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
		Binding:    binding,
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

func TestWriteVolumeManifest_EmptyCheckpointName(t *testing.T) {
	t.Parallel()

	nodeDir := setupNodeDir(t)

	// Even though the source has objects, an empty checkpoint name is a no-op.
	objs := []unstructured.Unstructured{makeObj("v1", "PersistentVolumeClaim", "pvc-1")}

	binding := snapshotapi.SnapshotDataBinding{
		TargetUID: "uid-1",
		Target:    snapshotapi.SnapshotSubjectRef{Name: "pvc-1"},
	}

	volNode := &source.Node{
		ManifestCheckpointName: "",
		Binding:                &binding,
	}

	src := &stubManifestSource{objs: objs}

	if err := volume.WriteVolumeManifest(context.Background(), src, nodeDir, volNode); err != nil {
		t.Fatalf("WriteVolumeManifest with empty checkpoint: %v", err)
	}

	entries, err := os.ReadDir(filepath.Join(nodeDir, archive.ManifestsDirName))
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 0 {
		t.Errorf("expected no files written for empty checkpoint, got %d", len(entries))
	}
}
