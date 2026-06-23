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

package snapimport

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

const targetNS = "dst"

var (
	snapshotGVR        = schema.GroupVersionResource{Group: "storage.deckhouse.io", Version: "v1alpha1", Resource: "snapshots"}
	contentGVR         = schema.GroupVersionResource{Group: "storage.deckhouse.io", Version: "v1alpha1", Resource: "snapshotcontents"}
	volumeSnapshotGVRt = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}
)

type uploadCall struct {
	ref  aggapi.NodeRef
	body uploadPayload
}

type stubUploader struct {
	calls []uploadCall
}

func (s *stubUploader) UploadManifests(_ context.Context, ref aggapi.NodeRef, body []byte) ([]byte, error) {
	var p uploadPayload
	_ = json.Unmarshal(body, &p)

	s.calls = append(s.calls, uploadCall{ref: ref, body: p})

	return []byte(`{"status":"Success"}`), nil
}

type stubVolumes struct {
	ensure []string
	upload []string
	// uploader, when set, lets EnsureDataImport snapshot how many manifest uploads had
	// completed at the moment the first data import started — used to assert the
	// manifests-before-data ordering that prevents the leaf-DataImport deadlock.
	uploader               *stubUploader
	manifestsAtFirstEnsure int
}

func (s *stubVolumes) DataImportName(leaf PlannedNode) string {
	return leaf.Name
}

func (s *stubVolumes) EnsureDataImport(_ context.Context, leaf PlannedNode, _ string) (string, error) {
	if len(s.ensure) == 0 && s.uploader != nil {
		s.manifestsAtFirstEnsure = len(s.uploader.calls)
	}

	s.ensure = append(s.ensure, leaf.Name)

	return leaf.Name, nil
}

func (s *stubVolumes) UploadVolumeData(_ context.Context, leaf PlannedNode, _, _ string) error {
	s.upload = append(s.upload, leaf.Name)

	return nil
}

func testMapper() meta.RESTMapper {
	m := meta.NewDefaultRESTMapper(nil)
	m.Add(schema.GroupVersionKind{Group: "storage.deckhouse.io", Version: "v1alpha1", Kind: "Snapshot"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Group: "snapshot.storage.k8s.io", Version: "v1", Kind: "VolumeSnapshot"}, meta.RESTScopeNamespace)

	return m
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func readyConditions(types ...string) []interface{} {
	conds := make([]interface{}, 0, len(types))
	for _, t := range types {
		conds = append(conds, map[string]interface{}{"type": t, "status": "True"})
	}

	return conds
}

func newFakeDynamic(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	gvrToListKind := map[schema.GroupVersionResource]string{
		snapshotGVR:        "SnapshotList",
		contentGVR:         "SnapshotContentList",
		volumeSnapshotGVRt: "VolumeSnapshotList",
	}

	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), gvrToListKind, objs...)
}

const rootSnapshotUID = "root-uid"

func readyRootSnapshot() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "Snapshot",
		"metadata":   map[string]interface{}{"namespace": targetNS, "name": "root", "uid": rootSnapshotUID},
		// An import-mode root that the controller has already materialized to Ready: it keeps
		// its spec.source.import marker, so ensureMarker reconciles (not rejects) it on re-run.
		"spec": map[string]interface{}{
			"source": map[string]interface{}{"import": map[string]interface{}{}},
		},
		"status": map[string]interface{}{
			"boundSnapshotContentName": "content-root",
			"conditions":               readyConditions("Ready"),
		},
	}}
}

func readyContent() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "SnapshotContent",
		"metadata":   map[string]interface{}{"name": "content-root"},
		"status": map[string]interface{}{
			"conditions": readyConditions("ManifestsReady", "VolumesReady", "ChildrenReady", "Ready"),
		},
	}}
}

func baseConfig(input string, up *stubUploader, vol *stubVolumes, dyn *dynamicfake.FakeDynamicClient) Config {
	return Config{
		Namespace:    targetNS,
		InputDir:     input,
		TTL:          "1h",
		Uploader:     up,
		Volumes:      vol,
		Dynamic:      dyn,
		Mapper:       testMapper(),
		Log:          discardLogger(),
		PollInterval: time.Millisecond,
		Timeout:      2 * time.Second,
	}
}

func TestRun_ImportsBottomUp(t *testing.T) {
	root := buildTwoLevelArchive(t)

	up := &stubUploader{}
	vol := &stubVolumes{}
	dyn := newFakeDynamic(readyRootSnapshot(), readyContent())

	if err := Run(context.Background(), baseConfig(root, up, vol, dyn)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(up.calls) != 2 {
		t.Fatalf("expected 2 manifest uploads, got %d", len(up.calls))
	}

	// Leaf uploaded before root.
	if up.calls[0].ref.Kind != "VolumeSnapshot" || up.calls[0].ref.Name != "pvc-1" {
		t.Errorf("first upload = %s/%s, want VolumeSnapshot/pvc-1", up.calls[0].ref.Kind, up.calls[0].ref.Name)
	}

	if up.calls[1].ref.Kind != "Snapshot" || up.calls[1].ref.Name != "root" {
		t.Errorf("second upload = %s/%s, want Snapshot/root", up.calls[1].ref.Kind, up.calls[1].ref.Name)
	}

	// Uploads target the import namespace.
	if up.calls[0].ref.Namespace != targetNS {
		t.Errorf("upload namespace = %q, want %q", up.calls[0].ref.Namespace, targetNS)
	}

	// Leaf has no childRefs; root references the leaf.
	if len(up.calls[0].body.ChildRefs) != 0 {
		t.Errorf("leaf childRefs = %v, want empty", up.calls[0].body.ChildRefs)
	}

	if len(up.calls[1].body.ChildRefs) != 1 || up.calls[1].body.ChildRefs[0].Name != "pvc-1" {
		t.Errorf("root childRefs = %v, want [pvc-1]", up.calls[1].body.ChildRefs)
	}

	// Volume data imported for the leaf.
	if len(vol.ensure) != 1 || vol.ensure[0] != "pvc-1" {
		t.Errorf("EnsureDataImport calls = %v, want [pvc-1]", vol.ensure)
	}

	if len(vol.upload) != 1 || vol.upload[0] != "pvc-1" {
		t.Errorf("UploadVolumeData calls = %v, want [pvc-1]", vol.upload)
	}

	// The leaf import-mode VolumeSnapshot CR was created.
	if _, err := dyn.Resource(volumeSnapshotGVRt).Namespace(targetNS).Get(context.Background(), "pvc-1", metav1.GetOptions{}); err != nil {
		t.Errorf("VolumeSnapshot import CR not created: %v", err)
	}
}

// TestRun_UploadsAllManifestsBeforeData locks in the manifests-before-data ordering: a data
// leaf's SVDM DataImport stays Pending until the leaf VolumeSnapshot has a bound
// SnapshotContent (which needs the parent content -> the parent's manifests upload), so
// importing leaf data before all ancestor manifests are uploaded would deadlock.
func TestRun_UploadsAllManifestsBeforeData(t *testing.T) {
	root := buildTwoLevelArchive(t)

	up := &stubUploader{}
	vol := &stubVolumes{uploader: up, manifestsAtFirstEnsure: -1}
	dyn := newFakeDynamic(readyRootSnapshot(), readyContent())

	if err := Run(context.Background(), baseConfig(root, up, vol, dyn)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if vol.manifestsAtFirstEnsure != len(up.calls) {
		t.Errorf("first data import started after %d/%d manifest uploads; all manifests must be uploaded before any volume data import",
			vol.manifestsAtFirstEnsure, len(up.calls))
	}
}

func TestRun_LeafCarriesParentOwnerRef(t *testing.T) {
	root := buildTwoLevelArchive(t)

	dyn := newFakeDynamic(readyRootSnapshot(), readyContent())

	if err := Run(context.Background(), baseConfig(root, &stubUploader{}, &stubVolumes{}, dyn)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	leaf, err := dyn.Resource(volumeSnapshotGVRt).Namespace(targetNS).Get(context.Background(), "pvc-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get leaf VolumeSnapshot: %v", err)
	}

	refs := leaf.GetOwnerReferences()
	if len(refs) != 1 {
		t.Fatalf("leaf ownerReferences = %d, want 1 (parent Snapshot)", len(refs))
	}

	ref := refs[0]
	if ref.Kind != "Snapshot" || ref.Name != "root" || ref.APIVersion != "storage.deckhouse.io/v1alpha1" {
		t.Errorf("leaf parent ownerRef = %s/%s (%s), want Snapshot/root (storage.deckhouse.io/v1alpha1)", ref.Kind, ref.Name, ref.APIVersion)
	}

	if ref.UID != rootSnapshotUID {
		t.Errorf("leaf parent ownerRef uid = %q, want %q (server-assigned parent UID)", ref.UID, rootSnapshotUID)
	}

	// A CSI VolumeSnapshot leaf is a visibility leaf, not a controller-owned child.
	if ref.Controller != nil && *ref.Controller {
		t.Errorf("leaf parent ownerRef should not be controller-owned")
	}
}

func TestRun_FilesystemDataFailsFast(t *testing.T) {
	root := t.TempDir()
	writeArchiveNode(t, root, archiveNode{
		apiVersion: "storage.deckhouse.io/v1alpha1",
		kind:       "Snapshot",
		name:       "root",
	})

	leaf := childDir(root, "VolumeSnapshot", "pvc-1")
	writeArchiveNode(t, leaf, archiveNode{
		apiVersion: "snapshot.storage.k8s.io/v1",
		kind:       "VolumeSnapshot",
		name:       "pvc-1",
	})

	if err := os.WriteFile(filepath.Join(leaf, archive.FsTarName), []byte("tar"), 0o600); err != nil {
		t.Fatalf("write data.tar: %v", err)
	}

	up := &stubUploader{}
	vol := &stubVolumes{}
	dyn := newFakeDynamic(readyRootSnapshot(), readyContent())

	err := Run(context.Background(), baseConfig(root, up, vol, dyn))
	if err == nil {
		t.Fatal("expected filesystem-data error, got nil")
	}

	if len(up.calls) != 0 || len(vol.ensure) != 0 {
		t.Errorf("no cluster mutations should happen on filesystem-data preflight failure: uploads=%d ensures=%d", len(up.calls), len(vol.ensure))
	}

	// The leaf import-mode CR must not have been created.
	if _, gErr := dyn.Resource(volumeSnapshotGVRt).Namespace(targetNS).Get(context.Background(), "pvc-1", metav1.GetOptions{}); gErr == nil {
		t.Error("VolumeSnapshot import CR should not be created when preflight fails")
	}
}

func TestRun_LeafWithoutBlockDataFailsFast(t *testing.T) {
	root := t.TempDir()
	writeArchiveNode(t, root, archiveNode{
		apiVersion: "storage.deckhouse.io/v1alpha1",
		kind:       "Snapshot",
		name:       "root",
	})

	leaf := childDir(root, "VolumeSnapshot", "pvc-1")
	writeArchiveNode(t, leaf, archiveNode{
		apiVersion: "snapshot.storage.k8s.io/v1",
		kind:       "VolumeSnapshot",
		name:       "pvc-1",
		// no blockData, no data.tar: an invalid data leaf.
	})

	up := &stubUploader{}
	vol := &stubVolumes{}

	err := Run(context.Background(), baseConfig(root, up, vol, newFakeDynamic(readyRootSnapshot(), readyContent())))
	if err == nil {
		t.Fatal("expected missing-block-data error, got nil")
	}

	if len(up.calls) != 0 || len(vol.ensure) != 0 {
		t.Errorf("no cluster mutations should happen on missing-block-data preflight failure: uploads=%d ensures=%d", len(up.calls), len(vol.ensure))
	}
}

func TestRun_LeafManifestsArrayShape(t *testing.T) {
	root := buildTwoLevelArchive(t)

	up := &stubUploader{}
	dyn := newFakeDynamic(readyRootSnapshot(), readyContent())

	if err := Run(context.Background(), baseConfig(root, up, &stubVolumes{}, dyn)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var items []map[string]interface{}
	if err := json.Unmarshal(up.calls[0].body.Manifests, &items); err != nil {
		t.Fatalf("manifests is not a JSON array: %v", err)
	}

	if len(items) != 1 || items[0]["kind"] != "PersistentVolumeClaim" {
		t.Errorf("leaf manifests = %v, want one PersistentVolumeClaim", items)
	}
}

func TestRun_UnsupportedNodeFailsFast(t *testing.T) {
	root := t.TempDir()
	writeArchiveNode(t, root, archiveNode{
		apiVersion: "storage.deckhouse.io/v1alpha1",
		kind:       "Snapshot",
		name:       "root",
	})

	demo := childDir(root, "DemoVirtualMachineSnapshot", "vm-1")
	writeArchiveNode(t, demo, archiveNode{
		apiVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1",
		kind:       "DemoVirtualMachineSnapshot",
		name:       "vm-1",
	})

	up := &stubUploader{}
	dyn := newFakeDynamic(readyRootSnapshot(), readyContent())

	err := Run(context.Background(), baseConfig(root, up, &stubVolumes{}, dyn))
	if err == nil {
		t.Fatal("expected unsupported-node error, got nil")
	}

	if len(up.calls) != 0 {
		t.Errorf("no uploads should happen when an unsupported node is present, got %d", len(up.calls))
	}
}

func TestRun_RootMustBeSnapshot(t *testing.T) {
	root := t.TempDir()
	writeArchiveNode(t, root, archiveNode{
		apiVersion: "snapshot.storage.k8s.io/v1",
		kind:       "VolumeSnapshot",
		name:       "pvc-1",
		blockData:  []byte("x"),
	})

	up := &stubUploader{}
	dyn := newFakeDynamic()

	if err := Run(context.Background(), baseConfig(root, up, &stubVolumes{}, dyn)); err == nil {
		t.Fatal("expected error when archive root is not a Snapshot, got nil")
	}
}

func captureModeRootSnapshot() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "Snapshot",
		"metadata":   map[string]interface{}{"namespace": targetNS, "name": "root", "uid": "capture-uid"},
		// A live capture-mode Snapshot (no import marker) that merely shares the import name.
		"spec": map[string]interface{}{
			"source": map[string]interface{}{"namespaceName": targetNS},
		},
	}}
}

func TestRun_RefusesNonImportModeExisting(t *testing.T) {
	root := buildTwoLevelArchive(t)

	up := &stubUploader{}
	vol := &stubVolumes{}
	// Pre-seed a capture-mode Snapshot/root: the importer must refuse to mutate it.
	dyn := newFakeDynamic(captureModeRootSnapshot())

	err := Run(context.Background(), baseConfig(root, up, vol, dyn))
	if err == nil {
		t.Fatal("expected refusal to mutate a non-import-mode object, got nil")
	}

	if len(up.calls) != 0 || len(vol.ensure) != 0 {
		t.Errorf("no manifests/data mutations should happen when an existing object is not import-mode: uploads=%d ensures=%d", len(up.calls), len(vol.ensure))
	}

	// The leaf import-mode CR must not have been created.
	if _, gErr := dyn.Resource(volumeSnapshotGVRt).Namespace(targetNS).Get(context.Background(), "pvc-1", metav1.GetOptions{}); gErr == nil {
		t.Error("leaf VolumeSnapshot import CR should not be created when the run aborts")
	}
}

func TestRun_Validation(t *testing.T) {
	root := buildTwoLevelArchive(t)

	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"no namespace", func(c *Config) { c.Namespace = "" }},
		{"no input", func(c *Config) { c.InputDir = "" }},
		{"no uploader", func(c *Config) { c.Uploader = nil }},
		{"no volumes", func(c *Config) { c.Volumes = nil }},
		{"no dynamic", func(c *Config) { c.Dynamic = nil }},
		{"no mapper", func(c *Config) { c.Mapper = nil }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig(root, &stubUploader{}, &stubVolumes{}, newFakeDynamic())
			tc.mutate(&cfg)

			if err := Run(context.Background(), cfg); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func ownerRef(name, uid string) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       name,
		UID:        types.UID(uid),
	}
}

func TestAddOwnerRefs_RefreshesStaleParentUID(t *testing.T) {
	obj := &unstructured.Unstructured{}
	obj.SetOwnerReferences([]metav1.OwnerReference{ownerRef("root", "old-uid")})

	changed := addOwnerRefs(obj, []metav1.OwnerReference{ownerRef("root", "new-uid")})
	if !changed {
		t.Fatal("addOwnerRefs should report a change when the parent UID drifted")
	}

	refs := obj.GetOwnerReferences()
	if len(refs) != 1 {
		t.Fatalf("ownerReferences = %d, want 1 (UID refreshed in place, not duplicated)", len(refs))
	}

	if refs[0].UID != "new-uid" {
		t.Errorf("ownerRef UID = %q, want new-uid (refreshed for retried import)", refs[0].UID)
	}
}

func TestAddOwnerRefs_NoOpWhenUnchanged(t *testing.T) {
	obj := &unstructured.Unstructured{}
	obj.SetOwnerReferences([]metav1.OwnerReference{ownerRef("root", "uid-1")})

	if addOwnerRefs(obj, []metav1.OwnerReference{ownerRef("root", "uid-1")}) {
		t.Error("addOwnerRefs should be a no-op when the desired ref already matches")
	}
}
