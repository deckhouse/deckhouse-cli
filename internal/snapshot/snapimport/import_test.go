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
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

func baseConfig(input string, up *stubUploader, vol VolumeImporter, dyn *dynamicfake.FakeDynamicClient) Config {
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

// buildThreeLevelArchive writes: root Snapshot -> domain child -> VS leaf with block data.
// Returns the root dir.
func buildThreeLevelArchive(t *testing.T) string {
	t.Helper()

	root := t.TempDir()

	writeArchiveNode(t, root, archiveNode{
		apiVersion: "storage.deckhouse.io/v1alpha1",
		kind:       "Snapshot",
		name:       "root",
		namespace:  "src",
	})

	domain := childDir(root, "DemoVirtualMachineSnapshot", "vm-1")
	writeArchiveNode(t, domain, archiveNode{
		apiVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1",
		kind:       "DemoVirtualMachineSnapshot",
		name:       "vm-1",
		namespace:  "src",
	})

	leaf := childDir(domain, "VolumeSnapshot", "pvc-1")
	writeArchiveNode(t, leaf, archiveNode{
		apiVersion: "snapshot.storage.k8s.io/v1",
		kind:       "VolumeSnapshot",
		name:       "pvc-1",
		namespace:  "src",
		blockData:  []byte("rawbytes"),
	})

	return root
}

// readyImportLeafVS returns a CSI VolumeSnapshot in import mode that the controller has
// already bound, so waitLeafReady can read its status.boundSnapshotContentName.
func readyImportLeafVS() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "snapshot.storage.k8s.io/v1",
		"kind":       "VolumeSnapshot",
		"metadata":   map[string]interface{}{"namespace": targetNS, "name": "pvc-1", "uid": "vs-uid"},
		"spec": map[string]interface{}{
			"source": map[string]interface{}{"dataImportName": "pvc-1"},
		},
		"status": map[string]interface{}{
			"boundSnapshotContentName": "content-leaf",
		},
	}}
}

// readyLeafContent returns a SnapshotContent (for a single data-leaf import) with all
// four readiness conditions True.
func readyLeafContent() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "SnapshotContent",
		"metadata":   map[string]interface{}{"name": "content-leaf"},
		"status": map[string]interface{}{
			"conditions": readyConditions("ManifestsReady", "VolumesReady", "ChildrenReady", "Ready"),
		},
	}}
}

// TestFilterPlanToSubtree_SelectLeaf verifies that filtering to a VolumeSnapshot leaf
// returns only that leaf in post-order.
func TestFilterPlanToSubtree_SelectLeaf(t *testing.T) {
	root := buildThreeLevelArchive(t)

	plan, err := BuildPlan(root)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	filtered, err := filterPlanToSubtree(plan, "VolumeSnapshot", "pvc-1")
	if err != nil {
		t.Fatalf("filterPlanToSubtree: %v", err)
	}

	if len(filtered) != 1 {
		t.Fatalf("expected 1 node in subtree, got %d", len(filtered))
	}

	if filtered[0].Kind != "VolumeSnapshot" || filtered[0].Name != "pvc-1" {
		t.Errorf("subtree node = %s/%s, want VolumeSnapshot/pvc-1", filtered[0].Kind, filtered[0].Name)
	}
}

// TestFilterPlanToSubtree_SelectDomainSubtree verifies that filtering to the domain node
// returns [VS leaf, domain node] in post-order (leaf first, domain last).
func TestFilterPlanToSubtree_SelectDomainSubtree(t *testing.T) {
	root := buildThreeLevelArchive(t)

	plan, err := BuildPlan(root)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	filtered, err := filterPlanToSubtree(plan, "DemoVirtualMachineSnapshot", "vm-1")
	if err != nil {
		t.Fatalf("filterPlanToSubtree: %v", err)
	}

	if len(filtered) != 2 {
		t.Fatalf("expected 2 nodes in subtree (leaf + domain), got %d", len(filtered))
	}

	// Post-order: leaf first, domain last.
	if filtered[0].Kind != "VolumeSnapshot" || filtered[0].Name != "pvc-1" {
		t.Errorf("first node = %s/%s, want VolumeSnapshot/pvc-1", filtered[0].Kind, filtered[0].Name)
	}

	if filtered[1].Kind != "DemoVirtualMachineSnapshot" || filtered[1].Name != "vm-1" {
		t.Errorf("last node = %s/%s, want DemoVirtualMachineSnapshot/vm-1", filtered[1].Kind, filtered[1].Name)
	}
}

// TestFilterPlanToSubtree_SelectRoot verifies that filtering to the root returns the
// entire plan unchanged.
func TestFilterPlanToSubtree_SelectRoot(t *testing.T) {
	root := buildThreeLevelArchive(t)

	plan, err := BuildPlan(root)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	filtered, err := filterPlanToSubtree(plan, "Snapshot", "root")
	if err != nil {
		t.Fatalf("filterPlanToSubtree: %v", err)
	}

	if len(filtered) != len(plan) {
		t.Errorf("selecting root should return full plan (%d nodes), got %d", len(plan), len(filtered))
	}
}

// TestFilterPlanToSubtree_NotFound verifies that a missing kind/name returns an error.
func TestFilterPlanToSubtree_NotFound(t *testing.T) {
	root := buildTwoLevelArchive(t)

	plan, err := BuildPlan(root)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	if _, err := filterPlanToSubtree(plan, "Snapshot", "nonexistent"); err == nil {
		t.Fatal("expected error for missing node, got nil")
	}
}

// TestRun_SelectedNode_AggregatorFails verifies that selecting a domain aggregator node
// as the import root fails fast before any cluster mutation.
func TestRun_SelectedNode_AggregatorFails(t *testing.T) {
	root := buildThreeLevelArchive(t)

	up := &stubUploader{}
	vol := &stubVolumes{}
	dyn := newFakeDynamic(readyRootSnapshot(), readyContent())

	cfg := baseConfig(root, up, vol, dyn)
	cfg.SelectedNodeKind = "DemoVirtualMachineSnapshot"
	cfg.SelectedNodeName = "vm-1"

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error selecting a domain aggregator, got nil")
	}

	if len(up.calls) != 0 || len(vol.ensure) != 0 {
		t.Errorf("no cluster mutations should occur on aggregator-selection error: uploads=%d ensures=%d", len(up.calls), len(vol.ensure))
	}
}

// TestRun_SelectedNode_SingleLeafWorks verifies that importing a single VolumeSnapshot
// leaf subtree succeeds: only that leaf is processed and waitLeafReady resolves the
// bound SnapshotContent to completion.
func TestRun_SelectedNode_SingleLeafWorks(t *testing.T) {
	// Single-node archive: the root directory IS the VS leaf.
	leafDir := t.TempDir()
	writeArchiveNode(t, leafDir, archiveNode{
		apiVersion: "snapshot.storage.k8s.io/v1",
		kind:       "VolumeSnapshot",
		name:       "pvc-1",
		namespace:  "src",
		blockData:  []byte("rawbytes"),
	})

	up := &stubUploader{}
	vol := &stubVolumes{}
	dyn := newFakeDynamic(readyImportLeafVS(), readyLeafContent())

	cfg := baseConfig(leafDir, up, vol, dyn)
	cfg.SelectedNodeKind = "VolumeSnapshot"
	cfg.SelectedNodeName = "pvc-1"

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with single leaf selected: %v", err)
	}

	// One manifest upload for the leaf.
	if len(up.calls) != 1 {
		t.Errorf("expected 1 manifest upload for single-leaf import, got %d", len(up.calls))
	}

	// Volume data imported for the leaf.
	if len(vol.ensure) != 1 || vol.ensure[0] != "pvc-1" {
		t.Errorf("EnsureDataImport calls = %v, want [pvc-1]", vol.ensure)
	}
}

// TestRun_SelectedNode_UnknownNodeFails verifies that selecting a node that does not
// exist in the archive returns an error.
func TestRun_SelectedNode_UnknownNodeFails(t *testing.T) {
	root := buildTwoLevelArchive(t)

	up := &stubUploader{}
	dyn := newFakeDynamic(readyRootSnapshot(), readyContent())

	cfg := baseConfig(root, up, &stubVolumes{}, dyn)
	cfg.SelectedNodeKind = "Snapshot"
	cfg.SelectedNodeName = "no-such-snapshot"

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for missing selected node, got nil")
	}

	if len(up.calls) != 0 {
		t.Errorf("no uploads should happen when selected node is not found, got %d", len(up.calls))
	}
}

// buildMultiLeafArchive creates a root Snapshot with n CSI VolumeSnapshot leaves, each
// carrying block data. Returns the root dir and the leaf names (leaf-0 … leaf-(n-1)).
func buildMultiLeafArchive(t *testing.T, n int) (string, []string) {
	t.Helper()

	root := t.TempDir()

	writeArchiveNode(t, root, archiveNode{
		apiVersion: "storage.deckhouse.io/v1alpha1",
		kind:       "Snapshot",
		name:       "root",
		namespace:  "src",
	})

	names := make([]string, 0, n)

	for i := range n {
		name := fmt.Sprintf("leaf-%d", i)
		names = append(names, name)

		leaf := childDir(root, "VolumeSnapshot", name)
		writeArchiveNode(t, leaf, archiveNode{
			apiVersion: "snapshot.storage.k8s.io/v1",
			kind:       "VolumeSnapshot",
			name:       name,
			namespace:  "src",
			blockData:  []byte("rawbytes"),
		})
	}

	return root, names
}

// concStubVolumes tracks the peak concurrency of UploadVolumeData calls so tests can
// assert that errgroup.SetLimit(Workers) is honoured.
type concStubVolumes struct {
	inflight atomic.Int64
	maxSeen  atomic.Int64
	mu       sync.Mutex
	imported []string
}

func (s *concStubVolumes) DataImportName(leaf PlannedNode) string { return leaf.Name }

func (s *concStubVolumes) EnsureDataImport(_ context.Context, leaf PlannedNode, _ string) (string, error) {
	return leaf.Name, nil
}

func (s *concStubVolumes) UploadVolumeData(ctx context.Context, leaf PlannedNode, _, _ string) error {
	cur := s.inflight.Add(1)
	defer s.inflight.Add(-1)

	// CAS loop to update max (race-safe without locking).
	for {
		prev := s.maxSeen.Load()
		if cur <= prev {
			break
		}

		if s.maxSeen.CompareAndSwap(prev, cur) {
			break
		}
	}

	// Brief pause so goroutines overlap and the peak is observable.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Millisecond):
	}

	s.mu.Lock()
	s.imported = append(s.imported, leaf.Name)
	s.mu.Unlock()

	return nil
}

// TestRun_Pass2b_ConcurrencyBounded verifies that pass 2b runs data-leaf volume uploads
// with at most cfg.Workers goroutines in flight at once.
func TestRun_Pass2b_ConcurrencyBounded(t *testing.T) {
	const numLeaves = 6
	const workers = 2

	root, _ := buildMultiLeafArchive(t, numLeaves)

	vol := &concStubVolumes{}
	up := &stubUploader{}
	dyn := newFakeDynamic(readyRootSnapshot(), readyContent())

	cfg := baseConfig(root, up, vol, dyn)
	cfg.Workers = workers

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	vol.mu.Lock()
	imported := len(vol.imported)
	vol.mu.Unlock()

	if imported != numLeaves {
		t.Errorf("expected %d leaves imported, got %d", numLeaves, imported)
	}

	max := vol.maxSeen.Load()
	if max > workers {
		t.Errorf("peak concurrency = %d, want <= %d (Workers limit not honoured)", max, workers)
	}
}

// errorOnceStubVolumes causes one named leaf to fail immediately; all other leaves block
// until ctx is cancelled, recording that they were properly cancelled by the errgroup.
type errorOnceStubVolumes struct {
	errorLeaf string
	mu        sync.Mutex
	cancelled []string
}

func (s *errorOnceStubVolumes) DataImportName(leaf PlannedNode) string { return leaf.Name }

func (s *errorOnceStubVolumes) EnsureDataImport(_ context.Context, leaf PlannedNode, _ string) (string, error) {
	return leaf.Name, nil
}

func (s *errorOnceStubVolumes) UploadVolumeData(ctx context.Context, leaf PlannedNode, _, _ string) error {
	if leaf.Name == s.errorLeaf {
		return fmt.Errorf("injected upload error for %s", leaf.Name)
	}

	// Block until errgroup cancels ctx when the failing leaf returns its error.
	<-ctx.Done()

	s.mu.Lock()
	s.cancelled = append(s.cancelled, leaf.Name)
	s.mu.Unlock()

	return ctx.Err()
}

// TestRun_Pass2b_ErrorCancelsSiblings verifies that when one data-leaf upload fails
// the errgroup cancels the derived ctx, unblocking sibling goroutines that are waiting.
func TestRun_Pass2b_ErrorCancelsSiblings(t *testing.T) {
	// Two leaves: leaf-0 errors immediately; leaf-1 blocks until ctx is cancelled.
	root, _ := buildMultiLeafArchive(t, 2)

	vol := &errorOnceStubVolumes{errorLeaf: "leaf-0"}
	up := &stubUploader{}
	dyn := newFakeDynamic(readyRootSnapshot(), readyContent())

	cfg := baseConfig(root, up, vol, dyn)
	cfg.Workers = 2 // both leaves start simultaneously

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error from failing leaf, got nil")
	}

	if !strings.Contains(err.Error(), "injected upload error") {
		t.Errorf("expected injected error message, got: %v", err)
	}

	// The sibling leaf must have been cancelled (not leaked or hung).
	vol.mu.Lock()
	numCancelled := len(vol.cancelled)
	vol.mu.Unlock()

	if numCancelled == 0 {
		t.Error("expected sibling leaf to be cancelled via errgroup context, got 0 cancelled")
	}
}

// warnCapture is a slog.Handler that collects Warn-or-above log messages for assertions.
type warnCapture struct {
	mu   sync.Mutex
	msgs []string
}

func (h *warnCapture) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *warnCapture) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		h.mu.Lock()
		h.msgs = append(h.msgs, r.Message)
		h.mu.Unlock()
	}

	return nil
}

func (h *warnCapture) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *warnCapture) WithGroup(_ string) slog.Handler { return h }

func (h *warnCapture) warnMessages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]string, len(h.msgs))
	copy(out, h.msgs)

	return out
}

// TestPreflightNamespace_CleanNamespace_Passes verifies that an empty target namespace
// produces no conflicts.
func TestPreflightNamespace_CleanNamespace_Passes(t *testing.T) {
	root := buildTwoLevelArchive(t)

	plan, err := BuildPlan(root)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	cfg := baseConfig(root, &stubUploader{}, &stubVolumes{}, newFakeDynamic())

	if err := preflightNamespace(context.Background(), cfg, plan); err != nil {
		t.Fatalf("preflightNamespace on clean namespace: %v", err)
	}
}

// TestPreflightNamespace_ImportModeMarker_NotConflict verifies that pre-existing
// import-mode markers from a prior run of the same import are never treated as conflicts.
func TestPreflightNamespace_ImportModeMarker_NotConflict(t *testing.T) {
	root := buildTwoLevelArchive(t)

	plan, err := BuildPlan(root)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	// Both planned nodes pre-exist as import-mode markers (a prior partial run).
	dyn := newFakeDynamic(readyRootSnapshot(), readyImportLeafVS())
	cfg := baseConfig(root, &stubUploader{}, &stubVolumes{}, dyn)

	if err := preflightNamespace(context.Background(), cfg, plan); err != nil {
		t.Fatalf("preflightNamespace with import-mode markers: %v", err)
	}
}

// TestPreflightNamespace_CaptureMode_Conflict verifies that a capture-mode object
// sharing a planned node's name is reported as a conflict.
func TestPreflightNamespace_CaptureMode_Conflict(t *testing.T) {
	root := buildTwoLevelArchive(t)

	plan, err := BuildPlan(root)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	// A live capture-mode Snapshot "root" exists in the target namespace.
	dyn := newFakeDynamic(captureModeRootSnapshot())
	cfg := baseConfig(root, &stubUploader{}, &stubVolumes{}, dyn)

	err = preflightNamespace(context.Background(), cfg, plan)
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}

	if !strings.Contains(err.Error(), "root") {
		t.Errorf("expected conflict error to name the object %q, got: %v", "root", err)
	}

	if !strings.Contains(err.Error(), "allow-existing") {
		t.Errorf("expected conflict error to mention --allow-existing, got: %v", err)
	}
}

// TestRun_NsPreflightAbortsBeforeMarkers verifies that a capture-mode conflict detected
// by preflightNamespace aborts the run before any cluster mutation (createMarkers).
func TestRun_NsPreflightAbortsBeforeMarkers(t *testing.T) {
	root := buildTwoLevelArchive(t)

	up := &stubUploader{}
	vol := &stubVolumes{}
	// Capture-mode Snapshot "root" pre-exists; preflightNamespace must abort before markers.
	dyn := newFakeDynamic(captureModeRootSnapshot())

	err := Run(context.Background(), baseConfig(root, up, vol, dyn))
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}

	if !strings.Contains(err.Error(), "allow-existing") {
		t.Errorf("expected preflight error to mention --allow-existing, got: %v", err)
	}

	if len(up.calls) != 0 || len(vol.ensure) != 0 {
		t.Errorf("no cluster mutations should happen before preflight: uploads=%d ensures=%d", len(up.calls), len(vol.ensure))
	}

	// The leaf VolumeSnapshot marker must not have been created (createMarkers never called).
	if _, gErr := dyn.Resource(volumeSnapshotGVRt).Namespace(targetNS).Get(context.Background(), "pvc-1", metav1.GetOptions{}); gErr == nil {
		t.Error("VolumeSnapshot import CR should not be created when namespace preflight fails")
	}
}

// TestRun_AllowExisting_ProceedsWithWarning verifies that --allow-existing downgrades
// the preflight conflict check to a warning; the run proceeds past preflightNamespace.
// The per-object reconcileExistingMarker protection is unaffected, so the run may still
// fail at createMarkers — confirming that preflightNamespace did NOT abort it.
func TestRun_AllowExisting_ProceedsWithWarning(t *testing.T) {
	root := buildTwoLevelArchive(t)

	up := &stubUploader{}
	vol := &stubVolumes{}
	// Capture-mode Snapshot "root" pre-exists. AllowExisting=true → preflight warns, not errors.
	// createMarkers subsequently hits reconcileExistingMarker which still refuses non-import-mode.
	dyn := newFakeDynamic(captureModeRootSnapshot())

	lh := &warnCapture{}
	cfg := baseConfig(root, up, vol, dyn)
	cfg.AllowExisting = true
	cfg.Log = slog.New(lh)

	err := Run(context.Background(), cfg)

	// Error must come from createMarkers (not from preflightNamespace).
	if err == nil {
		t.Fatal("expected error from createMarkers, got nil")
	}

	if strings.Contains(err.Error(), "allow-existing") {
		t.Errorf("error should NOT be the preflight conflict error (run should have passed preflight): %v", err)
	}

	if !strings.Contains(err.Error(), "refusing to mutate") {
		t.Errorf("expected 'refusing to mutate' error from reconcileExistingMarker, got: %v", err)
	}

	// A warning must have been emitted by preflightNamespace.
	warns := lh.warnMessages()
	if len(warns) == 0 {
		t.Error("expected at least one warning log from preflightNamespace, got none")
	}
}
