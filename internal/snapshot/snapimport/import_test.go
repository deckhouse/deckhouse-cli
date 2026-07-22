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
	clienttesting "k8s.io/client-go/testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

const targetNS = "dst"

var (
	snapshotGVR        = schema.GroupVersionResource{Group: "state-snapshotter.deckhouse.io", Version: "v1alpha1", Resource: "snapshots"}
	volumeSnapshotGVRt = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}
	demoDiskSnapGVR    = schema.GroupVersionResource{Group: "sds-unified-snapshots-poc.deckhouse.io", Version: "v1alpha1", Resource: "demovirtualdisksnapshots"}
	demoVMSnapGVR      = schema.GroupVersionResource{Group: "sds-unified-snapshots-poc.deckhouse.io", Version: "v1alpha1", Resource: "demovirtualmachinesnapshots"}
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
	mu     sync.Mutex
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
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.ensure) == 0 && s.uploader != nil {
		s.manifestsAtFirstEnsure = len(s.uploader.calls)
	}

	s.ensure = append(s.ensure, leaf.Name)

	return leaf.Name, nil
}

func (s *stubVolumes) UploadVolumeData(_ context.Context, leaf PlannedNode, _, _ string, _ func(int64), _ func(int), _ func()) error {
	s.mu.Lock()
	s.upload = append(s.upload, leaf.Name)
	s.mu.Unlock()

	return nil
}

func testMapper() meta.RESTMapper {
	m := meta.NewDefaultRESTMapper(nil)
	m.Add(schema.GroupVersionKind{Group: "state-snapshotter.deckhouse.io", Version: "v1alpha1", Kind: "Snapshot"}, meta.RESTScopeNamespace)
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

// newFakeDynamicRaw builds a fake dynamic client with no reactors: stored objects are returned
// verbatim, so a node without status.boundSnapshotContentName stays unbound. Used by the bind-gate
// tests that must observe an unbound node (timeout / partial-bind).
func newFakeDynamicRaw(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	gvrToListKind := map[schema.GroupVersionResource]string{
		snapshotGVR:        "SnapshotList",
		volumeSnapshotGVRt: "VolumeSnapshotList",
		demoDiskSnapGVR:    "DemoVirtualDiskSnapshotList",
		demoVMSnapGVR:      "DemoVirtualMachineSnapshotList",
	}

	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), gvrToListKind, objs...)
}

// newFakeDynamic builds a fake dynamic client that simulates the state-snapshotter binder:
// every Get stamps a non-empty status.boundSnapshotContentName on any node that lacks one, so
// Run's waitForBinds gate observes each planned node as bound — as it would in a real cluster
// once the binder cascades SnapshotContents from the import-mode markers. Tests that need a node
// to stay unbound use newFakeDynamicRaw instead.
func newFakeDynamic(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	dyn := newFakeDynamicRaw(objs...)
	stampBoundContentOnGet(dyn)

	return dyn
}

// stampBoundContentOnGet installs a get reactor returning a copy of the stored object with a
// non-empty status.boundSnapshotContentName when it is missing, mirroring the binder having bound
// the node's SnapshotContent. It is purely additive (never touches spec, conditions, or ownerRefs)
// and does not persist to the tracker, so it is invisible to every assertion except the bind gate.
func stampBoundContentOnGet(dyn *dynamicfake.FakeDynamicClient) {
	dyn.PrependReactor("get", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		ga, ok := action.(clienttesting.GetAction)
		if !ok {
			return false, nil, nil
		}

		obj, err := dyn.Tracker().Get(ga.GetResource(), ga.GetNamespace(), ga.GetName())
		if err != nil {
			return true, nil, err
		}

		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return false, nil, nil
		}

		out := u.DeepCopy()
		if boundContentName(out) == "" {
			_ = unstructured.SetNestedField(out.Object, out.GetName()+"-content", "status", "boundSnapshotContentName")
		}

		return true, out, nil
	})
}

// testDomainMapper extends testMapper with the DemoVirtualDiskSnapshot GVK so tests that
// drive domain data leaves can resolve their resource plural via the RESTMapper.
func testDomainMapper() meta.RESTMapper {
	m := meta.NewDefaultRESTMapper(nil)
	m.Add(schema.GroupVersionKind{Group: "state-snapshotter.deckhouse.io", Version: "v1alpha1", Kind: "Snapshot"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Group: "snapshot.storage.k8s.io", Version: "v1", Kind: "VolumeSnapshot"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Group: "sds-unified-snapshots-poc.deckhouse.io", Version: "v1alpha1", Kind: "DemoVirtualDiskSnapshot"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Group: "sds-unified-snapshots-poc.deckhouse.io", Version: "v1alpha1", Kind: "DemoVirtualMachineSnapshot"}, meta.RESTScopeNamespace)

	return m
}

const rootSnapshotUID = "root-uid"

func readyRootSnapshot() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "state-snapshotter.deckhouse.io/v1alpha1",
		"kind":       "Snapshot",
		"metadata":   map[string]interface{}{"namespace": targetNS, "name": "root", "uid": rootSnapshotUID},
		// An import-mode root that the controller has already materialized to Ready: it keeps
		// its spec.mode: Import marker, so ensureMarker reconciles (not rejects) it on re-run.
		"spec": map[string]interface{}{
			"mode": "Import",
		},
		"status": map[string]interface{}{
			"conditions": readyConditions("Ready"),
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
	dyn := newFakeDynamic(readyRootSnapshot())

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
	dyn := newFakeDynamic(readyRootSnapshot())

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

	dyn := newFakeDynamic(readyRootSnapshot())

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
	if ref.Kind != "Snapshot" || ref.Name != "root" || ref.APIVersion != "state-snapshotter.deckhouse.io/v1alpha1" {
		t.Errorf("leaf parent ownerRef = %s/%s (%s), want Snapshot/root (state-snapshotter.deckhouse.io/v1alpha1)", ref.Kind, ref.Name, ref.APIVersion)
	}

	if ref.UID != rootSnapshotUID {
		t.Errorf("leaf parent ownerRef uid = %q, want %q (server-assigned parent UID)", ref.UID, rootSnapshotUID)
	}

	// A CSI VolumeSnapshot leaf is a visibility leaf, not a controller-owned child.
	if ref.Controller != nil && *ref.Controller {
		t.Errorf("leaf parent ownerRef should not be controller-owned")
	}
}

// TestPreflight_FilesystemDataPasses verifies that a VolumeSnapshot data leaf carrying
// filesystem-volume data (data.tar) now passes preflight and allows Run to succeed.
// The companion TestRun_LeafWithoutBlockDataFailsFast covers the case where a leaf has
// neither block nor filesystem data (that must still be rejected).
func TestPreflight_FilesystemDataPasses(t *testing.T) {
	root := t.TempDir()
	writeArchiveNode(t, root, archiveNode{
		apiVersion: "state-snapshotter.deckhouse.io/v1alpha1",
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
	dyn := newFakeDynamic(readyRootSnapshot())

	if err := Run(context.Background(), baseConfig(root, up, vol, dyn)); err != nil {
		t.Fatalf("filesystem-data leaf must pass preflight and allow Run to succeed, got: %v", err)
	}

	// Manifests uploaded and volume import triggered for the FS leaf.
	if len(up.calls) == 0 {
		t.Error("expected manifest uploads, got none")
	}

	if len(vol.ensure) == 0 || vol.ensure[0] != "pvc-1" {
		t.Errorf("EnsureDataImport calls = %v, want [pvc-1]", vol.ensure)
	}

	// The leaf import-mode VolumeSnapshot CR must have been created.
	if _, gErr := dyn.Resource(volumeSnapshotGVRt).Namespace(targetNS).Get(context.Background(), "pvc-1", metav1.GetOptions{}); gErr != nil {
		t.Errorf("VolumeSnapshot import CR not created: %v", gErr)
	}
}

func TestRun_LeafWithoutBlockDataFailsFast(t *testing.T) {
	root := t.TempDir()
	writeArchiveNode(t, root, archiveNode{
		apiVersion: "state-snapshotter.deckhouse.io/v1alpha1",
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

	err := Run(context.Background(), baseConfig(root, up, vol, newFakeDynamic(readyRootSnapshot())))
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
	dyn := newFakeDynamic(readyRootSnapshot())

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

// TestNodeDisplayLabel verifies nodeDisplayLabel's fallback contract: it prefers the
// original captured source object's identity (SourceObjectRef.Kind/Name) when the
// archive recorded one, and falls back to the snapshot CR's own Kind/Name otherwise
// (core Snapshot nodes and CSI VolumeSnapshot data leaves never carry a
// SourceObjectRef — see archive.SnapshotYAML.SourceObjectRef's doc comment).
func TestNodeDisplayLabel(t *testing.T) {
	cases := []struct {
		name string
		node PlannedNode
		want string
	}{
		{
			name: "prefers_source_object_ref",
			node: PlannedNode{
				Kind: "DemoVirtualDiskSnapshot", Name: "dvd-snap-1",
				SourceObjectRef: &archive.SourceObjectRef{Kind: "DemoVirtualDisk", Name: "disk-a"},
			},
			want: "DemoVirtualDisk/disk-a",
		},
		{
			name: "falls_back_when_nil_source_object_ref",
			node: PlannedNode{Kind: "VolumeSnapshot", Name: "nss-vs-agg-pvc"},
			want: "VolumeSnapshot/nss-vs-agg-pvc",
		},
		{
			name: "falls_back_when_source_object_ref_name_empty",
			node: PlannedNode{
				Kind: "Snapshot", Name: "root",
				SourceObjectRef: &archive.SourceObjectRef{Kind: "Namespace"},
			},
			want: "Snapshot/root",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := nodeDisplayLabel(tc.node); got != tc.want {
				t.Errorf("nodeDisplayLabel(%+v) = %q, want %q", tc.node, got, tc.want)
			}
		})
	}
}

// buildDomainDataLeafArchive creates: root Snapshot → DemoVirtualDiskSnapshot with block
// data and a SourceObjectRef. Returns the root dir.
func buildDomainDataLeafArchive(t *testing.T) string {
	t.Helper()

	root := t.TempDir()

	writeArchiveNode(t, root, archiveNode{
		apiVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		kind:       "Snapshot",
		name:       "root",
		namespace:  "src",
	})

	leafDir := childDir(root, "DemoVirtualDiskSnapshot", "dvd-snap-1")
	writeArchiveNode(t, leafDir, archiveNode{
		apiVersion: "sds-unified-snapshots-poc.deckhouse.io/v1alpha1",
		kind:       "DemoVirtualDiskSnapshot",
		name:       "dvd-snap-1",
		namespace:  "src",
		blockData:  []byte("rawbytes"),
		sourceObjectRef: &archive.SourceObjectRef{
			APIVersion: "demo.deckhouse.io/v1alpha1",
			Kind:       "DemoVirtualDisk",
			Name:       "disk-a",
		},
	})

	return root
}

// TestRun_DomainDataLeaf_EndToEnd verifies that a root Snapshot → domain data leaf
// (DemoVirtualDiskSnapshot with block data + SourceObjectRef) flows through the full
// import pipeline: markers are created top-down (root first, then the domain leaf),
// manifests are uploaded for both nodes, and the domain leaf's volume data is imported.
func TestRun_DomainDataLeaf_EndToEnd(t *testing.T) {
	root := buildDomainDataLeafArchive(t)

	up := &stubUploader{}
	vol := &stubVolumes{}
	dyn := newFakeDynamic(readyRootSnapshot())

	cfg := baseConfig(root, up, vol, dyn)
	cfg.Mapper = testDomainMapper()

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Two manifest uploads: domain leaf (post-order = leaf first) then root.
	if len(up.calls) != 2 {
		t.Fatalf("expected 2 manifest uploads, got %d", len(up.calls))
	}

	if up.calls[0].ref.Kind != "DemoVirtualDiskSnapshot" || up.calls[0].ref.Name != "dvd-snap-1" {
		t.Errorf("first upload = %s/%s, want DemoVirtualDiskSnapshot/dvd-snap-1", up.calls[0].ref.Kind, up.calls[0].ref.Name)
	}

	if up.calls[1].ref.Kind != "Snapshot" || up.calls[1].ref.Name != "root" {
		t.Errorf("second upload = %s/%s, want Snapshot/root", up.calls[1].ref.Kind, up.calls[1].ref.Name)
	}

	// Volume data imported for the domain leaf (importNodeData handles isDomainDataLeaf).
	if len(vol.ensure) != 1 || vol.ensure[0] != "dvd-snap-1" {
		t.Errorf("EnsureDataImport calls = %v, want [dvd-snap-1]", vol.ensure)
	}

	if len(vol.upload) != 1 || vol.upload[0] != "dvd-snap-1" {
		t.Errorf("UploadVolumeData calls = %v, want [dvd-snap-1]", vol.upload)
	}

	// The domain leaf import-mode CR must have been created with the unified import marker.
	leafObj, err := dyn.Resource(demoDiskSnapGVR).Namespace(targetNS).Get(context.Background(), "dvd-snap-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("DemoVirtualDiskSnapshot import CR not created: %v", err)
	}

	if mode, _, _ := unstructured.NestedString(leafObj.Object, "spec", "mode"); mode != "Import" {
		t.Errorf("domain leaf marker must set spec.mode: Import, got %q", mode)
	}

	// The domain leaf carries a child->parent ownerRef pointing to the root Snapshot.
	refs := leafObj.GetOwnerReferences()
	if len(refs) != 1 {
		t.Fatalf("domain leaf ownerReferences = %d, want 1 (parent Snapshot)", len(refs))
	}

	ref := refs[0]
	if ref.Kind != "Snapshot" || ref.Name != "root" {
		t.Errorf("domain leaf parent ownerRef = %s/%s, want Snapshot/root", ref.Kind, ref.Name)
	}

	// A domain data leaf is a controller-owned child (unlike CSI VS leaves which are not).
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("domain data leaf parent ownerRef should be controller-owned")
	}
}

// TestRun_ManifestOnlyDomainNode_Imports verifies that a manifest-only domain node — a
// domain snapshot with neither volume data nor child snapshots (e.g. a disk-less
// DemoVirtualMachineSnapshot) — is client-importable: it gets the unified
// spec.mode: Import marker, its manifests are uploaded, it carries a controller-owned
// child->parent ownerRef, and no DataImport is created (it has no data leg). It is
// import-equivalent to a structural Snapshot child.
func TestRun_ManifestOnlyDomainNode_Imports(t *testing.T) {
	root := t.TempDir()
	writeArchiveNode(t, root, archiveNode{
		apiVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		kind:       "Snapshot",
		name:       "root",
	})

	demo := childDir(root, "DemoVirtualMachineSnapshot", "vm-1")
	writeArchiveNode(t, demo, archiveNode{
		apiVersion: "sds-unified-snapshots-poc.deckhouse.io/v1alpha1",
		kind:       "DemoVirtualMachineSnapshot",
		name:       "vm-1",
	})

	up := &stubUploader{}
	vol := &stubVolumes{}
	dyn := newFakeDynamic(readyRootSnapshot())

	cfg := baseConfig(root, up, vol, dyn)
	cfg.Mapper = testDomainMapper()

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("manifest-only domain node should import, got: %v", err)
	}

	// Two manifest uploads: the manifest-only domain node (post-order = leaf first), then root.
	if len(up.calls) != 2 {
		t.Fatalf("expected 2 manifest uploads, got %d", len(up.calls))
	}

	if up.calls[0].ref.Kind != "DemoVirtualMachineSnapshot" || up.calls[0].ref.Name != "vm-1" {
		t.Errorf("first upload = %s/%s, want DemoVirtualMachineSnapshot/vm-1", up.calls[0].ref.Kind, up.calls[0].ref.Name)
	}

	// A manifest-only domain node has no data leg: no DataImport is ensured/uploaded.
	if len(vol.ensure) != 0 || len(vol.upload) != 0 {
		t.Errorf("manifest-only domain node must not import volume data: ensure=%v upload=%v", vol.ensure, vol.upload)
	}

	// The domain node import-mode CR was created with the unified import marker.
	vmObj, err := dyn.Resource(demoVMSnapGVR).Namespace(targetNS).Get(context.Background(), "vm-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("DemoVirtualMachineSnapshot import CR not created: %v", err)
	}

	if mode, _, _ := unstructured.NestedString(vmObj.Object, "spec", "mode"); mode != "Import" {
		t.Errorf("manifest-only domain node marker must set spec.mode: Import, got %q", mode)
	}

	// It carries a controller-owned child->parent ownerRef pointing to the root Snapshot.
	refs := vmObj.GetOwnerReferences()
	if len(refs) != 1 || refs[0].Kind != "Snapshot" || refs[0].Name != "root" {
		t.Fatalf("manifest-only domain node must carry a parent Snapshot ownerRef, got %+v", refs)
	}

	if refs[0].Controller == nil || !*refs[0].Controller {
		t.Errorf("manifest-only domain node parent ownerRef should be controller-owned")
	}
}

// TestRun_SelectedNode_ManifestOnlyDomainNodeFails verifies that selecting a manifest-only
// domain node as a standalone --node root fails fast (it has no parent SnapshotContent to
// attach to), with a clear message, before any cluster mutation.
func TestRun_SelectedNode_ManifestOnlyDomainNodeFails(t *testing.T) {
	root := t.TempDir()
	writeArchiveNode(t, root, archiveNode{
		apiVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		kind:       "Snapshot",
		name:       "root",
	})

	demo := childDir(root, "DemoVirtualMachineSnapshot", "vm-1")
	writeArchiveNode(t, demo, archiveNode{
		apiVersion: "sds-unified-snapshots-poc.deckhouse.io/v1alpha1",
		kind:       "DemoVirtualMachineSnapshot",
		name:       "vm-1",
	})

	up := &stubUploader{}
	vol := &stubVolumes{}
	dyn := newFakeDynamic(readyRootSnapshot())

	cfg := baseConfig(root, up, vol, dyn)
	cfg.Mapper = testDomainMapper()
	cfg.SelectedNodeKind = "DemoVirtualMachineSnapshot"
	cfg.SelectedNodeName = "vm-1"

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error selecting a manifest-only domain node as standalone root, got nil")
	}

	if !strings.Contains(err.Error(), "manifest-only domain node") {
		t.Errorf("expected manifest-only-domain-node error, got: %v", err)
	}

	if len(up.calls) != 0 || len(vol.ensure) != 0 {
		t.Errorf("no cluster mutations should occur on selection error: uploads=%d ensures=%d", len(up.calls), len(vol.ensure))
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
		"apiVersion": "state-snapshotter.deckhouse.io/v1alpha1",
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
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
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
		apiVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		kind:       "Snapshot",
		name:       "root",
		namespace:  "src",
	})

	domain := childDir(root, "DemoVirtualMachineSnapshot", "vm-1")
	writeArchiveNode(t, domain, archiveNode{
		apiVersion: "sds-unified-snapshots-poc.deckhouse.io/v1alpha1",
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
// already materialized to Ready, so waitLeafReady observes its own namespaced Ready
// condition (no cluster-scoped SnapshotContent read).
func readyImportLeafVS() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "snapshot.storage.k8s.io/v1",
		"kind":       "VolumeSnapshot",
		"metadata":   map[string]interface{}{"namespace": targetNS, "name": "pvc-1", "uid": "vs-uid"},
		"spec": map[string]interface{}{
			"mode": "Import",
		},
		"status": map[string]interface{}{
			"conditions": readyConditions("Ready"),
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
	dyn := newFakeDynamic(readyRootSnapshot())

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
	dyn := newFakeDynamic(readyImportLeafVS())

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
	dyn := newFakeDynamic(readyRootSnapshot())

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
		apiVersion: "state-snapshotter.deckhouse.io/v1alpha1",
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

func (s *concStubVolumes) UploadVolumeData(ctx context.Context, leaf PlannedNode, _, _ string, _ func(int64), _ func(int), _ func()) error {
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
	dyn := newFakeDynamic(readyRootSnapshot())

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

func (s *errorOnceStubVolumes) UploadVolumeData(ctx context.Context, leaf PlannedNode, _, _ string, _ func(int64), _ func(int), _ func()) error {
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
	dyn := newFakeDynamic(readyRootSnapshot())

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

// buildAggregatorWithDomainLeafArchive creates a three-level archive:
//
//	root Snapshot → DemoVirtualMachineSnapshot/vm-1 (aggregator, no volume data)
//	             → DemoVirtualDiskSnapshot/dvd-1 (domain data leaf, block data + SourceObjectRef)
func buildAggregatorWithDomainLeafArchive(t *testing.T) string {
	t.Helper()

	root := t.TempDir()

	writeArchiveNode(t, root, archiveNode{
		apiVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		kind:       "Snapshot",
		name:       "root",
		namespace:  "src",
	})

	aggDir := childDir(root, "DemoVirtualMachineSnapshot", "vm-1")
	writeArchiveNode(t, aggDir, archiveNode{
		apiVersion: "sds-unified-snapshots-poc.deckhouse.io/v1alpha1",
		kind:       "DemoVirtualMachineSnapshot",
		name:       "vm-1",
		namespace:  "src",
	})

	leafDir := childDir(aggDir, "DemoVirtualDiskSnapshot", "dvd-1")
	writeArchiveNode(t, leafDir, archiveNode{
		apiVersion: "sds-unified-snapshots-poc.deckhouse.io/v1alpha1",
		kind:       "DemoVirtualDiskSnapshot",
		name:       "dvd-1",
		namespace:  "src",
		blockData:  []byte("rawbytes"),
		sourceObjectRef: &archive.SourceObjectRef{
			APIVersion: "demo.deckhouse.io/v1alpha1",
			Kind:       "DemoVirtualDisk",
			Name:       "disk-a",
		},
	})

	return root
}

// TestPreflight_AggregatorTreePasses verifies that a full archive containing a domain
// aggregator (DemoVirtualMachineSnapshot) and its data-leaf child passes preflight: the
// aggregator is reconstructed server-side as a non-root node, so a full-tree import is allowed.
func TestPreflight_AggregatorTreePasses(t *testing.T) {
	archiveRoot := buildAggregatorWithDomainLeafArchive(t)

	plan, err := BuildPlan(archiveRoot)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	if err := preflight(plan); err != nil {
		t.Errorf("expected preflight to pass for a full aggregator tree, got: %v", err)
	}
}

// TestRun_FullAggregatorImport verifies that a full-archive import of a tree containing a
// domain aggregator (DemoVirtualMachineSnapshot) succeeds end-to-end:
//   - import-mode markers are created for the root, the aggregator, and the data leaf
//   - manifests are uploaded for every node; the aggregator's upload carries its child ref
//   - a DataImport is created ONLY for the data leaf (the aggregator carries no own data)
func TestRun_FullAggregatorImport(t *testing.T) {
	archiveRoot := buildAggregatorWithDomainLeafArchive(t)

	up := &stubUploader{}
	vol := &stubVolumes{}
	dyn := newFakeDynamic(readyRootSnapshot())

	cfg := baseConfig(archiveRoot, up, vol, dyn)
	cfg.Mapper = testDomainMapper()

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("full aggregator import: %v", err)
	}

	// A DataImport is created only for the data leaf, never for the aggregator.
	if len(vol.ensure) != 1 || vol.ensure[0] != "dvd-1" {
		t.Errorf("expected exactly one DataImport for the data leaf dvd-1, got: %v", vol.ensure)
	}

	// The aggregator marker must have been created in the target namespace.
	if _, err := dyn.Resource(demoVMSnapGVR).Namespace(targetNS).Get(context.Background(), "vm-1", metav1.GetOptions{}); err != nil {
		t.Errorf("aggregator import CR DemoVirtualMachineSnapshot/vm-1 should have been created: %v", err)
	}

	// The aggregator's upload must carry its data-leaf child ref so the server can aggregate it.
	var aggUpload *uploadCall
	for i := range up.calls {
		if up.calls[i].ref.Kind == "DemoVirtualMachineSnapshot" && up.calls[i].ref.Name == "vm-1" {
			aggUpload = &up.calls[i]

			break
		}
	}

	if aggUpload == nil {
		t.Fatalf("expected a manifests upload for the aggregator DemoVirtualMachineSnapshot/vm-1; got calls: %+v", up.calls)
	}

	if len(aggUpload.body.ChildRefs) != 1 || aggUpload.body.ChildRefs[0].Name != "dvd-1" {
		t.Errorf("aggregator upload should carry child ref DemoVirtualDiskSnapshot/dvd-1, got: %+v", aggUpload.body.ChildRefs)
	}
}

// TestPreflight_DomainDataLeafDirectlyUnderRootPasses verifies that a plan with a domain data
// leaf directly under the root Snapshot (no aggregator ancestor) passes preflight.
func TestPreflight_DomainDataLeafDirectlyUnderRootPasses(t *testing.T) {
	archiveRoot := buildDomainDataLeafArchive(t)

	plan, err := BuildPlan(archiveRoot)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	if err := preflight(plan); err != nil {
		t.Errorf("expected preflight to pass for domain data leaf under root, got: %v", err)
	}
}

// TestPreflight_VSLeafPasses verifies that a plan with a CSI VolumeSnapshot data leaf
// directly under the root Snapshot passes preflight.
func TestPreflight_VSLeafPasses(t *testing.T) {
	archiveRoot := buildTwoLevelArchive(t)

	plan, err := BuildPlan(archiveRoot)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	if err := preflight(plan); err != nil {
		t.Errorf("expected preflight to pass for VS leaf under root, got: %v", err)
	}
}

// TestApplyDefaults_Workers asserts that a zero Workers field is filled to 5 (the default).
func TestApplyDefaults_Workers(t *testing.T) {
	cases := []struct {
		name    string
		workers int
		want    int
	}{
		{name: "zero filled to default", workers: 0, want: defaultWorkers},
		{name: "positive kept", workers: 3, want: 3},
		{name: "negative filled to default", workers: -1, want: defaultWorkers},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := applyDefaults(Config{Workers: tc.workers})

			if cfg.Workers != tc.want {
				t.Errorf("Workers: got %d, want %d", cfg.Workers, tc.want)
			}
		})
	}
}

// TestWaitForBinds_AllBoundReturnsNil verifies the happy path: when every planned node reports a
// non-empty status.boundSnapshotContentName (here via the binder-simulating reactor), the bind
// gate returns nil without waiting out the timeout.
func TestWaitForBinds_AllBoundReturnsNil(t *testing.T) {
	root := buildTwoLevelArchive(t)

	plan, err := BuildPlan(root)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	// Both nodes exist as import markers; newFakeDynamic's binder reactor stamps them bound.
	dyn := newFakeDynamic(readyRootSnapshot(), readyImportLeafVS())
	cfg := baseConfig(root, &stubUploader{}, &stubVolumes{}, dyn)
	cfg.Timeout = 10 * time.Second // generous: success must NOT depend on the timeout elapsing.

	start := time.Now()
	if err := waitForBinds(context.Background(), cfg, plan); err != nil {
		t.Fatalf("waitForBinds with all nodes bound: %v", err)
	}

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("waitForBinds returned after %v; a fully-bound plan must return promptly", elapsed)
	}
}

// TestWaitForBinds_TimesOutWhenUnbound verifies that nodes which never bind (raw client, no binder
// reactor) drive the gate to a timeout error that surfaces status.boundSnapshotContentName and
// names every still-unbound node.
func TestWaitForBinds_TimesOutWhenUnbound(t *testing.T) {
	root := buildTwoLevelArchive(t)

	plan, err := BuildPlan(root)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	// Raw client: the nodes exist but never gain status.boundSnapshotContentName.
	dyn := newFakeDynamicRaw(readyRootSnapshot(), readyImportLeafVS())
	cfg := baseConfig(root, &stubUploader{}, &stubVolumes{}, dyn)

	timeout := 40 * time.Millisecond
	cfg.Timeout = timeout

	start := time.Now()
	err = waitForBinds(context.Background(), cfg, plan)

	if err == nil {
		t.Fatal("expected a bind timeout for never-bound nodes, got nil")
	}

	if !strings.Contains(err.Error(), "timeout") || !strings.Contains(err.Error(), "boundSnapshotContentName") {
		t.Errorf("timeout error should mention timeout and boundSnapshotContentName, got: %v", err)
	}

	for _, want := range []string{"Snapshot/root", "VolumeSnapshot/pvc-1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("timeout error should name the unbound node %q, got: %v", want, err)
		}
	}

	if elapsed := time.Since(start); elapsed < timeout {
		t.Errorf("must wait at least the timeout (%v) before giving up, waited %v", timeout, elapsed)
	}
}

// TestWaitForBinds_OnlyUnboundNodesReported verifies the still-pending filtering: an already-bound
// node is dropped from the wait set, so the timeout error reports only the node that never binds.
func TestWaitForBinds_OnlyUnboundNodesReported(t *testing.T) {
	root := buildTwoLevelArchive(t)

	plan, err := BuildPlan(root)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	// root is pre-bound; the leaf never binds (raw client, no binder reactor).
	boundRoot := readyRootSnapshot()
	if err := unstructured.SetNestedField(boundRoot.Object, "root-content", "status", "boundSnapshotContentName"); err != nil {
		t.Fatalf("seed bound root: %v", err)
	}

	dyn := newFakeDynamicRaw(boundRoot, readyImportLeafVS())
	cfg := baseConfig(root, &stubUploader{}, &stubVolumes{}, dyn)
	cfg.Timeout = 40 * time.Millisecond

	err = waitForBinds(context.Background(), cfg, plan)
	if err == nil {
		t.Fatal("expected a bind timeout for the unbound leaf, got nil")
	}

	if !strings.Contains(err.Error(), "VolumeSnapshot/pvc-1") {
		t.Errorf("timeout error should name the unbound leaf, got: %v", err)
	}

	if strings.Contains(err.Error(), "Snapshot/root") {
		t.Errorf("an already-bound node must NOT be reported as still pending, got: %v", err)
	}

	if !strings.Contains(err.Error(), "1 import node(s)") {
		t.Errorf("timeout error should report exactly one still-pending node, got: %v", err)
	}
}

// TestRun_BlocksOnUnboundNodes verifies the gate ordering end-to-end: with nodes that never bind,
// Run creates the markers (pass 1) but fails at the bind gate BEFORE any manifests upload (pass 2a)
// or volume data import (pass 2b) — no upload/data mutation may precede the bind.
func TestRun_BlocksOnUnboundNodes(t *testing.T) {
	root := buildTwoLevelArchive(t)

	up := &stubUploader{}
	vol := &stubVolumes{}
	// Raw client, root pre-seeded so createMarkers reconciles it; nodes never bind.
	dyn := newFakeDynamicRaw(readyRootSnapshot())

	cfg := baseConfig(root, up, vol, dyn)
	cfg.Timeout = 60 * time.Millisecond

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected Run to fail at the bind gate when nodes never bind, got nil")
	}

	if !strings.Contains(err.Error(), "bind a SnapshotContent") {
		t.Errorf("expected a bind-gate timeout error, got: %v", err)
	}

	if len(up.calls) != 0 || len(vol.ensure) != 0 {
		t.Errorf("no manifests upload or data import may precede the bind gate: uploads=%d ensures=%d", len(up.calls), len(vol.ensure))
	}

	// Pass 1 still ran: the leaf import-mode marker was created before the gate blocked.
	if _, gErr := dyn.Resource(volumeSnapshotGVRt).Namespace(targetNS).Get(context.Background(), "pvc-1", metav1.GetOptions{}); gErr != nil {
		t.Errorf("expected the leaf import marker to be created in pass 1 before the bind gate: %v", gErr)
	}
}
