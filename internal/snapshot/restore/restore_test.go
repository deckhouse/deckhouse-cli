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

package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
)

const (
	testNS   = "default"
	testSnap = "my-snap"
)

var (
	snapshotGVR = schema.GroupVersionResource{Group: "state-snapshotter.deckhouse.io", Version: "v1alpha1", Resource: "snapshots"}
	pvcGVR      = schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"}
	cmGVR       = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	pvGVR       = schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}
	vsGVR       = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}
)

// stubSource records the call and returns a canned manifest array body.
type stubSource struct {
	body []byte
	err  error

	gotRef aggapi.NodeRef
	gotNS  string
	calls  int
}

func (s *stubSource) RestoreManifests(_ context.Context, ref aggapi.NodeRef, targetNamespace string) ([]byte, error) {
	s.calls++
	s.gotRef = ref
	s.gotNS = targetNamespace

	return s.body, s.err
}

// testMapper resolves every kind the restore tests apply, with the right scope.
// defaultGroupVersions are required for version-less RESTMapping(gk) lookups used
// by preflightLeaves when resolving spec.dataSourceRef / spec.dataSource targets
// (those fields carry apiGroup+kind but no version).
func testMapper() meta.RESTMapper {
	m := meta.NewDefaultRESTMapper([]schema.GroupVersion{
		{Group: "", Version: "v1"},
		{Group: "state-snapshotter.deckhouse.io", Version: "v1alpha1"},
		{Group: "snapshot.storage.k8s.io", Version: "v1"},
	})
	m.Add(schema.GroupVersionKind{Group: "state-snapshotter.deckhouse.io", Version: "v1alpha1", Kind: "Snapshot"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Version: "v1", Kind: "PersistentVolumeClaim"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Version: "v1", Kind: "PersistentVolume"}, meta.RESTScopeRoot)
	m.Add(schema.GroupVersionKind{Group: "snapshot.storage.k8s.io", Version: "v1", Kind: "VolumeSnapshot"}, meta.RESTScopeNamespace)

	return m
}

// addSSAReactor installs a reactor that simulates Server-Side Apply semantics using
// pure JSON merge patch logic: absent objects are created from the patch body; present
// objects are updated by merging (fields not in the patch are preserved). This is used
// instead of the tracker's built-in Apply method, which uses strategic merge patch — a
// strategy that fails for generic unstructured objects whose extra fields (e.g. "data"
// in ConfigMap) are absent from the Go struct that strategic merge patch inspects.
func addSSAReactor(dyn *dynamicfake.FakeDynamicClient) {
	tracker := dyn.Tracker()

	dyn.PrependReactor("patch", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		pa, ok := action.(clienttesting.PatchAction)
		if !ok || pa.GetPatchType() != types.ApplyPatchType {
			return false, nil, nil
		}

		gvr := action.GetResource()
		ns := pa.GetNamespace()
		name := pa.GetName()

		patch := &unstructured.Unstructured{}
		if jsonErr := json.Unmarshal(pa.GetPatch(), &patch.Object); jsonErr != nil {
			return true, nil, jsonErr
		}

		existing, err := tracker.Get(gvr, ns, name, metav1.GetOptions{})
		if kubeerrors.IsNotFound(err) {
			// Object absent: create from patch body (SSA create semantics).
			if createErr := tracker.Create(gvr, patch, ns); createErr != nil {
				return true, nil, createErr
			}

			created, getErr := tracker.Get(gvr, ns, name, metav1.GetOptions{})

			return true, created, getErr
		}

		if err != nil {
			return true, nil, err
		}

		// Object present: merge the patch into the existing object, preserving
		// all fields the patch does not mention (JSON merge patch semantics).
		existingUnstr, castOK := existing.(*unstructured.Unstructured)
		if !castOK {
			return true, nil, fmt.Errorf("unexpected existing object type %T", existing)
		}

		merged := existingUnstr.DeepCopy()
		jsonMergeInto(merged.Object, patch.Object)

		if updateErr := tracker.Update(gvr, merged, ns); updateErr != nil {
			return true, nil, updateErr
		}

		updated, getErr := tracker.Get(gvr, ns, name, metav1.GetOptions{})

		return true, updated, getErr
	})
}

// jsonMergeInto applies RFC 7396 JSON merge patch semantics: for each key in src, the
// corresponding key in dst is set or replaced; nested maps are merged recursively; keys
// absent from src are preserved in dst.
func jsonMergeInto(dst, src map[string]interface{}) {
	for k, sv := range src {
		dv, ok := dst[k]
		if !ok {
			dst[k] = sv

			continue
		}

		dvm, isDstMap := dv.(map[string]interface{})
		svm, isSrcMap := sv.(map[string]interface{})

		if isDstMap && isSrcMap {
			jsonMergeInto(dvm, svm)
		} else {
			dst[k] = sv
		}
	}
}

// newFakeDynamic builds a fake dynamic client seeded with the given objects.
// It also installs a reactor for SSA create-if-not-exists semantics so that
// Patch(ApplyPatchType, …) works for new objects the same way as on a real cluster.
func newFakeDynamic(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	gvrToListKind := map[schema.GroupVersionResource]string{
		snapshotGVR: "SnapshotList",
		pvcGVR:      "PersistentVolumeClaimList",
		cmGVR:       "ConfigMapList",
		pvGVR:       "PersistentVolumeList",
		vsGVR:       "VolumeSnapshotList",
	}

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), gvrToListKind, objs...)

	addSSAReactor(dyn)

	return dyn
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// mustArray marshals the given objects into a JSON array body.
func mustArray(t *testing.T, objs ...map[string]interface{}) []byte {
	t.Helper()

	b, err := json.Marshal(objs)
	if err != nil {
		t.Fatalf("marshal manifest array: %v", err)
	}

	return b
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

// readySnapshot returns a Snapshot that passes preflight (Ready=True + bound content).
func readySnapshot() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "state-snapshotter.deckhouse.io/v1alpha1",
		"kind":       "Snapshot",
		"metadata": map[string]interface{}{
			"namespace": testNS,
			"name":      testSnap,
		},
		"status": map[string]interface{}{
			"boundSnapshotContentName": "snapcontent-1",
			"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "True"},
			},
		},
	}}
}

// readyVolumeSnapshot returns a CSI VolumeSnapshot with status.readyToUse=true,
// satisfying the leaf preflight check for PVCs that reference it.
func readyVolumeSnapshot(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "snapshot.storage.k8s.io/v1",
		"kind":       "VolumeSnapshot",
		"metadata": map[string]interface{}{
			"namespace": testNS,
			"name":      name,
		},
		"status": map[string]interface{}{
			"readyToUse": true,
		},
	}}
}

func configMapManifest(name string) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]interface{}{"name": name},
		"data":       map[string]interface{}{"k": "v"},
	}
}

func pvcManifest(name, phase string) map[string]interface{} {
	obj := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata":   map[string]interface{}{"name": name},
		"spec": map[string]interface{}{
			"dataSourceRef": map[string]interface{}{
				"apiGroup": "snapshot.storage.k8s.io",
				"kind":     "VolumeSnapshot",
				"name":     "vs-1",
			},
		},
	}
	if phase != "" {
		obj["status"] = map[string]interface{}{"phase": phase}
	}

	return obj
}

func baseConfig(src Source, dyn dynamic.Interface) Config {
	return Config{
		Namespace:    testNS,
		Snapshot:     testSnap,
		Source:       src,
		Dynamic:      dyn,
		Mapper:       testMapper(),
		Log:          discardLogger(),
		PollInterval: time.Millisecond,
	}
}

// TestRun_AppliesAllObjects verifies every returned object is applied as-is, the
// root ref is addressed correctly, and namespaced objects inherit the target namespace.
func TestRun_AppliesAllObjects(t *testing.T) {
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"), pvcManifest("pvc-1", ""))}
	dyn := newFakeDynamic(readySnapshot(), readyVolumeSnapshot("vs-1"))

	if err := Run(context.Background(), baseConfig(src, dyn)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if src.calls != 1 {
		t.Fatalf("expected exactly one RestoreManifests call, got %d", src.calls)
	}

	if src.gotNS != testNS {
		t.Errorf("targetNamespace: got %q, want %q", src.gotNS, testNS)
	}

	wantRef := aggapi.NodeRef{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       testSnap,
		Namespace:  testNS,
	}
	if src.gotRef != wantRef {
		t.Errorf("ref: got %+v, want %+v", src.gotRef, wantRef)
	}

	cm, err := dyn.Resource(cmGVR).Namespace(testNS).Get(context.Background(), "cm-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("ConfigMap not applied: %v", err)
	}

	if cm.GetNamespace() != testNS {
		t.Errorf("ConfigMap namespace: got %q, want %q", cm.GetNamespace(), testNS)
	}

	pvc, err := dyn.Resource(pvcGVR).Namespace(testNS).Get(context.Background(), "pvc-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("PVC not applied: %v", err)
	}

	ref, found, _ := unstructured.NestedString(pvc.Object, "spec", "dataSourceRef", "kind")
	if !found || ref != "VolumeSnapshot" {
		t.Errorf("PVC dataSourceRef.kind not preserved: found=%v value=%q", found, ref)
	}
}

// TestRun_AppliesClusterScopedObject verifies cluster-scoped objects are applied
// without a namespace.
func TestRun_AppliesClusterScopedObject(t *testing.T) {
	pv := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "PersistentVolume",
		"metadata":   map[string]interface{}{"name": "pv-1"},
	}

	src := &stubSource{body: mustArray(t, pv)}
	dyn := newFakeDynamic(readySnapshot())

	if err := Run(context.Background(), baseConfig(src, dyn)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, err := dyn.Resource(pvGVR).Get(context.Background(), "pv-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("PersistentVolume not applied: %v", err)
	}

	if got.GetNamespace() != "" {
		t.Errorf("cluster-scoped object got namespace %q, want empty", got.GetNamespace())
	}
}

// TestRun_UpdatesExistingObject verifies the upsert path updates a pre-existing object.
func TestRun_UpdatesExistingObject(t *testing.T) {
	existing := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]interface{}{
			"namespace":       testNS,
			"name":            "cm-1",
			"resourceVersion": "123",
		},
		"data": map[string]interface{}{"k": "old"},
	}}

	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(readySnapshot(), existing)

	if err := Run(context.Background(), baseConfig(src, dyn)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	cm, err := dyn.Resource(cmGVR).Namespace(testNS).Get(context.Background(), "cm-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}

	val, _, _ := unstructured.NestedString(cm.Object, "data", "k")
	if val != "v" {
		t.Errorf("ConfigMap not updated: data.k = %q, want %q", val, "v")
	}
}

// TestRun_UpdatePreservesLiveMetadata verifies that SSA does not clear server-managed
// metadata (finalizers, ownerReferences) absent from the restored manifest. SSA only
// owns fields d8 explicitly sets; fields set by other managers are not touched.
func TestRun_UpdatePreservesLiveMetadata(t *testing.T) {
	existing := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]interface{}{
			"namespace":  testNS,
			"name":       "cm-1",
			"finalizers": []interface{}{"example.com/protect"},
			"ownerReferences": []interface{}{
				map[string]interface{}{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"name":       "owner",
					"uid":        "abc-123",
				},
			},
		},
		"data": map[string]interface{}{"k": "old"},
	}}

	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(readySnapshot(), existing)

	if err := Run(context.Background(), baseConfig(src, dyn)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	cm, err := dyn.Resource(cmGVR).Namespace(testNS).Get(context.Background(), "cm-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}

	if fz := cm.GetFinalizers(); len(fz) != 1 || fz[0] != "example.com/protect" {
		t.Errorf("finalizers not preserved: %v", fz)
	}

	if owners := cm.GetOwnerReferences(); len(owners) != 1 || owners[0].Name != "owner" {
		t.Errorf("ownerReferences not preserved: %v", owners)
	}

	if val, _, _ := unstructured.NestedString(cm.Object, "data", "k"); val != "v" {
		t.Errorf("data not updated: data.k = %q, want %q", val, "v")
	}
}

// TestRun_WaitBound succeeds when the restored PVC reports Bound.
// The PVC is pre-seeded with Bound status to simulate CSI binding, since
// applyObject strips status from the SSA patch (status is a separate subresource).
func TestRun_WaitBound(t *testing.T) {
	// Pre-seed the PVC as already Bound; applyObject strips status from the SSA patch
	// so the existing Bound status is preserved by strategic-merge semantics.
	existing := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata": map[string]interface{}{
			"namespace": testNS,
			"name":      "pvc-1",
		},
		"status": map[string]interface{}{"phase": "Bound"},
	}}

	src := &stubSource{body: mustArray(t, pvcManifest("pvc-1", ""))}
	dyn := newFakeDynamic(readySnapshot(), readyVolumeSnapshot("vs-1"), existing)

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = 2 * time.Second

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with --wait: %v", err)
	}
}

// TestRun_WaitTimeout returns a timeout error when a restored PVC never binds.
func TestRun_WaitTimeout(t *testing.T) {
	src := &stubSource{body: mustArray(t, pvcManifest("pvc-1", "Pending"))}
	dyn := newFakeDynamic(readySnapshot(), readyVolumeSnapshot("vs-1"))

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = time.Millisecond

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	if !contains(err.Error(), "Bound") {
		t.Errorf("error %q does not mention Bound", err.Error())
	}
}

// TestRun_PreflightNotReady fails when the source Snapshot is not Ready=True.
func TestRun_PreflightNotReady(t *testing.T) {
	snap := readySnapshot()
	unstructured.RemoveNestedField(snap.Object, "status", "conditions")

	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(snap)

	err := Run(context.Background(), baseConfig(src, dyn))
	if err == nil {
		t.Fatal("expected preflight error, got nil")
	}

	if src.calls != 0 {
		t.Errorf("Source must not be called when preflight fails, got %d calls", src.calls)
	}
}

// TestRun_PreflightNoBoundContent fails when the Snapshot has no bound content.
func TestRun_PreflightNoBoundContent(t *testing.T) {
	snap := readySnapshot()
	unstructured.RemoveNestedField(snap.Object, "status", "boundSnapshotContentName")

	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(snap)

	if err := Run(context.Background(), baseConfig(src, dyn)); err == nil {
		t.Fatal("expected preflight error, got nil")
	}
}

// TestRun_SnapshotNotFound fails clearly when the source Snapshot is missing.
func TestRun_SnapshotNotFound(t *testing.T) {
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic()

	err := Run(context.Background(), baseConfig(src, dyn))
	if err == nil {
		t.Fatal("expected error for missing Snapshot, got nil")
	}
}

// TestRun_EmptyManifests fails when the server returns no objects.
func TestRun_EmptyManifests(t *testing.T) {
	src := &stubSource{body: []byte("[]")}
	dyn := newFakeDynamic(readySnapshot())

	err := Run(context.Background(), baseConfig(src, dyn))
	if err == nil {
		t.Fatal("expected error for empty manifests, got nil")
	}

	if !contains(err.Error(), "empty") {
		t.Errorf("error %q does not mention empty", err.Error())
	}
}

// TestRun_Validation rejects missing required fields.
func TestRun_Validation(t *testing.T) {
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(readySnapshot())

	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"no namespace", func(c *Config) { c.Namespace = "" }},
		{"no snapshot", func(c *Config) { c.Snapshot = "" }},
		{"no source", func(c *Config) { c.Source = nil }},
		{"no dynamic", func(c *Config) { c.Dynamic = nil }},
		{"no mapper", func(c *Config) { c.Mapper = nil }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig(src, dyn)
			tc.mutate(&cfg)

			if err := Run(context.Background(), cfg); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func TestDecodeManifestArray(t *testing.T) {
	t.Run("invalid json", func(t *testing.T) {
		if _, err := decodeManifestArray([]byte("{not json")); err == nil {
			t.Fatal("expected decode error, got nil")
		}
	})

	t.Run("empty array", func(t *testing.T) {
		objs, err := decodeManifestArray([]byte("[]"))
		if err != nil {
			t.Fatalf("decode empty array: %v", err)
		}

		if len(objs) != 0 {
			t.Errorf("got %d objects, want 0", len(objs))
		}
	})

	t.Run("two objects", func(t *testing.T) {
		body := mustArray(t, configMapManifest("a"), configMapManifest("b"))

		objs, err := decodeManifestArray(body)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}

		if len(objs) != 2 {
			t.Fatalf("got %d objects, want 2", len(objs))
		}

		if objs[0].GetName() != "a" || objs[1].GetName() != "b" {
			t.Errorf("order not preserved: %q, %q", objs[0].GetName(), objs[1].GetName())
		}
	})
}

func TestIsConditionTrue(t *testing.T) {
	cases := []struct {
		name string
		obj  map[string]interface{}
		want bool
	}{
		{
			name: "ready true",
			obj: map[string]interface{}{"status": map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "True"},
			}}},
			want: true,
		},
		{
			name: "ready false",
			obj: map[string]interface{}{"status": map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "False"},
			}}},
			want: false,
		},
		{
			name: "no conditions",
			obj:  map[string]interface{}{"status": map[string]interface{}{}},
			want: false,
		},
		{
			name: "other condition only",
			obj: map[string]interface{}{"status": map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "Progressing", "status": "True"},
			}}},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isConditionTrue(&unstructured.Unstructured{Object: tc.obj}, readyConditionType)
			if got != tc.want {
				t.Errorf("isConditionTrue() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestApplyObject_GetError surfaces non-NotFound Get errors instead of treating them
// as create.
func TestApplyObject_GetError(t *testing.T) {
	// A kind with no mapping triggers a resolution error in applyObject.
	src := &stubSource{body: mustArray(t, map[string]interface{}{
		"apiVersion": "example.com/v1",
		"kind":       "Unknown",
		"metadata":   map[string]interface{}{"name": "x"},
	})}
	dyn := newFakeDynamic(readySnapshot())

	if err := Run(context.Background(), baseConfig(src, dyn)); err == nil {
		t.Fatal("expected error applying an object with no REST mapping, got nil")
	}
}

// TestRun_ImmutableUpdateActionable verifies that an Invalid apply of a pre-existing
// object (e.g. a PVC whose spec.dataSourceRef is immutable) yields an actionable error.
func TestRun_ImmutableUpdateActionable(t *testing.T) {
	existing := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata": map[string]interface{}{
			"namespace": testNS,
			"name":      "pvc-1",
		},
	}}

	src := &stubSource{body: mustArray(t, pvcManifest("pvc-1", ""))}
	dyn := newFakeDynamic(readySnapshot(), readyVolumeSnapshot("vs-1"), existing)

	dyn.PrependReactor("patch", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		pa, ok := action.(clienttesting.PatchAction)
		if !ok || pa.GetPatchType() != types.ApplyPatchType {
			return false, nil, nil
		}

		return true, nil, kubeerrors.NewInvalid(
			schema.GroupKind{Kind: "PersistentVolumeClaim"},
			"pvc-1",
			field.ErrorList{field.Forbidden(field.NewPath("spec", "dataSourceRef"), "field is immutable")},
		)
	})

	err := Run(context.Background(), baseConfig(src, dyn))
	if err == nil {
		t.Fatal("expected error for immutable apply, got nil")
	}

	if !contains(err.Error(), "immutable") || !contains(err.Error(), "delete it") {
		t.Errorf("error %q is not actionable about immutability", err.Error())
	}
}

// patchCaptureDynamic is a minimal dynamic.Interface stub that captures PatchOptions
// for verifying that applyObject passes the correct options to ri.Patch.
// All methods except Resource, Namespace, and Patch panic if called.
type patchCaptureDynamic struct {
	gotOpts []metav1.PatchOptions
}

func (d *patchCaptureDynamic) Resource(_ schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return &patchCaptureResource{d: d}
}

type patchCaptureResource struct {
	d *patchCaptureDynamic
}

func (r *patchCaptureResource) Namespace(_ string) dynamic.ResourceInterface { return r }

func (r *patchCaptureResource) Patch(_ context.Context, _ string, _ types.PatchType, data []byte, opts metav1.PatchOptions, _ ...string) (*unstructured.Unstructured, error) {
	r.d.gotOpts = append(r.d.gotOpts, opts)

	obj := &unstructured.Unstructured{}
	if err := json.Unmarshal(data, &obj.Object); err != nil {
		return nil, err
	}

	return obj, nil
}

func (r *patchCaptureResource) Create(_ context.Context, _ *unstructured.Unstructured, _ metav1.CreateOptions, _ ...string) (*unstructured.Unstructured, error) {
	panic("patchCaptureResource: Create not implemented")
}

func (r *patchCaptureResource) Update(_ context.Context, _ *unstructured.Unstructured, _ metav1.UpdateOptions, _ ...string) (*unstructured.Unstructured, error) {
	panic("patchCaptureResource: Update not implemented")
}

func (r *patchCaptureResource) UpdateStatus(_ context.Context, _ *unstructured.Unstructured, _ metav1.UpdateOptions) (*unstructured.Unstructured, error) {
	panic("patchCaptureResource: UpdateStatus not implemented")
}

func (r *patchCaptureResource) Delete(_ context.Context, _ string, _ metav1.DeleteOptions, _ ...string) error {
	panic("patchCaptureResource: Delete not implemented")
}

func (r *patchCaptureResource) DeleteCollection(_ context.Context, _ metav1.DeleteOptions, _ metav1.ListOptions) error {
	panic("patchCaptureResource: DeleteCollection not implemented")
}

func (r *patchCaptureResource) Get(_ context.Context, _ string, _ metav1.GetOptions, _ ...string) (*unstructured.Unstructured, error) {
	panic("patchCaptureResource: Get not implemented")
}

func (r *patchCaptureResource) List(_ context.Context, _ metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	panic("patchCaptureResource: List not implemented")
}

func (r *patchCaptureResource) Watch(_ context.Context, _ metav1.ListOptions) (watch.Interface, error) {
	panic("patchCaptureResource: Watch not implemented")
}

func (r *patchCaptureResource) Apply(_ context.Context, _ string, _ *unstructured.Unstructured, _ metav1.ApplyOptions, _ ...string) (*unstructured.Unstructured, error) {
	panic("patchCaptureResource: Apply not implemented")
}

func (r *patchCaptureResource) ApplyStatus(_ context.Context, _ string, _ *unstructured.Unstructured, _ metav1.ApplyOptions) (*unstructured.Unstructured, error) {
	panic("patchCaptureResource: ApplyStatus not implemented")
}

// TestApplyObject_DryRunOptions verifies that applyObject passes DryRun:[All] in the
// PatchOptions when Config.DryRun is true. The fake dynamic client (dynamicfake) does not
// forward PatchOptions to recorded actions, so this test calls applyObject directly with a
// patchCaptureDynamic stub that captures the options passed to ri.Patch.
func TestApplyObject_DryRunOptions(t *testing.T) {
	mock := &patchCaptureDynamic{}

	cfg := Config{
		Namespace: testNS,
		DryRun:    true,
		Dynamic:   mock,
		Mapper:    testMapper(),
		Log:       discardLogger(),
	}

	obj := &unstructured.Unstructured{Object: configMapManifest("cm-1")}

	ns, err := applyObject(context.Background(), cfg, obj)
	if err != nil {
		t.Fatalf("applyObject: %v", err)
	}

	if ns != testNS {
		t.Errorf("namespace: got %q, want %q", ns, testNS)
	}

	if len(mock.gotOpts) != 1 {
		t.Fatalf("expected 1 Patch call, got %d", len(mock.gotOpts))
	}

	opts := mock.gotOpts[0]
	if len(opts.DryRun) != 1 || opts.DryRun[0] != metav1.DryRunAll {
		t.Errorf("DryRun options: got %v, want [%q]", opts.DryRun, metav1.DryRunAll)
	}
}

// TestRun_DryRun_NoObjectPersisted verifies that with DryRun=true, objects are not
// persisted in the cluster. A reactor simulates server dry-run by returning success
// without writing to the tracker; a subsequent Get must return NotFound.
func TestRun_DryRun_NoObjectPersisted(t *testing.T) {
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(readySnapshot())

	// Simulate server dry-run: intercept the SSA apply and return the patch body
	// without writing to the tracker.
	dyn.PrependReactor("patch", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		pa, ok := action.(clienttesting.PatchAction)
		if !ok || pa.GetPatchType() != types.ApplyPatchType {
			return false, nil, nil
		}

		obj := &unstructured.Unstructured{}
		if jsonErr := json.Unmarshal(pa.GetPatch(), &obj.Object); jsonErr != nil {
			return true, nil, jsonErr
		}

		return true, obj, nil
	})

	cfg := baseConfig(src, dyn)
	cfg.DryRun = true

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with DryRun: %v", err)
	}

	_, err := dyn.Resource(cmGVR).Namespace(testNS).Get(context.Background(), "cm-1", metav1.GetOptions{})
	if !kubeerrors.IsNotFound(err) {
		t.Errorf("expected NotFound after dry-run apply, got %v", err)
	}
}

// TestRun_DryRun_SkipsWait verifies that DryRun=true prevents entering the --wait PVC
// bind loop even when Wait=true. The PVC in the manifest is not pre-seeded as Bound, so
// if the wait loop were entered with a very short timeout it would return a timeout error.
func TestRun_DryRun_SkipsWait(t *testing.T) {
	src := &stubSource{body: mustArray(t, pvcManifest("pvc-1", ""))}
	dyn := newFakeDynamic(readySnapshot(), readyVolumeSnapshot("vs-1"))

	cfg := baseConfig(src, dyn)
	cfg.DryRun = true
	cfg.Wait = true
	cfg.Timeout = 50 * time.Millisecond

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with DryRun+Wait: %v", err)
	}
}

// TestRun_LeafPreflight_MissingAborts verifies that a missing VolumeSnapshot leaf
// causes Run to abort with an actionable error naming the leaf, without applying any object.
func TestRun_LeafPreflight_MissingAborts(t *testing.T) {
	// PVC references "vs-1" via spec.dataSourceRef; "vs-1" is not seeded in the fake.
	src := &stubSource{body: mustArray(t, pvcManifest("pvc-1", ""))}
	dyn := newFakeDynamic(readySnapshot())

	err := Run(context.Background(), baseConfig(src, dyn))
	if err == nil {
		t.Fatal("expected error for missing leaf, got nil")
	}

	if !contains(err.Error(), "VolumeSnapshot") || !contains(err.Error(), "vs-1") || !contains(err.Error(), "missing") {
		t.Errorf("error %q is not actionable about missing leaf", err.Error())
	}

	// No object must be applied since we aborted before any apply pass.
	_, getErr := dyn.Resource(pvcGVR).Namespace(testNS).Get(context.Background(), "pvc-1", metav1.GetOptions{})
	if !kubeerrors.IsNotFound(getErr) {
		t.Errorf("expected NotFound for unapplied PVC, got %v", getErr)
	}
}

// TestRun_LeafPreflight_NotReadyAborts verifies that a VolumeSnapshot with
// status.readyToUse=false causes Run to abort with a "not ready" error.
func TestRun_LeafPreflight_NotReadyAborts(t *testing.T) {
	notReadyVS := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "snapshot.storage.k8s.io/v1",
		"kind":       "VolumeSnapshot",
		"metadata":   map[string]interface{}{"namespace": testNS, "name": "vs-1"},
		"status":     map[string]interface{}{"readyToUse": false},
	}}

	src := &stubSource{body: mustArray(t, pvcManifest("pvc-1", ""))}
	dyn := newFakeDynamic(readySnapshot(), notReadyVS)

	err := Run(context.Background(), baseConfig(src, dyn))
	if err == nil {
		t.Fatal("expected error for not-ready leaf, got nil")
	}

	if !contains(err.Error(), "vs-1") || !contains(err.Error(), "not ready") {
		t.Errorf("error %q is not actionable about not-ready leaf", err.Error())
	}
}

// TestRun_LeafPreflight_ReadyProceeds verifies that when the referenced VolumeSnapshot
// leaf is present and readyToUse=true, the preflight passes and the restore proceeds.
func TestRun_LeafPreflight_ReadyProceeds(t *testing.T) {
	src := &stubSource{body: mustArray(t, pvcManifest("pvc-1", ""))}
	dyn := newFakeDynamic(readySnapshot(), readyVolumeSnapshot("vs-1"))

	if err := Run(context.Background(), baseConfig(src, dyn)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	_, err := dyn.Resource(pvcGVR).Namespace(testNS).Get(context.Background(), "pvc-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("PVC not applied after successful preflight: %v", err)
	}
}

// TestRun_LeafPreflight_NoPVCDataSource verifies that a PVC with no dataSourceRef or
// dataSource does not require any leaf and the restore proceeds without error.
func TestRun_LeafPreflight_NoPVCDataSource(t *testing.T) {
	pvcNoRef := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata":   map[string]interface{}{"name": "pvc-plain"},
		"spec":       map[string]interface{}{},
	}

	src := &stubSource{body: mustArray(t, pvcNoRef)}
	dyn := newFakeDynamic(readySnapshot())

	if err := Run(context.Background(), baseConfig(src, dyn)); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestRun_Preflight_DryRunFailureAborts verifies that when the implicit dry-run pass
// rejects an object (Invalid error), Run aborts before any real apply and no object
// is persisted in the cluster.
func TestRun_Preflight_DryRunFailureAborts(t *testing.T) {
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(readySnapshot())

	// Reject all SSA patches on configmaps, simulating admission failure.
	dyn.PrependReactor("patch", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		pa, ok := action.(clienttesting.PatchAction)
		if !ok || pa.GetPatchType() != types.ApplyPatchType {
			return false, nil, nil
		}

		return true, nil, kubeerrors.NewInvalid(
			schema.GroupKind{Kind: "ConfigMap"},
			"cm-1",
			field.ErrorList{field.Forbidden(field.NewPath("spec"), "rejected by webhook")},
		)
	})

	err := Run(context.Background(), baseConfig(src, dyn))
	if err == nil {
		t.Fatal("expected preflight error, got nil")
	}

	if !contains(err.Error(), "dry-run preflight") {
		t.Errorf("error %q does not mention 'dry-run preflight'", err.Error())
	}

	// Nothing must be persisted since the dry-run failed before the real apply.
	_, getErr := dyn.Resource(cmGVR).Namespace(testNS).Get(context.Background(), "cm-1", metav1.GetOptions{})
	if !kubeerrors.IsNotFound(getErr) {
		t.Errorf("expected NotFound after dry-run failure, got %v", getErr)
	}
}

// TestRun_Preflight_RealApplyAfterDryRun verifies that when the dry-run pass succeeds,
// the real apply pass runs exactly once per object, resulting in a persisted object.
// The SSA patch is intercepted to count calls: dry-run=1 + real=1 = 2 total.
func TestRun_Preflight_RealApplyAfterDryRun(t *testing.T) {
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(readySnapshot())

	var patchCount int

	dyn.PrependReactor("patch", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		pa, ok := action.(clienttesting.PatchAction)
		if !ok || pa.GetPatchType() != types.ApplyPatchType {
			return false, nil, nil
		}

		patchCount++

		return false, nil, nil // let the SSA reactor proceed normally
	})

	if err := Run(context.Background(), baseConfig(src, dyn)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if patchCount != 2 {
		t.Errorf("expected 2 SSA patches (1 dry-run + 1 real apply), got %d", patchCount)
	}

	cm, err := dyn.Resource(cmGVR).Namespace(testNS).Get(context.Background(), "cm-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("ConfigMap not persisted after real apply: %v", err)
	}

	val, _, _ := unstructured.NestedString(cm.Object, "data", "k")
	if val != "v" {
		t.Errorf("ConfigMap data.k: got %q, want %q", val, "v")
	}
}

// TestRun_ExplicitDryRun_OnlyOnePass verifies that when Config.DryRun is true, only the
// single (implicit/dry-run) apply pass runs and the real apply is skipped. Exactly one
// SSA patch per object is expected.
func TestRun_ExplicitDryRun_OnlyOnePass(t *testing.T) {
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(readySnapshot())

	var patchCount int

	dyn.PrependReactor("patch", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		pa, ok := action.(clienttesting.PatchAction)
		if !ok || pa.GetPatchType() != types.ApplyPatchType {
			return false, nil, nil
		}

		patchCount++

		return false, nil, nil
	})

	cfg := baseConfig(src, dyn)
	cfg.DryRun = true

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with DryRun: %v", err)
	}

	if patchCount != 1 {
		t.Errorf("expected 1 SSA patch (dry-run only, no real apply), got %d", patchCount)
	}
}

func TestIsNotFoundIntegration(t *testing.T) {
	// Sanity: the fake dynamic client returns a NotFound recognised by kubeerrors.
	dyn := newFakeDynamic()

	_, err := dyn.Resource(cmGVR).Namespace(testNS).Get(context.Background(), "missing", metav1.GetOptions{})
	if !kubeerrors.IsNotFound(err) {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

// TestApplyObject_PatchTypeIsSSA verifies that applyObject issues a Server-Side Apply
// patch (ApplyPatchType) rather than a Create+Update upsert.
func TestApplyObject_PatchTypeIsSSA(t *testing.T) {
	var gotPatchType types.PatchType

	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(readySnapshot())

	dyn.PrependReactor("patch", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		pa, ok := action.(clienttesting.PatchAction)
		if ok {
			gotPatchType = pa.GetPatchType()
		}

		return false, nil, nil // let the SSA create reactor proceed
	})

	if err := Run(context.Background(), baseConfig(src, dyn)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if gotPatchType != types.ApplyPatchType {
		t.Errorf("patch type: got %q, want ApplyPatchType (%q)", gotPatchType, types.ApplyPatchType)
	}
}

// TestApplyObject_ExtraAnnotationPreserved is the regression test for problem 7:
// applyObject must not clobber fields set by controllers that are absent from the
// captured manifest. With SSA the server only updates fields d8 owns; unowned
// annotations, labels and other fields set post-creation survive the apply.
func TestApplyObject_ExtraAnnotationPreserved(t *testing.T) {
	// Seed the object with an extra annotation added by a controller after creation.
	existing := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]interface{}{
			"namespace": testNS,
			"name":      "cm-1",
			"annotations": map[string]interface{}{
				"controller.io/managed-by": "some-controller",
			},
		},
		"data": map[string]interface{}{"k": "old"},
	}}

	// The restore manifest does not include the extra annotation.
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(readySnapshot(), existing)

	if err := Run(context.Background(), baseConfig(src, dyn)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	cm, err := dyn.Resource(cmGVR).Namespace(testNS).Get(context.Background(), "cm-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}

	// The extra annotation must survive the SSA apply.
	if ann := cm.GetAnnotations(); ann["controller.io/managed-by"] != "some-controller" {
		t.Errorf("extra annotation removed by SSA apply, want it preserved: annotations=%v", ann)
	}

	// The data field in the manifest must be updated.
	val, _, _ := unstructured.NestedString(cm.Object, "data", "k")
	if val != "v" {
		t.Errorf("data not updated: got %q, want %q", val, "v")
	}
}

// TestApplyObject_IdempotentApply verifies that applying the same manifest twice
// does not error (SSA re-apply is idempotent).
func TestApplyObject_IdempotentApply(t *testing.T) {
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(readySnapshot())

	if err := Run(context.Background(), baseConfig(src, dyn)); err != nil {
		t.Fatalf("first Run: %v", err)
	}

	if err := Run(context.Background(), baseConfig(src, dyn)); err != nil {
		t.Fatalf("second Run (idempotent): %v", err)
	}
}

// kindSearchMapper wraps meta.RESTMapper to support kind-without-group lookup:
// when GroupKind.Group is empty it searches all registered kinds regardless of
// group, mirroring the DeferredDiscoveryRESTMapper behaviour used in production.
// meta.DefaultRESTMapper only scans defaultGroupVersions that match the given
// group, so a domain kind looked up with an empty group is never found without
// this wrapper.
type kindSearchMapper struct {
	meta.RESTMapper
	kindToGVK map[string]schema.GroupVersionKind
}

func (m *kindSearchMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if gk.Group == "" {
		if gvk, ok := m.kindToGVK[gk.Kind]; ok {
			gk = schema.GroupKind{Group: gvk.Group, Kind: gvk.Kind}
			if len(versions) == 0 {
				versions = []string{gvk.Version}
			}
		}
	}

	return m.RESTMapper.RESTMapping(gk, versions...)
}

// testMapperWithDomain extends testMapper with a domain snapshot GVK so that
// resolveNodeRef tests can resolve domain kinds via the RESTMapper.
func testMapperWithDomain() meta.RESTMapper {
	base := meta.NewDefaultRESTMapper([]schema.GroupVersion{
		{Group: "", Version: "v1"},
		{Group: "state-snapshotter.deckhouse.io", Version: "v1alpha1"},
		{Group: "snapshot.storage.k8s.io", Version: "v1"},
		{Group: "demo.state-snapshotter.deckhouse.io", Version: "v1alpha1"},
	})
	base.Add(schema.GroupVersionKind{Group: "state-snapshotter.deckhouse.io", Version: "v1alpha1", Kind: "Snapshot"}, meta.RESTScopeNamespace)
	base.Add(schema.GroupVersionKind{Version: "v1", Kind: "PersistentVolumeClaim"}, meta.RESTScopeNamespace)
	base.Add(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}, meta.RESTScopeNamespace)
	base.Add(schema.GroupVersionKind{Version: "v1", Kind: "PersistentVolume"}, meta.RESTScopeRoot)
	base.Add(schema.GroupVersionKind{Group: "snapshot.storage.k8s.io", Version: "v1", Kind: "VolumeSnapshot"}, meta.RESTScopeNamespace)
	base.Add(schema.GroupVersionKind{Group: "demo.state-snapshotter.deckhouse.io", Version: "v1alpha1", Kind: "DemoVirtualDiskSnapshot"}, meta.RESTScopeNamespace)

	return &kindSearchMapper{
		RESTMapper: base,
		kindToGVK: map[string]schema.GroupVersionKind{
			"DemoVirtualDiskSnapshot": {Group: "demo.state-snapshotter.deckhouse.io", Version: "v1alpha1", Kind: "DemoVirtualDiskSnapshot"},
		},
	}
}

// TestRun_NoSelectedNode_UsesRootRef verifies that when no SelectedNode is set,
// RestoreManifests is called with the root Snapshot NodeRef.
func TestRun_NoSelectedNode_UsesRootRef(t *testing.T) {
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(readySnapshot())

	cfg := baseConfig(src, dyn)

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	wantRef := aggapi.NodeRef{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       testSnap,
		Namespace:  testNS,
	}

	if src.gotRef != wantRef {
		t.Errorf("NodeRef: got %+v, want %+v", src.gotRef, wantRef)
	}
}

// TestRun_SelectedNode_UsesNodeRef verifies that when SelectedNodeKind/SelectedNodeName
// are set, RestoreManifests is called with the selected node's NodeRef (resolved via
// the RESTMapper), not the root Snapshot ref. The root Snapshot preflight still runs.
func TestRun_SelectedNode_UsesNodeRef(t *testing.T) {
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(readySnapshot())

	cfg := Config{
		Namespace:        testNS,
		Snapshot:         testSnap,
		SelectedNodeKind: "DemoVirtualDiskSnapshot",
		SelectedNodeName: "nss-child-abc123",
		Source:           src,
		Dynamic:          dyn,
		Mapper:           testMapperWithDomain(),
		Log:              discardLogger(),
		PollInterval:     time.Millisecond,
	}

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	wantRef := aggapi.NodeRef{
		APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "DemoVirtualDiskSnapshot",
		Name:       "nss-child-abc123",
		Namespace:  testNS,
	}

	if src.gotRef != wantRef {
		t.Errorf("NodeRef: got %+v, want %+v", src.gotRef, wantRef)
	}
}

// logCapture is a slog.Handler that records every log record for assertion in tests.
type logCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *logCapture) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.records = append(c.records, r)

	return nil
}

func (c *logCapture) WithAttrs(_ []slog.Attr) slog.Handler { return c }

func (c *logCapture) WithGroup(_ string) slog.Handler { return c }

// countMsg returns the number of captured records whose Message equals msg.
func (c *logCapture) countMsg(msg string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := 0

	for _, r := range c.records {
		if r.Message == msg {
			n++
		}
	}

	return n
}

// TestRun_SelectedNode_UnknownKindErrors verifies that an unresolvable SelectedNodeKind
// returns an error before calling RestoreManifests.
func TestRun_SelectedNode_UnknownKindErrors(t *testing.T) {
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(readySnapshot())

	cfg := Config{
		Namespace:        testNS,
		Snapshot:         testSnap,
		SelectedNodeKind: "NoSuchKind",
		SelectedNodeName: "foo",
		Source:           src,
		Dynamic:          dyn,
		Mapper:           testMapper(),
		Log:              discardLogger(),
		PollInterval:     time.Millisecond,
	}

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for unknown SelectedNodeKind, got nil")
	}

	if !contains(err.Error(), "resolve selected node") {
		t.Errorf("error should mention 'resolve selected node', got: %v", err)
	}

	if src.calls != 0 {
		t.Errorf("RestoreManifests should not be called on resolve error, got %d calls", src.calls)
	}
}

// TestRun_NormalRestore_NoWouldApplyLog verifies that a normal (non-dry-run) restore
// emits zero per-object "would apply" log lines. The implicit dry-run preflight must be
// silent; only "applied" lines appear from the real pass.
func TestRun_NormalRestore_NoWouldApplyLog(t *testing.T) {
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"), configMapManifest("cm-2"))}
	dyn := newFakeDynamic(readySnapshot())
	cap := &logCapture{}
	cfg := baseConfig(src, dyn)
	cfg.Log = slog.New(cap)

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if n := cap.countMsg("would apply"); n != 0 {
		t.Errorf("normal restore emitted %d 'would apply' log lines (want 0); implicit preflight must be silent", n)
	}

	if n := cap.countMsg("applied"); n < 1 {
		t.Errorf("normal restore emitted %d 'applied' log lines (want >=1)", n)
	}
}

// TestRun_DryRun_LogsWouldApply verifies that an explicit --dry-run restore
// emits per-object "would apply" log lines and zero "applied" lines.
func TestRun_DryRun_LogsWouldApply(t *testing.T) {
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(readySnapshot())
	cap := &logCapture{}
	cfg := baseConfig(src, dyn)
	cfg.Log = slog.New(cap)
	cfg.DryRun = true

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with DryRun: %v", err)
	}

	if n := cap.countMsg("would apply"); n < 1 {
		t.Errorf("--dry-run restore emitted %d 'would apply' log lines (want >=1)", n)
	}

	if n := cap.countMsg("applied"); n != 0 {
		t.Errorf("--dry-run restore emitted %d 'applied' log lines (want 0)", n)
	}
}
