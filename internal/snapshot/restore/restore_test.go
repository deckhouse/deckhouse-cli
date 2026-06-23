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
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
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
	snapshotGVR = schema.GroupVersionResource{Group: "storage.deckhouse.io", Version: "v1alpha1", Resource: "snapshots"}
	pvcGVR      = schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"}
	cmGVR       = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	pvGVR       = schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}
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
func testMapper() meta.RESTMapper {
	m := meta.NewDefaultRESTMapper(nil)
	m.Add(schema.GroupVersionKind{Group: "storage.deckhouse.io", Version: "v1alpha1", Kind: "Snapshot"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Version: "v1", Kind: "PersistentVolumeClaim"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Version: "v1", Kind: "PersistentVolume"}, meta.RESTScopeRoot)

	return m
}

// newFakeDynamic builds a fake dynamic client seeded with the given objects.
func newFakeDynamic(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	gvrToListKind := map[schema.GroupVersionResource]string{
		snapshotGVR: "SnapshotList",
		pvcGVR:      "PersistentVolumeClaimList",
		cmGVR:       "ConfigMapList",
		pvGVR:       "PersistentVolumeList",
	}

	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), gvrToListKind, objs...)
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
		"apiVersion": "storage.deckhouse.io/v1alpha1",
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
	dyn := newFakeDynamic(readySnapshot())

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
		APIVersion: "storage.deckhouse.io/v1alpha1",
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

// TestRun_UpdatePreservesLiveMetadata verifies a full-object update does not strip live
// server-managed metadata (finalizers, ownerReferences) absent from the restored manifest.
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
func TestRun_WaitBound(t *testing.T) {
	src := &stubSource{body: mustArray(t, pvcManifest("pvc-1", "Bound"))}
	dyn := newFakeDynamic(readySnapshot())

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
	dyn := newFakeDynamic(readySnapshot())

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

// TestRun_ImmutableUpdateActionable verifies that an Invalid update of a pre-existing
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
	dyn := newFakeDynamic(readySnapshot(), existing)

	dyn.PrependReactor("update", "persistentvolumeclaims", func(_ clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, kubeerrors.NewInvalid(
			schema.GroupKind{Kind: "PersistentVolumeClaim"},
			"pvc-1",
			field.ErrorList{field.Forbidden(field.NewPath("spec", "dataSourceRef"), "field is immutable")},
		)
	})

	err := Run(context.Background(), baseConfig(src, dyn))
	if err == nil {
		t.Fatal("expected error for immutable update, got nil")
	}

	if !contains(err.Error(), "immutable") || !contains(err.Error(), "delete it") {
		t.Errorf("error %q is not actionable about immutability", err.Error())
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
