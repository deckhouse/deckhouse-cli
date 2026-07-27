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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	stdruntime "runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
)

const (
	testNS                   = "default"
	testSnap                 = "my-snap"
	domainDiskAPIVersion     = "sds-unified-snapshots-poc.deckhouse.io/v1alpha1"
	volumeSnapshotAPIVersion = "snapshot.storage.k8s.io/v1"
)

var (
	snapshotGVR   = schema.GroupVersionResource{Group: "state-snapshotter.deckhouse.io", Version: "v1alpha1", Resource: "snapshots"}
	pvcGVR        = schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"}
	cmGVR         = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	pvGVR         = schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}
	vsGVR         = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}
	domainDiskGVR = schema.GroupVersionResource{Group: "sds-unified-snapshots-poc.deckhouse.io", Version: "v1alpha1", Resource: "demovirtualdisksnapshots"}
	scGVR         = schema.GroupVersionResource{Group: "storage.k8s.io", Version: "v1", Resource: "storageclasses"}
)

// stubSource records the call and returns a canned manifest array body.
type stubSource struct {
	body      []byte
	err       error
	onCall    func()
	responses map[string]stubSourceResponse

	gotRef      aggapi.NodeRef
	gotNS       string
	gotOpts     aggapi.RestoreScopeOptions
	gotRefs     []aggapi.NodeRef
	gotOptsList []aggapi.RestoreScopeOptions
	calls       int
}

type stubSourceResponse struct {
	body []byte
	err  error
}

func (s *stubSource) RestoreManifestsScoped(_ context.Context, ref aggapi.NodeRef, targetNamespace string, opts aggapi.RestoreScopeOptions) ([]byte, error) {
	s.calls++
	s.gotRef = ref
	s.gotNS = targetNamespace
	s.gotOpts = opts
	s.gotRefs = append(s.gotRefs, ref)
	s.gotOptsList = append(s.gotOptsList, opts)

	if s.onCall != nil {
		s.onCall()
	}

	if response, exists := s.responses[nodeRefKey(ref)]; exists {
		return response.body, response.err
	}

	return s.body, s.err
}

type typedControlPlaneError struct {
	cause error
}

func (e *typedControlPlaneError) Error() string {
	return "blocked control-plane call: " + e.cause.Error()
}

func (e *typedControlPlaneError) Unwrap() error {
	return e.cause
}

type controlledDeadlineContext struct {
	context.Context

	done chan struct{}
	mu   sync.RWMutex
	err  error
}

func newControlledDeadlineContext(parent context.Context) *controlledDeadlineContext {
	return &controlledDeadlineContext{
		Context: parent,
		done:    make(chan struct{}),
	}
}

func (c *controlledDeadlineContext) Done() <-chan struct{} {
	return c.done
}

func (c *controlledDeadlineContext) Err() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.err
}

func (c *controlledDeadlineContext) expire() {
	c.finish(context.DeadlineExceeded)
}

func (c *controlledDeadlineContext) cancel() {
	c.finish(context.Canceled)
}

func (c *controlledDeadlineContext) finish(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.err != nil {
		return
	}

	c.err = err
	close(c.done)
}

type blockingSource struct {
	entered  chan struct{}
	returned chan struct{}
}

func (s *blockingSource) RestoreManifestsScoped(
	ctx context.Context,
	_ aggapi.NodeRef,
	_ string,
	_ aggapi.RestoreScopeOptions,
) ([]byte, error) {
	close(s.entered)
	defer close(s.returned)

	<-ctx.Done()

	return nil, &typedControlPlaneError{cause: ctx.Err()}
}

func assertNoRestoreMutation(t *testing.T, src *stubSource, dyn *dynamicfake.FakeDynamicClient) {
	t.Helper()

	if src.calls != 0 {
		t.Errorf("RestoreManifestsScoped must not be called, got %d calls", src.calls)
	}

	for _, action := range dyn.Actions() {
		switch action.GetVerb() {
		case "create", "delete", "delete-collection", "patch", "update":
			t.Errorf("cluster mutation %s %s must not occur", action.GetVerb(), action.GetResource().Resource)
		}
	}
}

func assertNoPatchActions(t *testing.T, dyn *dynamicfake.FakeDynamicClient) {
	t.Helper()

	for _, action := range dyn.Actions() {
		if action.GetVerb() == "patch" {
			t.Errorf("Patch must not occur for %s", action.GetResource().Resource)
		}
	}
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
		{Group: "storage.k8s.io", Version: "v1"},
	})
	m.Add(schema.GroupVersionKind{Group: "state-snapshotter.deckhouse.io", Version: "v1alpha1", Kind: "Snapshot"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Version: "v1", Kind: "PersistentVolumeClaim"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Version: "v1", Kind: "PersistentVolume"}, meta.RESTScopeRoot)
	m.Add(schema.GroupVersionKind{Group: "snapshot.storage.k8s.io", Version: "v1", Kind: "VolumeSnapshot"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Group: "storage.k8s.io", Version: "v1", Kind: "StorageClass"}, meta.RESTScopeRoot)

	return m
}

func clusterScopeTestMapper() meta.RESTMapper {
	groupVersions := []schema.GroupVersion{
		{Group: "", Version: "v1"},
		{Group: "state-snapshotter.deckhouse.io", Version: "v1alpha1"},
		{Group: "apiextensions.k8s.io", Version: "v1"},
		{Group: "rbac.authorization.k8s.io", Version: "v1"},
		{Group: "sds-unified-snapshots-poc.deckhouse.io", Version: "v1alpha1"},
	}
	mapper := meta.NewDefaultRESTMapper(groupVersions)
	mapper.Add(schema.GroupVersionKind{Group: "state-snapshotter.deckhouse.io", Version: "v1alpha1", Kind: "Snapshot"}, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: "PersistentVolume"}, meta.RESTScopeRoot)
	mapper.Add(schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"}, meta.RESTScopeRoot)
	mapper.Add(schema.GroupVersionKind{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRole"}, meta.RESTScopeRoot)
	mapper.Add(schema.GroupVersionKind{Group: "sds-unified-snapshots-poc.deckhouse.io", Version: "v1alpha1", Kind: "DemoVirtualDiskSnapshot"}, meta.RESTScopeRoot)

	return mapper
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
			if patch.GetUID() == "" {
				patch.SetUID(types.UID("uid-" + name))
			}

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
		snapshotGVR:                         "SnapshotList",
		pvcGVR:                              "PersistentVolumeClaimList",
		cmGVR:                               "ConfigMapList",
		pvGVR:                               "PersistentVolumeList",
		{Version: "v1", Resource: "pods"}:   "PodList",
		{Version: "v1", Resource: "events"}: "EventList",
		vsGVR:                               "VolumeSnapshotList",
		domainDiskGVR:                       "DemoVirtualDiskSnapshotList",
		scGVR:                               "StorageClassList",
	}

	seededObjects := make([]runtime.Object, 0, len(objs))

	for _, original := range objs {
		obj := original.DeepCopyObject()
		gvk := obj.GetObjectKind().GroupVersionKind()
		if gvk.Empty() {
			seededObjects = append(seededObjects, obj)

			continue
		}

		if accessor, ok := obj.(metav1.Object); ok && accessor.GetUID() == "" {
			accessor.SetUID(types.UID("uid-" + accessor.GetName()))
		}

		gvr, _ := meta.UnsafeGuessKindToResource(gvk)
		gvrToListKind[gvr] = gvk.Kind + "List"
		seededObjects = append(seededObjects, obj)
	}

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		gvrToListKind,
		seededObjects...,
	)

	addSSAReactor(dyn)

	return dyn
}

type dynamicRequestInterceptor func(context.Context, string, schema.GroupVersionResource, string, string) error
type dynamicListInterceptor func(
	context.Context,
	schema.GroupVersionResource,
	string,
	metav1.ListOptions,
) (*unstructured.UnstructuredList, bool, error)
type dynamicPatchTransformer func(
	schema.GroupVersionResource,
	string,
	string,
	metav1.PatchOptions,
	*unstructured.Unstructured,
) (*unstructured.Unstructured, error)

type interceptingDynamicClient struct {
	dynamic.Interface
	intercept      dynamicRequestInterceptor
	interceptList  dynamicListInterceptor
	transformPatch dynamicPatchTransformer
}

func (c *interceptingDynamicClient) Resource(gvr schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	resource := c.Interface.Resource(gvr)

	return &interceptingNamespaceableResource{
		NamespaceableResourceInterface: resource,
		gvr:                            gvr,
		intercept:                      c.intercept,
		interceptList:                  c.interceptList,
		transformPatch:                 c.transformPatch,
	}
}

type interceptingNamespaceableResource struct {
	dynamic.NamespaceableResourceInterface
	gvr            schema.GroupVersionResource
	intercept      dynamicRequestInterceptor
	interceptList  dynamicListInterceptor
	transformPatch dynamicPatchTransformer
}

func (r *interceptingNamespaceableResource) Namespace(namespace string) dynamic.ResourceInterface {
	return &interceptingResource{
		ResourceInterface: r.NamespaceableResourceInterface.Namespace(namespace),
		gvr:               r.gvr,
		namespace:         namespace,
		intercept:         r.intercept,
		interceptList:     r.interceptList,
		transformPatch:    r.transformPatch,
	}
}

func (r *interceptingNamespaceableResource) Get(
	ctx context.Context,
	name string,
	opts metav1.GetOptions,
	subresources ...string,
) (*unstructured.Unstructured, error) {
	if r.intercept != nil {
		if err := r.intercept(ctx, "get", r.gvr, "", name); err != nil {
			return nil, err
		}
	}

	return r.NamespaceableResourceInterface.Get(ctx, name, opts, subresources...)
}

func (r *interceptingNamespaceableResource) List(
	ctx context.Context,
	opts metav1.ListOptions,
) (*unstructured.UnstructuredList, error) {
	if r.interceptList != nil {
		result, handled, err := r.interceptList(ctx, r.gvr, "", opts)
		if handled {
			return result, err
		}
	}

	if r.intercept != nil {
		if err := r.intercept(ctx, "list", r.gvr, "", ""); err != nil {
			return nil, err
		}
	}

	return r.NamespaceableResourceInterface.List(ctx, opts)
}

func (r *interceptingNamespaceableResource) Patch(
	ctx context.Context,
	name string,
	patchType types.PatchType,
	data []byte,
	opts metav1.PatchOptions,
	subresources ...string,
) (*unstructured.Unstructured, error) {
	if r.intercept != nil {
		if err := r.intercept(ctx, "patch", r.gvr, "", name); err != nil {
			return nil, err
		}
	}

	result, err := r.NamespaceableResourceInterface.Patch(ctx, name, patchType, data, opts, subresources...)
	if err != nil || r.transformPatch == nil {
		return result, err
	}

	return r.transformPatch(r.gvr, "", name, opts, result)
}

type interceptingResource struct {
	dynamic.ResourceInterface
	gvr            schema.GroupVersionResource
	namespace      string
	intercept      dynamicRequestInterceptor
	interceptList  dynamicListInterceptor
	transformPatch dynamicPatchTransformer
}

func (r *interceptingResource) Get(
	ctx context.Context,
	name string,
	opts metav1.GetOptions,
	subresources ...string,
) (*unstructured.Unstructured, error) {
	if r.intercept != nil {
		if err := r.intercept(ctx, "get", r.gvr, r.namespace, name); err != nil {
			return nil, err
		}
	}

	return r.ResourceInterface.Get(ctx, name, opts, subresources...)
}

func (r *interceptingResource) List(
	ctx context.Context,
	opts metav1.ListOptions,
) (*unstructured.UnstructuredList, error) {
	if r.interceptList != nil {
		result, handled, err := r.interceptList(ctx, r.gvr, r.namespace, opts)
		if handled {
			return result, err
		}
	}

	if r.intercept != nil {
		if err := r.intercept(ctx, "list", r.gvr, r.namespace, ""); err != nil {
			return nil, err
		}
	}

	return r.ResourceInterface.List(ctx, opts)
}

func (r *interceptingResource) Patch(
	ctx context.Context,
	name string,
	patchType types.PatchType,
	data []byte,
	opts metav1.PatchOptions,
	subresources ...string,
) (*unstructured.Unstructured, error) {
	if r.intercept != nil {
		if err := r.intercept(ctx, "patch", r.gvr, r.namespace, name); err != nil {
			return nil, err
		}
	}

	result, err := r.ResourceInterface.Patch(ctx, name, patchType, data, opts, subresources...)
	if err != nil || r.transformPatch == nil {
		return result, err
	}

	return r.transformPatch(r.gvr, r.namespace, name, opts, result)
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
			"namespace":       testNS,
			"name":            testSnap,
			"resourceVersion": "100",
		},
		"status": map[string]interface{}{
			"boundSnapshotContentName": "snapcontent-1",
			"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "True"},
			},
		},
	}}
}

func snapshotWithChildren(snapshot *unstructured.Unstructured, refs ...map[string]interface{}) *unstructured.Unstructured {
	status, _ := snapshot.Object["status"].(map[string]interface{})
	children := make([]interface{}, 0, len(refs))

	for _, ref := range refs {
		children = append(children, ref)
	}

	status["childrenSnapshotRefs"] = children

	return snapshot
}

func snapshotChildRef(apiVersion, kind, name string) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": apiVersion,
		"kind":       kind,
		"name":       name,
	}
}

func snapshotListPage(continueToken string, items ...*unstructured.Unstructured) *unstructured.UnstructuredList {
	page := &unstructured.UnstructuredList{
		Items: make([]unstructured.Unstructured, 0, len(items)),
	}
	page.SetContinue(continueToken)

	for _, item := range items {
		page.Items = append(page.Items, *item.DeepCopy())
	}

	return page
}

func setSnapshotSourceRef(obj *unstructured.Unstructured, apiVersion, kind, name string) {
	status, _ := obj.Object["status"].(map[string]interface{})
	status["sourceRef"] = map[string]interface{}{
		"apiVersion": apiVersion,
		"kind":       kind,
		"namespace":  testNS,
		"name":       name,
		"uid":        name + "-uid",
	}
}

func setSnapshotMode(obj *unstructured.Unstructured, mode snapshotapi.SnapshotMode) {
	obj.Object["spec"] = map[string]interface{}{"mode": string(mode)}
}

func setImportSourceRefAnnotation(t *testing.T, obj *unstructured.Unstructured, apiVersion, kind, name string) {
	t.Helper()

	value, err := json.Marshal(snapshotapi.ImportSourceRef{
		APIVersion: apiVersion,
		Kind:       kind,
		Name:       name,
	})
	if err != nil {
		t.Fatalf("marshal test import source ref: %v", err)
	}

	obj.SetAnnotations(map[string]string{snapshotapi.AnnotationImportSourceRef: string(value)})
}

// notReadySnapshot returns a root Snapshot with Ready=False (e.g. ChildSnapshotDeleted).
func notReadySnapshot() *unstructured.Unstructured {
	snap := readySnapshot()
	_ = unstructured.SetNestedSlice(snap.Object, []interface{}{
		map[string]interface{}{"type": "Ready", "status": "False", "reason": "ChildSnapshotDeleted"},
	}, "status", "conditions")

	return snap
}

// readyDomainDiskSnapshot returns a DemoVirtualDiskSnapshot that passes selected-node preflight.
func readyDomainDiskSnapshot(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": domainDiskAPIVersion,
		"kind":       "DemoVirtualDiskSnapshot",
		"metadata": map[string]interface{}{
			"namespace":       testNS,
			"name":            name,
			"resourceVersion": "101",
		},
		"status": map[string]interface{}{
			"boundSnapshotContentName": "content-disk-1",
			"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "True"},
			},
		},
	}}
}

func readyGeneratedSnapshot(apiVersion, kind, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata": map[string]interface{}{
			"namespace":       testNS,
			"name":            name,
			"resourceVersion": "101",
		},
		"status": map[string]interface{}{
			"boundSnapshotContentName": "content-" + name,
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
		"apiVersion": volumeSnapshotAPIVersion,
		"kind":       "VolumeSnapshot",
		"metadata": map[string]interface{}{
			"namespace":       testNS,
			"name":            name,
			"resourceVersion": "101",
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

// pvcManifestSC is pvcManifest plus an explicit spec.storageClassName, used by the
// volumeBindingMode wait tests. An empty storageClassName is left unset, matching how a
// real manifest omits the field entirely rather than encoding it as an empty string.
func pvcManifestSC(name, phase, storageClassName string) map[string]interface{} {
	obj := pvcManifest(name, phase)

	if storageClassName != "" {
		spec, _ := obj["spec"].(map[string]interface{})
		spec["storageClassName"] = storageClassName
	}

	return obj
}

func restoredPVCObject(name, phase, storageClassName string, terminating bool) *unstructured.Unstructured {
	obj := pvcManifestSC(name, phase, storageClassName)
	metadata, _ := obj["metadata"].(map[string]interface{})
	metadata["namespace"] = testNS
	metadata["uid"] = "uid-" + name

	if phase == pvcPhaseBound {
		spec, _ := obj["spec"].(map[string]interface{})
		spec["volumeName"] = "pv-" + name
	}

	if terminating {
		metadata["deletionTimestamp"] = "2026-07-23T12:00:00Z"
	}

	return &unstructured.Unstructured{Object: obj}
}

func boundPVObject(pvcName string) *unstructured.Unstructured {
	return boundPVObjectWithUID(pvcName, "uid-pv-"+pvcName)
}

func boundPVObjectWithUID(pvcName, uid string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "PersistentVolume",
		"metadata": map[string]interface{}{
			"name": "pv-" + pvcName,
			"uid":  uid,
		},
		"spec": map[string]interface{}{
			"claimRef": map[string]interface{}{
				"namespace": testNS,
				"name":      pvcName,
				"uid":       "uid-" + pvcName,
			},
		},
	}}
}

func podConsumerObject(name, claimName string, terminal bool) *unstructured.Unstructured {
	phase := "Running"
	if terminal {
		phase = "Succeeded"
	}

	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"namespace": testNS,
			"name":      name,
		},
		"spec": map[string]interface{}{
			"volumes": []interface{}{
				map[string]interface{}{
					"name": "data",
					"persistentVolumeClaim": map[string]interface{}{
						"claimName": claimName,
					},
				},
			},
		},
		"status": map[string]interface{}{"phase": phase},
	}}
}

func pvcEventObject(name, pvcName, reason, message, timestamp string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Event",
		"metadata": map[string]interface{}{
			"namespace": testNS,
			"name":      name,
		},
		"involvedObject": map[string]interface{}{
			"kind":      pvcKind,
			"namespace": testNS,
			"name":      pvcName,
			"uid":       "uid-" + pvcName,
		},
		"reason":        reason,
		"message":       message,
		"lastTimestamp": timestamp,
	}}
}

// storageClassObj returns a StorageClass object for the volumeBindingMode wait tests.
// An empty bindingMode leaves the field unset, matching a real StorageClass that omits
// volumeBindingMode and so defaults to Immediate.
func storageClassObj(name, bindingMode string, isDefault bool) *unstructured.Unstructured {
	metadata := map[string]interface{}{"name": name}

	if isDefault {
		metadata["annotations"] = map[string]interface{}{
			defaultStorageClassAnnotation: "true",
		}
	}

	obj := map[string]interface{}{
		"apiVersion": "storage.k8s.io/v1",
		"kind":       "StorageClass",
		"metadata":   metadata,
	}

	if bindingMode != "" {
		obj["volumeBindingMode"] = bindingMode
	}

	return &unstructured.Unstructured{Object: obj}
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

func awaitBlockedRun(t *testing.T, cfg Config, entered <-chan struct{}) error {
	t.Helper()

	result := make(chan error, 1)
	go func() {
		result <- Run(context.Background(), cfg)
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("control-plane call was not reached")
	}

	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not return after the control-plane request timeout")

		return nil
	}
}

func assertReturnedBeforeRun(t *testing.T, returned <-chan struct{}) {
	t.Helper()

	select {
	case <-returned:
	default:
		t.Fatal("blocked control-plane call did not return before Run completed")
	}
}

func awaitTestSignal(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case <-signal:
	case <-timer.C:
		t.Fatal(failure)
	}
}

func awaitTestError(t *testing.T, result <-chan error, failure string) error {
	t.Helper()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case err := <-result:
		return err
	case <-timer.C:
		t.Fatal(failure)

		return nil
	}
}

func TestRun_ControlPlaneSourceTimeoutPreservesType(t *testing.T) {
	source := &blockingSource{
		entered:  make(chan struct{}),
		returned: make(chan struct{}),
	}
	dyn := newFakeDynamic(readySnapshot())
	cfg := baseConfig(source, dyn)
	cfg.ControlPlaneTimeout = 250 * time.Millisecond

	err := awaitBlockedRun(t, cfg, source.entered)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run error = %v, want context.DeadlineExceeded", err)
	}

	var typedErr *typedControlPlaneError
	if !errors.As(err, &typedErr) {
		t.Fatalf("Run error = %v, want typedControlPlaneError", err)
	}

	for _, want := range []string{"fetching restore manifests", "Snapshot default/my-snap"} {
		if !contains(err.Error(), want) {
			t.Errorf("Run error = %q, want %q", err, want)
		}
	}

	assertNoPatchActions(t, dyn)
	assertReturnedBeforeRun(t, source.returned)
}

func TestRun_ControlPlaneTimeoutsBeforeMutation(t *testing.T) {
	tests := []struct {
		name      string
		verb      string
		gvr       schema.GroupVersionResource
		object    string
		wantPhase string
		configure func(*dynamicfake.FakeDynamicClient) Config
	}{
		{
			name:      "root Get",
			verb:      "get",
			gvr:       snapshotGVR,
			object:    testSnap,
			wantPhase: "getting snapshot node Snapshot default/my-snap",
			configure: func(base *dynamicfake.FakeDynamicClient) Config {
				return baseConfig(&stubSource{body: mustArray(t, configMapManifest("unused"))}, base)
			},
		},
		{
			name:      "selected child Get",
			verb:      "get",
			gvr:       domainDiskGVR,
			object:    "selected-child",
			wantPhase: "getting snapshot child DemoVirtualDiskSnapshot default/selected-child",
			configure: func(base *dynamicfake.FakeDynamicClient) Config {
				cfg := baseConfig(&stubSource{body: mustArray(t, configMapManifest("unused"))}, base)
				cfg.Mapper = testMapperWithDomain()
				cfg.SelectedNodeKind = "DemoVirtualDiskSnapshot"
				cfg.SelectedNodeName = "selected-child"

				return cfg
			},
		},
		{
			name:      "missing child List",
			verb:      "list",
			gvr:       snapshotGVR,
			wantPhase: "listing snapshot child Snapshot default/missing-child to prove absence",
			configure: func(base *dynamicfake.FakeDynamicClient) Config {
				cfg := baseConfig(&stubSource{body: mustArray(t, configMapManifest("unused"))}, base)
				cfg.SelectedNodeKind = "ConfigMap"
				cfg.SelectedNodeName = "unrelated"

				return cfg
			},
		},
		{
			name:      "leaf Get",
			verb:      "get",
			gvr:       vsGVR,
			object:    "vs-1",
			wantPhase: "getting volume-snapshot leaf VolumeSnapshot default/vs-1",
			configure: func(base *dynamicfake.FakeDynamicClient) Config {
				return baseConfig(&stubSource{body: mustArray(t, pvcManifest("blocked-pvc", ""))}, base)
			},
		},
		{
			name:      "dry-run Patch",
			verb:      "patch",
			gvr:       cmGVR,
			object:    "blocked-config",
			wantPhase: `dry-run applying v1/ConfigMap "blocked-config"`,
			configure: func(base *dynamicfake.FakeDynamicClient) Config {
				return baseConfig(&stubSource{body: mustArray(t, configMapManifest("blocked-config"))}, base)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entered := make(chan struct{})
			returned := make(chan struct{})
			base := newFakeDynamic(controlPlaneTimeoutObjects(tc.name)...)

			dyn := &interceptingDynamicClient{
				Interface: base,
				intercept: func(
					ctx context.Context,
					verb string,
					gvr schema.GroupVersionResource,
					_ string,
					name string,
				) error {
					if verb != tc.verb || gvr != tc.gvr || (tc.object != "" && name != tc.object) {
						return nil
					}

					close(entered)
					defer close(returned)

					<-ctx.Done()

					return ctx.Err()
				},
			}

			cfg := tc.configure(base)
			cfg.Dynamic = dyn
			cfg.ControlPlaneTimeout = 250 * time.Millisecond

			err := awaitBlockedRun(t, cfg, entered)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Run error = %v, want context.DeadlineExceeded", err)
			}

			if !contains(err.Error(), tc.wantPhase) {
				t.Errorf("Run error = %q, want phase %q", err, tc.wantPhase)
			}

			assertNoPatchActions(t, base)
			assertReturnedBeforeRun(t, returned)
		})
	}
}

func controlPlaneTimeoutObjects(testName string) []runtime.Object {
	root := readySnapshot()

	switch testName {
	case "selected child Get":
		child := readyDomainDiskSnapshot("selected-child")
		snapshotWithChildren(
			root,
			snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
		)

		return []runtime.Object{root, child}
	case "missing child List":
		snapshotWithChildren(
			root,
			snapshotChildRef(root.GetAPIVersion(), root.GetKind(), "missing-child"),
		)

		return []runtime.Object{root}
	default:
		return []runtime.Object{root, readyVolumeSnapshot("vs-1")}
	}
}

func TestRun_ControlPlaneRESTMappingTimeout(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan struct{})
	mapper := restMappingInterceptor{
		RESTMapper: testMapper(),
		intercept: func(schema.GroupKind, ...string) (*meta.RESTMapping, error) {
			close(entered)
			defer close(returned)

			<-release

			return nil, errors.New("released mapper")
		},
	}

	dyn := newFakeDynamic(readySnapshot())
	cfg := baseConfig(&stubSource{body: mustArray(t, configMapManifest("unused"))}, dyn)
	cfg.Mapper = mapper
	cfg.ControlPlaneTimeout = 250 * time.Millisecond

	err := awaitBlockedRun(t, cfg, entered)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run error = %v, want context.DeadlineExceeded", err)
	}

	if !contains(err.Error(), "resolving snapshot hierarchy node Snapshot default/my-snap") {
		t.Fatalf("Run error = %q, want active RESTMapping phase", err)
	}

	assertNoPatchActions(t, dyn)

	close(release)

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("timed-out RESTMapping goroutine did not stop after discovery returned")
	}
}

func TestRun_ControlPlaneOuterCancellationPreservesCause(t *testing.T) {
	source := &blockingSource{
		entered:  make(chan struct{}),
		returned: make(chan struct{}),
	}
	dyn := newFakeDynamic(readySnapshot())
	cfg := baseConfig(source, dyn)
	cfg.ControlPlaneTimeout = time.Hour

	ctx, cancel := context.WithCancelCause(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- Run(ctx, cfg)
	}()

	select {
	case <-source.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Source call was not reached")
	}

	cause := errors.New("operator stopped restore")
	cancel(cause)

	var err error
	select {
	case err = <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not return after outer cancellation")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run error = %v, want context.Canceled", err)
	}

	if !errors.Is(err, cause) {
		t.Errorf("Run error = %v, want outer cancellation cause", err)
	}

	if errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run error = %v, do not want context.DeadlineExceeded", err)
	}

	assertNoPatchActions(t, dyn)
	assertReturnedBeforeRun(t, source.returned)
}

func TestRun_RealApplyTimeoutReportsPartialMutation(t *testing.T) {
	base := newFakeDynamic(readySnapshot())
	entered := make(chan struct{})
	returned := make(chan struct{})
	patchCalls := 0

	dyn := &interceptingDynamicClient{
		Interface: base,
		intercept: func(
			ctx context.Context,
			verb string,
			gvr schema.GroupVersionResource,
			_ string,
			_ string,
		) error {
			if verb != "patch" || gvr != cmGVR {
				return nil
			}

			patchCalls++
			if patchCalls != 4 {
				return nil
			}

			close(entered)
			defer close(returned)

			<-ctx.Done()

			return ctx.Err()
		},
	}

	source := &stubSource{body: mustArray(t,
		configMapManifest("first-config"),
		configMapManifest("second-config"),
	)}
	cfg := baseConfig(source, dyn)
	cfg.ControlPlaneTimeout = 250 * time.Millisecond

	err := awaitBlockedRun(t, cfg, entered)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run error = %v, want context.DeadlineExceeded", err)
	}

	for _, want := range []string{
		"restore apply stopped after 1 of 2 objects completed",
		"cluster may be partially applied",
		`applying v1/ConfigMap "second-config"`,
		"active object's outcome is unknown",
	} {
		if !contains(err.Error(), want) {
			t.Errorf("Run error = %q, want %q", err, want)
		}
	}

	if patchCalls != 4 {
		t.Errorf("Patch attempts = %d, want 4 (two dry-run and two real)", patchCalls)
	}

	assertReturnedBeforeRun(t, returned)
}

func TestRun_ControlPlaneTimeoutIsRenewedPerRequest(t *testing.T) {
	base := newFakeDynamic(readySnapshot())
	deadlines := make([]time.Time, 0, 4)

	dyn := &interceptingDynamicClient{
		Interface: base,
		intercept: func(
			ctx context.Context,
			verb string,
			gvr schema.GroupVersionResource,
			_ string,
			_ string,
		) error {
			if verb != "patch" || gvr != cmGVR {
				return nil
			}

			deadline, ok := ctx.Deadline()
			if !ok {
				return errors.New("Patch context has no deadline")
			}

			deadlines = append(deadlines, deadline)

			return nil
		},
	}

	source := &stubSource{body: mustArray(t,
		configMapManifest("first-config"),
		configMapManifest("second-config"),
	)}
	cfg := baseConfig(source, dyn)
	cfg.ControlPlaneTimeout = time.Minute

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(deadlines) != 4 {
		t.Fatalf("Patch deadline count = %d, want 4", len(deadlines))
	}

	for i := 1; i < len(deadlines); i++ {
		if !deadlines[i].After(deadlines[i-1]) {
			t.Errorf(
				"Patch deadline %d = %s, want later than request %d deadline %s",
				i,
				deadlines[i],
				i-1,
				deadlines[i-1],
			)
		}
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

func TestRun_RejectsClusterScopedObjectsBeforePatch(t *testing.T) {
	tests := []struct {
		name       string
		apiVersion string
		kind       string
		objectName string
	}{
		{name: "persistent volume", apiVersion: "v1", kind: "PersistentVolume", objectName: "pv-1"},
		{name: "custom resource definition", apiVersion: "apiextensions.k8s.io/v1", kind: "CustomResourceDefinition", objectName: "widgets.example.io"},
		{name: "cluster role", apiVersion: "rbac.authorization.k8s.io/v1", kind: "ClusterRole", objectName: "restore-admin"},
		{name: "domain-shaped response", apiVersion: domainDiskAPIVersion, kind: "DemoVirtualDiskSnapshot", objectName: "domain-cluster-object"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			manifest := map[string]interface{}{
				"apiVersion": tc.apiVersion,
				"kind":       tc.kind,
				"metadata":   map[string]interface{}{"name": tc.objectName, "namespace": testNS},
			}
			src := &stubSource{body: mustArray(t, manifest)}
			dyn := newFakeDynamic(readySnapshot())
			cfg := baseConfig(src, dyn)
			cfg.Mapper = clusterScopeTestMapper()

			err := Run(context.Background(), cfg)
			if err == nil {
				t.Fatal("Run unexpectedly accepted a cluster-scoped object")
			}

			for _, part := range []string{
				`apiVersion="` + tc.apiVersion + `"`,
				`kind="` + tc.kind + `"`,
				`name="` + tc.objectName + `"`,
				"namespace restore does not support cluster-scoped object",
				"remove it from the snapshot or edited manifests before retrying",
			} {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("Run error %q does not contain %q", err, part)
				}
			}

			assertNoPatchActions(t, dyn)
		})
	}
}

func TestRun_EditRejectsClusterScopedObjectBeforePatch(t *testing.T) {
	dir := t.TempDir()
	editedContent := `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.io
`
	contentFile := writeEditorContent(t, dir, "cluster-scoped.yaml", editedContent)
	editor := fakeEditorScript(t, dir, fmt.Sprintf(`cp '%s' "$1"`, contentFile))
	t.Setenv("EDITOR", editor)
	t.Setenv("KUBE_EDITOR", "")

	src := &stubSource{body: mustArray(t, configMapManifest("before-edit"))}
	dyn := newFakeDynamic(readySnapshot())
	cfg := baseConfig(src, dyn)
	cfg.Edit = true
	cfg.Mapper = clusterScopeTestMapper()

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("Run unexpectedly accepted an edited cluster-scoped object")
	}

	for _, part := range []string{
		`apiVersion="apiextensions.k8s.io/v1"`,
		`kind="CustomResourceDefinition"`,
		`name="widgets.example.io"`,
		"namespace restore does not support cluster-scoped object",
		"remove it from the snapshot or edited manifests before retrying",
	} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("Run error %q does not contain %q", err, part)
		}
	}

	assertNoPatchActions(t, dyn)
}

func TestRun_StagesAggregateAboveResponseLimitInPostOrder(t *testing.T) {
	const payloadBytes = 33 << 20

	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)

	var requestCount atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		count := requestCount.Add(1)

		if scope := request.URL.Query().Get("scope"); scope != string(aggapi.RestoreScopeNode) {
			t.Errorf("request %d scope = %q, want %q", count, scope, aggapi.RestoreScopeNode)
		}

		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(request.URL.Path, "/nss-child-large/"):
			writeLargeManifestResponse(t, w, "child-large", payloadBytes)
		case strings.Contains(request.URL.Path, "/my-snap/"):
			writeLargeManifestResponse(t, w, "root-large", payloadBytes)
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)

	mapper := testMapperWithDomain()
	aggClient, err := aggapi.NewClientForConfig(&rest.Config{
		Host:    server.URL,
		Timeout: 30 * time.Second,
	}, mapper)
	if err != nil {
		t.Fatalf("NewClientForConfig: %v", err)
	}

	child := readyDomainDiskSnapshot("nss-child-large")
	root := snapshotWithChildren(
		readySnapshot(),
		snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
	)
	dyn := newFakeDynamic(root, child)

	var (
		patchOrder   []string
		maxTempBytes int64
	)
	dyn.PrependReactor("patch", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		patchAction, ok := action.(clienttesting.PatchAction)
		if !ok {
			t.Fatalf("patch action has type %T", action)
		}

		var obj unstructured.Unstructured
		if err := json.Unmarshal(patchAction.GetPatch(), &obj.Object); err != nil {
			t.Fatalf("decode patch: %v", err)
		}
		patchOrder = append(patchOrder, obj.GetName())

		tempBytes := stagedResourceUsage(t, tempDir)
		if tempBytes > maxTempBytes {
			maxTempBytes = tempBytes
		}

		return true, obj.DeepCopy(), nil
	})

	stdruntime.GC()

	var baseline stdruntime.MemStats
	stdruntime.ReadMemStats(&baseline)

	stopSampling := make(chan struct{})
	heapPeak := make(chan uint64, 1)
	go sampleHeapPeak(stopSampling, heapPeak, baseline.HeapAlloc)

	cfg := baseConfig(aggClient, dyn)
	cfg.Mapper = mapper
	runErr := Run(context.Background(), cfg)
	close(stopSampling)
	peakHeap := <-heapPeak

	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}

	wantOrder := []string{"child-large", "root-large", "child-large", "root-large"}
	if !slices.Equal(patchOrder, wantOrder) {
		t.Fatalf("patch order = %v, want %v", patchOrder, wantOrder)
	}
	if requestCount.Load() != 2 {
		t.Fatalf("aggregated API requests = %d, want 2 bounded node responses", requestCount.Load())
	}

	if maxTempBytes <= aggapi.DefaultMaxResponseBytes {
		t.Fatalf("peak staged bytes = %d, want aggregate above per-response limit %d", maxTempBytes, aggapi.DefaultMaxResponseBytes)
	}
	if maxTempBytes > 2*payloadBytes+(1<<20) {
		t.Fatalf("peak staged bytes = %d, want at most %d", maxTempBytes, 2*payloadBytes+(1<<20))
	}

	const heapGrowthLimit = 768 << 20
	if growth := peakHeap - baseline.HeapAlloc; growth > heapGrowthLimit {
		t.Fatalf("peak heap growth = %d bytes, want at most %d", growth, heapGrowthLimit)
	}

	stageFiles, err := filepath.Glob(filepath.Join(tempDir, "d8-restore-stage-*.jsonl"))
	if err != nil {
		t.Fatalf("glob staging files: %v", err)
	}
	if len(stageFiles) != 0 {
		t.Fatalf("staging files remain after success: %v", stageFiles)
	}

	t.Logf(
		"resource evidence: response_payload_bytes=%d aggregate_staged_bytes=%d heap_growth_bytes=%d apply_order=%v",
		payloadBytes,
		maxTempBytes,
		peakHeap-baseline.HeapAlloc,
		patchOrder,
	)
}

func TestRun_CanonicalDuplicateIsCoalesced(t *testing.T) {
	child := readyDomainDiskSnapshot("nss-child-duplicate")
	root := snapshotWithChildren(
		readySnapshot(),
		snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
	)
	duplicate := configMapManifest("shared")
	childRef := aggapi.NodeRef{
		APIVersion: child.GetAPIVersion(),
		Kind:       child.GetKind(),
		Name:       child.GetName(),
		Namespace:  testNS,
	}
	rootRef := aggapi.NodeRef{
		APIVersion: snapshotapi.StorageGroup + "/" + snapshotapi.Version,
		Kind:       snapshotKind,
		Name:       testSnap,
		Namespace:  testNS,
	}
	src := &stubSource{responses: map[string]stubSourceResponse{
		nodeRefKey(childRef): {body: mustArray(t, duplicate)},
		nodeRefKey(rootRef):  {body: mustArray(t, duplicate)},
	}}
	dyn := newFakeDynamic(root, child)

	patches := 0
	dyn.PrependReactor("patch", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		patches++

		return false, nil, nil
	})

	cfg := baseConfig(src, dyn)
	cfg.Mapper = testMapperWithDomain()
	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if src.calls != 2 {
		t.Fatalf("bounded node fetches = %d, want 2", src.calls)
	}
	if patches != 2 {
		t.Fatalf("ConfigMap patches = %d, want one dry-run plus one real apply", patches)
	}
}

type manifestStageFaults struct {
	stages     []*manifestStage
	files      []*os.File
	labels     map[string]string
	closeErrs  map[string]error
	removeErrs map[string]error
	order      []string
}

func newManifestStageFaults(closeErrs, removeErrs map[string]error) *manifestStageFaults {
	return &manifestStageFaults{
		labels:     make(map[string]string),
		closeErrs:  closeErrs,
		removeErrs: removeErrs,
	}
}

func (f *manifestStageFaults) operations() manifestStageOperations {
	return manifestStageOperations{
		created: func(stage *manifestStage) {
			label := fmt.Sprintf("stage-%d", len(f.stages)+1)
			f.labels[stage.path] = label
			f.stages = append(f.stages, stage)
			f.files = append(f.files, stage.file)
		},
		closeFile: func(file *os.File) error {
			label := f.labels[file.Name()]
			f.order = append(f.order, "close "+label)

			return errors.Join(file.Close(), f.closeErrs[label])
		},
		removeFile: func(path string) error {
			label := f.labels[path]
			f.order = append(f.order, "remove "+label)

			if err := f.removeErrs[label]; err != nil {
				return err
			}

			return os.Remove(path)
		},
	}
}

func (f *manifestStageFaults) assertCleanup(
	t *testing.T,
	runErr error,
	wantCauses []error,
	wantOrder []string,
) {
	t.Helper()

	if runErr == nil {
		t.Fatal("Run unexpectedly succeeded despite manifest staging cleanup failure")
	}

	for _, cause := range wantCauses {
		if !errors.Is(runErr, cause) {
			t.Errorf("Run error = %v, want cause %v", runErr, cause)
		}
	}

	if !slices.Equal(f.order, wantOrder) {
		t.Errorf("cleanup order = %v, want %v", f.order, wantOrder)
	}

	for i, file := range f.files {
		if _, err := file.Write([]byte("closed")); !errors.Is(err, os.ErrClosed) {
			t.Errorf("stage-%d descriptor write error = %v, want os.ErrClosed", i+1, err)
		}
	}

	residue := make([]string, 0, len(f.stages))
	for _, stage := range f.stages {
		if _, err := os.Stat(stage.path); err != nil {
			t.Errorf("stat known staging residue %q: %v", stage.path, err)

			continue
		}

		residue = append(residue, stage.path)
	}

	if len(residue) != len(f.stages) {
		t.Errorf("known staging residue count = %d, want %d", len(residue), len(f.stages))
	}

	f.closeErrs = nil
	f.removeErrs = nil

	for _, stage := range f.stages {
		path := stage.path
		if err := stage.cleanup(); err != nil {
			t.Errorf("cleanup staging residue %q after fault release: %v", path, err)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("staging residue %q remains after fault release: %v", path, err)
		}
	}

	t.Logf(
		"cleanup evidence: causes=%d cleanup_order=%v residue=%d descriptors_closed=%d",
		len(wantCauses),
		wantOrder,
		len(residue),
		len(f.files),
	)
}

func TestManifestStage_UsesOneDescriptorAndCleansTemporaryFile(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)

	before := countOpenDescriptors(t)
	cfg := applyDefaults(Config{})
	stage, err := newManifestStage(cfg)
	if err != nil {
		t.Fatalf("newManifestStage: %v", err)
	}

	during := countOpenDescriptors(t)
	if delta := during - before; delta != 1 {
		if cleanupErr := stage.cleanup(); cleanupErr != nil {
			t.Errorf("cleanup staging after descriptor assertion: %v", cleanupErr)
		}

		t.Fatalf("open descriptor delta with staging active = %d, want 1", delta)
	}

	stagePath := stage.path
	if cleanupErr := stage.cleanup(); cleanupErr != nil {
		t.Fatalf("cleanup staging: %v", cleanupErr)
	}

	after := countOpenDescriptors(t)
	if after > before {
		t.Fatalf("open descriptors after cleanup = %d, baseline %d", after, before)
	}
	if _, statErr := os.Stat(stagePath); !os.IsNotExist(statErr) {
		t.Fatalf("staging path remains after cleanup: stat error = %v", statErr)
	}

	t.Logf("descriptor evidence: baseline=%d staged=%d after_cleanup=%d", before, during, after)
}

func TestRun_ReportsManifestStageCleanupFailures(t *testing.T) {
	t.Run("success with remove failure", func(t *testing.T) {
		tempDir := t.TempDir()
		t.Setenv("TMPDIR", tempDir)

		removeErr := errors.New("injected original stage remove failure")
		faults := newManifestStageFaults(nil, map[string]error{"stage-1": removeErr})
		src := &stubSource{body: mustArray(t, configMapManifest("cleanup-success"))}
		dyn := newFakeDynamic(readySnapshot())
		cfg := baseConfig(src, dyn)
		cfg.manifestStageOps = faults.operations()

		runErr := Run(context.Background(), cfg)
		faults.assertCleanup(
			t,
			runErr,
			[]error{removeErr},
			[]string{"close stage-1", "remove stage-1"},
		)
	})

	t.Run("late fetch error with close and remove failures", func(t *testing.T) {
		tempDir := t.TempDir()
		t.Setenv("TMPDIR", tempDir)

		primaryErr := errors.New("injected late root fetch failure")
		closeErr := errors.New("injected original stage close failure")
		removeErr := errors.New("injected original stage remove failure")
		faults := newManifestStageFaults(
			map[string]error{"stage-1": closeErr},
			map[string]error{"stage-1": removeErr},
		)
		child := readyDomainDiskSnapshot("nss-child-cleanup")
		root := snapshotWithChildren(
			readySnapshot(),
			snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
		)
		childRef := aggapi.NodeRef{
			APIVersion: child.GetAPIVersion(),
			Kind:       child.GetKind(),
			Name:       child.GetName(),
			Namespace:  testNS,
		}
		rootRef := aggapi.NodeRef{
			APIVersion: snapshotapi.StorageGroup + "/" + snapshotapi.Version,
			Kind:       snapshotKind,
			Name:       testSnap,
			Namespace:  testNS,
		}
		src := &stubSource{responses: map[string]stubSourceResponse{
			nodeRefKey(childRef): {body: mustArray(t, configMapManifest("staged-before-failure"))},
			nodeRefKey(rootRef):  {err: primaryErr},
		}}
		dyn := newFakeDynamic(root, child)
		cfg := baseConfig(src, dyn)
		cfg.Mapper = testMapperWithDomain()
		cfg.manifestStageOps = faults.operations()

		runErr := Run(context.Background(), cfg)
		faults.assertCleanup(
			t,
			runErr,
			[]error{primaryErr, closeErr, removeErr},
			[]string{"close stage-1", "remove stage-1"},
		)
		assertNoPatchActions(t, dyn)
	})

	t.Run("cancellation with cleanup failure", func(t *testing.T) {
		tempDir := t.TempDir()
		t.Setenv("TMPDIR", tempDir)

		cancelCause := errors.New("injected restore cancellation")
		removeErr := errors.New("injected canceled stage remove failure")
		faults := newManifestStageFaults(nil, map[string]error{"stage-1": removeErr})
		ctx, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(nil) })

		src := &stubSource{body: mustArray(t, configMapManifest("canceled-cleanup"))}
		src.onCall = func() {
			cancel(cancelCause)
		}
		dyn := newFakeDynamic(readySnapshot())
		cfg := baseConfig(src, dyn)
		cfg.manifestStageOps = faults.operations()

		runErr := Run(ctx, cfg)
		faults.assertCleanup(
			t,
			runErr,
			[]error{cancelCause, removeErr},
			[]string{"close stage-1", "remove stage-1"},
		)
		assertNoPatchActions(t, dyn)
	})

	t.Run("edited stage cleanup is LIFO", func(t *testing.T) {
		tempDir := t.TempDir()
		t.Setenv("TMPDIR", tempDir)

		editedContent := `apiVersion: v1
kind: ConfigMap
metadata:
  name: cleanup-edited
`
		contentFile := writeEditorContent(t, tempDir, "cleanup-edited.yaml", editedContent)
		editor := fakeEditorScript(t, tempDir, fmt.Sprintf(`cp '%s' "$1"`, contentFile))
		t.Setenv("EDITOR", editor)
		t.Setenv("KUBE_EDITOR", "")

		originalCloseErr := errors.New("injected original stage close failure")
		originalRemoveErr := errors.New("injected original stage remove failure")
		editedCloseErr := errors.New("injected edited stage close failure")
		editedRemoveErr := errors.New("injected edited stage remove failure")
		faults := newManifestStageFaults(
			map[string]error{
				"stage-1": originalCloseErr,
				"stage-2": editedCloseErr,
			},
			map[string]error{
				"stage-1": originalRemoveErr,
				"stage-2": editedRemoveErr,
			},
		)
		src := &stubSource{body: mustArray(t, configMapManifest("cleanup-original"))}
		dyn := newFakeDynamic(readySnapshot())
		cfg := baseConfig(src, dyn)
		cfg.Edit = true
		cfg.manifestStageOps = faults.operations()

		runErr := Run(context.Background(), cfg)
		faults.assertCleanup(
			t,
			runErr,
			[]error{originalCloseErr, originalRemoveErr, editedCloseErr, editedRemoveErr},
			[]string{
				"close stage-2",
				"remove stage-2",
				"close stage-1",
				"remove stage-1",
			},
		)
	})
}

func TestRun_LatePreflightFailuresCauseZeroPatchAndCleanup(t *testing.T) {
	childRef := aggapi.NodeRef{
		APIVersion: domainDiskAPIVersion,
		Kind:       "DemoVirtualDiskSnapshot",
		Name:       "nss-child-late",
		Namespace:  testNS,
	}

	conflicting := configMapManifest("duplicate")
	conflicting["data"] = map[string]interface{}{"k": "different"}
	cancelCause := errors.New("operator canceled during node staging")
	lateFetchErr := errors.New("late root fetch failure")

	tests := []struct {
		name          string
		childResponse stubSourceResponse
		rootResponse  stubSourceResponse
		stageBytes    int64
		stageObjects  int
		cancelOnCall  int
		wantCause     error
		wantText      string
	}{
		{
			name:          "oversized child response",
			childResponse: stubSourceResponse{err: fmt.Errorf("%w: child exceeds 64 MiB", aggapi.ErrResponseTooLarge)},
			rootResponse:  stubSourceResponse{body: mustArray(t, configMapManifest("root"))},
			wantCause:     aggapi.ErrResponseTooLarge,
		},
		{
			name:          "late root fetch failure",
			childResponse: stubSourceResponse{body: mustArray(t, configMapManifest("child"))},
			rootResponse:  stubSourceResponse{err: lateFetchErr},
			wantCause:     lateFetchErr,
		},
		{
			name:          "late invalid object",
			childResponse: stubSourceResponse{body: mustArray(t, configMapManifest("child"))},
			rootResponse: stubSourceResponse{body: mustArray(t, map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]interface{}{},
			})},
			wantText: "identity is incomplete",
		},
		{
			name:          "conflicting duplicate",
			childResponse: stubSourceResponse{body: mustArray(t, configMapManifest("duplicate"))},
			rootResponse:  stubSourceResponse{body: mustArray(t, conflicting)},
			wantText:      "conflicting duplicate restore object",
		},
		{
			name:          "cancellation after late fetch",
			childResponse: stubSourceResponse{body: mustArray(t, configMapManifest("child"))},
			rootResponse:  stubSourceResponse{body: mustArray(t, configMapManifest("root"))},
			cancelOnCall:  2,
			wantCause:     context.Canceled,
		},
		{
			name:          "temporary disk budget",
			childResponse: stubSourceResponse{body: mustArray(t, configMapManifest("child"))},
			rootResponse:  stubSourceResponse{body: mustArray(t, configMapManifest("root"))},
			stageBytes:    128,
			wantText:      "temporary-disk byte budget",
		},
		{
			name:          "staged object budget",
			childResponse: stubSourceResponse{body: mustArray(t, configMapManifest("child"))},
			rootResponse:  stubSourceResponse{body: mustArray(t, configMapManifest("root"))},
			stageObjects:  1,
			wantText:      "object budget",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			t.Setenv("TMPDIR", tempDir)

			child := readyDomainDiskSnapshot(childRef.Name)
			root := snapshotWithChildren(
				readySnapshot(),
				snapshotChildRef(childRef.APIVersion, childRef.Kind, childRef.Name),
			)
			dyn := newFakeDynamic(root, child)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)

			src := &stubSource{
				responses: map[string]stubSourceResponse{
					nodeRefKey(childRef): tc.childResponse,
					nodeRefKey(aggapi.NodeRef{
						APIVersion: snapshotapi.StorageGroup + "/" + snapshotapi.Version,
						Kind:       snapshotKind,
						Name:       testSnap,
						Namespace:  testNS,
					}): tc.rootResponse,
				},
			}
			if tc.cancelOnCall != 0 {
				src.onCall = func() {
					if src.calls == tc.cancelOnCall {
						cancel(cancelCause)
					}
				}
			}

			cfg := baseConfig(src, dyn)
			cfg.Mapper = testMapperWithDomain()
			cfg.maxStagedManifestBytes = tc.stageBytes
			cfg.maxStagedManifestObjects = tc.stageObjects

			err := Run(ctx, cfg)
			if err == nil {
				t.Fatal("Run unexpectedly succeeded")
			}
			if tc.wantCause != nil && !errors.Is(err, tc.wantCause) {
				t.Errorf("Run error = %v, want cause %v", err, tc.wantCause)
			}
			if tc.cancelOnCall != 0 && !errors.Is(err, cancelCause) {
				t.Errorf("Run error = %v, want cancellation cause %v", err, cancelCause)
			}
			if tc.wantText != "" && !strings.Contains(err.Error(), tc.wantText) {
				t.Errorf("Run error = %v, want text %q", err, tc.wantText)
			}

			assertNoPatchActions(t, dyn)

			stageFiles, globErr := filepath.Glob(filepath.Join(tempDir, "d8-restore-stage-*.jsonl"))
			if globErr != nil {
				t.Fatalf("glob staging files: %v", globErr)
			}
			if len(stageFiles) != 0 {
				t.Errorf("staging files remain after failure: %v", stageFiles)
			}
		})
	}
}

func writeLargeManifestResponse(t *testing.T, w io.Writer, name string, payloadBytes int) {
	t.Helper()

	if _, err := io.WriteString(w, `[{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"`+name+`"},"data":{"payload":"`); err != nil {
		return
	}

	chunk := strings.Repeat("x", 32<<10)
	remaining := payloadBytes
	for remaining > 0 {
		part := min(remaining, len(chunk))
		if _, err := io.WriteString(w, chunk[:part]); err != nil {
			return
		}
		remaining -= part
	}

	_, _ = io.WriteString(w, `"}}]`)
}

func sampleHeapPeak(stop <-chan struct{}, result chan<- uint64, initial uint64) {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	peak := initial
	for {
		select {
		case <-stop:
			result <- peak

			return
		case <-ticker.C:
			var stats stdruntime.MemStats
			stdruntime.ReadMemStats(&stats)
			if stats.HeapAlloc > peak {
				peak = stats.HeapAlloc
			}
		}
	}
}

func stagedResourceUsage(t *testing.T, tempDir string) int64 {
	t.Helper()

	paths, err := filepath.Glob(filepath.Join(tempDir, "d8-restore-stage-*.jsonl"))
	if err != nil {
		t.Fatalf("glob staging files: %v", err)
	}

	var total int64
	for _, path := range paths {
		info, statErr := os.Stat(path)
		if statErr != nil {
			t.Fatalf("stat staging file: %v", statErr)
		}
		total += info.Size()
	}

	return total
}

func countOpenDescriptors(t *testing.T) int {
	t.Helper()

	directory, err := os.Open("/dev/fd")
	if err != nil {
		t.Skipf("descriptor counting is unavailable: %v", err)
	}
	defer func() {
		if closeErr := directory.Close(); closeErr != nil {
			t.Errorf("close descriptor directory: %v", closeErr)
		}
	}()

	descriptors, err := directory.Readdirnames(-1)
	if err != nil {
		t.Skipf("descriptor counting is unavailable: %v", err)
	}

	return len(descriptors)
}

func TestRun_ManifestNamespacePreflightRejectsForeignNamespace(t *testing.T) {
	tests := []struct {
		name   string
		dryRun bool
	}{
		{name: "implicit dry-run and real apply", dryRun: false},
		{name: "explicit dry-run", dryRun: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			foreign := configMapManifest("foreign")
			metadata, _ := foreign["metadata"].(map[string]interface{})
			metadata["namespace"] = "other"

			src := &stubSource{body: mustArray(t, configMapManifest("valid"), foreign)}
			dyn := newFakeDynamic(readySnapshot())
			cfg := baseConfig(src, dyn)
			cfg.DryRun = tc.dryRun

			err := Run(context.Background(), cfg)
			if err == nil {
				t.Fatal("expected cross-namespace manifest error, got nil")
			}

			for _, value := range []string{
				`apiVersion="v1"`,
				`kind="ConfigMap"`,
				`name="foreign"`,
				`namespace "other"`,
				`required namespace is "default"`,
			} {
				if !contains(err.Error(), value) {
					t.Errorf("error %q does not contain %q", err.Error(), value)
				}
			}

			assertNoPatchActions(t, dyn)
		})
	}
}

func TestRun_ManifestNamespacePreflightAcceptsTargetScope(t *testing.T) {
	emptyNamespace := configMapManifest("empty-namespace")
	matchingNamespace := configMapManifest("matching-namespace")
	metadata, _ := matchingNamespace["metadata"].(map[string]interface{})
	metadata["namespace"] = testNS

	src := &stubSource{body: mustArray(t, emptyNamespace, matchingNamespace)}
	dyn := newFakeDynamic(readySnapshot())

	if err := Run(context.Background(), baseConfig(src, dyn)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, name := range []string{"empty-namespace", "matching-namespace"} {
		obj, err := dyn.Resource(cmGVR).Namespace(testNS).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get ConfigMap %s: %v", name, err)
		}

		if obj.GetNamespace() != testNS {
			t.Errorf("ConfigMap %s namespace = %q, want %q", name, obj.GetNamespace(), testNS)
		}
	}

}

func TestPreflightManifestNamespaces_NormalizesOnlyAfterValidation(t *testing.T) {
	objs := []unstructured.Unstructured{
		{Object: configMapManifest("empty")},
		{Object: configMapManifest("foreign")},
	}
	objs[1].SetNamespace("other")
	cfg := Config{
		Namespace: testNS,
		Mapper:    testMapper(),
	}

	if err := preflightManifestNamespaces(context.Background(), cfg, objs); err == nil {
		t.Fatal("expected cross-namespace manifest error, got nil")
	}

	if objs[0].GetNamespace() != "" {
		t.Errorf("first object was normalized before the complete set passed: namespace = %q", objs[0].GetNamespace())
	}

	objs[1].SetNamespace(testNS)

	if err := preflightManifestNamespaces(context.Background(), cfg, objs); err != nil {
		t.Fatalf("preflightManifestNamespaces: %v", err)
	}

	for i := range objs {
		if objs[i].GetNamespace() != testNS {
			t.Errorf("namespaced object %d namespace = %q, want %q", i, objs[i].GetNamespace(), testNS)
		}
	}
}

func TestRun_ManifestNamespacePreflightMapperErrorAbortsBeforePatch(t *testing.T) {
	mapperErr := errors.New("mapper unavailable")
	mapper := restMappingInterceptor{
		RESTMapper: testMapper(),
		intercept: func(groupKind schema.GroupKind, _ ...string) (*meta.RESTMapping, error) {
			if groupKind.Kind == "ConfigMap" {
				return nil, mapperErr
			}

			return nil, nil
		},
	}
	src := &stubSource{body: mustArray(t, configMapManifest("valid"))}
	dyn := newFakeDynamic(readySnapshot())
	cfg := baseConfig(src, dyn)
	cfg.Mapper = mapper

	err := Run(context.Background(), cfg)
	if !errors.Is(err, mapperErr) {
		t.Fatalf("Run error = %v, want mapper error", err)
	}

	assertNoPatchActions(t, dyn)
}

func TestRun_ManifestNamespacePreflightCancellationAbortsBeforePatch(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancelCause := errors.New("operator canceled namespace validation")
	src := &stubSource{
		body: mustArray(t, configMapManifest("valid")),
		onCall: func() {
			cancel(cancelCause)
		},
	}
	dyn := newFakeDynamic(readySnapshot())

	err := Run(ctx, baseConfig(src, dyn))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}

	if !errors.Is(err, cancelCause) {
		t.Fatalf("Run error = %v, want cancellation cause", err)
	}

	assertNoPatchActions(t, dyn)
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

func TestRun_PreflightExistingBoundPVCNoMutation(t *testing.T) {
	sources := []struct {
		name   string
		mutate func(*unstructured.Unstructured)
	}{
		{name: "matching"},
		{
			name: "different",
			mutate: func(pvc *unstructured.Unstructured) {
				ref, _, _ := unstructured.NestedMap(pvc.Object, "spec", "dataSourceRef")
				ref["name"] = "older-snapshot"
				_ = unstructured.SetNestedMap(pvc.Object, ref, "spec", "dataSourceRef")
			},
		},
		{
			name: "missing",
			mutate: func(pvc *unstructured.Unstructured) {
				unstructured.RemoveNestedField(pvc.Object, "spec", "dataSourceRef")
			},
		},
		{
			name: "malformed",
			mutate: func(pvc *unstructured.Unstructured) {
				_ = unstructured.SetNestedField(pvc.Object, "not-an-object", "spec", "dataSourceRef")
			},
		},
	}

	for _, source := range sources {
		for _, wait := range []bool{false, true} {
			for _, dryRun := range []bool{false, true} {
				name := fmt.Sprintf("%s/wait=%t/dry-run=%t", source.name, wait, dryRun)
				t.Run(name, func(t *testing.T) {
					existing := restoredPVCObject("pvc-stale", pvcPhaseBound, "", false)
					if source.mutate != nil {
						source.mutate(existing)
					}

					src := &stubSource{body: mustArray(
						t,
						pvcManifest("pvc-stale", ""),
						configMapManifest("must-not-apply"),
					)}
					dyn := newFakeDynamic(
						readySnapshot(),
						readyVolumeSnapshot("vs-1"),
						existing,
						boundPVObject("pvc-stale"),
					)
					cfg := baseConfig(src, dyn)
					cfg.Wait = wait
					cfg.DryRun = dryRun

					err := Run(context.Background(), cfg)
					if err == nil {
						t.Fatal("Run unexpectedly reused a pre-existing Bound PVC")
					}

					for _, want := range []string{
						"default/pvc-stale",
						`phase "Bound"`,
						"refusing to reuse it",
						"stale data",
					} {
						if !contains(err.Error(), want) {
							t.Errorf("Run error = %q, want %q", err, want)
						}
					}

					pvcGets := 0
					for _, action := range dyn.Actions() {
						if action.GetVerb() == "get" && action.GetResource() == pvcGVR {
							pvcGets++
						}

						switch action.GetVerb() {
						case "create", "delete", "delete-collection", "patch", "update":
							t.Errorf("mutation %s %s occurred after Bound refusal", action.GetVerb(), action.GetResource().Resource)
						}
					}

					if pvcGets != 1 {
						t.Errorf("PVC GET count = %d, want exactly 1 preflight read", pvcGets)
					}
				})
			}
		}
	}
}

func TestRun_AbsentPVCStillRestores(t *testing.T) {
	for _, dryRun := range []bool{false, true} {
		t.Run(fmt.Sprintf("dry-run=%t", dryRun), func(t *testing.T) {
			src := &stubSource{body: mustArray(t, pvcManifest("pvc-new", ""))}
			dyn := newFakeDynamic(readySnapshot(), readyVolumeSnapshot("vs-1"))
			cfg := baseConfig(src, dyn)
			cfg.DryRun = dryRun

			if err := Run(context.Background(), cfg); err != nil {
				t.Fatalf("Run with absent PVC: %v", err)
			}

			patches := 0
			for _, action := range dyn.Actions() {
				if action.GetVerb() == "patch" && action.GetResource() == pvcGVR {
					patches++
				}
			}

			wantPatches := 2
			if dryRun {
				wantPatches = 1
			}

			if patches != wantPatches {
				t.Errorf("PVC Patch count = %d, want %d", patches, wantPatches)
			}
		})
	}
}

func TestRun_RevalidatesPVCUIDAndRestoreSource(t *testing.T) {
	tests := []struct {
		name      string
		wait      bool
		mutate    func(*unstructured.Unstructured)
		wantError string
	}{
		{
			name: "no-wait UID replacement",
			mutate: func(pvc *unstructured.Unstructured) {
				pvc.SetUID("uid-recreated")
			},
			wantError: "changed UID after apply",
		},
		{
			name: "wait source replacement",
			wait: true,
			mutate: func(pvc *unstructured.Unstructured) {
				ref, _, _ := unstructured.NestedMap(pvc.Object, "spec", "dataSourceRef")
				ref["name"] = "other-snapshot"
				_ = unstructured.SetNestedMap(pvc.Object, ref, "spec", "dataSourceRef")
			},
			wantError: "dataSource/dataSourceRef identity changed after apply",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pvc := restoredPVCObject("pvc-race", pvcPhasePending, "immediate-sc", false)
			src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-race", "", "immediate-sc"))}
			dyn := newFakeDynamic(
				readySnapshot(),
				readyVolumeSnapshot("vs-1"),
				storageClassObj("immediate-sc", volumeBindingModeImmediate, false),
				pvc,
			)
			pvcGets := 0
			dyn.PrependReactor("get", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
				pvcGets++
				if pvcGets <= 2 {
					return false, nil, nil
				}

				replacement := pvc.DeepCopy()
				tc.mutate(replacement)

				return true, replacement, nil
			})

			cfg := baseConfig(src, dyn)
			cfg.Wait = tc.wait
			cfg.Timeout = time.Second

			err := Run(context.Background(), cfg)
			if err == nil {
				t.Fatal("Run accepted a replaced PVC")
			}

			for _, want := range []string{"default/pvc-race", tc.wantError} {
				if !contains(err.Error(), want) {
					t.Errorf("Run error = %q, want %q", err, want)
				}
			}

			if pvcGets != 3 {
				t.Errorf("PVC GET count = %d, want two preflights and one revalidation", pvcGets)
			}
		})
	}
}

func TestRun_RevalidatesBoundPVIdentityAgainstABA(t *testing.T) {
	pvc := restoredPVCObject("pvc-aba", pvcPhasePending, "immediate-sc", false)
	src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-aba", "", "immediate-sc"))}
	base := newFakeDynamic(
		readySnapshot(),
		readyVolumeSnapshot("vs-1"),
		storageClassObj("immediate-sc", volumeBindingModeImmediate, false),
		pvc,
		boundPVObjectWithUID("pvc-aba", "uid-pv-original"),
	)

	dyn := &interceptingDynamicClient{
		Interface: base,
		transformPatch: func(
			gvr schema.GroupVersionResource,
			namespace string,
			_ string,
			opts metav1.PatchOptions,
			result *unstructured.Unstructured,
		) (*unstructured.Unstructured, error) {
			if gvr != pvcGVR || len(opts.DryRun) != 0 {
				return result, nil
			}

			bound := restoredPVCObject("pvc-aba", pvcPhaseBound, "immediate-sc", false)
			if err := base.Tracker().Update(pvcGVR, bound, namespace); err != nil {
				return nil, err
			}

			return bound, nil
		},
	}
	pvGets := 0
	base.PrependReactor("get", "persistentvolumes", func(clienttesting.Action) (bool, runtime.Object, error) {
		pvGets++
		if pvGets == 1 {
			return true, boundPVObjectWithUID("pvc-aba", "uid-pv-original"), nil
		}

		return true, boundPVObjectWithUID("pvc-aba", "uid-pv-replacement"), nil
	})

	err := Run(context.Background(), baseConfig(src, dyn))
	if err == nil {
		t.Fatal("Run accepted a same-name PersistentVolume ABA replacement")
	}

	for _, want := range []string{
		"default/pvc-aba",
		"PersistentVolume identity changed after apply",
		`expected pv-pvc-aba UID "uid-pv-original"`,
		`observed pv-pvc-aba UID "uid-pv-replacement"`,
	} {
		if !contains(err.Error(), want) {
			t.Errorf("Run error = %q, want %q", err, want)
		}
	}

	if pvGets != 2 {
		t.Errorf("PersistentVolume GET count = %d, want record plus revalidation", pvGets)
	}
}

// TestRun_WaitBound succeeds only for a PVC created by this invocation.
func TestRun_WaitBound(t *testing.T) {
	src := &stubSource{body: mustArray(t, pvcManifest("pvc-1", ""))}
	dyn := newFakeDynamic(readySnapshot(), readyVolumeSnapshot("vs-1"), boundPVObject("pvc-1"))
	pvcGets := 0

	dyn.PrependReactor("get", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
		pvcGets++
		if pvcGets <= 2 {
			return false, nil, nil
		}

		return true, restoredPVCObject("pvc-1", pvcPhaseBound, "", false), nil
	})

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = 2 * time.Second

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with --wait: %v", err)
	}

	if pvcGets != 3 {
		t.Fatalf("PVC GET count = %d, want two preflights plus wait revalidation", pvcGets)
	}
}

// TestRun_WaitTimeout returns a timeout error when a restored PVC never binds.
func TestRun_WaitTimeout(t *testing.T) {
	src := &stubSource{body: mustArray(t, pvcManifest("pvc-1", "Pending"))}
	pvc := restoredPVCObject("pvc-1", pvcPhasePending, "", false)
	dyn := newFakeDynamic(readySnapshot(), readyVolumeSnapshot("vs-1"), pvc)

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

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error %q does not wrap context.DeadlineExceeded", err.Error())
	}
}

// TestRun_Wait_WFFCHealthyStates verifies the only two accepted WFFC states:
// non-terminating Pending is skipped without polling, while non-terminating Bound
// succeeds normally.
func TestRun_Wait_WFFCHealthyStates(t *testing.T) {
	cases := []struct {
		name           string
		phase          string
		wantPendingLog int
	}{
		{
			name:           "Pending is a non-blocking skip",
			phase:          pvcPhasePending,
			wantPendingLog: 1,
		},
		{
			name:  "Bound succeeds",
			phase: pvcPhaseBound,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sc := storageClassObj("wffc-sc", volumeBindingModeWFC, false)
			pvc := restoredPVCObject("pvc-1", pvcPhasePending, "wffc-sc", false)
			src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-1", pvcPhasePending, "wffc-sc"))}
			dyn := newFakeDynamic(readySnapshot(), readyVolumeSnapshot("vs-1"), sc, pvc, boundPVObject("pvc-1"))
			capture := &logCapture{}

			if tc.phase == pvcPhaseBound {
				pvcGets := 0
				dyn.PrependReactor("get", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
					pvcGets++
					if pvcGets <= 2 {
						return false, nil, nil
					}

					return true, restoredPVCObject("pvc-1", pvcPhaseBound, "wffc-sc", false), nil
				})
			}

			cfg := baseConfig(src, dyn)
			cfg.Log = slog.New(capture)
			cfg.Wait = true
			cfg.Timeout = time.Second

			if err := Run(context.Background(), cfg); err != nil {
				t.Fatalf("Run with a healthy WFFC PVC in phase %s: %v", tc.phase, err)
			}

			const pendingMessage = "PVC is WaitForFirstConsumer and Pending with no consumer yet; not waiting for Bound"
			if got := capture.countMsg(pendingMessage); got != tc.wantPendingLog {
				t.Errorf("Pending skip log count = %d, want %d", got, tc.wantPendingLog)
			}
		})
	}
}

func TestRun_Wait_WFFCSelectedOrConsumedPollsToBound(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*unstructured.Unstructured, *[]runtime.Object)
	}{
		{
			name: "selected node",
			prepare: func(pvc *unstructured.Unstructured, _ *[]runtime.Object) {
				pvc.SetAnnotations(map[string]string{selectedNodeAnnotation: "node-a"})
			},
		},
		{
			name: "live Pod consumer",
			prepare: func(_ *unstructured.Unstructured, objects *[]runtime.Object) {
				*objects = append(*objects, podConsumerObject("consumer", "pvc-active", false))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pvc := restoredPVCObject("pvc-active", pvcPhasePending, "wffc-sc", false)
			objects := []runtime.Object{
				readySnapshot(),
				readyVolumeSnapshot("vs-1"),
				storageClassObj("wffc-sc", volumeBindingModeWFC, false),
				pvc,
				boundPVObject("pvc-active"),
			}
			tc.prepare(pvc, &objects)

			dyn := newFakeDynamic(objects...)
			pvcGets := 0
			dyn.PrependReactor("get", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
				pvcGets++
				if pvcGets <= 3 {
					return false, nil, nil
				}

				return true, restoredPVCObject("pvc-active", pvcPhaseBound, "wffc-sc", false), nil
			})
			src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-active", "", "wffc-sc"))}
			cfg := baseConfig(src, dyn)
			cfg.Wait = true
			cfg.Timeout = time.Second

			if err := Run(context.Background(), cfg); err != nil {
				t.Fatalf("Run with active WFFC PVC: %v", err)
			}

			if pvcGets != 4 {
				t.Errorf("PVC GET count = %d, want two preflights, inspection, and Bound poll", pvcGets)
			}
		})
	}
}

func TestRun_Wait_WFFCProvisioningSignals(t *testing.T) {
	tests := []struct {
		name        string
		reason      string
		message     string
		wantTimeout bool
		wantError   []string
	}{
		{
			name:        "active provisioning polls until timeout",
			reason:      eventReasonProvisioning,
			message:     "External provisioner is provisioning volume for claim default/pvc-event",
			wantTimeout: true,
			wantError:   []string{"default/pvc-event", "restore wait timeout", "Bound"},
		},
		{
			name:      "terminal CSI failure preserves cause",
			reason:    eventReasonProvisioningFailed,
			message:   "failed to provision volume with StorageClass \"wffc-sc\": rpc error: code = InvalidArgument",
			wantError: []string{"default/pvc-event", eventReasonProvisioningFailed, "rpc error: code = InvalidArgument"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pvc := restoredPVCObject("pvc-event", pvcPhasePending, "wffc-sc", false)
			event := pvcEventObject(
				"event-1",
				"pvc-event",
				tc.reason,
				tc.message,
				"2026-07-27T12:00:00Z",
			)
			src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-event", "", "wffc-sc"))}
			dyn := newFakeDynamic(
				readySnapshot(),
				readyVolumeSnapshot("vs-1"),
				storageClassObj("wffc-sc", volumeBindingModeWFC, false),
				pvc,
				event,
			)
			cfg := baseConfig(src, dyn)
			cfg.Wait = true
			cfg.Timeout = time.Millisecond

			err := Run(context.Background(), cfg)
			if err == nil {
				t.Fatal("Run reported false success for an active or failed WFFC PVC")
			}

			for _, want := range tc.wantError {
				if !contains(err.Error(), want) {
					t.Errorf("Run error = %q, want %q", err, want)
				}
			}

			if tc.wantTimeout && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Run error = %v, want context.DeadlineExceeded", err)
			}

			eventLists := 0
			podLists := 0
			for _, action := range dyn.Actions() {
				if action.GetVerb() != "list" {
					continue
				}

				switch action.GetResource().Resource {
				case eventResource:
					eventLists++
				case podResource:
					podLists++
				}
			}

			if eventLists != 1 || podLists != 1 {
				t.Errorf("WFFC scan counts: events=%d pods=%d, want 1 each", eventLists, podLists)
			}
		})
	}
}

func TestRun_Wait_WFFCActiveCancellationPreservesCause(t *testing.T) {
	pvc := restoredPVCObject("pvc-cancel", pvcPhasePending, "wffc-sc", false)
	pvc.SetAnnotations(map[string]string{selectedNodeAnnotation: "node-a"})
	src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-cancel", "", "wffc-sc"))}
	dyn := newFakeDynamic(
		readySnapshot(),
		readyVolumeSnapshot("vs-1"),
		storageClassObj("wffc-sc", volumeBindingModeWFC, false),
		pvc,
	)
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("operator canceled active WFFC wait")
	pvcGets := 0
	dyn.PrependReactor("get", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
		pvcGets++
		if pvcGets == 4 {
			cancel(cause)
		}

		return false, nil, nil
	})
	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = time.Hour

	err := Run(ctx, cfg)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Fatalf("Run error = %v, want context cancellation and cause", err)
	}

	if pvcGets != 4 {
		t.Errorf("PVC GET count = %d, want cancellation after first active poll", pvcGets)
	}
}

// TestRun_Wait_ImmediateStillWaits verifies a Pending PVC on an explicit Immediate
// StorageClass is polled until it becomes Bound.
func TestRun_Wait_ImmediateStillWaits(t *testing.T) {
	sc := storageClassObj("immediate-sc", volumeBindingModeImmediate, false)
	pvc := restoredPVCObject("pvc-1", pvcPhasePending, "immediate-sc", false)
	src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-1", "Pending", "immediate-sc"))}
	dyn := newFakeDynamic(readySnapshot(), readyVolumeSnapshot("vs-1"), sc, pvc, boundPVObject("pvc-1"))

	var pvcGets int

	dyn.PrependReactor("get", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		getAction, ok := action.(clienttesting.GetAction)
		if !ok {
			return false, nil, nil
		}

		pvcGets++
		phase := pvcPhasePending
		if pvcGets == 3 {
			phase = pvcPhaseBound
		}

		return true, restoredPVCObject(getAction.GetName(), phase, "immediate-sc", false), nil
	})

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = time.Second

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with a Pending-then-Bound Immediate PVC: %v", err)
	}

	if pvcGets != 3 {
		t.Errorf("PVC GET count = %d, want 3", pvcGets)
	}
}

// TestRun_Wait_InvalidPVCStatesFailClosed verifies post-apply absence and terminal or
// ambiguous states cannot be mistaken for a healthy Pending or Bound PVC.
func TestRun_Wait_InvalidPVCStatesFailClosed(t *testing.T) {
	cases := []struct {
		name        string
		bindingMode string
		phase       string
		terminating bool
		notFound    bool
		wantError   []string
	}{
		{
			name:        "WFFC NotFound",
			bindingMode: volumeBindingModeWFC,
			notFound:    true,
			wantError:   []string{"default/pvc-terminal", "not found", "after apply"},
		},
		{
			name:        "WFFC Lost",
			bindingMode: volumeBindingModeWFC,
			phase:       pvcPhaseLost,
			wantError:   []string{"default/pvc-terminal", "terminal phase", pvcPhaseLost},
		},
		{
			name:        "WFFC missing phase",
			bindingMode: volumeBindingModeWFC,
			wantError:   []string{"default/pvc-terminal", "missing status.phase"},
		},
		{
			name:        "WFFC unknown phase",
			bindingMode: volumeBindingModeWFC,
			phase:       "Released",
			wantError:   []string{"default/pvc-terminal", "unrecognized phase", "Released"},
		},
		{
			name:        "WFFC terminating Pending",
			bindingMode: volumeBindingModeWFC,
			phase:       pvcPhasePending,
			terminating: true,
			wantError:   []string{"default/pvc-terminal", "terminating", "deletionTimestamp", pvcPhasePending},
		},
		{
			name:        "WFFC terminating Bound",
			bindingMode: volumeBindingModeWFC,
			phase:       pvcPhaseBound,
			terminating: true,
			wantError:   []string{"default/pvc-terminal", "terminating", "deletionTimestamp", pvcPhaseBound},
		},
		{
			name:        "Immediate Lost",
			bindingMode: volumeBindingModeImmediate,
			phase:       pvcPhaseLost,
			wantError:   []string{"default/pvc-terminal", "terminal phase", pvcPhaseLost},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sc := storageClassObj("wait-sc", tc.bindingMode, false)
			src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-terminal", tc.phase, "wait-sc"))}
			objects := []runtime.Object{readySnapshot(), readyVolumeSnapshot("vs-1"), sc}

			if !tc.notFound {
				initialPhase := tc.phase
				initialTerminating := tc.terminating
				if tc.phase == pvcPhaseBound {
					initialPhase = pvcPhasePending
					initialTerminating = false
				}

				objects = append(
					objects,
					restoredPVCObject("pvc-terminal", initialPhase, "wait-sc", initialTerminating),
					boundPVObject("pvc-terminal"),
				)
			}

			dyn := newFakeDynamic(objects...)
			if tc.notFound {
				pvcGets := 0
				dyn.PrependReactor("get", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
					pvcGets++
					if pvcGets <= 2 {
						return false, nil, nil
					}

					return true, nil, kubeerrors.NewNotFound(
						schema.GroupResource{Resource: pvcGVR.Resource},
						"pvc-terminal",
					)
				})
			} else if tc.phase == pvcPhaseBound {
				pvcGets := 0
				dyn.PrependReactor("get", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
					pvcGets++
					if pvcGets <= 2 {
						return false, nil, nil
					}

					return true, restoredPVCObject(
						"pvc-terminal",
						pvcPhaseBound,
						"wait-sc",
						tc.terminating,
					), nil
				})
			}

			capture := &logCapture{}
			cfg := baseConfig(src, dyn)
			cfg.Log = slog.New(capture)
			cfg.Wait = true
			cfg.Timeout = time.Hour

			err := Run(context.Background(), cfg)
			if err == nil {
				t.Fatal("expected invalid restored PVC state to fail")
			}

			for _, want := range tc.wantError {
				if !contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err.Error(), want)
				}
			}

			const pendingMessage = "PVC is WaitForFirstConsumer and Pending with no consumer yet; not waiting for Bound"
			if got := capture.countMsg(pendingMessage); got != 0 {
				t.Errorf("invalid state emitted %d misleading Pending success logs", got)
			}
		})
	}
}

// TestRun_Wait_MixedWFFCAndImmediate_OnlyImmediateAwaited seeds two Pending PVCs.
// The WFFC PVC must be checked once without
// polling, leaving the shared remaining budget available for the Immediate PVC.
func TestRun_Wait_MixedWFFCAndImmediate_OnlyImmediateAwaited(t *testing.T) {
	wffcSC := storageClassObj("wffc-sc", volumeBindingModeWFC, false)
	immediateSC := storageClassObj("immediate-sc", volumeBindingModeImmediate, false)
	pendingWFFC := restoredPVCObject("pvc-wffc", pvcPhasePending, "wffc-sc", false)
	pendingImmediate := restoredPVCObject("pvc-immediate", pvcPhasePending, "immediate-sc", false)

	src := &stubSource{body: mustArray(t,
		pvcManifestSC("pvc-wffc", "Pending", "wffc-sc"),
		pvcManifestSC("pvc-immediate", "", "immediate-sc"),
	)}
	dyn := newFakeDynamic(
		readySnapshot(),
		readyVolumeSnapshot("vs-1"),
		wffcSC,
		immediateSC,
		pendingWFFC,
		pendingImmediate,
		boundPVObject("pvc-immediate"),
	)
	immediateGets := 0
	dyn.PrependReactor("get", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		getAction, ok := action.(clienttesting.GetAction)
		if !ok || getAction.GetName() != "pvc-immediate" {
			return false, nil, nil
		}

		immediateGets++
		if immediateGets <= 2 {
			return false, nil, nil
		}

		return true, restoredPVCObject("pvc-immediate", pvcPhaseBound, "immediate-sc", false), nil
	})

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = time.Second

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with a mixed WFFC/Immediate PVC set: %v", err)
	}
}

// TestRun_Wait_EmptyStorageClassName_ResolvesDefault verifies a PVC with no
// spec.storageClassName resolves the cluster's annotated default StorageClass rather
// than being treated as an error or assumed Immediate: the default class here is WFFC,
// so a Pending PVC on it must not block --wait.
func TestRun_Wait_EmptyStorageClassName_ResolvesDefault(t *testing.T) {
	defaultSC := storageClassObj("default-sc", volumeBindingModeWFC, true)
	otherSC := storageClassObj("other-sc", volumeBindingModeImmediate, false)
	pvc := restoredPVCObject("pvc-1", pvcPhasePending, "", false)
	src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-1", "Pending", ""))}
	dyn := newFakeDynamic(readySnapshot(), readyVolumeSnapshot("vs-1"), defaultSC, otherSC, pvc)

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = time.Second

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with an empty storageClassName resolving to a WFFC default: %v", err)
	}
}

// TestRun_Wait_BindingModeCachedPerStorageClass verifies the StorageClass lookup is
// cached per class name: two PVCs sharing the same explicit storageClassName must
// resolve it with exactly one API call against the StorageClass resource, not one per
// PVC.
func TestRun_Wait_BindingModeCachedPerStorageClass(t *testing.T) {
	sc := storageClassObj("shared-sc", volumeBindingModeImmediate, false)

	src := &stubSource{body: mustArray(t,
		pvcManifestSC("pvc-a", "", "shared-sc"),
		pvcManifestSC("pvc-b", "", "shared-sc"),
	)}
	dyn := newFakeDynamic(
		readySnapshot(),
		readyVolumeSnapshot("vs-1"),
		sc,
		restoredPVCObject("pvc-a", pvcPhasePending, "shared-sc", false),
		restoredPVCObject("pvc-b", pvcPhasePending, "shared-sc", false),
		boundPVObject("pvc-a"),
		boundPVObject("pvc-b"),
	)
	pvcGets := make(map[string]int)
	dyn.PrependReactor("get", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		getAction, ok := action.(clienttesting.GetAction)
		if !ok {
			return false, nil, nil
		}

		name := getAction.GetName()
		pvcGets[name]++
		if pvcGets[name] <= 2 {
			return false, nil, nil
		}

		return true, restoredPVCObject(name, pvcPhaseBound, "shared-sc", false), nil
	})

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = 2 * time.Second

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with two PVCs sharing a StorageClass: %v", err)
	}

	scCalls := 0

	for _, action := range dyn.Actions() {
		if action.GetResource() == scGVR {
			scCalls++
		}
	}

	if scCalls != 1 {
		t.Errorf("expected exactly 1 StorageClass API call for 2 PVCs on the same class, got %d", scCalls)
	}
}

func TestRun_WaitBlockedAPICallsHonorControlPlaneTimeout(t *testing.T) {
	tests := []struct {
		name             string
		storageClassName string
		bindingMode      string
		blockGVR         schema.GroupVersionResource
		blockVerb        string
		wantError        string
	}{
		{
			name:             "StorageClass Get",
			storageClassName: "blocked-sc",
			blockGVR:         scGVR,
			blockVerb:        "get",
			wantError:        "StorageClass \"blocked-sc\"",
		},
		{
			name:      "default StorageClass List",
			blockGVR:  scGVR,
			blockVerb: "list",
			wantError: "listing StorageClasses",
		},
		{
			name:             "WFFC PVC Get",
			storageClassName: "wffc-sc",
			bindingMode:      volumeBindingModeWFC,
			blockGVR:         pvcGVR,
			blockVerb:        "get",
			wantError:        "PVC default/pvc-blocked",
		},
		{
			name:             "Immediate PVC Get",
			storageClassName: "immediate-sc",
			bindingMode:      volumeBindingModeImmediate,
			blockGVR:         pvcGVR,
			blockVerb:        "get",
			wantError:        "PVC default/pvc-blocked",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objects := []runtime.Object{readySnapshot(), readyVolumeSnapshot("vs-1")}
			if tc.bindingMode != "" {
				objects = append(objects, storageClassObj(tc.storageClassName, tc.bindingMode, false))
			}

			baseDynamic := newFakeDynamic(objects...)
			entered := make(chan struct{})
			returned := make(chan struct{})
			var enterOnce sync.Once
			var returnOnce sync.Once
			pvcGetCalls := 0

			dyn := &interceptingDynamicClient{
				Interface: baseDynamic,
				intercept: func(ctx context.Context, verb string, gvr schema.GroupVersionResource, _, _ string) error {
					if verb != tc.blockVerb || gvr != tc.blockGVR {
						return nil
					}

					if gvr == pvcGVR {
						pvcGetCalls++
						if pvcGetCalls <= 2 {
							return nil
						}
					}

					enterOnce.Do(func() {
						close(entered)
					})
					defer returnOnce.Do(func() {
						close(returned)
					})

					<-ctx.Done()

					return ctx.Err()
				},
			}

			src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-blocked", pvcPhasePending, tc.storageClassName))}
			cfg := baseConfig(src, dyn)
			cfg.Wait = true
			cfg.Timeout = time.Hour
			cfg.ControlPlaneTimeout = 250 * time.Millisecond

			result := make(chan error, 1)
			go func() {
				result <- Run(context.Background(), cfg)
			}()

			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("wait API request did not reach the blocking interceptor")
			}

			var err error

			select {
			case err = <-result:
			case <-time.After(5 * time.Second):
				t.Fatal("restore did not return after its wait timeout")
			}

			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Run error = %v, want context.DeadlineExceeded", err)
			}

			if !contains(err.Error(), tc.wantError) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantError)
			}

			select {
			case <-returned:
			default:
				t.Fatal("blocked API call did not return before Run completed")
			}
		})
	}
}

func TestRun_WaitRequestErrorsPreserveConcurrentContextCause(t *testing.T) {
	requests := []struct {
		name             string
		storageClassName string
		bindingMode      string
		blockGVR         schema.GroupVersionResource
		blockVerb        string
		requestObject    string
		wantPhase        string
	}{
		{
			name:             "StorageClass Get",
			storageClassName: "blocked-sc",
			blockGVR:         scGVR,
			blockVerb:        "get",
			requestObject:    "blocked-sc",
			wantPhase:        `getting StorageClass "blocked-sc"`,
		},
		{
			name:          "default StorageClass List",
			blockGVR:      scGVR,
			blockVerb:     "list",
			requestObject: "default-class-resolution",
			wantPhase:     "listing StorageClasses to resolve the default",
		},
		{
			name:             "PVC Get",
			storageClassName: "wffc-sc",
			bindingMode:      volumeBindingModeWFC,
			blockGVR:         pvcGVR,
			blockVerb:        "get",
			requestObject:    "pvc-blocked",
			wantPhase:        "getting restored PVC default/pvc-blocked",
		},
	}

	for _, request := range requests {
		for _, boundary := range []string{"outer cancellation", "shared wait deadline"} {
			t.Run(request.name+"/"+boundary, func(t *testing.T) {
				objects := []runtime.Object{readySnapshot(), readyVolumeSnapshot("vs-1")}
				if request.bindingMode != "" {
					objects = append(
						objects,
						storageClassObj(request.storageClassName, request.bindingMode, false),
					)
				}

				baseDynamic := newFakeDynamic(objects...)
				entered := make(chan struct{})
				release := make(chan struct{})
				returned := make(chan struct{})
				var (
					enterOnce   sync.Once
					releaseOnce sync.Once
					returnOnce  sync.Once
					requests    int
					pvcGetCalls int
				)

				requestErr := kubeerrors.NewForbidden(
					request.blockGVR.GroupResource(),
					request.requestObject,
					errors.New("API server rejected wait request"),
				)
				dyn := &interceptingDynamicClient{
					Interface: baseDynamic,
					intercept: func(
						_ context.Context,
						verb string,
						gvr schema.GroupVersionResource,
						_ string,
						_ string,
					) error {
						if verb != request.blockVerb || gvr != request.blockGVR {
							return nil
						}

						if gvr == pvcGVR {
							pvcGetCalls++
							if pvcGetCalls <= 2 {
								return nil
							}
						}

						requests++
						enterOnce.Do(func() {
							close(entered)
						})
						defer returnOnce.Do(func() {
							close(returned)
						})

						<-release

						return requestErr
					},
				}

				src := &stubSource{body: mustArray(
					t,
					pvcManifestSC("pvc-blocked", pvcPhasePending, request.storageClassName),
				)}
				cfg := baseConfig(src, dyn)
				cfg.Wait = true
				cfg.Timeout = time.Hour
				cfg.ControlPlaneTimeout = time.Hour

				runCtx := context.Background()
				releaseRequest := func() {
					releaseOnce.Do(func() {
						close(release)
					})
				}
				t.Cleanup(releaseRequest)

				var (
					boundaryCause   error
					triggerBoundary func()
					waitCanceled    <-chan struct{}
				)

				switch boundary {
				case "outer cancellation":
					parentCtx, cancel := context.WithCancelCause(context.Background())
					boundaryCause = errors.New("operator canceled restore at wait request boundary")
					runCtx = parentCtx
					triggerBoundary = func() {
						cancel(boundaryCause)
						releaseRequest()
					}
					t.Cleanup(func() {
						cancel(errors.New("test cleanup"))
					})
				case "shared wait deadline":
					cancelCalled := make(chan struct{})
					waitCanceled = cancelCalled
					var (
						waitCtx    *controlledDeadlineContext
						cancelOnce sync.Once
					)

					cfg.newWaitContext = func(
						parent context.Context,
						_ time.Duration,
					) (context.Context, context.CancelFunc) {
						waitCtx = newControlledDeadlineContext(parent)

						return waitCtx, func() {
							waitCtx.cancel()
							cancelOnce.Do(func() {
								close(cancelCalled)
							})
						}
					}
					boundaryCause = context.DeadlineExceeded
					triggerBoundary = func() {
						waitCtx.expire()
						releaseRequest()
					}
				default:
					t.Fatalf("unsupported boundary %q", boundary)
				}

				result := make(chan error, 1)
				go func() {
					result <- Run(runCtx, cfg)
				}()

				awaitTestSignal(t, entered, "wait API request did not reach the controlled boundary")
				triggerBoundary()

				err := awaitTestError(t, result, "restore did not return after the controlled wait boundary")

				var statusErr *kubeerrors.StatusError
				if !errors.As(err, &statusErr) {
					t.Fatalf("Run error = %v, want typed Kubernetes StatusError", err)
				}

				if statusErr != requestErr {
					t.Errorf("Run StatusError = %p, want exact request error %p", statusErr, requestErr)
				}

				if !errors.Is(err, boundaryCause) {
					t.Errorf("Run error = %v, want boundary cause %v", err, boundaryCause)
				}

				if boundary == "outer cancellation" && !errors.Is(err, context.Canceled) {
					t.Errorf("Run error = %v, want context.Canceled", err)
				}

				for _, want := range []string{request.wantPhase, "PVC default/pvc-blocked"} {
					if !contains(err.Error(), want) {
						t.Errorf("Run error = %q, want diagnostic %q", err, want)
					}
				}

				if requests != 1 {
					t.Errorf("controlled request count = %d, want 1", requests)
				}

				assertReturnedBeforeRun(t, returned)

				if waitCanceled != nil {
					select {
					case <-waitCanceled:
					default:
						t.Fatal("shared wait context was not canceled before Run completed")
					}
				}
			})
		}
	}
}

func TestRun_WaitParentCancellationPreservesCause(t *testing.T) {
	baseDynamic := newFakeDynamic(readySnapshot(), readyVolumeSnapshot("vs-1"))
	entered := make(chan struct{})
	returned := make(chan struct{})
	var enterOnce sync.Once
	var returnOnce sync.Once

	dyn := &interceptingDynamicClient{
		Interface: baseDynamic,
		intercept: func(ctx context.Context, verb string, gvr schema.GroupVersionResource, _, _ string) error {
			if verb != "get" || gvr != scGVR {
				return nil
			}

			enterOnce.Do(func() {
				close(entered)
			})
			defer returnOnce.Do(func() {
				close(returned)
			})

			<-ctx.Done()

			return ctx.Err()
		},
	}

	src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-canceled", pvcPhasePending, "blocked-sc"))}
	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = time.Hour

	parentCtx, cancel := context.WithCancelCause(context.Background())
	result := make(chan error, 1)

	go func() {
		result <- Run(parentCtx, cfg)
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("wait API request did not reach the blocking interceptor")
	}

	parentCause := errors.New("operator canceled restore")
	cancel(parentCause)

	var err error

	select {
	case err = <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not return after parent cancellation")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run error = %v, want context.Canceled", err)
	}

	if !errors.Is(err, parentCause) {
		t.Errorf("Run error = %v, want parent cancellation cause", err)
	}

	if errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run error = %v, do not want context.DeadlineExceeded", err)
	}

	select {
	case <-returned:
	default:
		t.Fatal("blocked API call did not return before Run completed")
	}
}

func TestRun_WaitMultiplePVCsShareOneDeadline(t *testing.T) {
	firstSC := storageClassObj("first-sc", volumeBindingModeImmediate, false)
	secondSC := storageClassObj("second-sc", volumeBindingModeImmediate, false)
	firstPVC := restoredPVCObject("pvc-first", pvcPhasePending, "first-sc", false)
	secondPVC := restoredPVCObject("pvc-second", pvcPhasePending, "second-sc", false)
	baseDynamic := newFakeDynamic(
		readySnapshot(),
		readyVolumeSnapshot("vs-1"),
		firstSC,
		secondSC,
		firstPVC,
		secondPVC,
		boundPVObject("pvc-first"),
		boundPVObject("pvc-second"),
	)
	basePVCGets := make(map[string]int)
	baseDynamic.PrependReactor("get", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		getAction, ok := action.(clienttesting.GetAction)
		if !ok {
			return false, nil, nil
		}

		name := getAction.GetName()
		basePVCGets[name]++
		if basePVCGets[name] <= 2 {
			return false, nil, nil
		}

		className := "first-sc"
		if name == "pvc-second" {
			className = "second-sc"
		}

		return true, restoredPVCObject(name, pvcPhaseBound, className, false), nil
	})

	var (
		deadlineMu sync.Mutex
		deadlines  []time.Time
		pvcGets    = make(map[string]int)
	)

	dyn := &interceptingDynamicClient{
		Interface: baseDynamic,
		intercept: func(ctx context.Context, verb string, gvr schema.GroupVersionResource, _, name string) error {
			if verb != "get" || (gvr != scGVR && gvr != pvcGVR) {
				return nil
			}

			if gvr == pvcGVR {
				pvcGets[name]++
				if pvcGets[name] <= 2 {
					return nil
				}
			}

			deadline, ok := ctx.Deadline()
			if !ok {
				return errors.New("wait API request has no deadline")
			}

			deadlineMu.Lock()
			deadlines = append(deadlines, deadline)
			deadlineMu.Unlock()

			return nil
		},
	}

	src := &stubSource{body: mustArray(t,
		pvcManifestSC("pvc-first", "", "first-sc"),
		pvcManifestSC("pvc-second", "", "second-sc"),
	)}
	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = time.Minute
	cfg.ControlPlaneTimeout = 2 * time.Minute

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with two Bound PVCs: %v", err)
	}

	deadlineMu.Lock()
	defer deadlineMu.Unlock()

	if len(deadlines) != 4 {
		t.Fatalf("wait API deadline count = %d, want 4", len(deadlines))
	}

	for i := 1; i < len(deadlines); i++ {
		if !deadlines[i].Equal(deadlines[0]) {
			t.Errorf("wait API deadline %d = %s, want shared deadline %s", i, deadlines[i], deadlines[0])
		}
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

// TestPreflightRootSnapshot_DegradedMessage verifies preflightRootSnapshot surfaces an
// accurate DEGRADED-specific error (mentioning --node) for a recoverable degradation
// reason, keeps the existing generic message for any other Ready=False reason, and does
// not fail on the Ready check at all when Ready=True.
func TestPreflightRootSnapshot_DegradedMessage(t *testing.T) {
	otherReasonSnapshot := notReadySnapshot()
	if err := unstructured.SetNestedSlice(otherReasonSnapshot.Object, []interface{}{
		map[string]interface{}{"type": "Ready", "status": "False", "reason": "ExportFailed", "message": "export timed out"},
	}, "status", "conditions"); err != nil {
		t.Fatalf("SetNestedSlice: %v", err)
	}

	cases := []struct {
		name         string
		snap         *unstructured.Unstructured
		wantErr      bool
		wantSubstrs  []string
		rejectSubstr string
	}{
		{
			name:        "degraded reason surfaces DEGRADED message and --node hint",
			snap:        notReadySnapshot(),
			wantErr:     true,
			wantSubstrs: []string{"DEGRADED", "--node", "ChildSnapshotDeleted"},
		},
		{
			name:         "other Ready=False reason keeps the generic message",
			snap:         otherReasonSnapshot,
			wantErr:      true,
			wantSubstrs:  []string{"cannot restore an incomplete snapshot"},
			rejectSubstr: "DEGRADED",
		},
		{
			name:    "Ready=True proceeds past the Ready check",
			snap:    readySnapshot(),
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dyn := newFakeDynamic(tc.snap)
			cfg := baseConfig(&stubSource{}, dyn)

			err := preflightRootSnapshot(context.Background(), cfg)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}

			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if err == nil {
				return
			}

			for _, substr := range tc.wantSubstrs {
				if !contains(err.Error(), substr) {
					t.Errorf("error %q does not contain %q", err.Error(), substr)
				}
			}

			if tc.rejectSubstr != "" && contains(err.Error(), tc.rejectSubstr) {
				t.Errorf("error %q unexpectedly contains %q", err.Error(), tc.rejectSubstr)
			}
		})
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
		{"unknown scope", func(c *Config) { c.Scope = "recursive" }},
		{"filter kind without name", func(c *Config) {
			c.Scope = aggapi.RestoreScopeNode
			c.FilterKind = "ConfigMap"
		}},
		{"filter without node scope", func(c *Config) {
			c.Scope = aggapi.RestoreScopeSubtree
			c.FilterKind = "ConfigMap"
			c.FilterName = "cm-1"
		}},
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
	t.Parallel()

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
			t.Parallel()

			got := isConditionTrue(&unstructured.Unstructured{Object: tc.obj})
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

	result, err := applyObject(context.Background(), cfg, obj)
	if err != nil {
		t.Fatalf("applyObject: %v", err)
	}

	if result.namespace != testNS {
		t.Errorf("namespace: got %q, want %q", result.namespace, testNS)
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

// testMapperWithDomain uses meta.DefaultRESTMapper directly. Its exact-group
// lookup semantics match the production controller-runtime mapper delegation.
func testMapperWithDomain() meta.RESTMapper {
	base := meta.NewDefaultRESTMapper([]schema.GroupVersion{
		{Group: "", Version: "v1"},
		{Group: "state-snapshotter.deckhouse.io", Version: "v1alpha1"},
		{Group: "snapshot.storage.k8s.io", Version: "v1"},
		{Group: "sds-unified-snapshots-poc.deckhouse.io", Version: "v1alpha1"},
	})
	base.Add(schema.GroupVersionKind{Group: "state-snapshotter.deckhouse.io", Version: "v1alpha1", Kind: "Snapshot"}, meta.RESTScopeNamespace)
	base.Add(schema.GroupVersionKind{Version: "v1", Kind: "PersistentVolumeClaim"}, meta.RESTScopeNamespace)
	base.Add(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}, meta.RESTScopeNamespace)
	base.Add(schema.GroupVersionKind{Version: "v1", Kind: "PersistentVolume"}, meta.RESTScopeRoot)
	base.Add(schema.GroupVersionKind{Group: "snapshot.storage.k8s.io", Version: "v1", Kind: "VolumeSnapshot"}, meta.RESTScopeNamespace)
	base.Add(schema.GroupVersionKind{Group: "sds-unified-snapshots-poc.deckhouse.io", Version: "v1alpha1", Kind: "DemoVirtualDiskSnapshot"}, meta.RESTScopeNamespace)

	return base
}

func testMapperWithGenerated(gvks ...schema.GroupVersionKind) meta.RESTMapper {
	groupVersions := []schema.GroupVersion{
		{Group: "", Version: "v1"},
		{Group: "state-snapshotter.deckhouse.io", Version: "v1alpha1"},
	}

	for _, gvk := range gvks {
		groupVersions = append(groupVersions, gvk.GroupVersion())
	}

	mapper := meta.NewDefaultRESTMapper(groupVersions)
	mapper.Add(
		schema.GroupVersionKind{
			Group:   "state-snapshotter.deckhouse.io",
			Version: "v1alpha1",
			Kind:    "Snapshot",
		},
		meta.RESTScopeNamespace,
	)
	mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}, meta.RESTScopeNamespace)

	for _, gvk := range gvks {
		mapper.Add(gvk, meta.RESTScopeNamespace)
	}

	return mapper
}

type restMappingInterceptor struct {
	meta.RESTMapper
	intercept func(schema.GroupKind, ...string) (*meta.RESTMapping, error)
}

func (m restMappingInterceptor) RESTMapping(
	groupKind schema.GroupKind,
	versions ...string,
) (*meta.RESTMapping, error) {
	if m.intercept != nil {
		mapping, err := m.intercept(groupKind, versions...)
		if mapping != nil || err != nil {
			return mapping, err
		}
	}

	return m.RESTMapper.RESTMapping(groupKind, versions...)
}

func TestRun_SelectedNodeAPIVersionValidationPrecedesAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		kind       string
		nodeName   string
		apiVersion string
		wantErr    string
	}{
		{
			name:       "API version without node",
			apiVersion: "apps/v1",
			wantErr:    "SelectedNodeAPIVersion requires a selected node",
		},
		{
			name:       "malformed API version",
			kind:       "Deployment",
			nodeName:   "demo",
			apiVersion: "apps/v1/extra",
			wantErr:    "parse Kubernetes apiVersion",
		},
		{
			name:    "kind without name",
			kind:    "Deployment",
			wantErr: "SelectedNodeKind and SelectedNodeName must be set together",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := &stubSource{}
			dyn := newFakeDynamic()
			cfg := baseConfig(src, dyn)
			cfg.SelectedNodeKind = tc.kind
			cfg.SelectedNodeName = tc.nodeName
			cfg.SelectedNodeAPIVersion = tc.apiVersion

			err := Run(context.Background(), cfg)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Run error = %v, want text %q", err, tc.wantErr)
			}

			if len(dyn.Actions()) != 0 {
				t.Errorf("dynamic API actions = %v, want none", dyn.Actions())
			}

			assertNoRestoreMutation(t, src, dyn)
		})
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

// TestRun_SelectedNode_ResolvesWithinRootTree verifies generated snapshot-CR
// selectors and original captured-source selectors resolve with production
// RESTMapper semantics and always address the real snapshot CR in the aggregated API.
func TestRun_SelectedNode_ResolvesWithinRootTree(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		child        func() *unstructured.Unstructured
		selectedKind string
		selectedName string
		wantRef      aggapi.NodeRef
	}{
		{
			name: "success: generated domain snapshot identity",
			child: func() *unstructured.Unstructured {
				return readyDomainDiskSnapshot("nss-child-domain")
			},
			selectedKind: "DemoVirtualDiskSnapshot",
			selectedName: "nss-child-domain",
			wantRef: aggapi.NodeRef{
				APIVersion: domainDiskAPIVersion,
				Kind:       "DemoVirtualDiskSnapshot",
				Name:       "nss-child-domain",
				Namespace:  testNS,
			},
		},
		{
			name: "success: generated VolumeSnapshot identity",
			child: func() *unstructured.Unstructured {
				return readyVolumeSnapshot("nss-vs-pvc")
			},
			selectedKind: "VolumeSnapshot",
			selectedName: "nss-vs-pvc",
			wantRef: aggapi.NodeRef{
				APIVersion: volumeSnapshotAPIVersion,
				Kind:       "VolumeSnapshot",
				Name:       "nss-vs-pvc",
				Namespace:  testNS,
			},
		},
		{
			name: "success: original source identity maps to snapshot CR",
			child: func() *unstructured.Unstructured {
				obj := readyDomainDiskSnapshot("nss-child-original")
				setSnapshotSourceRef(obj, "virtualization.deckhouse.io/v1alpha2", "DemoVirtualDisk", "bk-disk-a")

				return obj
			},
			selectedKind: "DemoVirtualDisk",
			selectedName: "bk-disk-a",
			wantRef: aggapi.NodeRef{
				APIVersion: domainDiskAPIVersion,
				Kind:       "DemoVirtualDiskSnapshot",
				Name:       "nss-child-original",
				Namespace:  testNS,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			child := tc.child()
			root := snapshotWithChildren(
				readySnapshot(),
				snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
			)
			src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
			dyn := newFakeDynamic(root, child)

			cfg := Config{
				Namespace:        testNS,
				Snapshot:         testSnap,
				SelectedNodeKind: tc.selectedKind,
				SelectedNodeName: tc.selectedName,
				Source:           src,
				Dynamic:          dyn,
				Mapper:           testMapperWithDomain(),
				Log:              discardLogger(),
				PollInterval:     time.Millisecond,
			}

			if err := Run(context.Background(), cfg); err != nil {
				t.Fatalf("Run: %v", err)
			}

			if src.gotRef != tc.wantRef {
				t.Errorf("NodeRef: got %+v, want %+v", src.gotRef, tc.wantRef)
			}
		})
	}
}

func TestRun_SelectedNode_HierarchyMappingsMustBeNamespaced(t *testing.T) {
	t.Parallel()

	const (
		childName       = "generated-child"
		aliasAPIVersion = "aliases.example.io/v1beta1"
		aliasKind       = "AliasedSnapshot"
	)

	type testCase struct {
		name                 string
		apiVersion           string
		kind                 string
		scope                meta.RESTScope
		refNamespace         string
		emptyObjectNamespace bool
		sourceAlias          bool
		nestedChild          bool
		wantSuccess          bool
	}

	tests := []testCase{
		{
			name:        "namespaced core resource",
			apiVersion:  "v1",
			kind:        "ConfigMap",
			scope:       meta.RESTScopeNamespace,
			wantSuccess: true,
		},
		{
			name:        "namespaced CRD source alias",
			apiVersion:  domainDiskAPIVersion,
			kind:        "DemoVirtualDiskSnapshot",
			scope:       meta.RESTScopeNamespace,
			sourceAlias: true,
			wantSuccess: true,
		},
		{
			name:                 "namespaced CRD version alias with empty response namespace",
			apiVersion:           aliasAPIVersion,
			kind:                 aliasKind,
			scope:                meta.RESTScopeNamespace,
			emptyObjectNamespace: true,
			wantSuccess:          true,
		},
		{
			name:         "cluster-scoped core resource with legacy empty namespace",
			apiVersion:   "v1",
			kind:         "PersistentVolume",
			scope:        meta.RESTScopeRoot,
			refNamespace: "",
		},
		{
			name:         "cluster-scoped CRD with foreign namespace and descendants",
			apiVersion:   domainDiskAPIVersion,
			kind:         "DemoVirtualDiskSnapshot",
			scope:        meta.RESTScopeRoot,
			refNamespace: "other",
			nestedChild:  true,
		},
	}

	run := func(t *testing.T, tc testCase) (string, int, *stubSource, *dynamicfake.FakeDynamicClient) {
		t.Helper()

		gvk := schema.FromAPIVersionAndKind(tc.apiVersion, tc.kind)
		child := readyGeneratedSnapshot(tc.apiVersion, tc.kind, childName)
		selectedAPIVersion := tc.apiVersion
		selectedKind := tc.kind
		selectedName := childName

		if tc.sourceAlias {
			setSnapshotSourceRef(child, "v1", "ConfigMap", "source-object")
			selectedAPIVersion = "v1"
			selectedKind = "ConfigMap"
			selectedName = "source-object"
		}

		if tc.nestedChild {
			snapshotWithChildren(
				child,
				snapshotChildRef(volumeSnapshotAPIVersion, "VolumeSnapshot", "must-not-traverse"),
			)
		}

		ref := snapshotChildRef(tc.apiVersion, tc.kind, childName)
		ref["namespace"] = tc.refNamespace
		root := snapshotWithChildren(readySnapshot(), ref)
		src := &stubSource{body: mustArray(t, configMapManifest("restored"))}

		var mapper meta.RESTMapper
		switch {
		case tc.apiVersion == "v1":
			mapper = testMapper()
		case tc.apiVersion == domainDiskAPIVersion:
			mapper = testMapperWithDomain()
		default:
			mapper = testMapperWithGenerated(gvk)
		}

		mapper.(*meta.DefaultRESTMapper).Add(gvk, tc.scope)

		dyn := newFakeDynamic(root, child)
		if tc.emptyObjectNamespace {
			childGVR, _ := meta.UnsafeGuessKindToResource(gvk)
			emptyNamespaceChild := child.DeepCopy()
			emptyNamespaceChild.SetNamespace("")
			dyn.PrependReactor("get", childGVR.Resource, func(action clienttesting.Action) (bool, runtime.Object, error) {
				getAction, ok := action.(clienttesting.GetAction)
				if !ok || action.GetResource() != childGVR || getAction.GetName() != childName {
					return false, nil, nil
				}

				return true, emptyNamespaceChild.DeepCopy(), nil
			})
		}

		cfg := Config{
			Namespace:              testNS,
			Snapshot:               testSnap,
			SelectedNodeKind:       selectedKind,
			SelectedNodeName:       selectedName,
			SelectedNodeAPIVersion: selectedAPIVersion,
			Source:                 src,
			Dynamic:                dyn,
			Mapper:                 mapper,
			Log:                    discardLogger(),
		}

		err := Run(context.Background(), cfg)
		childGVR, _ := meta.UnsafeGuessKindToResource(gvk)
		childGets := 0

		for _, action := range dyn.Actions() {
			getAction, ok := action.(clienttesting.GetAction)
			if ok && action.GetResource() == childGVR && getAction.GetName() == childName {
				childGets++
			}
		}

		if err == nil {
			return "", childGets, src, dyn
		}

		return err.Error(), childGets, src, dyn
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotError, childGets, src, dyn := run(t, tc)
			if tc.wantSuccess {
				if gotError != "" {
					t.Fatalf("Run error = %q, want success", gotError)
				}

				if childGets != 1 {
					t.Errorf("child GET calls = %d, want 1", childGets)
				}

				if src.calls != 1 {
					t.Errorf("RestoreManifestsScoped calls = %d, want 1", src.calls)
				}

				return
			}

			if gotError == "" {
				t.Fatal("Run unexpectedly succeeded")
			}

			for _, part := range []string{
				tc.apiVersion,
				tc.kind + "/" + childName,
				"namespace-local contract",
				"cluster-scoped",
			} {
				if !strings.Contains(gotError, part) {
					t.Errorf("Run error %q does not contain %q", gotError, part)
				}
			}

			if childGets != 0 {
				t.Errorf("child GET calls = %d, want none before scope validation", childGets)
			}

			for _, action := range dyn.Actions() {
				getAction, ok := action.(clienttesting.GetAction)
				if ok && getAction.GetName() == "must-not-traverse" {
					t.Errorf("descendant API action occurred before parent scope validation: %v", action)
				}
			}

			assertNoRestoreMutation(t, src, dyn)

			repeatedError, repeatedGets, repeatedSource, repeatedDynamic := run(t, tc)
			if repeatedError != gotError {
				t.Errorf("repeated error = %q, want deterministic %q", repeatedError, gotError)
			}

			if repeatedGets != 0 {
				t.Errorf("repeated child GET calls = %d, want none", repeatedGets)
			}

			assertNoRestoreMutation(t, repeatedSource, repeatedDynamic)
		})
	}
}

func TestRun_SelectedNode_HierarchyMappingAndAPIErrorsFailClosed(t *testing.T) {
	t.Parallel()

	const (
		apiVersion = "errors.example.io/v1"
		kind       = "ErrorSnapshot"
		childName  = "error-child"
	)

	errDiscovery := errors.New("discovery unavailable")
	tests := []struct {
		name          string
		apiVersion    string
		mapperError   error
		apiError      error
		wantError     error
		wantChildGets int
	}{
		{
			name:        "discovery error",
			apiVersion:  apiVersion,
			mapperError: errDiscovery,
			wantError:   errDiscovery,
		},
		{
			name:       "unknown GVK",
			apiVersion: "unknown.example.io/v1",
		},
		{
			name:          "child API cancellation",
			apiVersion:    apiVersion,
			apiError:      context.Canceled,
			wantError:     context.Canceled,
			wantChildGets: 1,
		},
		{
			name:          "child API timeout",
			apiVersion:    apiVersion,
			apiError:      context.DeadlineExceeded,
			wantError:     context.DeadlineExceeded,
			wantChildGets: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gvk := schema.FromAPIVersionAndKind(tc.apiVersion, kind)
			root := snapshotWithChildren(
				readySnapshot(),
				snapshotChildRef(tc.apiVersion, kind, childName),
			)
			child := readyGeneratedSnapshot(tc.apiVersion, kind, childName)
			src := &stubSource{body: mustArray(t, configMapManifest("restored"))}
			dyn := newFakeDynamic(root, child)
			mapper := testMapperWithGenerated()
			if tc.name != "unknown GVK" {
				mapper = testMapperWithGenerated(gvk)
			}

			if tc.mapperError != nil {
				baseMapper := mapper
				mapper = restMappingInterceptor{
					RESTMapper: baseMapper,
					intercept: func(groupKind schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
						if groupKind == gvk.GroupKind() && slices.Equal(versions, []string{gvk.Version}) {
							return nil, tc.mapperError
						}

						return nil, nil
					},
				}
			}

			if tc.apiError != nil {
				childGVR, _ := meta.UnsafeGuessKindToResource(gvk)
				dyn.PrependReactor("get", childGVR.Resource, func(action clienttesting.Action) (bool, runtime.Object, error) {
					getAction, ok := action.(clienttesting.GetAction)
					if !ok || action.GetResource() != childGVR || getAction.GetName() != childName {
						return false, nil, nil
					}

					return true, nil, tc.apiError
				})
			}

			cfg := Config{
				Namespace:              testNS,
				Snapshot:               testSnap,
				SelectedNodeKind:       kind,
				SelectedNodeName:       childName,
				SelectedNodeAPIVersion: tc.apiVersion,
				Source:                 src,
				Dynamic:                dyn,
				Mapper:                 mapper,
				Log:                    discardLogger(),
			}

			err := Run(context.Background(), cfg)
			if err == nil {
				t.Fatal("Run unexpectedly succeeded")
			}

			if tc.wantError != nil && !errors.Is(err, tc.wantError) {
				t.Errorf("Run error = %v, want errors.Is(%v)", err, tc.wantError)
			}

			childGVR, _ := meta.UnsafeGuessKindToResource(gvk)
			childGets := 0
			for _, action := range dyn.Actions() {
				getAction, ok := action.(clienttesting.GetAction)
				if ok && action.GetResource() == childGVR && getAction.GetName() == childName {
					childGets++
				}
			}

			if childGets != tc.wantChildGets {
				t.Errorf("child GET calls = %d, want %d", childGets, tc.wantChildGets)
			}

			if tc.mapperError != nil && !strings.Contains(err.Error(), "resolve resource") {
				t.Errorf("Run error %q does not identify resource discovery", err)
			}

			if tc.name == "unknown GVK" && !strings.Contains(err.Error(), "no matches for kind") {
				t.Errorf("Run error %q does not identify unknown GVK", err)
			}

			assertNoRestoreMutation(t, src, dyn)
		})
	}
}

func TestRun_RootHierarchyMappingMustBeNamespaced(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		selectedRoot bool
	}{
		{name: "full root restore"},
		{name: "selected root restore", selectedRoot: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mapper := testMapper()
			rootGVK := schema.GroupVersionKind{
				Group:   snapshotapi.StorageGroup,
				Version: snapshotapi.Version,
				Kind:    snapshotKind,
			}
			mapper.(*meta.DefaultRESTMapper).Add(rootGVK, meta.RESTScopeRoot)
			src := &stubSource{body: mustArray(t, configMapManifest("restored"))}
			dyn := newFakeDynamic(readySnapshot())
			cfg := baseConfig(src, dyn)
			cfg.Mapper = mapper

			if tc.selectedRoot {
				cfg.SelectedNodeKind = snapshotKind
				cfg.SelectedNodeName = testSnap
				cfg.SelectedNodeAPIVersion = rootGVK.GroupVersion().String()
			}

			err := Run(context.Background(), cfg)
			if err == nil {
				t.Fatal("Run unexpectedly succeeded")
			}

			for _, part := range []string{
				rootGVK.GroupVersion().String(),
				snapshotKind + "/" + testSnap,
				"namespace-local contract",
				"cluster-scoped",
			} {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("Run error %q does not contain %q", err, part)
				}
			}

			for _, action := range dyn.Actions() {
				if action.GetVerb() == "get" && action.GetResource() == snapshotGVR {
					t.Errorf("root GET occurred before scope validation: %v", action)
				}
			}

			assertNoRestoreMutation(t, src, dyn)
		})
	}
}

func TestRun_SelectedNode_APIVersionDisambiguatesGeneratedIdentity(t *testing.T) {
	t.Parallel()

	const (
		sharedKind = "SharedSnapshot"
		sharedName = "shared"
	)

	apiVersions := []string{
		"v1",
		"alpha.example.io/v1alpha1",
		"alpha.example.io/v1beta1",
		"beta.example.io/v1",
	}
	gvks := make([]schema.GroupVersionKind, 0, len(apiVersions))
	objects := make([]runtime.Object, 0, len(apiVersions)+1)
	childRefs := make([]map[string]interface{}, 0, len(apiVersions))

	for _, apiVersion := range apiVersions {
		gv := schema.FromAPIVersionAndKind(apiVersion, sharedKind)
		gvks = append(gvks, gv)
		objects = append(objects, readyGeneratedSnapshot(apiVersion, sharedKind, sharedName))
		childRefs = append(childRefs, snapshotChildRef(apiVersion, sharedKind, sharedName))
	}

	objects = append(objects, snapshotWithChildren(readySnapshot(), childRefs...))

	tests := []struct {
		name               string
		selectedAPIVersion string
		wantAPIVersion     string
		wantErrSubstrs     []string
	}{
		{
			name: "unqualified identity is ambiguous",
			wantErrSubstrs: []string{
				"ambiguous",
				"matching apiVersions: v1",
				"matching apiVersions: alpha.example.io/v1alpha1",
				"matching apiVersions: alpha.example.io/v1beta1",
				"matching apiVersions: beta.example.io/v1",
				"--node-api-version v1",
				"--node-api-version alpha.example.io/v1beta1",
			},
		},
		{
			name:               "core API version",
			selectedAPIVersion: "v1",
			wantAPIVersion:     "v1",
		},
		{
			name:               "named group stored version",
			selectedAPIVersion: "alpha.example.io/v1alpha1",
			wantAPIVersion:     "alpha.example.io/v1alpha1",
		},
		{
			name:               "same group alternate version",
			selectedAPIVersion: "alpha.example.io/v1beta1",
			wantAPIVersion:     "alpha.example.io/v1beta1",
		},
		{
			name:               "same version different group",
			selectedAPIVersion: "beta.example.io/v1",
			wantAPIVersion:     "beta.example.io/v1",
		},
		{
			name:               "well formed API version has no match",
			selectedAPIVersion: "gamma.example.io/v1",
			wantErrSubstrs:     []string{"gamma.example.io/v1 SharedSnapshot/shared", "does not belong"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
			dyn := newFakeDynamic(objects...)
			cfg := Config{
				Namespace:              testNS,
				Snapshot:               testSnap,
				SelectedNodeKind:       sharedKind,
				SelectedNodeName:       sharedName,
				SelectedNodeAPIVersion: tc.selectedAPIVersion,
				Source:                 src,
				Dynamic:                dyn,
				Mapper:                 testMapperWithGenerated(gvks...),
				Log:                    discardLogger(),
			}

			err := Run(context.Background(), cfg)
			if len(tc.wantErrSubstrs) != 0 {
				if err == nil {
					t.Fatal("Run unexpectedly succeeded")
				}

				for _, substr := range tc.wantErrSubstrs {
					if !strings.Contains(err.Error(), substr) {
						t.Errorf("Run error %q does not contain %q", err, substr)
					}
				}

				assertNoRestoreMutation(t, src, dyn)
			} else {
				if err != nil {
					t.Fatalf("Run: %v", err)
				}

				if src.calls != 1 {
					t.Fatalf("RestoreManifestsScoped calls = %d, want 1", src.calls)
				}

				if src.gotRef.APIVersion != tc.wantAPIVersion ||
					src.gotRef.Kind != sharedKind ||
					src.gotRef.Name != sharedName {
					t.Errorf("NodeRef = %+v, want %s %s/%s", src.gotRef, tc.wantAPIVersion, sharedKind, sharedName)
				}
			}

			hierarchyGets := 0
			for _, action := range dyn.Actions() {
				if action.GetVerb() == "get" &&
					(action.GetResource() == snapshotGVR || action.GetResource().Resource == "sharedsnapshots") {
					hierarchyGets++
				}
			}

			wantHierarchyGets := len(apiVersions) + 1
			if tc.wantAPIVersion != "" {
				wantHierarchyGets = slices.Index(apiVersions, tc.wantAPIVersion) + 2
			}

			if hierarchyGets != wantHierarchyGets {
				t.Errorf("hierarchy GET calls = %d, want %d", hierarchyGets, wantHierarchyGets)
			}
		})
	}
}

func TestResolveNodeRef_IterativeDepthFirstTraversal(t *testing.T) {
	t.Parallel()

	target := readyDomainDiskSnapshot("target")
	grandchild := snapshotWithChildren(
		readyDomainDiskSnapshot("grandchild"),
		snapshotChildRef(target.GetAPIVersion(), target.GetKind(), target.GetName()),
	)
	child := snapshotWithChildren(
		readyDomainDiskSnapshot("child"),
		snapshotChildRef(grandchild.GetAPIVersion(), grandchild.GetKind(), grandchild.GetName()),
	)
	root := snapshotWithChildren(
		readySnapshot(),
		snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
	)
	baseDynamic := newFakeDynamic(root, child, grandchild, target)

	var getOrder []string
	dyn := &interceptingDynamicClient{
		Interface: baseDynamic,
		intercept: func(
			_ context.Context,
			verb string,
			_ schema.GroupVersionResource,
			_ string,
			name string,
		) error {
			if verb == "get" {
				getOrder = append(getOrder, name)
			}

			return nil
		},
	}
	cfg := Config{
		Namespace:              testNS,
		Snapshot:               testSnap,
		SelectedNodeKind:       target.GetKind(),
		SelectedNodeName:       target.GetName(),
		SelectedNodeAPIVersion: target.GetAPIVersion(),
		Dynamic:                dyn,
		Mapper:                 testMapperWithDomain(),
	}

	var (
		maxNodes int
		maxStack int
	)

	ref, obj, err := cfg.resolveNodeRefWithLimits(
		context.Background(),
		hierarchyWalkLimits{maxDepth: 3, maxNodes: 4},
		func(stats hierarchyWalkStats) {
			maxNodes = max(maxNodes, stats.nodes)
			maxStack = max(maxStack, stats.stackDepth)
		},
	)
	if err != nil {
		t.Fatalf("resolveNodeRefWithLimits: %v", err)
	}

	if ref.Name != target.GetName() || obj.GetName() != target.GetName() {
		t.Errorf("resolved (%+v, %s), want target %q", ref, obj.GetName(), target.GetName())
	}

	wantOrder := []string{testSnap, child.GetName(), grandchild.GetName(), target.GetName()}
	if !slices.Equal(getOrder, wantOrder) {
		t.Errorf("hierarchy GET order = %v, want depth-first parent-before-child order %v", getOrder, wantOrder)
	}

	if maxNodes != 4 {
		t.Errorf("maximum accounted nodes = %d, want 4", maxNodes)
	}

	if maxStack > 4 {
		t.Errorf("maximum iterative stack depth = %d, want at most 4", maxStack)
	}
}

func TestResolveNodeRef_RejectsRecursiveGraphShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		objects      func() []runtime.Object
		wantGetOrder []string
		wantRef      string
	}{
		{
			name: "self cycle",
			objects: func() []runtime.Object {
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(
						snapshotapi.StorageGroup+"/"+snapshotapi.Version,
						snapshotKind,
						testSnap,
					),
				)

				return []runtime.Object{root}
			},
			wantGetOrder: []string{testSnap},
			wantRef:      testSnap,
		},
		{
			name: "multi-node cycle",
			objects: func() []runtime.Object {
				child := snapshotWithChildren(
					readyDomainDiskSnapshot("cycle-child"),
					snapshotChildRef(
						snapshotapi.StorageGroup+"/"+snapshotapi.Version,
						snapshotKind,
						testSnap,
					),
				)
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
				)

				return []runtime.Object{root, child}
			},
			wantGetOrder: []string{testSnap, "cycle-child"},
			wantRef:      testSnap,
		},
		{
			name: "duplicate sibling",
			objects: func() []runtime.Object {
				child := readyDomainDiskSnapshot("duplicate")
				ref := snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName())
				root := snapshotWithChildren(readySnapshot(), ref, ref)

				return []runtime.Object{root, child}
			},
			wantGetOrder: []string{testSnap, "duplicate"},
			wantRef:      "duplicate",
		},
		{
			name: "diamond",
			objects: func() []runtime.Object {
				shared := readyDomainDiskSnapshot("shared")
				left := snapshotWithChildren(
					readyDomainDiskSnapshot("left"),
					snapshotChildRef(shared.GetAPIVersion(), shared.GetKind(), shared.GetName()),
				)
				right := snapshotWithChildren(
					readyDomainDiskSnapshot("right"),
					snapshotChildRef(shared.GetAPIVersion(), shared.GetKind(), shared.GetName()),
				)
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(left.GetAPIVersion(), left.GetKind(), left.GetName()),
					snapshotChildRef(right.GetAPIVersion(), right.GetKind(), right.GetName()),
				)

				return []runtime.Object{root, left, right, shared}
			},
			wantGetOrder: []string{testSnap, "left", "shared", "right"},
			wantRef:      "shared",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			baseDynamic := newFakeDynamic(tc.objects()...)

			var getOrder []string
			dyn := &interceptingDynamicClient{
				Interface: baseDynamic,
				intercept: func(
					_ context.Context,
					verb string,
					_ schema.GroupVersionResource,
					_ string,
					name string,
				) error {
					if verb == "get" {
						getOrder = append(getOrder, name)
					}

					return nil
				},
			}
			cfg := Config{
				Namespace:        testNS,
				Snapshot:         testSnap,
				SelectedNodeKind: "NeverMatches",
				SelectedNodeName: "never",
				Dynamic:          dyn,
				Mapper:           testMapperWithDomain(),
			}

			_, _, err := cfg.resolveNodeRefWithLimits(
				context.Background(),
				hierarchyWalkLimits{maxDepth: 8, maxNodes: 16},
				nil,
			)
			if err == nil {
				t.Fatal("resolveNodeRefWithLimits unexpectedly succeeded")
			}

			for _, part := range []string{"duplicate or cyclic ref", tc.wantRef} {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("error %q does not contain %q", err, part)
				}
			}

			if !slices.Equal(getOrder, tc.wantGetOrder) {
				t.Errorf("hierarchy GET order = %v, want %v", getOrder, tc.wantGetOrder)
			}
		})
	}
}

func TestResolveNodeRef_LimitBoundaries(t *testing.T) {
	t.Parallel()

	first := readyDomainDiskSnapshot("first")
	second := readyDomainDiskSnapshot("second")
	chainRoot := snapshotWithChildren(
		readySnapshot(),
		snapshotChildRef(first.GetAPIVersion(), first.GetKind(), first.GetName()),
	)
	chainFirst := snapshotWithChildren(
		first,
		snapshotChildRef(second.GetAPIVersion(), second.GetKind(), second.GetName()),
	)
	flatFirst := readyDomainDiskSnapshot("flat-first")
	flatSecond := readyDomainDiskSnapshot("flat-second")
	missingBudgetSecond := readyDomainDiskSnapshot("budget-second")

	tests := []struct {
		name         string
		limits       hierarchyWalkLimits
		objects      []runtime.Object
		selectedName string
		wantSuccess  bool
		wantError    []string
		wantGets     []string
	}{
		{
			name:         "depth at limit succeeds",
			limits:       hierarchyWalkLimits{maxDepth: 2, maxNodes: 3},
			objects:      []runtime.Object{chainRoot, chainFirst, second},
			selectedName: second.GetName(),
			wantSuccess:  true,
			wantGets:     []string{testSnap, first.GetName(), second.GetName()},
		},
		{
			name:         "depth over limit fails before get",
			limits:       hierarchyWalkLimits{maxDepth: 1, maxNodes: 3},
			objects:      []runtime.Object{chainRoot, chainFirst, second},
			selectedName: second.GetName(),
			wantError:    []string{"depth budget of 1", "depth 2", second.GetName()},
			wantGets:     []string{testSnap, first.GetName()},
		},
		{
			name:   "node count at limit completes",
			limits: hierarchyWalkLimits{maxDepth: 1, maxNodes: 3},
			objects: []runtime.Object{
				snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(flatFirst.GetAPIVersion(), flatFirst.GetKind(), flatFirst.GetName()),
					snapshotChildRef(flatSecond.GetAPIVersion(), flatSecond.GetKind(), flatSecond.GetName()),
				),
				flatFirst,
				flatSecond,
			},
			selectedName: "absent",
			wantError:    []string{"does not belong"},
			wantGets:     []string{testSnap, flatFirst.GetName(), flatSecond.GetName()},
		},
		{
			name:   "missing ref consumes final node slot",
			limits: hierarchyWalkLimits{maxDepth: 1, maxNodes: 2},
			objects: []runtime.Object{
				snapshotWithChildren(
					notReadySnapshot(),
					snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", "missing"),
					snapshotChildRef(
						missingBudgetSecond.GetAPIVersion(),
						missingBudgetSecond.GetKind(),
						missingBudgetSecond.GetName(),
					),
				),
				missingBudgetSecond,
			},
			selectedName: "absent",
			wantError: []string{
				"node budget of 2",
				"including missing children",
				missingBudgetSecond.GetName(),
			},
			wantGets: []string{testSnap, "missing"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			baseDynamic := newFakeDynamic(tc.objects...)

			var getOrder []string
			dyn := &interceptingDynamicClient{
				Interface: baseDynamic,
				intercept: func(
					_ context.Context,
					verb string,
					_ schema.GroupVersionResource,
					_ string,
					name string,
				) error {
					if verb == "get" {
						getOrder = append(getOrder, name)
					}

					return nil
				},
			}
			cfg := Config{
				Namespace:              testNS,
				Snapshot:               testSnap,
				SelectedNodeKind:       "DemoVirtualDiskSnapshot",
				SelectedNodeName:       tc.selectedName,
				SelectedNodeAPIVersion: domainDiskAPIVersion,
				Dynamic:                dyn,
				Mapper:                 testMapperWithDomain(),
			}

			ref, _, err := cfg.resolveNodeRefWithLimits(context.Background(), tc.limits, nil)
			if tc.wantSuccess {
				if err != nil {
					t.Fatalf("resolveNodeRefWithLimits: %v", err)
				}

				if ref.Name != tc.selectedName {
					t.Errorf("resolved ref = %+v, want name %q", ref, tc.selectedName)
				}
			} else {
				if err == nil {
					t.Fatal("resolveNodeRefWithLimits unexpectedly succeeded")
				}

				for _, part := range tc.wantError {
					if !strings.Contains(err.Error(), part) {
						t.Errorf("error %q does not contain %q", err, part)
					}
				}
			}

			if !slices.Equal(getOrder, tc.wantGets) {
				t.Errorf("hierarchy GET order = %v, want %v", getOrder, tc.wantGets)
			}
		})
	}
}

func TestResolveNodeRef_ExactGeneratedSelectorStopsAtAuthoritativeRef(t *testing.T) {
	t.Parallel()

	alias := readyDomainDiskSnapshot("earlier-alias")
	target := readyDomainDiskSnapshot("target")
	setSnapshotSourceRef(alias, target.GetAPIVersion(), target.GetKind(), target.GetName())
	targetStatus, _ := target.Object["status"].(map[string]interface{})
	targetStatus["childrenSnapshotRefs"] = []interface{}{"malformed descendant"}
	root := readySnapshot()
	rootStatus, _ := root.Object["status"].(map[string]interface{})
	rootStatus["childrenSnapshotRefs"] = []interface{}{
		snapshotChildRef(alias.GetAPIVersion(), alias.GetKind(), alias.GetName()),
		snapshotChildRef(target.GetAPIVersion(), target.GetKind(), target.GetName()),
		"malformed unrelated sibling",
	}
	baseDynamic := newFakeDynamic(root, alias, target)

	var getOrder []string
	dyn := &interceptingDynamicClient{
		Interface: baseDynamic,
		intercept: func(
			_ context.Context,
			verb string,
			_ schema.GroupVersionResource,
			_ string,
			name string,
		) error {
			if verb == "get" {
				getOrder = append(getOrder, name)
			}

			return nil
		},
	}
	cfg := Config{
		Namespace:              testNS,
		Snapshot:               testSnap,
		SelectedNodeKind:       target.GetKind(),
		SelectedNodeName:       target.GetName(),
		SelectedNodeAPIVersion: target.GetAPIVersion(),
		Dynamic:                dyn,
		Mapper:                 testMapperWithDomain(),
	}

	ref, _, err := cfg.resolveNodeRefWithLimits(
		context.Background(),
		hierarchyWalkLimits{maxDepth: 1, maxNodes: 3},
		nil,
	)
	if err != nil {
		t.Fatalf("resolveNodeRefWithLimits: %v", err)
	}

	if ref.Name != target.GetName() {
		t.Errorf("resolved ref = %+v, want target %q", ref, target.GetName())
	}

	wantOrder := []string{testSnap, alias.GetName(), target.GetName()}
	if !slices.Equal(getOrder, wantOrder) {
		t.Errorf("hierarchy GET order = %v, want %v", getOrder, wantOrder)
	}
}

func TestRun_SelectedNode_CancellationStopsBetweenHierarchyNodes(t *testing.T) {
	t.Parallel()

	first := readyDomainDiskSnapshot("first")
	second := readyDomainDiskSnapshot("second")
	root := snapshotWithChildren(
		readySnapshot(),
		snapshotChildRef(first.GetAPIVersion(), first.GetKind(), first.GetName()),
		snapshotChildRef(second.GetAPIVersion(), second.GetKind(), second.GetName()),
	)
	baseDynamic := newFakeDynamic(root, first, second)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	cause := errors.New("operator canceled hierarchy walk")

	var getOrder []string
	dyn := &interceptingDynamicClient{
		Interface: baseDynamic,
		intercept: func(
			_ context.Context,
			verb string,
			_ schema.GroupVersionResource,
			_ string,
			name string,
		) error {
			if verb != "get" {
				return nil
			}

			getOrder = append(getOrder, name)
			if name == first.GetName() {
				cancel(cause)
			}

			return nil
		},
	}
	src := &stubSource{body: mustArray(t, configMapManifest("must-not-apply"))}
	cfg := Config{
		Namespace:        testNS,
		Snapshot:         testSnap,
		SelectedNodeKind: "NeverMatches",
		SelectedNodeName: "never",
		Source:           src,
		Dynamic:          dyn,
		Mapper:           testMapperWithDomain(),
		Log:              discardLogger(),
	}

	err := Run(ctx, cfg)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}

	if !errors.Is(err, cause) {
		t.Errorf("error = %v, want cancellation cause %v", err, cause)
	}

	wantOrder := []string{testSnap, first.GetName()}
	if !slices.Equal(getOrder, wantOrder) {
		t.Errorf("hierarchy GET order = %v, want %v", getOrder, wantOrder)
	}

	assertNoRestoreMutation(t, src, baseDynamic)
}

func TestResolveNodeRef_ProductionDepthBudget(t *testing.T) {
	t.Parallel()

	const overLimitDepth = restoreHierarchyMaxDepth + 1

	nodes := make([]*unstructured.Unstructured, 0, overLimitDepth)
	objects := make([]runtime.Object, 0, overLimitDepth+1)
	root := readySnapshot()
	objects = append(objects, root)

	for depth := 1; depth <= overLimitDepth; depth++ {
		node := readyDomainDiskSnapshot(fmt.Sprintf("depth-%03d", depth))
		nodes = append(nodes, node)
		objects = append(objects, node)
	}

	snapshotWithChildren(
		root,
		snapshotChildRef(nodes[0].GetAPIVersion(), nodes[0].GetKind(), nodes[0].GetName()),
	)
	for i := 0; i+1 < len(nodes); i++ {
		snapshotWithChildren(
			nodes[i],
			snapshotChildRef(nodes[i+1].GetAPIVersion(), nodes[i+1].GetKind(), nodes[i+1].GetName()),
		)
	}

	baseDynamic := newFakeDynamic(objects...)

	var hierarchyGets int
	dyn := &interceptingDynamicClient{
		Interface: baseDynamic,
		intercept: func(
			_ context.Context,
			verb string,
			_ schema.GroupVersionResource,
			_ string,
			_ string,
		) error {
			if verb == "get" {
				hierarchyGets++
			}

			return nil
		},
	}
	cfg := Config{
		Namespace:              testNS,
		Snapshot:               testSnap,
		SelectedNodeKind:       nodes[len(nodes)-1].GetKind(),
		SelectedNodeName:       nodes[len(nodes)-1].GetName(),
		SelectedNodeAPIVersion: nodes[len(nodes)-1].GetAPIVersion(),
		Dynamic:                dyn,
		Mapper:                 testMapperWithDomain(),
	}

	maxStack := 0
	_, _, err := cfg.resolveNodeRefWithLimits(
		context.Background(),
		hierarchyWalkLimits{
			maxDepth: restoreHierarchyMaxDepth,
			maxNodes: restoreHierarchyMaxNodes,
		},
		func(stats hierarchyWalkStats) {
			maxStack = max(maxStack, stats.stackDepth)
		},
	)
	if err == nil {
		t.Fatal("resolveNodeRefWithLimits unexpectedly succeeded")
	}

	for _, part := range []string{
		fmt.Sprintf("depth budget of %d", restoreHierarchyMaxDepth),
		fmt.Sprintf("depth %d", overLimitDepth),
		nodes[len(nodes)-1].GetName(),
	} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not contain %q", err, part)
		}
	}

	if hierarchyGets != restoreHierarchyMaxDepth+1 {
		t.Errorf("hierarchy GET calls = %d, want %d", hierarchyGets, restoreHierarchyMaxDepth+1)
	}

	if maxStack != restoreHierarchyMaxDepth+1 {
		t.Errorf("maximum iterative stack depth = %d, want %d", maxStack, restoreHierarchyMaxDepth+1)
	}
}

func TestResolveNodeRef_ProductionNodeBudgetBoundsWideWalk(t *testing.T) {
	t.Parallel()

	childRefs := make([]map[string]interface{}, 0, restoreHierarchyMaxNodes)
	for i := 0; i < restoreHierarchyMaxNodes; i++ {
		childRefs = append(childRefs, snapshotChildRef(
			domainDiskAPIVersion,
			"DemoVirtualDiskSnapshot",
			fmt.Sprintf("wide-%05d", i),
		))
	}

	root := snapshotWithChildren(readySnapshot(), childRefs...)
	baseDynamic := newFakeDynamic(root)

	var (
		activeChildGets int
		childGets       int
		maxConcurrent   int
	)

	baseDynamic.PrependReactor("get", domainDiskGVR.Resource, func(action clienttesting.Action) (bool, runtime.Object, error) {
		getAction, ok := action.(clienttesting.GetAction)
		if !ok {
			return true, nil, fmt.Errorf("unexpected action type %T", action)
		}

		activeChildGets++
		childGets++
		maxConcurrent = max(maxConcurrent, activeChildGets)
		defer func() {
			activeChildGets--
		}()

		return true, readyDomainDiskSnapshot(getAction.GetName()), nil
	})

	cfg := Config{
		Namespace:        testNS,
		Snapshot:         testSnap,
		SelectedNodeKind: "NeverMatches",
		SelectedNodeName: "never",
		Dynamic:          baseDynamic,
		Mapper:           testMapperWithDomain(),
	}

	var (
		maxNodes int
		maxStack int
	)

	_, _, err := cfg.resolveNodeRefWithLimits(
		context.Background(),
		hierarchyWalkLimits{
			maxDepth: restoreHierarchyMaxDepth,
			maxNodes: restoreHierarchyMaxNodes,
		},
		func(stats hierarchyWalkStats) {
			maxNodes = max(maxNodes, stats.nodes)
			maxStack = max(maxStack, stats.stackDepth)
		},
	)
	if err == nil {
		t.Fatal("resolveNodeRefWithLimits unexpectedly succeeded")
	}

	for _, part := range []string{
		fmt.Sprintf("node budget of %d", restoreHierarchyMaxNodes),
		"wide-09999",
	} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not contain %q", err, part)
		}
	}

	if childGets != restoreHierarchyMaxNodes-1 {
		t.Errorf("child GET calls = %d, want %d", childGets, restoreHierarchyMaxNodes-1)
	}

	if maxNodes != restoreHierarchyMaxNodes {
		t.Errorf("maximum accounted nodes = %d, want %d", maxNodes, restoreHierarchyMaxNodes)
	}

	if maxStack != 2 {
		t.Errorf("maximum iterative stack depth = %d, want 2 for a wide one-level hierarchy", maxStack)
	}

	if maxConcurrent != 1 || activeChildGets != 0 {
		t.Errorf("child GET concurrency = max %d active %d, want max 1 and quiescent", maxConcurrent, activeChildGets)
	}
}

func TestRun_SelectedNode_GeneratedIdentityUsesStoredVersionAfterConversion(t *testing.T) {
	t.Parallel()

	const (
		storedAPIVersion    = "conversion.example.io/v1alpha1"
		convertedAPIVersion = "conversion.example.io/v1beta1"
		kind                = "ConvertedSnapshot"
		name                = "converted"
	)

	tests := []struct {
		name               string
		selectedAPIVersion string
		wantMatch          bool
	}{
		{
			name:               "stored child ref version matches",
			selectedAPIVersion: storedAPIVersion,
			wantMatch:          true,
		},
		{
			name:               "converted response version is not generated identity",
			selectedAPIVersion: convertedAPIVersion,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			storedGVK := schema.FromAPIVersionAndKind(storedAPIVersion, kind)
			child := readyGeneratedSnapshot(storedAPIVersion, kind, name)
			converted := child.DeepCopy()
			converted.SetAPIVersion(convertedAPIVersion)
			root := snapshotWithChildren(
				readySnapshot(),
				snapshotChildRef(storedAPIVersion, kind, name),
			)
			dyn := newFakeDynamic(root, child)
			storedGVR, _ := meta.UnsafeGuessKindToResource(storedGVK)
			dyn.PrependReactor("get", storedGVR.Resource, func(action clienttesting.Action) (bool, runtime.Object, error) {
				if action.GetResource() != storedGVR {
					return false, nil, nil
				}

				getAction, ok := action.(clienttesting.GetAction)
				if !ok || getAction.GetName() != name {
					return false, nil, nil
				}

				return true, converted.DeepCopy(), nil
			})

			src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
			cfg := Config{
				Namespace:              testNS,
				Snapshot:               testSnap,
				SelectedNodeKind:       kind,
				SelectedNodeName:       name,
				SelectedNodeAPIVersion: tc.selectedAPIVersion,
				Source:                 src,
				Dynamic:                dyn,
				Mapper:                 testMapperWithGenerated(storedGVK),
				Log:                    discardLogger(),
			}

			err := Run(context.Background(), cfg)
			if !tc.wantMatch {
				if err == nil || !strings.Contains(err.Error(), "does not belong") {
					t.Fatalf("Run error = %v, want no-match error", err)
				}

				assertNoRestoreMutation(t, src, dyn)

				return
			}

			if err != nil {
				t.Fatalf("Run: %v", err)
			}

			if src.gotRef.APIVersion != storedAPIVersion {
				t.Errorf("selected apiVersion = %q, want stored ref version %q", src.gotRef.APIVersion, storedAPIVersion)
			}
		})
	}
}

func TestRun_SelectedNode_ResolvesImportSourceAliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		objects      func(*testing.T) []runtime.Object
		selectedKind string
		selectedName string
		wantRef      aggapi.NodeRef
	}{
		{
			name: "exact annotation match on domain import",
			objects: func(t *testing.T) []runtime.Object {
				child := readyDomainDiskSnapshot("nss-child-imported")
				setSnapshotMode(child, snapshotapi.SnapshotModeImport)
				setImportSourceRefAnnotation(
					t,
					child,
					"virtualization.deckhouse.io/v1alpha2",
					"VirtualDisk",
					"disk-a",
				)
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
				)

				return []runtime.Object{root, child}
			},
			selectedKind: "VirtualDisk",
			selectedName: "disk-a",
			wantRef: aggapi.NodeRef{
				APIVersion: domainDiskAPIVersion,
				Kind:       "DemoVirtualDiskSnapshot",
				Name:       "nss-child-imported",
				Namespace:  testNS,
			},
		},
		{
			name: "renamed VolumeSnapshot import resolves original PVC",
			objects: func(t *testing.T) []runtime.Object {
				child := readyVolumeSnapshot("nss-vs-generated")
				setSnapshotMode(child, snapshotapi.SnapshotModeImport)
				setImportSourceRefAnnotation(t, child, "v1", "PersistentVolumeClaim", "pvc-before-import")
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
				)

				return []runtime.Object{root, child}
			},
			selectedKind: "PersistentVolumeClaim",
			selectedName: "pvc-before-import",
			wantRef: aggapi.NodeRef{
				APIVersion: volumeSnapshotAPIVersion,
				Kind:       "VolumeSnapshot",
				Name:       "nss-vs-generated",
				Namespace:  testNS,
			},
		},
		{
			name: "root import survives source namespace change",
			objects: func(t *testing.T) []runtime.Object {
				root := readySnapshot()
				setSnapshotMode(root, snapshotapi.SnapshotModeImport)
				setImportSourceRefAnnotation(t, root, "v1", "Namespace", "source-namespace")

				return []runtime.Object{root}
			},
			selectedKind: "Namespace",
			selectedName: "source-namespace",
			wantRef: aggapi.NodeRef{
				APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
				Kind:       "Snapshot",
				Name:       testSnap,
				Namespace:  testNS,
			},
		},
		{
			name: "preserved alias differs from current producer identity",
			objects: func(t *testing.T) []runtime.Object {
				child := readyDomainDiskSnapshot("nss-child-reimported")
				setSnapshotMode(child, snapshotapi.SnapshotModeImport)
				setSnapshotSourceRef(
					child,
					"virtualization.deckhouse.io/v1alpha3",
					"VirtualDisk",
					"disk-after-import",
				)
				setImportSourceRefAnnotation(
					t,
					child,
					"virtualization.deckhouse.io/v1alpha2",
					"VirtualDisk",
					"disk-before-import",
				)
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
				)

				return []runtime.Object{root, child}
			},
			selectedKind: "VirtualDisk",
			selectedName: "disk-before-import",
			wantRef: aggapi.NodeRef{
				APIVersion: domainDiskAPIVersion,
				Kind:       "DemoVirtualDiskSnapshot",
				Name:       "nss-child-reimported",
				Namespace:  testNS,
			},
		},
		{
			name: "UID and API version differences do not duplicate one node",
			objects: func(t *testing.T) []runtime.Object {
				child := readyDomainDiskSnapshot("nss-child-one-node")
				setSnapshotMode(child, snapshotapi.SnapshotModeImport)
				setSnapshotSourceRef(
					child,
					"virtualization.deckhouse.io/v1alpha3",
					"VirtualDisk",
					"disk-shared",
				)
				setImportSourceRefAnnotation(
					t,
					child,
					"virtualization.deckhouse.io/v1alpha2",
					"VirtualDisk",
					"disk-shared",
				)
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
				)

				return []runtime.Object{root, child}
			},
			selectedKind: "VirtualDisk",
			selectedName: "disk-shared",
			wantRef: aggapi.NodeRef{
				APIVersion: domainDiskAPIVersion,
				Kind:       "DemoVirtualDiskSnapshot",
				Name:       "nss-child-one-node",
				Namespace:  testNS,
			},
		},
		{
			name: "capture node ignores spoofed import annotation",
			objects: func(_ *testing.T) []runtime.Object {
				child := readyDomainDiskSnapshot("nss-child-capture")
				child.SetAnnotations(map[string]string{
					snapshotapi.AnnotationImportSourceRef: "{not-json",
				})
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
				)

				return []runtime.Object{root, child}
			},
			selectedKind: "DemoVirtualDiskSnapshot",
			selectedName: "nss-child-capture",
			wantRef: aggapi.NodeRef{
				APIVersion: domainDiskAPIVersion,
				Kind:       "DemoVirtualDiskSnapshot",
				Name:       "nss-child-capture",
				Namespace:  testNS,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
			dyn := newFakeDynamic(tc.objects(t)...)
			cfg := Config{
				Namespace:        testNS,
				Snapshot:         testSnap,
				SelectedNodeKind: tc.selectedKind,
				SelectedNodeName: tc.selectedName,
				Source:           src,
				Dynamic:          dyn,
				Mapper:           testMapperWithDomain(),
				Log:              discardLogger(),
				PollInterval:     time.Millisecond,
			}

			if err := Run(context.Background(), cfg); err != nil {
				t.Fatalf("Run: %v", err)
			}

			if src.gotRef != tc.wantRef {
				t.Errorf("NodeRef: got %+v, want %+v", src.gotRef, tc.wantRef)
			}
		})
	}
}

func TestRun_SelectedNode_APIVersionFiltersOriginalAliases(t *testing.T) {
	t.Parallel()

	const (
		sourceKind = "VirtualDisk"
		sourceName = "shared-source"
	)

	tests := []struct {
		name               string
		selectedAPIVersion string
		wantGeneratedName  string
		wantErrSubstrs     []string
	}{
		{
			name: "unqualified aliases are ambiguous",
			wantErrSubstrs: []string{
				"ambiguous",
				"virtualization.deckhouse.io/v1alpha2",
				"legacy.example.io/v1",
				"--node-api-version virtualization.deckhouse.io/v1alpha2",
				"--node-api-version legacy.example.io/v1",
			},
		},
		{
			name:               "status source ref exact version",
			selectedAPIVersion: "virtualization.deckhouse.io/v1alpha2",
			wantGeneratedName:  "nss-current",
		},
		{
			name:               "import annotation exact version",
			selectedAPIVersion: "legacy.example.io/v1",
			wantGeneratedName:  "nss-imported",
		},
		{
			name:               "wrong alias version matches nothing",
			selectedAPIVersion: "virtualization.deckhouse.io/v1beta1",
			wantErrSubstrs:     []string{"does not belong"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			current := readyDomainDiskSnapshot("nss-current")
			setSnapshotSourceRef(
				current,
				"virtualization.deckhouse.io/v1alpha2",
				sourceKind,
				sourceName,
			)

			imported := readyDomainDiskSnapshot("nss-imported")
			setSnapshotMode(imported, snapshotapi.SnapshotModeImport)
			setImportSourceRefAnnotation(t, imported, "legacy.example.io/v1", sourceKind, sourceName)

			root := snapshotWithChildren(
				readySnapshot(),
				snapshotChildRef(current.GetAPIVersion(), current.GetKind(), current.GetName()),
				snapshotChildRef(imported.GetAPIVersion(), imported.GetKind(), imported.GetName()),
			)
			src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
			dyn := newFakeDynamic(root, current, imported)
			cfg := Config{
				Namespace:              testNS,
				Snapshot:               testSnap,
				SelectedNodeKind:       sourceKind,
				SelectedNodeName:       sourceName,
				SelectedNodeAPIVersion: tc.selectedAPIVersion,
				Source:                 src,
				Dynamic:                dyn,
				Mapper:                 testMapperWithDomain(),
				Log:                    discardLogger(),
			}

			err := Run(context.Background(), cfg)
			if len(tc.wantErrSubstrs) != 0 {
				if err == nil {
					t.Fatal("Run unexpectedly succeeded")
				}

				for _, substr := range tc.wantErrSubstrs {
					if !strings.Contains(err.Error(), substr) {
						t.Errorf("Run error %q does not contain %q", err, substr)
					}
				}

				assertNoRestoreMutation(t, src, dyn)

				return
			}

			if err != nil {
				t.Fatalf("Run: %v", err)
			}

			if src.calls != 1 {
				t.Fatalf("RestoreManifestsScoped calls = %d, want 1", src.calls)
			}

			if src.gotRef.APIVersion != domainDiskAPIVersion ||
				src.gotRef.Kind != "DemoVirtualDiskSnapshot" ||
				src.gotRef.Name != tc.wantGeneratedName {
				t.Errorf("NodeRef = %+v, want generated node %q", src.gotRef, tc.wantGeneratedName)
			}
		})
	}
}

func TestRun_SelectedNode_RejectsInvalidImportSourceAliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		objects        func(*testing.T) []runtime.Object
		selectedKind   string
		selectedName   string
		wantErrSubstrs []string
		wantOrder      []string
	}{
		{
			name: "legacy import missing annotation has no original alias",
			objects: func(_ *testing.T) []runtime.Object {
				child := readyDomainDiskSnapshot("nss-child-legacy")
				setSnapshotMode(child, snapshotapi.SnapshotModeImport)
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
				)

				return []runtime.Object{root, child}
			},
			selectedKind:   "VirtualDisk",
			selectedName:   "disk-before-import",
			wantErrSubstrs: []string{"does not belong", "VirtualDisk/disk-before-import"},
		},
		{
			name: "capture annotation is not an original alias",
			objects: func(t *testing.T) []runtime.Object {
				child := readyDomainDiskSnapshot("nss-child-capture")
				setImportSourceRefAnnotation(t, child, "virtualization.deckhouse.io/v1alpha2", "VirtualDisk", "spoofed")
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
				)

				return []runtime.Object{root, child}
			},
			selectedKind:   "VirtualDisk",
			selectedName:   "spoofed",
			wantErrSubstrs: []string{"does not belong", "VirtualDisk/spoofed"},
		},
		{
			name: "malformed annotation cannot fall back to generated identity",
			objects: func(_ *testing.T) []runtime.Object {
				child := readyDomainDiskSnapshot("nss-child-malformed")
				setSnapshotMode(child, snapshotapi.SnapshotModeImport)
				child.SetAnnotations(map[string]string{
					snapshotapi.AnnotationImportSourceRef: "{not-json",
				})
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
				)

				return []runtime.Object{root, child}
			},
			selectedKind: "DemoVirtualDiskSnapshot",
			selectedName: "nss-child-malformed",
			wantErrSubstrs: []string{
				"malformed",
				snapshotapi.AnnotationImportSourceRef,
				"nss-child-malformed",
			},
		},
		{
			name: "forbidden namespace makes annotation non-canonical",
			objects: func(_ *testing.T) []runtime.Object {
				child := readyDomainDiskSnapshot("nss-child-conflicting")
				setSnapshotMode(child, snapshotapi.SnapshotModeImport)
				child.SetAnnotations(map[string]string{
					snapshotapi.AnnotationImportSourceRef: `{"apiVersion":"virtualization.deckhouse.io/v1alpha2","kind":"VirtualDisk","name":"disk-a","namespace":"old"}`,
				})
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
				)

				return []runtime.Object{root, child}
			},
			selectedKind: "VirtualDisk",
			selectedName: "disk-a",
			wantErrSubstrs: []string{
				"non-canonical",
				snapshotapi.AnnotationImportSourceRef,
				"namespace",
			},
		},
		{
			name: "malformed status source ref cannot fall back to annotation",
			objects: func(t *testing.T) []runtime.Object {
				child := readyDomainDiskSnapshot("nss-child-bad-status")
				setSnapshotMode(child, snapshotapi.SnapshotModeImport)
				status, _ := child.Object["status"].(map[string]interface{})
				status["sourceRef"] = map[string]interface{}{
					"apiVersion": "virtualization.deckhouse.io/v1alpha3",
					"kind":       "VirtualDisk",
				}
				setImportSourceRefAnnotation(t, child, "virtualization.deckhouse.io/v1alpha2", "VirtualDisk", "disk-a")
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
				)

				return []runtime.Object{root, child}
			},
			selectedKind:   "VirtualDisk",
			selectedName:   "disk-a",
			wantErrSubstrs: []string{"status.sourceRef is incomplete", "nss-child-bad-status"},
		},
		{
			name: "malformed annotation cannot fall back to status source ref",
			objects: func(_ *testing.T) []runtime.Object {
				child := readyDomainDiskSnapshot("nss-child-bad-annotation")
				setSnapshotMode(child, snapshotapi.SnapshotModeImport)
				setSnapshotSourceRef(child, "virtualization.deckhouse.io/v1alpha2", "VirtualDisk", "disk-a")
				child.SetAnnotations(map[string]string{
					snapshotapi.AnnotationImportSourceRef: `{"apiVersion":"","kind":"VirtualDisk","name":"disk-a"}`,
				})
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
				)

				return []runtime.Object{root, child}
			},
			selectedKind: "VirtualDisk",
			selectedName: "disk-a",
			wantErrSubstrs: []string{
				"malformed",
				"apiVersion, kind, and name are required",
				"nss-child-bad-annotation",
			},
		},
		{
			name: "status source ref rejects non-string UID",
			objects: func(_ *testing.T) []runtime.Object {
				child := readyDomainDiskSnapshot("nss-child-bad-uid")
				setSnapshotSourceRef(child, "virtualization.deckhouse.io/v1alpha2", "VirtualDisk", "disk-a")
				status, _ := child.Object["status"].(map[string]interface{})
				sourceRef, _ := status["sourceRef"].(map[string]interface{})
				sourceRef["uid"] = int64(7)
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
				)

				return []runtime.Object{root, child}
			},
			selectedKind:   "VirtualDisk",
			selectedName:   "disk-a",
			wantErrSubstrs: []string{"status.sourceRef.uid", "unexpected type int64"},
		},
		{
			name: "status source ref rejects malformed API version",
			objects: func(_ *testing.T) []runtime.Object {
				child := readyDomainDiskSnapshot("nss-child-bad-source-version")
				setSnapshotSourceRef(child, "virtualization.deckhouse.io/v1/extra", "VirtualDisk", "disk-a")
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
				)

				return []runtime.Object{root, child}
			},
			selectedKind:   "VirtualDisk",
			selectedName:   "disk-a",
			wantErrSubstrs: []string{"status.sourceRef.apiVersion", "invalid", "nss-child-bad-source-version"},
		},
		{
			name: "import annotation rejects malformed API group",
			objects: func(t *testing.T) []runtime.Object {
				child := readyDomainDiskSnapshot("nss-child-bad-import-version")
				setSnapshotMode(child, snapshotapi.SnapshotModeImport)
				setImportSourceRefAnnotation(t, child, "Virtualization.Example/v1", "VirtualDisk", "disk-a")
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
				)

				return []runtime.Object{root, child}
			},
			selectedKind: "VirtualDisk",
			selectedName: "disk-a",
			wantErrSubstrs: []string{
				"malformed",
				snapshotapi.AnnotationImportSourceRef,
				"invalid API group",
			},
		},
		{
			name: "child ref rejects malformed API version before child get",
			objects: func(_ *testing.T) []runtime.Object {
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef("invalid.example.io/v1/extra", "DemoVirtualDiskSnapshot", "must-not-get"),
				)

				return []runtime.Object{root}
			},
			selectedKind:   "DemoVirtualDiskSnapshot",
			selectedName:   "must-not-get",
			wantErrSubstrs: []string{"childrenSnapshotRefs", "invalid apiVersion", "element 0"},
		},
		{
			name: "duplicate original names across API versions are ambiguous",
			objects: func(t *testing.T) []runtime.Object {
				first := readyDomainDiskSnapshot("nss-child-first-import")
				setSnapshotMode(first, snapshotapi.SnapshotModeImport)
				setImportSourceRefAnnotation(
					t,
					first,
					"virtualization.deckhouse.io/v1alpha2",
					"VirtualDisk",
					"disk-shared",
				)

				second := readyDomainDiskSnapshot("nss-child-second-import")
				setSnapshotMode(second, snapshotapi.SnapshotModeImport)
				setImportSourceRefAnnotation(
					t,
					second,
					"virtualization.deckhouse.io/v1alpha3",
					"VirtualDisk",
					"disk-shared",
				)
				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(first.GetAPIVersion(), first.GetKind(), first.GetName()),
					snapshotChildRef(second.GetAPIVersion(), second.GetKind(), second.GetName()),
				)

				return []runtime.Object{root, first, second}
			},
			selectedKind: "VirtualDisk",
			selectedName: "disk-shared",
			wantErrSubstrs: []string{
				"ambiguous",
				"nss-child-first-import",
				"nss-child-second-import",
				"--node-api-version virtualization.deckhouse.io/v1alpha2",
				"--node-api-version virtualization.deckhouse.io/v1alpha3",
			},
			wantOrder: []string{"nss-child-first-import", "nss-child-second-import"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
			dyn := newFakeDynamic(tc.objects(t)...)
			cfg := Config{
				Namespace:        testNS,
				Snapshot:         testSnap,
				SelectedNodeKind: tc.selectedKind,
				SelectedNodeName: tc.selectedName,
				Source:           src,
				Dynamic:          dyn,
				Mapper:           testMapperWithDomain(),
				Log:              discardLogger(),
				PollInterval:     time.Millisecond,
			}

			err := Run(context.Background(), cfg)
			if err == nil {
				t.Fatal("expected selection error, got nil")
			}

			for _, substr := range tc.wantErrSubstrs {
				if !contains(err.Error(), substr) {
					t.Errorf("error %q does not contain %q", err.Error(), substr)
				}
			}

			if len(tc.wantOrder) == 2 &&
				strings.Index(err.Error(), tc.wantOrder[0]) >= strings.Index(err.Error(), tc.wantOrder[1]) {
				t.Errorf("error %q does not preserve candidate order %v", err.Error(), tc.wantOrder)
			}

			assertNoRestoreMutation(t, src, dyn)
		})
	}
}

func TestRun_SelectedNode_ClientFailureStopsTraversal(t *testing.T) {
	t.Parallel()

	errGetNode := errors.New("get selected node")
	tests := []struct {
		name        string
		fail        func(context.Context, context.CancelFunc) error
		wantErr     error
		wantActions []string
	}{
		{
			name: "fake client error",
			fail: func(_ context.Context, _ context.CancelFunc) error {
				return errGetNode
			},
			wantErr:     errGetNode,
			wantActions: []string{testSnap, "nss-child-failing"},
		},
		{
			name: "parent cancellation during child get",
			fail: func(ctx context.Context, cancel context.CancelFunc) error {
				cancel()
				<-ctx.Done()

				return ctx.Err()
			},
			wantErr:     context.Canceled,
			wantActions: []string{testSnap, "nss-child-failing"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			grandchild := readyDomainDiskSnapshot("nss-grandchild-must-not-get")
			child := snapshotWithChildren(
				readyDomainDiskSnapshot("nss-child-failing"),
				snapshotChildRef(grandchild.GetAPIVersion(), grandchild.GetKind(), grandchild.GetName()),
			)
			root := snapshotWithChildren(
				readySnapshot(),
				snapshotChildRef(child.GetAPIVersion(), child.GetKind(), child.GetName()),
			)
			dyn := newFakeDynamic(root, child, grandchild)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var (
				mu      sync.Mutex
				actions []string
			)

			wrapped := &interceptingDynamicClient{
				Interface: dyn,
				intercept: func(
					requestCtx context.Context,
					verb string,
					_ schema.GroupVersionResource,
					_ string,
					name string,
				) error {
					if verb != "get" {
						return nil
					}

					mu.Lock()
					actions = append(actions, name)
					mu.Unlock()

					if name == child.GetName() {
						return tc.fail(requestCtx, cancel)
					}

					return nil
				},
			}
			src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
			cfg := Config{
				Namespace:              testNS,
				Snapshot:               testSnap,
				SelectedNodeKind:       "DemoVirtualDiskSnapshot",
				SelectedNodeName:       grandchild.GetName(),
				SelectedNodeAPIVersion: domainDiskAPIVersion,
				Source:                 src,
				Dynamic:                wrapped,
				Mapper:                 testMapperWithDomain(),
				Log:                    discardLogger(),
				PollInterval:           time.Millisecond,
			}

			err := Run(ctx, cfg)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Run error = %v, want errors.Is(_, %v)", err, tc.wantErr)
			}

			mu.Lock()
			gotActions := append([]string(nil), actions...)
			mu.Unlock()

			if !slices.Equal(gotActions, tc.wantActions) {
				t.Errorf("Get actions = %v, want %v", gotActions, tc.wantActions)
			}

			assertNoRestoreMutation(t, src, dyn)
		})
	}
}

// TestRun_SelectedNode_RootNotReady_ChildReadyProceeds verifies that a Ready child
// subtree can be restored when the root Snapshot is Ready=False (ChildSnapshotDeleted)
// and a different referenced sibling CR has already been deleted.
func TestRun_SelectedNode_RootNotReady_ChildReadyProceeds(t *testing.T) {
	t.Parallel()

	const childName = "nss-child-ready"

	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	child := readyDomainDiskSnapshot(childName)
	root := snapshotWithChildren(
		notReadySnapshot(),
		snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", childName),
		snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", "nss-child-deleted"),
	)
	dyn := newFakeDynamic(root, child)

	cfg := Config{
		Namespace:        testNS,
		Snapshot:         testSnap,
		SelectedNodeKind: "DemoVirtualDiskSnapshot",
		SelectedNodeName: childName,
		Source:           src,
		Dynamic:          dyn,
		Mapper:           testMapperWithDomain(),
		Log:              discardLogger(),
		PollInterval:     time.Millisecond,
	}

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if src.calls != 1 {
		t.Fatalf("expected RestoreManifests call, got %d", src.calls)
	}
}

func TestRun_SelectedNode_MissingChildOutcomes(t *testing.T) {
	t.Parallel()

	const (
		liveName    = "nss-child-live"
		missingName = "nss-child-missing"
	)

	tests := []struct {
		name           string
		objects        func() []runtime.Object
		selectedKind   string
		selectedName   string
		wantSuccess    bool
		wantErrSubstrs []string
		rejectErr      string
	}{
		{
			name: "authoritative direct missing generated ref",
			objects: func() []runtime.Object {
				root := snapshotWithChildren(
					notReadySnapshot(),
					snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", missingName),
				)

				return []runtime.Object{root}
			},
			selectedKind: "DemoVirtualDiskSnapshot",
			selectedName: missingName,
			wantErrSubstrs: []string{
				"belongs to Snapshot",
				"generated child ref",
				domainDiskAPIVersion,
				"deleted",
			},
			rejectErr: "does not belong",
		},
		{
			name: "authoritative nested missing generated ref",
			objects: func() []runtime.Object {
				parent := snapshotWithChildren(
					readyDomainDiskSnapshot("nss-parent"),
					snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", missingName),
				)
				root := snapshotWithChildren(
					notReadySnapshot(),
					snapshotChildRef(parent.GetAPIVersion(), parent.GetKind(), parent.GetName()),
				)

				return []runtime.Object{root, parent}
			},
			selectedKind: "DemoVirtualDiskSnapshot",
			selectedName: missingName,
			wantErrSubstrs: []string{
				"belongs to Snapshot",
				"generated child ref",
				missingName,
				"deleted",
			},
			rejectErr: "does not belong",
		},
		{
			name: "live generated target tolerates unrelated missing sibling",
			objects: func() []runtime.Object {
				live := readyDomainDiskSnapshot(liveName)
				root := snapshotWithChildren(
					notReadySnapshot(),
					snapshotChildRef(live.GetAPIVersion(), live.GetKind(), live.GetName()),
					snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", missingName),
				)

				return []runtime.Object{root, live}
			},
			selectedKind: "DemoVirtualDiskSnapshot",
			selectedName: liveName,
			wantSuccess:  true,
		},
		{
			name: "original alias cannot prove uniqueness in incomplete graph",
			objects: func() []runtime.Object {
				live := readyDomainDiskSnapshot(liveName)
				setSnapshotSourceRef(live, "virtualization.deckhouse.io/v1alpha2", "VirtualDisk", "disk-a")
				root := snapshotWithChildren(
					notReadySnapshot(),
					snapshotChildRef(live.GetAPIVersion(), live.GetKind(), live.GetName()),
					snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", missingName),
				)

				return []runtime.Object{root, live}
			},
			selectedKind: "VirtualDisk",
			selectedName: "disk-a",
			wantErrSubstrs: []string{
				"cannot prove",
				"original-source selector",
				"hierarchy is incomplete",
				"--node DemoVirtualDiskSnapshot/" + liveName,
				"--node-api-version " + domainDiskAPIVersion,
			},
			rejectErr: "does not belong",
		},
		{
			name: "no match in incomplete graph is not non-membership",
			objects: func() []runtime.Object {
				root := snapshotWithChildren(
					notReadySnapshot(),
					snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", missingName),
				)

				return []runtime.Object{root}
			},
			selectedKind: "VirtualDisk",
			selectedName: "unknown-original-alias",
			wantErrSubstrs: []string{
				"cannot prove whether",
				"hierarchy is incomplete",
				missingName,
			},
			rejectErr: "does not belong",
		},
		{
			name: "no match in complete graph proves non-membership",
			objects: func() []runtime.Object {
				return []runtime.Object{readySnapshot()}
			},
			selectedKind:   "VirtualDisk",
			selectedName:   "unknown-original-alias",
			wantErrSubstrs: []string{"does not belong"},
		},
		{
			name: "legacy parent without resource version cannot prove absence",
			objects: func() []runtime.Object {
				root := snapshotWithChildren(
					notReadySnapshot(),
					snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", missingName),
				)
				root.SetResourceVersion("")

				return []runtime.Object{root}
			},
			selectedKind: "DemoVirtualDiskSnapshot",
			selectedName: missingName,
			wantErrSubstrs: []string{
				"cannot prove absence",
				"parent",
				"metadata.resourceVersion",
			},
			rejectErr: "deleted",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
			dyn := newFakeDynamic(tc.objects()...)
			cfg := Config{
				Namespace:        testNS,
				Snapshot:         testSnap,
				SelectedNodeKind: tc.selectedKind,
				SelectedNodeName: tc.selectedName,
				Source:           src,
				Dynamic:          dyn,
				Mapper:           testMapperWithDomain(),
				Log:              discardLogger(),
			}

			err := Run(context.Background(), cfg)
			if tc.wantSuccess {
				if err != nil {
					t.Fatalf("Run: %v", err)
				}

				if src.calls != 1 {
					t.Fatalf("RestoreManifestsScoped calls = %d, want 1", src.calls)
				}

				return
			}

			if err == nil {
				t.Fatal("Run unexpectedly succeeded")
			}

			for _, substr := range tc.wantErrSubstrs {
				if !strings.Contains(err.Error(), substr) {
					t.Errorf("Run error %q does not contain %q", err, substr)
				}
			}

			if tc.rejectErr != "" && strings.Contains(err.Error(), tc.rejectErr) {
				t.Errorf("Run error %q unexpectedly contains %q", err, tc.rejectErr)
			}

			assertNoRestoreMutation(t, src, dyn)
		})
	}
}

func TestRun_SelectedNode_MissingChildProofAPIFailures(t *testing.T) {
	t.Parallel()

	const missingName = "nss-child-missing"

	errTransient := errors.New("transient list failure")
	errGetForbidden := kubeerrors.NewForbidden(
		schema.GroupResource{
			Group:    domainDiskGVR.Group,
			Resource: domainDiskGVR.Resource,
		},
		missingName,
		errors.New("get denied"),
	)
	errListForbidden := kubeerrors.NewForbidden(
		schema.GroupResource{
			Group:    domainDiskGVR.Group,
			Resource: domainDiskGVR.Resource,
		},
		"",
		errors.New("list denied"),
	)

	tests := []struct {
		name           string
		ctx            func() (context.Context, context.CancelFunc)
		intercept      dynamicRequestInterceptor
		interceptList  dynamicListInterceptor
		wantIs         error
		wantForbidden  bool
		wantErrSubstrs []string
	}{
		{
			name: "forbidden child get remains fatal",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			intercept: func(
				_ context.Context,
				verb string,
				gvr schema.GroupVersionResource,
				_ string,
				name string,
			) error {
				if verb == "get" && gvr == domainDiskGVR && name == missingName {
					return errGetForbidden
				}

				return nil
			},
			wantIs:         errGetForbidden,
			wantForbidden:  true,
			wantErrSubstrs: []string{"get snapshot child", "get denied"},
		},
		{
			name: "transient list error is not absence",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			interceptList: func(
				_ context.Context,
				gvr schema.GroupVersionResource,
				_ string,
				_ metav1.ListOptions,
			) (*unstructured.UnstructuredList, bool, error) {
				if gvr != domainDiskGVR {
					return nil, false, nil
				}

				return nil, true, errTransient
			},
			wantIs:         errTransient,
			wantErrSubstrs: []string{"prove absence", "transient list failure"},
		},
		{
			name: "forbidden list is not absence",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			interceptList: func(
				_ context.Context,
				gvr schema.GroupVersionResource,
				_ string,
				_ metav1.ListOptions,
			) (*unstructured.UnstructuredList, bool, error) {
				if gvr != domainDiskGVR {
					return nil, false, nil
				}

				return nil, true, errListForbidden
			},
			wantIs:         errListForbidden,
			wantForbidden:  true,
			wantErrSubstrs: []string{"prove absence", "list denied"},
		},
		{
			name: "cancellation during list remains causal",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			interceptList: func(
				ctx context.Context,
				gvr schema.GroupVersionResource,
				_ string,
				_ metav1.ListOptions,
			) (*unstructured.UnstructuredList, bool, error) {
				if gvr != domainDiskGVR {
					return nil, false, nil
				}

				return nil, true, context.Canceled
			},
			wantIs:         context.Canceled,
			wantErrSubstrs: []string{"prove absence", "context canceled"},
		},
		{
			name: "deadline during list remains causal",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			interceptList: func(
				_ context.Context,
				gvr schema.GroupVersionResource,
				_ string,
				_ metav1.ListOptions,
			) (*unstructured.UnstructuredList, bool, error) {
				if gvr != domainDiskGVR {
					return nil, false, nil
				}

				return nil, true, context.DeadlineExceeded
			},
			wantIs:         context.DeadlineExceeded,
			wantErrSubstrs: []string{"prove absence", "context deadline exceeded"},
		},
		{
			name: "incomplete final page is not absence",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			interceptList: func(
				_ context.Context,
				gvr schema.GroupVersionResource,
				_ string,
				_ metav1.ListOptions,
			) (*unstructured.UnstructuredList, bool, error) {
				if gvr != domainDiskGVR {
					return nil, false, nil
				}

				remaining := int64(1)
				page := snapshotListPage("")
				page.SetRemainingItemCount(&remaining)

				return page, true, nil
			},
			wantErrSubstrs: []string{"incomplete final page", "remainingItemCount=1"},
		},
		{
			name: "repeated continue token is not absence",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			interceptList: func(
				_ context.Context,
				gvr schema.GroupVersionResource,
				_ string,
				_ metav1.ListOptions,
			) (*unstructured.UnstructuredList, bool, error) {
				if gvr != domainDiskGVR {
					return nil, false, nil
				}

				return snapshotListPage("same-token"), true, nil
			},
			wantErrSubstrs: []string{"repeated continue token", "same-token"},
		},
		{
			name: "child appearing in list exposes deletion race",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			interceptList: func(
				_ context.Context,
				gvr schema.GroupVersionResource,
				_ string,
				_ metav1.ListOptions,
			) (*unstructured.UnstructuredList, bool, error) {
				if gvr != domainDiskGVR {
					return nil, false, nil
				}

				return snapshotListPage("", readyDomainDiskSnapshot(missingName)), true, nil
			},
			wantErrSubstrs: []string{"appeared in the collection", "hierarchy changed", "retry"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := snapshotWithChildren(
				notReadySnapshot(),
				snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", missingName),
			)
			src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
			baseDynamic := newFakeDynamic(root)
			dyn := &interceptingDynamicClient{
				Interface:     baseDynamic,
				intercept:     tc.intercept,
				interceptList: tc.interceptList,
			}
			cfg := Config{
				Namespace:              testNS,
				Snapshot:               testSnap,
				SelectedNodeKind:       "DemoVirtualDiskSnapshot",
				SelectedNodeName:       missingName,
				SelectedNodeAPIVersion: domainDiskAPIVersion,
				Source:                 src,
				Dynamic:                dyn,
				Mapper:                 testMapperWithDomain(),
				Log:                    discardLogger(),
			}
			ctx, cancel := tc.ctx()
			defer cancel()

			err := Run(ctx, cfg)
			if err == nil {
				t.Fatal("Run unexpectedly succeeded")
			}

			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Errorf("Run error = %v, want errors.Is(_, %v)", err, tc.wantIs)
			}

			if tc.wantForbidden && !kubeerrors.IsForbidden(err) {
				t.Errorf("Run error = %v, want forbidden classification", err)
			}

			for _, substr := range tc.wantErrSubstrs {
				if !strings.Contains(err.Error(), substr) {
					t.Errorf("Run error %q does not contain %q", err, substr)
				}
			}

			assertNoRestoreMutation(t, src, baseDynamic)

			for _, action := range baseDynamic.Actions() {
				if action.GetVerb() == "watch" {
					t.Errorf("absence proof unexpectedly used a watch: %v", action)
				}
			}
		})
	}
}

func TestRun_SelectedNode_MissingChildProofPagination(t *testing.T) {
	t.Parallel()

	const (
		liveName    = "nss-child-live"
		missingName = "nss-child-missing"
	)

	live := readyDomainDiskSnapshot(liveName)
	root := snapshotWithChildren(
		notReadySnapshot(),
		snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", missingName),
		snapshotChildRef(live.GetAPIVersion(), live.GetKind(), live.GetName()),
	)
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	baseDynamic := newFakeDynamic(root, live)

	var listOptions []metav1.ListOptions
	dyn := &interceptingDynamicClient{
		Interface: baseDynamic,
		interceptList: func(
			_ context.Context,
			gvr schema.GroupVersionResource,
			namespace string,
			opts metav1.ListOptions,
		) (*unstructured.UnstructuredList, bool, error) {
			if gvr != domainDiskGVR {
				return nil, false, nil
			}

			if namespace != testNS {
				return nil, true, fmt.Errorf("list namespace = %q, want %q", namespace, testNS)
			}

			listOptions = append(listOptions, opts)
			if len(listOptions) == 1 {
				return snapshotListPage("next-page"), true, nil
			}

			return snapshotListPage(""), true, nil
		},
	}
	cfg := Config{
		Namespace:              testNS,
		Snapshot:               testSnap,
		SelectedNodeKind:       live.GetKind(),
		SelectedNodeName:       live.GetName(),
		SelectedNodeAPIVersion: live.GetAPIVersion(),
		Source:                 src,
		Dynamic:                dyn,
		Mapper:                 testMapperWithDomain(),
		Log:                    discardLogger(),
	}

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if src.calls != 1 {
		t.Fatalf("RestoreManifestsScoped calls = %d, want 1", src.calls)
	}

	if len(listOptions) != 2 {
		t.Fatalf("List calls = %d, want 2", len(listOptions))
	}

	first := listOptions[0]
	if first.ResourceVersion != root.GetResourceVersion() ||
		first.ResourceVersionMatch != metav1.ResourceVersionMatchNotOlderThan {
		t.Errorf(
			"first List resource-version options = (%q, %q), want (%q, %q)",
			first.ResourceVersion,
			first.ResourceVersionMatch,
			root.GetResourceVersion(),
			metav1.ResourceVersionMatchNotOlderThan,
		)
	}

	if first.Continue != "" || first.Limit != missingChildProofPageLimit {
		t.Errorf("first List pagination = continue %q limit %d", first.Continue, first.Limit)
	}

	second := listOptions[1]
	if second.Continue != "next-page" || second.Limit != missingChildProofPageLimit {
		t.Errorf("second List pagination = continue %q limit %d", second.Continue, second.Limit)
	}

	if second.ResourceVersion != "" || second.ResourceVersionMatch != "" {
		t.Errorf(
			"continued List unexpectedly reset snapshot options: resourceVersion=%q match=%q",
			second.ResourceVersion,
			second.ResourceVersionMatch,
		)
	}

	wantSelector := "metadata.name=" + missingName
	if first.FieldSelector != wantSelector || second.FieldSelector != wantSelector {
		t.Errorf(
			"List field selectors = %q, %q, want %q",
			first.FieldSelector,
			second.FieldSelector,
			wantSelector,
		)
	}
}

func TestRun_SelectedNode_LiveAndMissingGeneratedCollisionIsAmbiguous(t *testing.T) {
	t.Parallel()

	const (
		kind           = "SharedSnapshot"
		name           = "shared"
		liveVersion    = "shared.example.io/v1alpha1"
		missingVersion = "shared.example.io/v1beta1"
	)

	live := readyGeneratedSnapshot(liveVersion, kind, name)
	root := snapshotWithChildren(
		notReadySnapshot(),
		snapshotChildRef(liveVersion, kind, name),
		snapshotChildRef(missingVersion, kind, name),
	)
	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	baseDynamic := newFakeDynamic(root, live)
	dyn := &interceptingDynamicClient{
		Interface: baseDynamic,
		interceptList: func(
			_ context.Context,
			gvr schema.GroupVersionResource,
			_ string,
			_ metav1.ListOptions,
		) (*unstructured.UnstructuredList, bool, error) {
			if gvr.Group != "shared.example.io" || gvr.Version != "v1beta1" {
				return nil, false, nil
			}

			return snapshotListPage(""), true, nil
		},
	}
	cfg := Config{
		Namespace:        testNS,
		Snapshot:         testSnap,
		SelectedNodeKind: kind,
		SelectedNodeName: name,
		Source:           src,
		Dynamic:          dyn,
		Mapper: testMapperWithGenerated(
			schema.FromAPIVersionAndKind(liveVersion, kind),
			schema.FromAPIVersionAndKind(missingVersion, kind),
		),
		Log: discardLogger(),
	}

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("Run unexpectedly succeeded")
	}

	for _, text := range []string{
		"ambiguous within incomplete Snapshot",
		"live " + liveVersion,
		"deleted " + missingVersion,
		"--node-api-version " + liveVersion,
		"--node-api-version " + missingVersion,
	} {
		if !strings.Contains(err.Error(), text) {
			t.Errorf("Run error %q does not contain %q", err, text)
		}
	}

	assertNoRestoreMutation(t, src, baseDynamic)
}

func TestRun_SelectedNode_MissingChildMappingFailures(t *testing.T) {
	t.Parallel()

	const missingName = "nss-child-missing"

	tests := []struct {
		name         string
		apiVersion   string
		mapper       func(string) meta.RESTMapper
		wantErrParts []string
	}{
		{
			name:       "cluster scope cannot prove namespace-local absence",
			apiVersion: domainDiskAPIVersion,
			mapper: func(apiVersion string) meta.RESTMapper {
				gv := schema.FromAPIVersionAndKind(apiVersion, "DemoVirtualDiskSnapshot")
				mapper := testMapperWithGenerated(gv)
				mapper.(*meta.DefaultRESTMapper).Add(gv, meta.RESTScopeRoot)

				return mapper
			},
			wantErrParts: []string{"namespace-local contract", "cluster-scoped"},
		},
		{
			name:       "unserved child API version remains mapper error",
			apiVersion: "missing.example.io/v1alpha1",
			mapper: func(_ string) meta.RESTMapper {
				mapper := testMapperWithGenerated(
					schema.GroupVersionKind{
						Group:   "missing.example.io",
						Version: "v1beta1",
						Kind:    "DemoVirtualDiskSnapshot",
					},
				)

				return mapper
			},
			wantErrParts: []string{"resolve resource", "no matches for kind", "v1alpha1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := snapshotWithChildren(
				notReadySnapshot(),
				snapshotChildRef(tc.apiVersion, "DemoVirtualDiskSnapshot", missingName),
			)
			src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
			dyn := newFakeDynamic(root)
			cfg := Config{
				Namespace:              testNS,
				Snapshot:               testSnap,
				SelectedNodeKind:       "DemoVirtualDiskSnapshot",
				SelectedNodeName:       missingName,
				SelectedNodeAPIVersion: tc.apiVersion,
				Source:                 src,
				Dynamic:                dyn,
				Mapper:                 tc.mapper(tc.apiVersion),
				Log:                    discardLogger(),
			}

			err := Run(context.Background(), cfg)
			if err == nil {
				t.Fatal("Run unexpectedly succeeded")
			}

			for _, part := range tc.wantErrParts {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("Run error %q does not contain %q", err, part)
				}
			}

			assertNoRestoreMutation(t, src, dyn)
		})
	}
}

func TestRun_SelectedNode_IncompleteAliasOutcomeIsDeterministic(t *testing.T) {
	t.Parallel()

	const (
		liveName    = "nss-child-live"
		missingName = "nss-child-missing"
	)

	orders := [][]map[string]interface{}{
		{
			snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", liveName),
			snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", missingName),
		},
		{
			snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", missingName),
			snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", liveName),
		},
	}

	var wantError string
	for i, refs := range orders {
		live := readyDomainDiskSnapshot(liveName)
		setSnapshotSourceRef(live, "virtualization.deckhouse.io/v1alpha2", "VirtualDisk", "disk-a")
		root := snapshotWithChildren(notReadySnapshot(), refs...)
		src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
		dyn := newFakeDynamic(root, live)
		cfg := Config{
			Namespace:        testNS,
			Snapshot:         testSnap,
			SelectedNodeKind: "VirtualDisk",
			SelectedNodeName: "disk-a",
			Source:           src,
			Dynamic:          dyn,
			Mapper:           testMapperWithDomain(),
			Log:              discardLogger(),
		}

		err := Run(context.Background(), cfg)
		if err == nil {
			t.Fatalf("order %d: Run unexpectedly succeeded", i)
		}

		if i == 0 {
			wantError = err.Error()
		} else if err.Error() != wantError {
			t.Errorf("order %d error = %q, want deterministic %q", i, err, wantError)
		}

		assertNoRestoreMutation(t, src, dyn)
	}
}

// TestRun_SelectedNode_ChildNotReadyAborts verifies selected-node preflight fails when
// the selected child is not Ready, even if the root Snapshot is Ready.
func TestRun_SelectedNode_ChildNotReadyAborts(t *testing.T) {
	t.Parallel()

	const childName = "nss-child-not-ready"

	child := readyDomainDiskSnapshot(childName)
	_ = unstructured.SetNestedSlice(child.Object, []interface{}{
		map[string]interface{}{"type": "Ready", "status": "False", "reason": "DataCapturePending"},
	}, "status", "conditions")

	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	root := snapshotWithChildren(
		readySnapshot(),
		snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", childName),
	)
	dyn := newFakeDynamic(root, child)

	cfg := Config{
		Namespace:        testNS,
		Snapshot:         testSnap,
		SelectedNodeKind: "DemoVirtualDiskSnapshot",
		SelectedNodeName: childName,
		Source:           src,
		Dynamic:          dyn,
		Mapper:           testMapperWithDomain(),
		Log:              discardLogger(),
		PollInterval:     time.Millisecond,
	}

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected preflight error, got nil")
	}

	if src.calls != 0 {
		t.Errorf("Source must not be called when selected-node preflight fails, got %d calls", src.calls)
	}
}

// TestRun_SelectedNode_RejectedBeforeMutation verifies root membership, root
// existence, and selector uniqueness are established before manifest compilation or SSA.
func TestRun_SelectedNode_RejectedBeforeMutation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		objects        func() []runtime.Object
		selectedKind   string
		selectedName   string
		wantErrSubstrs []string
	}{
		{
			name: "error: node from another root is rejected",
			objects: func() []runtime.Object {
				return []runtime.Object{
					readySnapshot(),
					readyDomainDiskSnapshot("nss-unrelated"),
				}
			},
			selectedKind:   "DemoVirtualDiskSnapshot",
			selectedName:   "nss-unrelated",
			wantErrSubstrs: []string{"does not belong", testSnap},
		},
		{
			name: "error: missing positional root cannot be bypassed",
			objects: func() []runtime.Object {
				return []runtime.Object{
					readyDomainDiskSnapshot("nss-existing"),
				}
			},
			selectedKind:   "DemoVirtualDiskSnapshot",
			selectedName:   "nss-existing",
			wantErrSubstrs: []string{"get root Snapshot", testSnap, "not found"},
		},
		{
			name: "error: duplicate original source identity is ambiguous",
			objects: func() []runtime.Object {
				first := readyDomainDiskSnapshot("nss-child-first")
				setSnapshotSourceRef(first, "virtualization.deckhouse.io/v1alpha2", "DemoVirtualDisk", "bk-shared")

				second := readyDomainDiskSnapshot("nss-child-second")
				setSnapshotSourceRef(second, "virtualization.deckhouse.io/v1alpha2", "DemoVirtualDisk", "bk-shared")

				root := snapshotWithChildren(
					readySnapshot(),
					snapshotChildRef(first.GetAPIVersion(), first.GetKind(), first.GetName()),
					snapshotChildRef(second.GetAPIVersion(), second.GetKind(), second.GetName()),
				)

				return []runtime.Object{root, first, second}
			},
			selectedKind: "DemoVirtualDisk",
			selectedName: "bk-shared",
			wantErrSubstrs: []string{
				"ambiguous",
				"nss-child-first",
				"nss-child-second",
				"--node DemoVirtualDiskSnapshot/nss-child-first",
				"--node-api-version " + domainDiskAPIVersion,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
			dyn := newFakeDynamic(tc.objects()...)

			cfg := Config{
				Namespace:        testNS,
				Snapshot:         testSnap,
				SelectedNodeKind: tc.selectedKind,
				SelectedNodeName: tc.selectedName,
				Source:           src,
				Dynamic:          dyn,
				Mapper:           testMapperWithDomain(),
				Log:              discardLogger(),
				PollInterval:     time.Millisecond,
			}

			err := Run(context.Background(), cfg)
			if err == nil {
				t.Fatal("expected selection error, got nil")
			}

			for _, substr := range tc.wantErrSubstrs {
				if !contains(err.Error(), substr) {
					t.Errorf("error %q does not contain %q", err.Error(), substr)
				}
			}

			assertNoRestoreMutation(t, src, dyn)
		})
	}
}

// TestRun_ScopeOptions_ForwardedToSource verifies Run passes cfg.Scope/FilterKind/FilterName
// through to Source.RestoreManifestsScoped unchanged, for the default (zero-value), scope=node,
// and scope=node+object-filter combinations the cmd/restore flags can produce.
func TestRun_ScopeOptions_ForwardedToSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		scope    aggapi.RestoreScope
		wantOpts aggapi.RestoreScopeOptions
	}{
		{
			name:     "default subtree is fetched in bounded node units",
			scope:    "",
			wantOpts: aggapi.RestoreScopeOptions{Scope: aggapi.RestoreScopeNode},
		},
		{
			name:     "scope=node, no object filter",
			scope:    aggapi.RestoreScopeNode,
			wantOpts: aggapi.RestoreScopeOptions{Scope: aggapi.RestoreScopeNode},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
			dyn := newFakeDynamic(readySnapshot())

			cfg := baseConfig(src, dyn)
			cfg.Scope = tc.scope

			if err := Run(context.Background(), cfg); err != nil {
				t.Fatalf("Run: %v", err)
			}

			if src.gotOpts != tc.wantOpts {
				t.Errorf("RestoreScopeOptions: got %+v, want %+v", src.gotOpts, tc.wantOpts)
			}
		})
	}
}

// TestRun_ScopeNodeWithObjectFilter_ForwardedToSource verifies that FilterKind/FilterName set
// alongside Scope==RestoreScopeNode on a SelectedNode restore reach Source unchanged, addressed
// at the selected node's NodeRef (not the root).
func TestRun_ScopeNodeWithObjectFilter_ForwardedToSource(t *testing.T) {
	t.Parallel()

	const childName = "nss-child-abc123"

	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	child := readyDomainDiskSnapshot(childName)
	root := snapshotWithChildren(
		readySnapshot(),
		snapshotChildRef(domainDiskAPIVersion, "DemoVirtualDiskSnapshot", childName),
	)
	dyn := newFakeDynamic(root, child)

	cfg := Config{
		Namespace:        testNS,
		Snapshot:         testSnap,
		SelectedNodeKind: "DemoVirtualDiskSnapshot",
		SelectedNodeName: childName,
		Scope:            aggapi.RestoreScopeNode,
		FilterKind:       "DemoVirtualDisk",
		FilterName:       "bk-disk-a",
		Source:           src,
		Dynamic:          dyn,
		Mapper:           testMapperWithDomain(),
		Log:              discardLogger(),
		PollInterval:     time.Millisecond,
	}

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	wantOpts := aggapi.RestoreScopeOptions{
		Scope:      aggapi.RestoreScopeNode,
		FilterKind: "DemoVirtualDisk",
		FilterName: "bk-disk-a",
	}

	if src.gotOpts != wantOpts {
		t.Errorf("RestoreScopeOptions: got %+v, want %+v", src.gotOpts, wantOpts)
	}

	wantRef := aggapi.NodeRef{
		APIVersion: "sds-unified-snapshots-poc.deckhouse.io/v1alpha1",
		Kind:       "DemoVirtualDiskSnapshot",
		Name:       childName,
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

// TestRun_SelectedNode_UnknownSelectorErrors verifies a selector absent from the
// positional root hierarchy returns an error before calling RestoreManifests.
func TestRun_SelectedNode_UnknownSelectorErrors(t *testing.T) {
	t.Parallel()

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

	if !contains(err.Error(), "NoSuchKind") {
		t.Errorf("error should mention unknown kind, got: %v", err)
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
