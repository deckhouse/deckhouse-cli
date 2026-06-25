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

// Package restore implements in-namespace restore of a snapshot tree.
//
// Restore is a single GET against the root Snapshot's manifests-with-data-restoration
// aggregated subresource (the server compiles the whole subtree, delegating domain
// subtrees internally) followed by a Server-Side Apply of every returned object.
// The compiler already rewrites PVCs with spec.dataSourceRef -> VolumeSnapshot (and a
// domain controller sets the dataSource on VirtualDiskSnapshot for domain disks), so
// CSI provisions volume data from the snapshot that already exists in the target
// namespace. There is no VolumeRestoreRequest and no SnapshotContent BFS.
//
// Cross-namespace restore is intentionally out of scope: it is modelled as
// download (namespace A) -> import (namespace B) -> restore (in namespace B), which
// recreates the Snapshot and the VolumeSnapshot/VirtualDiskSnapshot leaves in B.
package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
)

const (
	snapshotKind = "Snapshot"
	pvcKind      = "PersistentVolumeClaim"

	// fieldManager is the SSA field manager name used for all restore applies.
	fieldManager = "d8-snapshot-restore"

	readyConditionType = "Ready"
	pvcPhaseBound      = "Bound"

	defaultTimeout      = 10 * time.Minute
	defaultPollInterval = 2 * time.Second
)

// Source reads the apply-ready manifest array for a snapshot subtree from the
// state-snapshotter aggregated API. It is satisfied by *aggapi.Client and stubbed in tests.
type Source interface {
	RestoreManifests(ctx context.Context, ref aggapi.NodeRef, targetNamespace string) ([]byte, error)
}

// Config holds all parameters for one in-namespace restore run.
type Config struct {
	// Namespace is both the source Snapshot namespace and the restore target namespace.
	Namespace string
	// Snapshot is the name of the root Snapshot to restore.
	Snapshot string

	// Wait, when true, blocks until all restored PersistentVolumeClaims reach Bound.
	Wait bool
	// Timeout bounds the Bound wait (only used when Wait is true).
	Timeout time.Duration
	// PollInterval is the Bound polling cadence (only used when Wait is true).
	PollInterval time.Duration

	// Source fetches the apply-ready manifests (manifests-with-data-restoration).
	Source Source
	// Dynamic applies the restored objects and reads PVC status during the wait.
	Dynamic dynamic.Interface
	// Mapper resolves object GVKs to resources and their namespacing scope.
	Mapper meta.RESTMapper
	// Log receives progress output.
	Log *slog.Logger
}

// pvcRef identifies a restored PVC to wait on.
type pvcRef struct {
	namespace string
	name      string
}

// Run executes an in-namespace restore: preflight the root Snapshot, fetch the
// apply-ready manifests for the target namespace, apply every object as-is, and
// optionally wait for restored PVCs to bind.
func Run(ctx context.Context, cfg Config) error {
	cfg = applyDefaults(cfg)

	if err := validate(cfg); err != nil {
		return err
	}

	if err := preflight(ctx, cfg); err != nil {
		return fmt.Errorf("preflight %s/%s: %w", cfg.Namespace, cfg.Snapshot, err)
	}

	rootRef := aggapi.NodeRef{
		APIVersion: snapshotapi.StorageGroup + "/" + snapshotapi.Version,
		Kind:       snapshotKind,
		Name:       cfg.Snapshot,
		Namespace:  cfg.Namespace,
	}

	raw, err := cfg.Source.RestoreManifests(ctx, rootRef, cfg.Namespace)
	if err != nil {
		return fmt.Errorf("fetch restore manifests for %s/%s: %w", cfg.Namespace, cfg.Snapshot, err)
	}

	objs, err := decodeManifestArray(raw)
	if err != nil {
		return fmt.Errorf("decode restore manifests for %s/%s: %w", cfg.Namespace, cfg.Snapshot, err)
	}

	if len(objs) == 0 {
		return fmt.Errorf("restore manifests for %s/%s are empty", cfg.Namespace, cfg.Snapshot)
	}

	cfg.Log.Info("applying restore manifests",
		slog.String("namespace", cfg.Namespace),
		slog.String("snapshot", cfg.Snapshot),
		slog.Int("objects", len(objs)))

	pvcs, err := applyAll(ctx, cfg, objs)
	if err != nil {
		return err
	}

	if !cfg.Wait {
		return nil
	}

	return waitPVCsBound(ctx, cfg, pvcs)
}

// applyDefaults fills zero-valued optional fields with their defaults.
func applyDefaults(cfg Config) Config {
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}

	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}

	return cfg
}

// validate checks that all required dependencies and identifiers are set.
func validate(cfg Config) error {
	switch {
	case cfg.Namespace == "":
		return fmt.Errorf("restore: Namespace must be set")
	case cfg.Snapshot == "":
		return fmt.Errorf("restore: Snapshot must be set")
	case cfg.Source == nil:
		return fmt.Errorf("restore: Source must be set")
	case cfg.Dynamic == nil:
		return fmt.Errorf("restore: Dynamic client must be set")
	case cfg.Mapper == nil:
		return fmt.Errorf("restore: Mapper must be set")
	default:
		return nil
	}
}

// preflight verifies the source Snapshot is Ready and has a bound SnapshotContent.
func preflight(ctx context.Context, cfg Config) error {
	gvr, _, err := cfg.resourceFor(schema.GroupVersionKind{
		Group:   snapshotapi.StorageGroup,
		Version: snapshotapi.Version,
		Kind:    snapshotKind,
	})
	if err != nil {
		return fmt.Errorf("resolve Snapshot resource: %w", err)
	}

	snap, err := cfg.Dynamic.Resource(gvr).Namespace(cfg.Namespace).Get(ctx, cfg.Snapshot, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get Snapshot: %w", err)
	}

	if !isConditionTrue(snap, readyConditionType) {
		return fmt.Errorf("snapshot is not Ready=True (cannot restore an incomplete snapshot)")
	}

	bound, _, _ := unstructured.NestedString(snap.Object, "status", "boundSnapshotContentName")
	if bound == "" {
		return fmt.Errorf("snapshot has no status.boundSnapshotContentName (not yet bound)")
	}

	return nil
}

// applyAll upserts every object in order and returns the refs of restored PVCs.
func applyAll(ctx context.Context, cfg Config, objs []unstructured.Unstructured) ([]pvcRef, error) {
	var pvcs []pvcRef

	for i := range objs {
		obj := &objs[i]

		ns, err := applyObject(ctx, cfg, obj)
		if err != nil {
			return nil, fmt.Errorf("apply %s/%s %q: %w", obj.GetAPIVersion(), obj.GetKind(), obj.GetName(), err)
		}

		if obj.GetKind() == pvcKind {
			pvcs = append(pvcs, pvcRef{namespace: ns, name: obj.GetName()})
		}
	}

	return pvcs, nil
}

// applyObject applies a single object to the cluster using Server-Side Apply (SSA).
// SSA merges only the fields d8 sets; fields owned by other managers (controllers,
// webhooks) are not touched. Namespaced objects without a namespace inherit the target
// namespace. It returns the effective namespace the object was applied into.
func applyObject(ctx context.Context, cfg Config, obj *unstructured.Unstructured) (string, error) {
	gvr, namespaced, err := cfg.resourceFor(obj.GroupVersionKind())
	if err != nil {
		return "", err
	}

	var (
		ri dynamic.ResourceInterface
		ns string
	)

	if namespaced {
		ns = obj.GetNamespace()
		if ns == "" {
			ns = cfg.Namespace
			obj.SetNamespace(ns)
		}

		ri = cfg.Dynamic.Resource(gvr).Namespace(ns)
	} else {
		ri = cfg.Dynamic.Resource(gvr)
	}

	// Strip server-managed fields that SSA rejects or manages independently.
	obj.SetResourceVersion("")
	obj.SetManagedFields(nil)
	delete(obj.Object, "status")

	jsonBytes, err := json.Marshal(obj.Object)
	if err != nil {
		return "", fmt.Errorf("marshal object for apply: %w", err)
	}

	force := true

	if _, patchErr := ri.Patch(ctx, obj.GetName(), types.ApplyPatchType, jsonBytes, metav1.PatchOptions{
		FieldManager: fieldManager,
		Force:        &force,
	}); patchErr != nil {
		// Immutable fields (e.g. a PVC's spec.dataSourceRef) cause an Invalid error:
		// surface an actionable error instead of a raw API rejection.
		if kubeerrors.IsInvalid(patchErr) {
			return "", fmt.Errorf("already exists with immutable fields differing from the snapshot; "+
				"delete it and re-run restore: %w", patchErr)
		}

		return "", fmt.Errorf("apply: %w", patchErr)
	}

	cfg.Log.Info("applied",
		slog.String("kind", obj.GetKind()),
		slog.String("name", obj.GetName()),
		slog.String("namespace", ns))

	return ns, nil
}

// waitPVCsBound blocks until every restored PVC reports status.phase == Bound or the
// configured timeout elapses.
//
// Scope: only PVCs that appear in the applied manifest set are awaited. Disk-backed PVCs
// for domain objects are recreated asynchronously by the domain controller (they are not
// part of manifests-with-data-restoration output), so they are intentionally not tracked
// here; awaiting them would require knowledge of the domain controller's naming/labeling.
func waitPVCsBound(ctx context.Context, cfg Config, pvcs []pvcRef) error {
	if len(pvcs) == 0 {
		return nil
	}

	gvr, _, err := cfg.resourceFor(schema.GroupVersionKind{Version: "v1", Kind: pvcKind})
	if err != nil {
		return fmt.Errorf("resolve PersistentVolumeClaim resource: %w", err)
	}

	cfg.Log.Info("waiting for restored PVCs to bind", slog.Int("count", len(pvcs)))

	deadline := time.Now().Add(cfg.Timeout)

	for _, ref := range pvcs {
		if err := waitOnePVCBound(ctx, cfg, gvr, ref, deadline); err != nil {
			return err
		}
	}

	cfg.Log.Info("all restored PVCs are Bound", slog.Int("count", len(pvcs)))

	return nil
}

// waitOnePVCBound polls a single PVC until it is Bound or the shared deadline passes.
func waitOnePVCBound(ctx context.Context, cfg Config, gvr schema.GroupVersionResource, ref pvcRef, deadline time.Time) error {
	for {
		pvc, err := cfg.Dynamic.Resource(gvr).Namespace(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
		if err == nil {
			phase, _, _ := unstructured.NestedString(pvc.Object, "status", "phase")
			if phase == pvcPhaseBound {
				cfg.Log.Info("PVC bound", slog.String("namespace", ref.namespace), slog.String("name", ref.name))

				return nil
			}
		} else if !kubeerrors.IsNotFound(err) {
			return fmt.Errorf("get PVC %s/%s: %w", ref.namespace, ref.name, err)
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for PVC %s/%s to become Bound", ref.namespace, ref.name)
		}

		if !sleepCtx(ctx, cfg.PollInterval) {
			return ctx.Err()
		}
	}
}

// resourceFor resolves a GVK to its resource and whether it is namespaced.
func (cfg Config) resourceFor(gvk schema.GroupVersionKind) (schema.GroupVersionResource, bool, error) {
	mapping, err := cfg.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return schema.GroupVersionResource{}, false, fmt.Errorf("resolve resource for %s: %w", gvk.String(), err)
	}

	return mapping.Resource, mapping.Scope.Name() == meta.RESTScopeNameNamespace, nil
}

// isConditionTrue reports whether status.conditions[type==condType].status == "True".
func isConditionTrue(obj *unstructured.Unstructured, condType string) bool {
	conds, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return false
	}

	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}

		t, _, _ := unstructured.NestedString(m, "type")
		if t != condType {
			continue
		}

		status, _, _ := unstructured.NestedString(m, "status")

		return status == string(metav1.ConditionTrue)
	}

	return false
}

// decodeManifestArray parses a JSON array of Kubernetes objects into unstructured values.
func decodeManifestArray(data []byte) ([]unstructured.Unstructured, error) {
	var rawItems []map[string]interface{}
	if err := json.Unmarshal(data, &rawItems); err != nil {
		return nil, fmt.Errorf("unmarshal object array: %w", err)
	}

	objs := make([]unstructured.Unstructured, 0, len(rawItems))
	for _, item := range rawItems {
		objs = append(objs, unstructured.Unstructured{Object: item})
	}

	return objs, nil
}

// sleepCtx sleeps for d or returns false if ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
