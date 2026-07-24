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
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
)

const (
	snapshotKind = "Snapshot"
	pvcKind      = "PersistentVolumeClaim"
	pvcResource  = "persistentvolumeclaims"

	// fieldManager is the SSA field manager name used for all restore applies.
	fieldManager = "d8-snapshot-restore"

	readyConditionType = "Ready"
	conditionFalse     = "False"
	pvcPhaseBound      = "Bound"
	pvcPhaseLost       = "Lost"
	pvcPhasePending    = "Pending"

	// volumeSnapshotGroup is the CSI VolumeSnapshot API group. Readiness is
	// determined by status.readyToUse (bool), not by conditions.
	volumeSnapshotGroup = "snapshot.storage.k8s.io"

	// storageClassGroup and storageClassResource identify the cluster-scoped
	// StorageClass API used to resolve a PVC's effective volumeBindingMode.
	storageClassGroup    = "storage.k8s.io"
	storageClassResource = "storageclasses"

	// volumeBindingModeWFC marks a StorageClass whose PVCs are, by design, left
	// Pending until a Pod schedules against them: provisioning does not even
	// start before that. Waiting for such a PVC to become Bound would block
	// --wait forever on a standalone PVC that has no consumer yet.
	volumeBindingModeWFC = "WaitForFirstConsumer"
	// volumeBindingModeImmediate is Kubernetes' own default when a StorageClass
	// omits volumeBindingMode.
	volumeBindingModeImmediate = "Immediate"

	// defaultStorageClassAnnotation marks the cluster's default StorageClass,
	// used to resolve a PVC whose spec.storageClassName is empty.
	defaultStorageClassAnnotation = "storageclass.kubernetes.io/is-default-class"

	defaultTimeout      = 10 * time.Minute
	defaultPollInterval = 2 * time.Second
)

// leafRef identifies a volume-snapshot leaf referenced by a PVC's spec.dataSourceRef
// or spec.dataSource (apiGroup + kind + name). Used as a dedup key.
type leafRef struct {
	group string
	kind  string
	name  string
}

// Source reads the apply-ready manifest array for a snapshot subtree (or, with
// aggapi.RestoreScopeNode and an object filter, a single captured object) from the
// state-snapshotter aggregated API. It is satisfied by *aggapi.Client and stubbed in tests.
type Source interface {
	RestoreManifestsScoped(ctx context.Context, ref aggapi.NodeRef, targetNamespace string, opts aggapi.RestoreScopeOptions) ([]byte, error)
}

// Config holds all parameters for one in-namespace restore run.
type Config struct {
	// Namespace is both the source Snapshot namespace and the restore target namespace.
	Namespace string
	// Snapshot is the name of the root Snapshot to restore.
	Snapshot string

	// SelectedNodeKind restricts the restore to a single node subtree when non-empty.
	// The selector is resolved within Snapshot's status.childrenSnapshotRefs hierarchy
	// by generated snapshot-CR identity, captured status.sourceRef identity, or the
	// original archive identity preserved on an import-mode marker.
	// RestoreManifestsScoped is called with the matched node's real snapshot-CR NodeRef.
	// Preflight checks the selected node's Ready (or readyToUse for VolumeSnapshot), not
	// the root — so a Ready child can be restored even when the root is
	// Ready=False/ChildSnapshotDeleted.
	SelectedNodeKind string
	// SelectedNodeName is the name of the selected node. Required when SelectedNodeKind is set.
	SelectedNodeName string
	// SelectedNodeAPIVersion optionally restricts generated and original identities to
	// one exact Kubernetes apiVersion. Core resources use "v1"; named groups use
	// "<group>/<version>".
	SelectedNodeAPIVersion string

	// Scope narrows the server-side manifest compilation: aggapi.RestoreScopeSubtree (the
	// zero value behaves identically) compiles the addressed node and its whole subtree;
	// aggapi.RestoreScopeNode compiles only the addressed node, with no descendants.
	Scope aggapi.RestoreScope
	// FilterKind and FilterName, when both set, restrict the restore to a single captured
	// object within the addressed node. The server accepts this only together with
	// Scope == aggapi.RestoreScopeNode (see validate in cmd/restore) and 400s otherwise.
	FilterKind string
	FilterName string
	// FilterAPIVersion further disambiguates FilterKind/FilterName when the node captures
	// more than one object of the same kind+name under different API versions. Not yet
	// exposed as a CLI flag (kind+name is unambiguous within a node); forwarded as-is.
	FilterAPIVersion string

	// Edit, when true, opens the resolved manifests in the user's preferred editor
	// (kubectl-style: $KUBE_EDITOR, $EDITOR, vi) before the preflight and apply
	// passes. A non-zero editor exit, unchanged content, or empty content aborts
	// the restore without applying anything.
	Edit bool

	// DryRun, when true, passes DryRunAll to every SSA apply so the API server
	// validates and admits objects without persisting them. The --wait loop is
	// skipped entirely in dry-run mode because nothing was created.
	DryRun bool
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

	// silenceApplyLog suppresses the per-object "would apply"/"applied" log line in
	// applyObject. Run sets it true on the implicit dry-run preflight config so operators
	// only see those messages on an explicit --dry-run request; it is never a CLI flag.
	silenceApplyLog bool
}

// pvcRef identifies a restored PVC to wait on.
type pvcRef struct {
	namespace string
	name      string
	// storageClassName is the PVC's spec.storageClassName as applied (may be
	// empty; an empty value resolves to the cluster's default StorageClass,
	// not to Immediate binding, when the effective volumeBindingMode is
	// resolved in waitPVCsBound).
	storageClassName string
}

// Run executes an in-namespace restore: anchor selection to the positional Snapshot,
// preflight the addressed node, fetch apply-ready manifests for the target namespace,
// apply every object as-is, and optionally wait for restored PVCs to bind.
func Run(ctx context.Context, cfg Config) error {
	cfg = applyDefaults(cfg)

	if err := validate(cfg); err != nil {
		return err
	}

	rootRef := aggapi.NodeRef{
		APIVersion: snapshotapi.StorageGroup + "/" + snapshotapi.Version,
		Kind:       snapshotKind,
		Name:       cfg.Snapshot,
		Namespace:  cfg.Namespace,
	}

	targetRef := rootRef

	if cfg.SelectedNodeKind == "" {
		if err := preflightRootSnapshot(ctx, cfg); err != nil {
			return fmt.Errorf("preflight %s/%s: %w", cfg.Namespace, cfg.Snapshot, err)
		}
	} else {
		ref, obj, err := cfg.resolveNodeRef(ctx)
		if err != nil {
			return fmt.Errorf(
				"resolve selected node %s within Snapshot %s/%s: %w",
				cfg.selectedNodeDescription(),
				cfg.Namespace,
				cfg.Snapshot,
				err,
			)
		}

		if err := preflightSelectedNode(ref, obj); err != nil {
			return fmt.Errorf("preflight %s/%s: %w", cfg.SelectedNodeKind, cfg.SelectedNodeName, err)
		}

		targetRef = ref
	}

	raw, err := cfg.Source.RestoreManifestsScoped(ctx, targetRef, cfg.Namespace, aggapi.RestoreScopeOptions{
		Scope:            cfg.Scope,
		FilterKind:       cfg.FilterKind,
		FilterName:       cfg.FilterName,
		FilterAPIVersion: cfg.FilterAPIVersion,
	})
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

	if cfg.Edit {
		objs, err = editManifests(objs)
		if err != nil {
			return fmt.Errorf("restore edit: %w", err)
		}
	}

	// Preflight: verify every PVC data-source leaf exists and is ready before
	// applying anything. The API server does not validate cross-object existence
	// of spec.dataSourceRef at admission, so an absent leaf causes a PVC to stay
	// Pending forever without this check.
	if err := preflightLeaves(ctx, cfg, objs); err != nil {
		return err
	}

	cfg.Log.Info("applying restore manifests",
		slog.String("namespace", cfg.Namespace),
		slog.String("snapshot", cfg.Snapshot),
		slog.Int("objects", len(objs)))

	// Implicit dry-run preflight: validate every object without mutating the cluster.
	// Any admission failure here aborts before any real apply.
	dryRunCfg := cfg
	dryRunCfg.DryRun = true
	// Silence per-object log for the implicit pass; keep it when the user requested
	// --dry-run because this is then the only apply pass the user sees output from.
	dryRunCfg.silenceApplyLog = !cfg.DryRun

	if _, err := applyAll(ctx, dryRunCfg, objs); err != nil {
		return fmt.Errorf("dry-run preflight: %w", err)
	}

	cfg.Log.Info("validated restore manifests (dry-run)",
		slog.String("namespace", cfg.Namespace),
		slog.String("snapshot", cfg.Snapshot),
		slog.Int("objects", len(objs)))

	// With --dry-run, only the validation pass runs; nothing has been mutated.
	if cfg.DryRun {
		return nil
	}

	// Real apply pass: every object passed the dry-run, so we apply without DryRun.
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
	case (cfg.SelectedNodeKind == "") != (cfg.SelectedNodeName == ""):
		return fmt.Errorf("restore: SelectedNodeKind and SelectedNodeName must be set together")
	case cfg.SelectedNodeAPIVersion != "" && cfg.SelectedNodeKind == "":
		return fmt.Errorf("restore: SelectedNodeAPIVersion requires a selected node")
	case cfg.Source == nil:
		return fmt.Errorf("restore: Source must be set")
	case cfg.Dynamic == nil:
		return fmt.Errorf("restore: Dynamic client must be set")
	case cfg.Mapper == nil:
		return fmt.Errorf("restore: Mapper must be set")
	default:
		return ValidateNodeAPIVersion(cfg.SelectedNodeAPIVersion)
	}
}

// ValidateNodeAPIVersion validates the canonical apiVersion syntax accepted by
// --node-api-version and persisted Kubernetes object identities.
func ValidateNodeAPIVersion(apiVersion string) error {
	if apiVersion == "" {
		return nil
	}

	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return fmt.Errorf("parse Kubernetes apiVersion: %w", err)
	}

	if gv.Version == "" || gv.String() != apiVersion {
		return fmt.Errorf("must be 'v1' for the core group or '<group>/<version>' for a named group")
	}

	if versionErrors := validation.IsDNS1035Label(gv.Version); len(versionErrors) != 0 {
		return fmt.Errorf("invalid version %q: %s", gv.Version, strings.Join(versionErrors, "; "))
	}

	if gv.Group == "" {
		return nil
	}

	if groupErrors := validation.IsDNS1123Subdomain(gv.Group); len(groupErrors) != 0 {
		return fmt.Errorf("invalid API group %q: %s", gv.Group, strings.Join(groupErrors, "; "))
	}

	return nil
}

// preflightRootSnapshot verifies the source Snapshot is Ready and has a bound SnapshotContent.
func preflightRootSnapshot(ctx context.Context, cfg Config) error {
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

	if !isConditionTrue(snap) {
		status, reason, message := readyConditionDetail(snap, readyConditionType)
		if status == conditionFalse && source.IsDegradedReason(reason) {
			return fmt.Errorf("snapshot is DEGRADED (reason=%s: %s): a namespaced child was deleted but "+
				"its data is intact in the content-layer trash; a full-subtree restore of the root is "+
				"blocked, but --node <ready child> can restore an intact subtree", reason, message)
		}

		return fmt.Errorf("snapshot is not Ready=True (cannot restore an incomplete snapshot)")
	}

	bound, _, _ := unstructured.NestedString(snap.Object, "status", "boundSnapshotContentName")
	if bound == "" {
		return fmt.Errorf("snapshot has no status.boundSnapshotContentName (not yet bound)")
	}

	return nil
}

// preflightSelectedNode verifies the selected subtree root is ready to restore:
// VolumeSnapshot → status.readyToUse=true; other snapshot CRs → Ready=True + bound content.
func preflightSelectedNode(ref aggapi.NodeRef, obj *unstructured.Unstructured) error {
	if ref.IsVolumeSnapshotLeaf() {
		ready, found, _ := unstructured.NestedBool(obj.Object, "status", "readyToUse")
		if !found || !ready {
			return fmt.Errorf("VolumeSnapshot %s/%s is not readyToUse=true", ref.Namespace, ref.Name)
		}

		return nil
	}

	if !isConditionTrue(obj) {
		return fmt.Errorf("%s %s/%s is not Ready=True (cannot restore an incomplete subtree)", ref.Kind, ref.Namespace, ref.Name)
	}

	bound, _, _ := unstructured.NestedString(obj.Object, "status", "boundSnapshotContentName")
	if bound == "" {
		return fmt.Errorf("%s %s/%s has no status.boundSnapshotContentName (not yet bound)", ref.Kind, ref.Namespace, ref.Name)
	}

	return nil
}

// preflightLeaves verifies that every volume-snapshot leaf referenced by a PVC
// spec.dataSourceRef or spec.dataSource exists and is ready in cfg.Namespace.
// All failures are aggregated into a single actionable error so the user can fix
// them in one pass. This check is always active (read-only; also strengthens dry-run).
func preflightLeaves(ctx context.Context, cfg Config, objs []unstructured.Unstructured) error {
	refs := collectLeafRefs(objs)
	if len(refs) == 0 {
		return nil
	}

	var errs []string

	for _, ref := range refs {
		gvr, namespaced, err := cfg.resourceForGroupKind(ref.group, ref.kind)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s/%s: cannot resolve resource: %v", ref.kind, ref.name, err))

			continue
		}

		var ri dynamic.ResourceInterface
		if namespaced {
			ri = cfg.Dynamic.Resource(gvr).Namespace(cfg.Namespace)
		} else {
			ri = cfg.Dynamic.Resource(gvr)
		}

		obj, getErr := ri.Get(ctx, ref.name, metav1.GetOptions{})
		if kubeerrors.IsNotFound(getErr) {
			errs = append(errs, fmt.Sprintf("%s/%s: missing", ref.kind, ref.name))

			continue
		}

		if getErr != nil {
			errs = append(errs, fmt.Sprintf("%s/%s: get error: %v", ref.kind, ref.name, getErr))

			continue
		}

		if !isLeafReady(obj, ref) {
			errs = append(errs, fmt.Sprintf("%s/%s: not ready", ref.kind, ref.name))
		}
	}

	if len(errs) == 0 {
		return nil
	}

	return fmt.Errorf("volume-snapshot leaves not ready: %s", strings.Join(errs, "; "))
}

// collectLeafRefs scans decoded objects for PVCs and returns the distinct volume-snapshot
// leaves referenced by spec.dataSourceRef and spec.dataSource. Both fields carry
// {apiGroup, kind, name}; duplicates are deduplicated by {group, kind, name}.
func collectLeafRefs(objs []unstructured.Unstructured) []leafRef {
	seen := make(map[leafRef]struct{})
	refs := make([]leafRef, 0)

	for i := range objs {
		obj := &objs[i]
		if obj.GetKind() != pvcKind {
			continue
		}

		for _, fieldPath := range [][]string{
			{"spec", "dataSourceRef"},
			{"spec", "dataSource"},
		} {
			m, found, _ := unstructured.NestedMap(obj.Object, fieldPath...)
			if !found || len(m) == 0 {
				continue
			}

			group, _, _ := unstructured.NestedString(m, "apiGroup")
			kind, _, _ := unstructured.NestedString(m, "kind")
			name, _, _ := unstructured.NestedString(m, "name")

			if kind == "" || name == "" {
				continue
			}

			ref := leafRef{group: group, kind: kind, name: name}
			if _, ok := seen[ref]; !ok {
				seen[ref] = struct{}{}
				refs = append(refs, ref)
			}
		}
	}

	return refs
}

// isLeafReady reports whether a volume-snapshot leaf object is ready to serve as a
// PVC data source. For CSI VolumeSnapshots (snapshot.storage.k8s.io), readiness is
// status.readyToUse==true. For domain kinds (VirtualDiskSnapshot, etc.), readiness
// is either status.phase=="Ready" or a Ready=True condition.
func isLeafReady(obj *unstructured.Unstructured, ref leafRef) bool {
	if ref.group == volumeSnapshotGroup {
		ready, found, _ := unstructured.NestedBool(obj.Object, "status", "readyToUse")

		return found && ready
	}

	phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	if phase == "Ready" {
		return true
	}

	return isConditionTrue(obj)
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
			scName, _, _ := unstructured.NestedString(obj.Object, "spec", "storageClassName")
			pvcs = append(pvcs, pvcRef{namespace: ns, name: obj.GetName(), storageClassName: scName})
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

	patchOpts := metav1.PatchOptions{
		FieldManager: fieldManager,
		Force:        &force,
	}

	if cfg.DryRun {
		patchOpts.DryRun = []string{metav1.DryRunAll}
	}

	if _, patchErr := ri.Patch(ctx, obj.GetName(), types.ApplyPatchType, jsonBytes, patchOpts); patchErr != nil {
		// Immutable fields (e.g. a PVC's spec.dataSourceRef) cause an Invalid error:
		// surface an actionable error instead of a raw API rejection.
		if kubeerrors.IsInvalid(patchErr) {
			return "", fmt.Errorf("already exists with immutable fields differing from the snapshot; "+
				"delete it and re-run restore: %w", patchErr)
		}

		return "", fmt.Errorf("apply: %w", patchErr)
	}

	if !cfg.silenceApplyLog {
		if cfg.DryRun {
			cfg.Log.Info("would apply",
				slog.String("kind", obj.GetKind()),
				slog.String("name", obj.GetName()),
				slog.String("namespace", ns))
		} else {
			cfg.Log.Info("applied",
				slog.String("kind", obj.GetKind()),
				slog.String("name", obj.GetName()),
				slog.String("namespace", ns))
		}
	}

	return ns, nil
}

// waitPVCsBound blocks until every restored PVC reports status.phase == Bound or the
// configured timeout elapses.
//
// Scope: only PVCs that appear in the applied manifest set are awaited. Disk-backed PVCs
// for domain objects are recreated asynchronously by the domain controller (they are not
// part of manifests-with-data-restoration output), so they are intentionally not tracked
// here; awaiting them would require knowledge of the domain controller's naming/labeling.
//
// A PVC whose effective StorageClass volumeBindingMode is WaitForFirstConsumer is checked
// once (never polled): provisioning does not even start until a Pod schedules against it,
// so a Pending WFFC PVC with no consumer is a normal, non-blocking state, not a failure to
// wait out. Polling it against the shared deadline would let one such PVC starve the
// remaining Immediate PVCs of their fair share of cfg.Timeout, so it is skipped up front,
// before the deadline is ever consulted for it.
func waitPVCsBound(ctx context.Context, cfg Config, pvcs []pvcRef) error {
	if len(pvcs) == 0 {
		return nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	gvr := schema.GroupVersionResource{Version: "v1", Resource: pvcResource}
	scGVR := schema.GroupVersionResource{Group: storageClassGroup, Version: "v1", Resource: storageClassResource}

	cfg.Log.Info("waiting for restored PVCs to bind", slog.Int("count", len(pvcs)))

	bindingModes := make(map[string]string)

	var (
		boundCount   int
		skippedCount int
	)

	for _, ref := range pvcs {
		mode, err := resolveVolumeBindingMode(waitCtx, cfg, scGVR, ref.storageClassName, bindingModes)
		if err != nil {
			return fmt.Errorf("resolve volume binding mode for PVC %s/%s: %w", ref.namespace, ref.name, err)
		}

		if mode == volumeBindingModeWFC {
			if err := checkWFFCPVCOnce(waitCtx, cfg, gvr, ref); err != nil {
				return err
			}

			skippedCount++

			continue
		}

		if err := waitOnePVCBound(waitCtx, cfg, gvr, ref); err != nil {
			return err
		}

		boundCount++
	}

	cfg.Log.Info("finished waiting for restored PVCs",
		slog.Int("bound", boundCount),
		slog.Int("skipped_wait_for_first_consumer", skippedCount))

	return nil
}

// resolveVolumeBindingMode returns the effective volumeBindingMode for a PVC's
// StorageClass, resolving the cluster's default StorageClass when className is empty
// (spec.storageClassName can be legitimately unset). Results are cached per StorageClass
// name so a restore with many PVCs on the same class issues one API call per class, not
// one per PVC; the empty-name case is cached under a distinct key since it requires a
// List rather than a Get.
func resolveVolumeBindingMode(ctx context.Context, cfg Config, scGVR schema.GroupVersionResource, className string, cache map[string]string) (string, error) {
	cacheKey := className
	if cacheKey == "" {
		cacheKey = "\x00default"
	}

	if mode, ok := cache[cacheKey]; ok {
		return mode, nil
	}

	var (
		sc  *unstructured.Unstructured
		err error
	)

	if className != "" {
		sc, err = cfg.Dynamic.Resource(scGVR).Get(ctx, className, metav1.GetOptions{})
		if err != nil {
			if ctxErr := waitContextError(ctx, fmt.Sprintf("getting StorageClass %q", className)); ctxErr != nil {
				return "", ctxErr
			}

			return "", fmt.Errorf("get StorageClass %q: %w", className, err)
		}
	} else {
		sc, err = findDefaultStorageClass(ctx, cfg, scGVR)
		if err != nil {
			return "", err
		}

		if sc == nil {
			cfg.Log.Info("no default StorageClass is annotated; assuming Immediate binding for PVCs with an empty storageClassName")

			cache[cacheKey] = volumeBindingModeImmediate

			return volumeBindingModeImmediate, nil
		}
	}

	mode, _, _ := unstructured.NestedString(sc.Object, "volumeBindingMode")
	if mode == "" {
		mode = volumeBindingModeImmediate
	}

	cache[cacheKey] = mode

	return mode, nil
}

// findDefaultStorageClass returns the cluster's default StorageClass (annotated
// storageclass.kubernetes.io/is-default-class: "true"), or nil if none carries the
// annotation.
func findDefaultStorageClass(ctx context.Context, cfg Config, scGVR schema.GroupVersionResource) (*unstructured.Unstructured, error) {
	list, err := cfg.Dynamic.Resource(scGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		if ctxErr := waitContextError(ctx, "listing StorageClasses to resolve the default"); ctxErr != nil {
			return nil, ctxErr
		}

		return nil, fmt.Errorf("list StorageClasses: %w", err)
	}

	for i := range list.Items {
		sc := &list.Items[i]

		if sc.GetAnnotations()[defaultStorageClassAnnotation] == "true" {
			return sc, nil
		}
	}

	return nil, nil
}

// checkWFFCPVCOnce checks a WaitForFirstConsumer PVC's state exactly once, never
// polling it: a non-terminating Pending PVC with no consumer yet is expected under this
// binding mode and must not consume the shared --wait deadline. An already-Bound PVC
// (e.g. a consumer already existed) is still reported as bound.
func checkWFFCPVCOnce(ctx context.Context, cfg Config, gvr schema.GroupVersionResource, ref pvcRef) error {
	phase, err := getPVCWaitPhase(ctx, cfg, gvr, ref)
	if err != nil {
		return err
	}

	if phase == pvcPhaseBound {
		cfg.Log.Info("PVC bound",
			slog.String("namespace", ref.namespace),
			slog.String("name", ref.name),
			slog.String("phase", phase))

		return nil
	}

	cfg.Log.Info("PVC is WaitForFirstConsumer and Pending with no consumer yet; not waiting for Bound",
		slog.String("namespace", ref.namespace),
		slog.String("name", ref.name),
		slog.String("phase", phase))

	return nil
}

// waitOnePVCBound polls a single non-terminating Pending PVC until it is Bound or the
// shared wait context expires. Every other observed state is terminal for restore waiting.
func waitOnePVCBound(ctx context.Context, cfg Config, gvr schema.GroupVersionResource, ref pvcRef) error {
	for {
		phase, err := getPVCWaitPhase(ctx, cfg, gvr, ref)
		if err != nil {
			return err
		}

		if phase == pvcPhaseBound {
			cfg.Log.Info("PVC bound",
				slog.String("namespace", ref.namespace),
				slog.String("name", ref.name),
				slog.String("phase", phase))

			return nil
		}

		if !sleepCtx(ctx, cfg.PollInterval) {
			return waitContextError(
				ctx,
				fmt.Sprintf("waiting for PVC %s/%s to become Bound; observed phase %q", ref.namespace, ref.name, phase),
			)
		}
	}
}

func getPVCWaitPhase(ctx context.Context, cfg Config, gvr schema.GroupVersionResource, ref pvcRef) (string, error) {
	pvc, err := cfg.Dynamic.Resource(gvr).Namespace(ref.namespace).Get(ctx, ref.name, metav1.GetOptions{})
	if kubeerrors.IsNotFound(err) {
		return "", fmt.Errorf("restored PVC %s/%s was not found after apply: %w", ref.namespace, ref.name, err)
	}

	if err != nil {
		if ctxErr := waitContextError(ctx, fmt.Sprintf("getting restored PVC %s/%s", ref.namespace, ref.name)); ctxErr != nil {
			return "", ctxErr
		}

		return "", fmt.Errorf("get restored PVC %s/%s: %w", ref.namespace, ref.name, err)
	}

	phase, found, err := unstructured.NestedString(pvc.Object, "status", "phase")
	if err != nil {
		return "", fmt.Errorf("read status.phase of restored PVC %s/%s: %w", ref.namespace, ref.name, err)
	}

	observedPhase := phase
	if !found || phase == "" {
		observedPhase = "<missing>"
	}

	if deletionTimestamp := pvc.GetDeletionTimestamp(); deletionTimestamp != nil {
		return "", fmt.Errorf(
			"restored PVC %s/%s is terminating with deletionTimestamp %s; observed phase %q",
			ref.namespace,
			ref.name,
			deletionTimestamp.UTC().Format(time.RFC3339),
			observedPhase,
		)
	}

	switch phase {
	case pvcPhaseBound, pvcPhasePending:
		return phase, nil
	case pvcPhaseLost:
		return "", fmt.Errorf("restored PVC %s/%s is in terminal phase %q", ref.namespace, ref.name, phase)
	case "":
		return "", fmt.Errorf("restored PVC %s/%s has missing status.phase", ref.namespace, ref.name)
	default:
		return "", fmt.Errorf("restored PVC %s/%s has unrecognized phase %q", ref.namespace, ref.name, phase)
	}
}

func waitContextError(ctx context.Context, phase string) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("restore wait timeout while %s: %w", phase, err)
	}

	cause := context.Cause(ctx)
	if cause != nil && !errors.Is(cause, err) {
		err = errors.Join(err, cause)
	}

	return fmt.Errorf("restore wait canceled while %s: %w", phase, err)
}

// resourceFor resolves a GVK to its resource and whether it is namespaced.
func (cfg Config) resourceFor(gvk schema.GroupVersionKind) (schema.GroupVersionResource, bool, error) {
	mapping, err := cfg.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return schema.GroupVersionResource{}, false, fmt.Errorf("resolve resource for %s: %w", gvk.String(), err)
	}

	return mapping.Resource, mapping.Scope.Name() == meta.RESTScopeNameNamespace, nil
}

// resourceForGroupKind resolves a GroupKind to its preferred-version resource without
// requiring a known version. Used by preflightLeaves where only apiGroup+kind are known
// (spec.dataSourceRef / spec.dataSource do not carry the API version).
func (cfg Config) resourceForGroupKind(group, kind string) (schema.GroupVersionResource, bool, error) {
	mapping, err := cfg.Mapper.RESTMapping(schema.GroupKind{Group: group, Kind: kind})
	if err != nil {
		return schema.GroupVersionResource{}, false, fmt.Errorf("resolve resource for %s/%s: %w", group, kind, err)
	}

	return mapping.Resource, mapping.Scope.Name() == meta.RESTScopeNameNamespace, nil
}

type nodeMatch struct {
	ref         aggapi.NodeRef
	obj         *unstructured.Unstructured
	apiVersions []string
}

type nodeIdentity struct {
	apiVersion string
	kind       string
	name       string
}

type nodeResolver struct {
	cfg     Config
	seen    map[string]struct{}
	matches []nodeMatch
}

// resolveNodeRef resolves the selector only within the hierarchy rooted at the
// positional Snapshot. Child refs carry the real snapshot GVK, so no cross-group
// kind guess is needed.
func (cfg Config) resolveNodeRef(ctx context.Context) (aggapi.NodeRef, *unstructured.Unstructured, error) {
	rootRef := aggapi.NodeRef{
		APIVersion: snapshotapi.StorageGroup + "/" + snapshotapi.Version,
		Kind:       snapshotKind,
		Name:       cfg.Snapshot,
		Namespace:  cfg.Namespace,
	}

	root, err := cfg.getSnapshotNode(ctx, rootRef)
	if err != nil {
		return aggapi.NodeRef{}, nil, fmt.Errorf("get root Snapshot %s/%s: %w", cfg.Namespace, cfg.Snapshot, err)
	}

	resolver := nodeResolver{
		cfg:  cfg,
		seen: make(map[string]struct{}),
	}

	if err := resolver.visit(ctx, rootRef, root); err != nil {
		return aggapi.NodeRef{}, nil, err
	}

	switch len(resolver.matches) {
	case 0:
		return aggapi.NodeRef{}, nil, fmt.Errorf(
			"%s does not belong to Snapshot %s/%s",
			cfg.selectedNodeDescription(),
			cfg.Namespace,
			cfg.Snapshot,
		)
	case 1:
		return resolver.matches[0].ref, resolver.matches[0].obj, nil
	default:
		candidates := make([]string, 0, len(resolver.matches))
		reruns := make([]string, 0, len(resolver.matches))
		apiVersionCounts := make(map[string]int)

		for _, match := range resolver.matches {
			for _, apiVersion := range match.apiVersions {
				apiVersionCounts[apiVersion]++
			}
		}

		for _, match := range resolver.matches {
			candidates = append(candidates, fmt.Sprintf(
				"%s %s/%s (matching apiVersions: %s)",
				match.ref.APIVersion,
				match.ref.Kind,
				match.ref.Name,
				strings.Join(match.apiVersions, ", "),
			))

			for _, apiVersion := range match.apiVersions {
				kind := cfg.SelectedNodeKind
				name := cfg.SelectedNodeName

				if apiVersionCounts[apiVersion] > 1 {
					apiVersion = match.ref.APIVersion
					kind = match.ref.Kind
					name = match.ref.Name
				}

				reruns = append(reruns, fmt.Sprintf(
					"d8 snapshot restore %s -n %s --node %s/%s --node-api-version %s",
					cfg.Snapshot,
					cfg.Namespace,
					kind,
					name,
					apiVersion,
				))
			}
		}

		return aggapi.NodeRef{}, nil, fmt.Errorf(
			"%s is ambiguous within Snapshot %s/%s; matching snapshot nodes: %s; rerun with an exact apiVersion: %s",
			cfg.selectedNodeDescription(),
			cfg.Namespace,
			cfg.Snapshot,
			strings.Join(candidates, ", "),
			strings.Join(reruns, " or "),
		)
	}
}

func (cfg Config) selectedNodeDescription() string {
	identity := cfg.SelectedNodeKind + "/" + cfg.SelectedNodeName
	if cfg.SelectedNodeAPIVersion == "" {
		return identity
	}

	return cfg.SelectedNodeAPIVersion + " " + identity
}

func (r *nodeResolver) visit(ctx context.Context, ref aggapi.NodeRef, obj *unstructured.Unstructured) error {
	key := ref.APIVersion + "/" + ref.Kind + "/" + ref.Name
	if _, ok := r.seen[key]; ok {
		return fmt.Errorf("snapshot hierarchy contains duplicate or cyclic ref %s %s/%s", ref.APIVersion, ref.Kind, ref.Name)
	}

	r.seen[key] = struct{}{}

	apiVersions, err := r.matchingAPIVersions(obj, ref)
	if err != nil {
		return err
	}

	if len(apiVersions) != 0 {
		r.matches = append(r.matches, nodeMatch{ref: ref, obj: obj, apiVersions: apiVersions})
	}

	childRefs, err := snapshotChildRefs(obj)
	if err != nil {
		return fmt.Errorf("%s %s/%s: status.childrenSnapshotRefs: %w", ref.APIVersion, ref.Kind, ref.Name, err)
	}

	for _, childRef := range childRefs {
		child := aggapi.NodeRef{
			APIVersion: childRef.APIVersion,
			Kind:       childRef.Kind,
			Name:       childRef.Name,
			Namespace:  r.cfg.Namespace,
		}

		childObj, getErr := r.cfg.getSnapshotNode(ctx, child)
		if kubeerrors.IsNotFound(getErr) {
			continue
		}

		if getErr != nil {
			return fmt.Errorf("get snapshot child %s %s/%s: %w", child.APIVersion, child.Kind, child.Name, getErr)
		}

		if err := r.visit(ctx, child, childObj); err != nil {
			return err
		}
	}

	return nil
}

func (r *nodeResolver) matchingAPIVersions(obj *unstructured.Unstructured, ref aggapi.NodeRef) ([]string, error) {
	sourceRef, hasSourceRef, err := snapshotSourceIdentity(obj, ref)
	if err != nil {
		return nil, err
	}

	importSourceRef, hasImportSourceRef, err := importSourceIdentity(obj, ref)
	if err != nil {
		return nil, err
	}

	identities := make([]nodeIdentity, 0, 3)
	identities = append(identities, nodeIdentity{
		apiVersion: ref.APIVersion,
		kind:       ref.Kind,
		name:       ref.Name,
	})

	if hasSourceRef {
		identities = append(identities, sourceRef)
	}

	if hasImportSourceRef {
		identities = append(identities, importSourceRef)
	}

	apiVersions := make([]string, 0, len(identities))
	seenAPIVersions := make(map[string]struct{}, len(identities))

	for _, identity := range identities {
		if !identity.matches(
			r.cfg.SelectedNodeKind,
			r.cfg.SelectedNodeName,
			r.cfg.SelectedNodeAPIVersion,
		) {
			continue
		}

		if _, seen := seenAPIVersions[identity.apiVersion]; seen {
			continue
		}

		seenAPIVersions[identity.apiVersion] = struct{}{}
		apiVersions = append(apiVersions, identity.apiVersion)
	}

	return apiVersions, nil
}

func snapshotSourceIdentity(
	obj *unstructured.Unstructured,
	ref aggapi.NodeRef,
) (nodeIdentity, bool, error) {
	sourceRef, found, err := unstructured.NestedMap(obj.Object, "status", "sourceRef")
	if err != nil {
		return nodeIdentity{}, false, fmt.Errorf(
			"%s %s/%s: status.sourceRef is not an object: %w",
			ref.APIVersion,
			ref.Kind,
			ref.Name,
			err,
		)
	}

	if !found {
		return nodeIdentity{}, false, nil
	}

	sourceAPIVersion, _ := sourceRef["apiVersion"].(string)
	sourceKind, _ := sourceRef["kind"].(string)
	sourceName, _ := sourceRef["name"].(string)

	if sourceAPIVersion == "" || sourceKind == "" || sourceName == "" {
		return nodeIdentity{}, false, fmt.Errorf(
			"%s %s/%s: status.sourceRef is incomplete (apiVersion/kind/name required)",
			ref.APIVersion,
			ref.Kind,
			ref.Name,
		)
	}

	if err := ValidateNodeAPIVersion(sourceAPIVersion); err != nil {
		return nodeIdentity{}, false, fmt.Errorf(
			"%s %s/%s: status.sourceRef.apiVersion %q is invalid: %w",
			ref.APIVersion,
			ref.Kind,
			ref.Name,
			sourceAPIVersion,
			err,
		)
	}

	for _, optionalField := range []string{"namespace", "uid"} {
		if value, exists := sourceRef[optionalField]; exists {
			if _, ok := value.(string); !ok {
				return nodeIdentity{}, false, fmt.Errorf(
					"%s %s/%s: status.sourceRef.%s has unexpected type %T",
					ref.APIVersion,
					ref.Kind,
					ref.Name,
					optionalField,
					value,
				)
			}
		}
	}

	return nodeIdentity{
		apiVersion: sourceAPIVersion,
		kind:       sourceKind,
		name:       sourceName,
	}, true, nil
}

func importSourceIdentity(
	obj *unstructured.Unstructured,
	ref aggapi.NodeRef,
) (nodeIdentity, bool, error) {
	mode, _, _ := unstructured.NestedString(obj.Object, "spec", "mode")
	if mode != string(snapshotapi.SnapshotModeImport) {
		return nodeIdentity{}, false, nil
	}

	value, found := obj.GetAnnotations()[snapshotapi.AnnotationImportSourceRef]
	if !found {
		return nodeIdentity{}, false, nil
	}

	var sourceRef snapshotapi.ImportSourceRef
	if err := json.Unmarshal([]byte(value), &sourceRef); err != nil {
		return nodeIdentity{}, false, fmt.Errorf(
			"%s %s/%s: malformed %s annotation: %w",
			ref.APIVersion,
			ref.Kind,
			ref.Name,
			snapshotapi.AnnotationImportSourceRef,
			err,
		)
	}

	if sourceRef.APIVersion == "" || sourceRef.Kind == "" || sourceRef.Name == "" {
		return nodeIdentity{}, false, fmt.Errorf(
			"%s %s/%s: malformed %s annotation: apiVersion, kind, and name are required",
			ref.APIVersion,
			ref.Kind,
			ref.Name,
			snapshotapi.AnnotationImportSourceRef,
		)
	}

	if err := ValidateNodeAPIVersion(sourceRef.APIVersion); err != nil {
		return nodeIdentity{}, false, fmt.Errorf(
			"%s %s/%s: malformed %s annotation: apiVersion %q is invalid: %w",
			ref.APIVersion,
			ref.Kind,
			ref.Name,
			snapshotapi.AnnotationImportSourceRef,
			sourceRef.APIVersion,
			err,
		)
	}

	canonical, err := json.Marshal(sourceRef)
	if err != nil {
		return nodeIdentity{}, false, fmt.Errorf("marshal import source reference: %w", err)
	}

	if string(canonical) != value {
		return nodeIdentity{}, false, fmt.Errorf(
			"%s %s/%s: non-canonical %s annotation %q",
			ref.APIVersion,
			ref.Kind,
			ref.Name,
			snapshotapi.AnnotationImportSourceRef,
			value,
		)
	}

	return nodeIdentity{
		apiVersion: sourceRef.APIVersion,
		kind:       sourceRef.Kind,
		name:       sourceRef.Name,
	}, true, nil
}

func (i nodeIdentity) matches(kind, name, apiVersion string) bool {
	return i.kind == kind && i.name == name && (apiVersion == "" || i.apiVersion == apiVersion)
}

func (cfg Config) getSnapshotNode(ctx context.Context, ref aggapi.NodeRef) (*unstructured.Unstructured, error) {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("parse apiVersion %q: %w", ref.APIVersion, err)
	}

	gvr, namespaced, err := cfg.resourceFor(schema.GroupVersionKind{
		Group:   gv.Group,
		Version: gv.Version,
		Kind:    ref.Kind,
	})
	if err != nil {
		return nil, err
	}

	if namespaced {
		obj, err := cfg.Dynamic.Resource(gvr).Namespace(cfg.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		return obj, nil
	}

	obj, err := cfg.Dynamic.Resource(gvr).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func snapshotChildRefs(obj *unstructured.Unstructured) ([]snapshotapi.SnapshotChildRef, error) {
	rawRefs, found, err := unstructured.NestedSlice(obj.Object, "status", "childrenSnapshotRefs")
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, nil
	}

	refs := make([]snapshotapi.SnapshotChildRef, 0, len(rawRefs))
	for i, rawRef := range rawRefs {
		m, ok := rawRef.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("element %d has unexpected type %T", i, rawRef)
		}

		apiVersion, _ := m["apiVersion"].(string)
		kind, _ := m["kind"].(string)
		name, _ := m["name"].(string)

		if apiVersion == "" || kind == "" || name == "" {
			return nil, fmt.Errorf("element %d is incomplete (apiVersion/kind/name required)", i)
		}

		if err := ValidateNodeAPIVersion(apiVersion); err != nil {
			return nil, fmt.Errorf("element %d has invalid apiVersion %q: %w", i, apiVersion, err)
		}

		refs = append(refs, snapshotapi.SnapshotChildRef{
			APIVersion: apiVersion,
			Kind:       kind,
			Name:       name,
		})
	}

	return refs, nil
}

// isConditionTrue reports whether status.conditions[type==Ready].status == "True".
func isConditionTrue(obj *unstructured.Unstructured) bool {
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
		if t != readyConditionType {
			continue
		}

		status, _, _ := unstructured.NestedString(m, "status")

		return status == string(metav1.ConditionTrue)
	}

	return false
}

// readyConditionDetail returns the status, reason and message of obj's status.conditions
// entry whose type == condType. It returns empty strings when the condition is absent or
// malformed — callers must not treat that as an error, only as "no extra detail available".
func readyConditionDetail(obj *unstructured.Unstructured, condType string) (string, string, string) {
	conds, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return "", "", ""
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
		reason, _, _ := unstructured.NestedString(m, "reason")
		message, _, _ := unstructured.NestedString(m, "message")

		return status, reason, message
	}

	return "", "", ""
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
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
