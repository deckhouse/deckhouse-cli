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
	"k8s.io/apimachinery/pkg/fields"
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

	missingChildProofPageLimit int64 = 100
	missingChildProofMaxPages  int   = 100

	// A successful full walk processes at most maxNodes-1 child edges. It is
	// sequential, keeps at most maxDepth+1 parent frames, and issues at most one
	// GET plus missingChildProofMaxPages LISTs per child.
	restoreHierarchyMaxDepth = 64
	restoreHierarchyMaxNodes = 10_000

	defaultTimeout      = 10 * time.Minute
	defaultPollInterval = 2 * time.Second

	// DefaultControlPlaneTimeout bounds one restore control-plane call.
	DefaultControlPlaneTimeout = 30 * time.Second
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
	// ControlPlaneTimeout bounds each Source, discovery, Get, List, and Patch call
	// independently. It is not a deadline for the complete restore.
	ControlPlaneTimeout time.Duration

	// Source fetches the apply-ready manifests (manifests-with-data-restoration).
	Source Source
	// Dynamic applies the restored objects and reads PVC status during the wait.
	Dynamic dynamic.Interface
	// Mapper resolves object GVKs to resources and their namespacing scope.
	Mapper meta.RESTMapper
	// Log receives progress output.
	Log *slog.Logger

	// newWaitContext is a test seam for controlling the shared wait boundary.
	newWaitContext func(context.Context, time.Duration) (context.Context, context.CancelFunc)
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

	raw, err := controlPlaneRequest(
		ctx,
		cfg.controlPlaneTimeout(),
		fmt.Sprintf("fetching restore manifests for %s %s/%s", targetRef.Kind, targetRef.Namespace, targetRef.Name),
		func(requestCtx context.Context) ([]byte, error) {
			return cfg.Source.RestoreManifestsScoped(requestCtx, targetRef, cfg.Namespace, aggapi.RestoreScopeOptions{
				Scope:            cfg.Scope,
				FilterKind:       cfg.FilterKind,
				FilterName:       cfg.FilterName,
				FilterAPIVersion: cfg.FilterAPIVersion,
			})
		},
	)
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

	if err := preflightManifestNamespaces(ctx, cfg, objs); err != nil {
		return err
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

	if _, _, err := applyAll(ctx, dryRunCfg, objs); err != nil {
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
	pvcs, applied, err := applyAll(ctx, cfg, objs)
	if err != nil {
		return fmt.Errorf(
			"restore apply stopped after %d of %d objects completed; the cluster may be partially applied and the active object's outcome is unknown: %w",
			applied,
			len(objs),
			err,
		)
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

	if cfg.ControlPlaneTimeout <= 0 {
		cfg.ControlPlaneTimeout = DefaultControlPlaneTimeout
	}

	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}

	return cfg
}

func (cfg Config) controlPlaneTimeout() time.Duration {
	if cfg.ControlPlaneTimeout <= 0 {
		return DefaultControlPlaneTimeout
	}

	return cfg.ControlPlaneTimeout
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

// preflightManifestNamespaces rejects namespaced objects that escape the restore
// target. Empty namespaces are normalized only after every object is validated.
func preflightManifestNamespaces(ctx context.Context, cfg Config, objs []unstructured.Unstructured) error {
	namespaced := make([]bool, len(objs))

	for i := range objs {
		if err := manifestNamespaceContextError(ctx); err != nil {
			return err
		}

		obj := &objs[i]

		_, isNamespaced, err := cfg.resourceFor(
			ctx,
			obj.GroupVersionKind(),
			fmt.Sprintf(
				"resolving namespace scope for restore manifest apiVersion=%q kind=%q name=%q",
				obj.GetAPIVersion(),
				obj.GetKind(),
				obj.GetName(),
			),
		)
		if err != nil {
			return fmt.Errorf(
				"resolve namespace scope for restore manifest apiVersion=%q kind=%q name=%q: %w",
				obj.GetAPIVersion(),
				obj.GetKind(),
				obj.GetName(),
				err,
			)
		}

		namespaced[i] = isNamespaced
		if !isNamespaced {
			continue
		}

		namespace := obj.GetNamespace()
		if namespace != "" && namespace != cfg.Namespace {
			return fmt.Errorf(
				"restore manifest apiVersion=%q kind=%q name=%q has namespace %q, but required namespace is %q",
				obj.GetAPIVersion(),
				obj.GetKind(),
				obj.GetName(),
				namespace,
				cfg.Namespace,
			)
		}
	}

	if err := manifestNamespaceContextError(ctx); err != nil {
		return err
	}

	for i := range objs {
		if namespaced[i] && objs[i].GetNamespace() == "" {
			objs[i].SetNamespace(cfg.Namespace)
		}
	}

	return nil
}

func manifestNamespaceContextError(ctx context.Context) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}

	cause := context.Cause(ctx)
	if cause != nil && !errors.Is(cause, err) {
		err = errors.Join(err, cause)
	}

	return fmt.Errorf("validate restore manifest namespaces: %w", err)
}

func controlPlaneRequest[T any](
	ctx context.Context,
	timeout time.Duration,
	phase string,
	call func(context.Context) (T, error),
) (T, error) {
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	value, err := call(requestCtx)
	if err != nil {
		var zero T

		return zero, controlPlaneRequestError(ctx, requestCtx, timeout, phase, err)
	}

	return value, nil
}

func controlPlaneRequestError(
	ctx context.Context,
	requestCtx context.Context,
	timeout time.Duration,
	phase string,
	requestErr error,
) error {
	if outerErr := contextCauseError(ctx); outerErr != nil {
		return fmt.Errorf("restore canceled while %s: %w", phase, errors.Join(requestErr, outerErr))
	}

	if errors.Is(requestCtx.Err(), context.DeadlineExceeded) ||
		errors.Is(requestErr, context.DeadlineExceeded) {
		if !errors.Is(requestErr, context.DeadlineExceeded) {
			requestErr = errors.Join(requestErr, context.DeadlineExceeded)
		}

		return fmt.Errorf(
			"restore control-plane request timed out after %s while %s: %w",
			timeout,
			phase,
			requestErr,
		)
	}

	return fmt.Errorf("restore control-plane request failed while %s: %w", phase, requestErr)
}

func contextCauseError(ctx context.Context) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}

	cause := context.Cause(ctx)
	if cause != nil && !errors.Is(cause, err) {
		return errors.Join(err, cause)
	}

	return err
}

type restMappingResult struct {
	mapping *meta.RESTMapping
	err     error
}

func (cfg Config) restMapping(
	ctx context.Context,
	phase string,
	groupKind schema.GroupKind,
	versions ...string,
) (*meta.RESTMapping, error) {
	timeout := cfg.controlPlaneTimeout()

	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resultCh := make(chan restMappingResult, 1)

	go func() {
		mapping, err := cfg.Mapper.RESTMapping(groupKind, versions...)
		resultCh <- restMappingResult{mapping: mapping, err: err}
	}()

	select {
	case result := <-resultCh:
		if result.err != nil {
			return nil, controlPlaneRequestError(
				ctx,
				requestCtx,
				timeout,
				phase,
				result.err,
			)
		}

		return result.mapping, nil
	case <-requestCtx.Done():
		select {
		case result := <-resultCh:
			if result.err != nil {
				return nil, controlPlaneRequestError(
					ctx,
					requestCtx,
					timeout,
					phase,
					result.err,
				)
			}

			return result.mapping, nil
		default:
			return nil, controlPlaneRequestError(
				ctx,
				requestCtx,
				timeout,
				phase,
				requestCtx.Err(),
			)
		}
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
	ref := aggapi.NodeRef{
		APIVersion: snapshotapi.StorageGroup + "/" + snapshotapi.Version,
		Kind:       snapshotKind,
		Name:       cfg.Snapshot,
		Namespace:  cfg.Namespace,
	}

	snap, err := cfg.getSnapshotNode(ctx, ref)
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

	var errs preflightLeafErrors

	for _, ref := range refs {
		gvr, namespaced, err := cfg.resourceForGroupKind(
			ctx,
			ref.group,
			ref.kind,
			fmt.Sprintf("resolving volume-snapshot leaf %s/%s", ref.kind, ref.name),
		)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("%s/%s: cannot resolve resource: %w", ref.kind, ref.name, err)
			}

			errs = append(errs, fmt.Errorf("%s/%s: cannot resolve resource: %w", ref.kind, ref.name, err))

			continue
		}

		var ri dynamic.ResourceInterface
		if namespaced {
			ri = cfg.Dynamic.Resource(gvr).Namespace(cfg.Namespace)
		} else {
			ri = cfg.Dynamic.Resource(gvr)
		}

		obj, getErr := controlPlaneRequest(
			ctx,
			cfg.controlPlaneTimeout(),
			fmt.Sprintf("getting volume-snapshot leaf %s %s/%s", ref.kind, cfg.Namespace, ref.name),
			func(requestCtx context.Context) (*unstructured.Unstructured, error) {
				return ri.Get(requestCtx, ref.name, metav1.GetOptions{})
			},
		)
		if kubeerrors.IsNotFound(getErr) {
			errs = append(errs, fmt.Errorf("%s/%s: missing: %w", ref.kind, ref.name, getErr))

			continue
		}

		if getErr != nil {
			if errors.Is(getErr, context.Canceled) || errors.Is(getErr, context.DeadlineExceeded) {
				return fmt.Errorf("%s/%s: get error: %w", ref.kind, ref.name, getErr)
			}

			errs = append(errs, fmt.Errorf("%s/%s: get error: %w", ref.kind, ref.name, getErr))

			continue
		}

		if !isLeafReady(obj, ref) {
			errs = append(errs, fmt.Errorf("%s/%s: not ready", ref.kind, ref.name))
		}
	}

	if len(errs) == 0 {
		return nil
	}

	return fmt.Errorf("volume-snapshot leaves not ready: %w", errs)
}

type preflightLeafErrors []error

func (errs preflightLeafErrors) Error() string {
	messages := make([]string, 0, len(errs))
	for _, err := range errs {
		messages = append(messages, err.Error())
	}

	return strings.Join(messages, "; ")
}

func (errs preflightLeafErrors) Unwrap() []error {
	return errs
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
func applyAll(ctx context.Context, cfg Config, objs []unstructured.Unstructured) ([]pvcRef, int, error) {
	var pvcs []pvcRef

	for i := range objs {
		obj := &objs[i]

		ns, err := applyObject(ctx, cfg, obj)
		if err != nil {
			return nil, i, fmt.Errorf("apply %s/%s %q: %w", obj.GetAPIVersion(), obj.GetKind(), obj.GetName(), err)
		}

		if obj.GetKind() == pvcKind {
			scName, _, _ := unstructured.NestedString(obj.Object, "spec", "storageClassName")
			pvcs = append(pvcs, pvcRef{namespace: ns, name: obj.GetName(), storageClassName: scName})
		}
	}

	return pvcs, len(objs), nil
}

// applyObject applies a single object to the cluster using Server-Side Apply (SSA).
// SSA merges only the fields d8 sets; fields owned by other managers (controllers,
// webhooks) are not touched. Namespaced objects without a namespace inherit the target
// namespace. It returns the effective namespace the object was applied into.
func applyObject(ctx context.Context, cfg Config, obj *unstructured.Unstructured) (string, error) {
	phase := "applying"
	if cfg.DryRun {
		phase = "dry-run applying"
	}

	gvr, namespaced, err := cfg.resourceFor(
		ctx,
		obj.GroupVersionKind(),
		fmt.Sprintf("%s %s/%s %q", phase, obj.GetAPIVersion(), obj.GetKind(), obj.GetName()),
	)
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

	_, patchErr := controlPlaneRequest(
		ctx,
		cfg.controlPlaneTimeout(),
		fmt.Sprintf("%s %s/%s %q", phase, obj.GetAPIVersion(), obj.GetKind(), obj.GetName()),
		func(requestCtx context.Context) (*unstructured.Unstructured, error) {
			return ri.Patch(requestCtx, obj.GetName(), types.ApplyPatchType, jsonBytes, patchOpts)
		},
	)
	if patchErr != nil {
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

	newWaitContext := context.WithTimeout
	if cfg.newWaitContext != nil {
		newWaitContext = cfg.newWaitContext
	}

	waitCtx, cancel := newWaitContext(ctx, cfg.Timeout)
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
		sc, err = controlPlaneRequest(
			ctx,
			cfg.controlPlaneTimeout(),
			fmt.Sprintf("getting StorageClass %q", className),
			func(requestCtx context.Context) (*unstructured.Unstructured, error) {
				return cfg.Dynamic.Resource(scGVR).Get(requestCtx, className, metav1.GetOptions{})
			},
		)
		if err != nil {
			if ctxErr := waitContextError(ctx, fmt.Sprintf("getting StorageClass %q", className)); ctxErr != nil {
				err = errors.Join(err, ctxErr)
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
	list, err := controlPlaneRequest(
		ctx,
		cfg.controlPlaneTimeout(),
		"listing StorageClasses to resolve the default",
		func(requestCtx context.Context) (*unstructured.UnstructuredList, error) {
			return cfg.Dynamic.Resource(scGVR).List(requestCtx, metav1.ListOptions{})
		},
	)
	if err != nil {
		if ctxErr := waitContextError(ctx, "listing StorageClasses to resolve the default"); ctxErr != nil {
			err = errors.Join(err, ctxErr)
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
	pvc, err := controlPlaneRequest(
		ctx,
		cfg.controlPlaneTimeout(),
		fmt.Sprintf("getting restored PVC %s/%s", ref.namespace, ref.name),
		func(requestCtx context.Context) (*unstructured.Unstructured, error) {
			return cfg.Dynamic.Resource(gvr).Namespace(ref.namespace).Get(requestCtx, ref.name, metav1.GetOptions{})
		},
	)
	if kubeerrors.IsNotFound(err) {
		return "", fmt.Errorf("restored PVC %s/%s was not found after apply: %w", ref.namespace, ref.name, err)
	}

	if err != nil {
		if ctxErr := waitContextError(ctx, fmt.Sprintf("getting restored PVC %s/%s", ref.namespace, ref.name)); ctxErr != nil {
			err = errors.Join(err, ctxErr)
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
func (cfg Config) resourceFor(
	ctx context.Context,
	gvk schema.GroupVersionKind,
	phase string,
) (schema.GroupVersionResource, bool, error) {
	mapping, err := cfg.restMapping(ctx, phase, gvk.GroupKind(), gvk.Version)
	if err != nil {
		return schema.GroupVersionResource{}, false, fmt.Errorf("resolve resource for %s: %w", gvk.String(), err)
	}

	return mapping.Resource, mapping.Scope.Name() == meta.RESTScopeNameNamespace, nil
}

// resourceForGroupKind resolves a GroupKind to its preferred-version resource without
// requiring a known version. Used by preflightLeaves where only apiGroup+kind are known
// (spec.dataSourceRef / spec.dataSource do not carry the API version).
func (cfg Config) resourceForGroupKind(
	ctx context.Context,
	group string,
	kind string,
	phase string,
) (schema.GroupVersionResource, bool, error) {
	mapping, err := cfg.restMapping(ctx, phase, schema.GroupKind{Group: group, Kind: kind})
	if err != nil {
		return schema.GroupVersionResource{}, false, fmt.Errorf("resolve resource for %s/%s: %w", group, kind, err)
	}

	return mapping.Resource, mapping.Scope.Name() == meta.RESTScopeNameNamespace, nil
}

type nodeMatch struct {
	ref         aggapi.NodeRef
	obj         *unstructured.Unstructured
	apiVersions []string
	generated   bool
}

type nodeIdentity struct {
	apiVersion string
	kind       string
	name       string
	generated  bool
}

type nodeResolver struct {
	cfg           Config
	limits        hierarchyWalkLimits
	seen          map[string]struct{}
	matches       []nodeMatch
	missingRefs   []aggapi.NodeRef
	authoritative *nodeMatch
	nodeCount     int
	observe       func(hierarchyWalkStats)
}

type hierarchyWalkLimits struct {
	maxDepth int
	maxNodes int
}

type hierarchyWalkStats struct {
	nodes      int
	stackDepth int
}

type hierarchyWalkFrame struct {
	ref       aggapi.NodeRef
	obj       *unstructured.Unstructured
	depth     int
	childRefs []interface{}
	nextChild int
}

// resolveNodeRef resolves the selector only within the hierarchy rooted at the
// positional Snapshot. Child refs carry the real snapshot GVK, so no cross-group
// kind guess is needed.
func (cfg Config) resolveNodeRef(ctx context.Context) (aggapi.NodeRef, *unstructured.Unstructured, error) {
	return cfg.resolveNodeRefWithLimits(ctx, hierarchyWalkLimits{
		maxDepth: restoreHierarchyMaxDepth,
		maxNodes: restoreHierarchyMaxNodes,
	}, nil)
}

func (cfg Config) resolveNodeRefWithLimits(
	ctx context.Context,
	limits hierarchyWalkLimits,
	observe func(hierarchyWalkStats),
) (aggapi.NodeRef, *unstructured.Unstructured, error) {
	if limits.maxDepth < 0 {
		return aggapi.NodeRef{}, nil, fmt.Errorf("snapshot hierarchy max depth must be non-negative")
	}

	if limits.maxNodes <= 0 {
		return aggapi.NodeRef{}, nil, fmt.Errorf("snapshot hierarchy max nodes must be positive")
	}

	rootRef := aggapi.NodeRef{
		APIVersion: snapshotapi.StorageGroup + "/" + snapshotapi.Version,
		Kind:       snapshotKind,
		Name:       cfg.Snapshot,
		Namespace:  cfg.Namespace,
	}

	resolver := nodeResolver{
		cfg:     cfg,
		limits:  limits,
		seen:    make(map[string]struct{}, limits.maxNodes),
		observe: observe,
	}

	if err := resolver.reserveNode(rootRef, 0); err != nil {
		return aggapi.NodeRef{}, nil, err
	}

	root, err := cfg.getSnapshotNode(ctx, rootRef)
	if err != nil {
		return aggapi.NodeRef{}, nil, fmt.Errorf("get root Snapshot %s/%s: %w", cfg.Namespace, cfg.Snapshot, err)
	}

	if err := resolver.walk(ctx, rootRef, root); err != nil {
		return aggapi.NodeRef{}, nil, err
	}

	if resolver.authoritative != nil {
		return resolver.authoritative.ref, resolver.authoritative.obj, nil
	}

	switch len(resolver.matches) {
	case 0:
		missingMatches := resolver.matchingMissingRefs()
		if len(missingMatches) == 1 {
			missing := missingMatches[0]

			return aggapi.NodeRef{}, nil, fmt.Errorf(
				"%s belongs to Snapshot %s/%s as generated child ref %s %s/%s, but that child is deleted; retry after the snapshot hierarchy is reconciled",
				cfg.selectedNodeDescription(),
				cfg.Namespace,
				cfg.Snapshot,
				missing.APIVersion,
				missing.Kind,
				missing.Name,
			)
		}

		if len(missingMatches) > 1 {
			return aggapi.NodeRef{}, nil, cfg.ambiguousMissingNodeError(missingMatches)
		}

		if len(resolver.missingRefs) != 0 {
			return aggapi.NodeRef{}, nil, fmt.Errorf(
				"cannot prove whether %s belongs to Snapshot %s/%s because the hierarchy is incomplete; referenced child nodes are deleted: %s",
				cfg.selectedNodeDescription(),
				cfg.Namespace,
				cfg.Snapshot,
				formatNodeRefs(resolver.missingRefs),
			)
		}

		return aggapi.NodeRef{}, nil, fmt.Errorf(
			"%s does not belong to Snapshot %s/%s",
			cfg.selectedNodeDescription(),
			cfg.Namespace,
			cfg.Snapshot,
		)
	case 1:
		match := resolver.matches[0]
		missingMatches := resolver.matchingMissingRefs()

		if match.generated && len(missingMatches) != 0 {
			return aggapi.NodeRef{}, nil, cfg.ambiguousLiveAndMissingNodeError(match, missingMatches)
		}

		if !match.generated && len(resolver.missingRefs) != 0 {
			return aggapi.NodeRef{}, nil, fmt.Errorf(
				"cannot prove that original-source selector %s uniquely identifies %s %s/%s in Snapshot %s/%s because the hierarchy is incomplete; referenced child nodes are deleted: %s; retry with %s",
				cfg.selectedNodeDescription(),
				match.ref.APIVersion,
				match.ref.Kind,
				match.ref.Name,
				cfg.Namespace,
				cfg.Snapshot,
				formatNodeRefs(resolver.missingRefs),
				cfg.generatedNodeRerun(match.ref),
			)
		}

		return match.ref, match.obj, nil
	default:
		return aggapi.NodeRef{}, nil, cfg.ambiguousNodeError(resolver.matches)
	}
}

func (cfg Config) ambiguousNodeError(matches []nodeMatch) error {
	candidates := make([]string, 0, len(matches))
	reruns := make([]string, 0, len(matches))
	apiVersionCounts := make(map[string]int)

	for _, match := range matches {
		for _, apiVersion := range match.apiVersions {
			apiVersionCounts[apiVersion]++
		}
	}

	for _, match := range matches {
		candidates = append(candidates, fmt.Sprintf(
			"%s %s/%s (matching apiVersions: %s)",
			match.ref.APIVersion,
			match.ref.Kind,
			match.ref.Name,
			strings.Join(match.apiVersions, ", "),
		))

		for _, matchingAPIVersion := range match.apiVersions {
			apiVersion := matchingAPIVersion
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

	return fmt.Errorf(
		"%s is ambiguous within Snapshot %s/%s; matching snapshot nodes: %s; rerun with an exact apiVersion: %s",
		cfg.selectedNodeDescription(),
		cfg.Namespace,
		cfg.Snapshot,
		strings.Join(candidates, ", "),
		strings.Join(reruns, " or "),
	)
}

func (cfg Config) ambiguousLiveAndMissingNodeError(match nodeMatch, refs []aggapi.NodeRef) error {
	candidates := make([]string, 0, 1+len(refs))
	candidates = append(candidates, fmt.Sprintf(
		"live %s %s/%s",
		match.ref.APIVersion,
		match.ref.Kind,
		match.ref.Name,
	))
	reruns := make([]string, 0, 1+len(refs))
	reruns = append(reruns, cfg.generatedNodeRerun(match.ref))

	for _, ref := range refs {
		candidates = append(candidates, fmt.Sprintf("deleted %s %s/%s", ref.APIVersion, ref.Kind, ref.Name))
		reruns = append(reruns, cfg.generatedNodeRerun(ref))
	}

	return fmt.Errorf(
		"%s is ambiguous within incomplete Snapshot %s/%s; matching generated child refs: %s; retry with an exact generated identity: %s",
		cfg.selectedNodeDescription(),
		cfg.Namespace,
		cfg.Snapshot,
		strings.Join(candidates, ", "),
		strings.Join(reruns, " or "),
	)
}

func (cfg Config) ambiguousMissingNodeError(refs []aggapi.NodeRef) error {
	candidates := make([]string, 0, len(refs))
	reruns := make([]string, 0, len(refs))

	for _, ref := range refs {
		candidates = append(candidates, fmt.Sprintf("%s %s/%s", ref.APIVersion, ref.Kind, ref.Name))
		reruns = append(reruns, cfg.generatedNodeRerun(ref))
	}

	return fmt.Errorf(
		"%s matches multiple deleted child refs in Snapshot %s/%s: %s; retry with an exact generated identity: %s",
		cfg.selectedNodeDescription(),
		cfg.Namespace,
		cfg.Snapshot,
		strings.Join(candidates, ", "),
		strings.Join(reruns, " or "),
	)
}

func (cfg Config) generatedNodeRerun(ref aggapi.NodeRef) string {
	return fmt.Sprintf(
		"d8 snapshot restore %s -n %s --node %s/%s --node-api-version %s",
		cfg.Snapshot,
		cfg.Namespace,
		ref.Kind,
		ref.Name,
		ref.APIVersion,
	)
}

func formatNodeRefs(refs []aggapi.NodeRef) string {
	values := make([]string, 0, len(refs))
	for _, ref := range refs {
		values = append(values, fmt.Sprintf("%s %s/%s", ref.APIVersion, ref.Kind, ref.Name))
	}

	return strings.Join(values, ", ")
}

func (cfg Config) selectedNodeDescription() string {
	identity := cfg.SelectedNodeKind + "/" + cfg.SelectedNodeName
	if cfg.SelectedNodeAPIVersion == "" {
		return identity
	}

	return cfg.SelectedNodeAPIVersion + " " + identity
}

func (r *nodeResolver) enterNode(
	ref aggapi.NodeRef,
	obj *unstructured.Unstructured,
	depth int,
) (hierarchyWalkFrame, bool, error) {
	apiVersions, generated, err := r.matchingAPIVersions(obj, ref)
	if err != nil {
		return hierarchyWalkFrame{}, false, err
	}

	if len(apiVersions) != 0 {
		match := nodeMatch{
			ref:         ref,
			obj:         obj,
			apiVersions: apiVersions,
			generated:   generated,
		}
		r.matches = append(r.matches, match)

		if generated &&
			r.cfg.SelectedNodeAPIVersion != "" &&
			ref.APIVersion == r.cfg.SelectedNodeAPIVersion &&
			ref.Kind == r.cfg.SelectedNodeKind &&
			ref.Name == r.cfg.SelectedNodeName {
			r.authoritative = &match

			return hierarchyWalkFrame{}, true, nil
		}
	}

	childRefs, err := snapshotChildRefValues(obj)
	if err != nil {
		return hierarchyWalkFrame{}, false, fmt.Errorf(
			"%s %s/%s: status.childrenSnapshotRefs: %w",
			ref.APIVersion,
			ref.Kind,
			ref.Name,
			err,
		)
	}

	return hierarchyWalkFrame{
		ref:       ref,
		obj:       obj,
		depth:     depth,
		childRefs: childRefs,
	}, false, nil
}

func (r *nodeResolver) walk(
	ctx context.Context,
	rootRef aggapi.NodeRef,
	root *unstructured.Unstructured,
) error {
	rootFrame, done, err := r.enterNode(rootRef, root, 0)
	if err != nil {
		return err
	}

	if done {
		return nil
	}

	stackCapacity := min(r.limits.maxDepth, r.limits.maxNodes-1) + 1
	stack := make([]hierarchyWalkFrame, 0, stackCapacity)
	stack = append(stack, rootFrame)
	r.report(len(stack))

	for len(stack) != 0 {
		if err := hierarchyWalkContextError(ctx); err != nil {
			return err
		}

		frame := &stack[len(stack)-1]
		if frame.nextChild == len(frame.childRefs) {
			stack = stack[:len(stack)-1]
			r.report(len(stack))

			continue
		}

		childIndex := frame.nextChild
		frame.nextChild++

		childRef, err := snapshotChildRefAt(frame.childRefs, childIndex)
		if err != nil {
			return fmt.Errorf(
				"%s %s/%s: status.childrenSnapshotRefs: %w",
				frame.ref.APIVersion,
				frame.ref.Kind,
				frame.ref.Name,
				err,
			)
		}

		child := aggapi.NodeRef{
			APIVersion: childRef.APIVersion,
			Kind:       childRef.Kind,
			Name:       childRef.Name,
			Namespace:  r.cfg.Namespace,
		}
		childDepth := frame.depth + 1

		if err := r.reserveNode(child, childDepth); err != nil {
			return err
		}

		r.report(len(stack))

		childObj, missing, getErr := r.cfg.getSnapshotChild(ctx, frame.ref, frame.obj, child)
		if getErr != nil {
			return fmt.Errorf("get snapshot child %s %s/%s: %w", child.APIVersion, child.Kind, child.Name, getErr)
		}

		if missing {
			r.missingRefs = append(r.missingRefs, child)

			continue
		}

		childFrame, done, err := r.enterNode(child, childObj, childDepth)
		if err != nil {
			return err
		}

		if done {
			return nil
		}

		stack = append(stack, childFrame)
		r.report(len(stack))
	}

	return nil
}

func (r *nodeResolver) reserveNode(ref aggapi.NodeRef, depth int) error {
	if r.nodeCount >= r.limits.maxNodes {
		return fmt.Errorf(
			"snapshot hierarchy exceeds node budget of %d while adding %s %s/%s at depth %d; the root and every referenced child, including missing children, count toward the limit",
			r.limits.maxNodes,
			ref.APIVersion,
			ref.Kind,
			ref.Name,
			depth,
		)
	}

	r.nodeCount++

	key := nodeRefKey(ref)
	if _, seen := r.seen[key]; seen {
		return duplicateNodeRefError(ref)
	}

	if depth > r.limits.maxDepth {
		return fmt.Errorf(
			"snapshot hierarchy exceeds depth budget of %d at %s %s/%s (depth %d; root depth is 0)",
			r.limits.maxDepth,
			ref.APIVersion,
			ref.Kind,
			ref.Name,
			depth,
		)
	}

	r.seen[key] = struct{}{}

	return nil
}

func (r *nodeResolver) report(stackDepth int) {
	if r.observe == nil {
		return
	}

	r.observe(hierarchyWalkStats{
		nodes:      r.nodeCount,
		stackDepth: stackDepth,
	})
}

func hierarchyWalkContextError(ctx context.Context) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}

	cause := context.Cause(ctx)
	if cause != nil && !errors.Is(cause, err) {
		err = errors.Join(err, cause)
	}

	return fmt.Errorf("walk snapshot hierarchy: %w", err)
}

func nodeRefKey(ref aggapi.NodeRef) string {
	return ref.APIVersion + "/" + ref.Kind + "/" + ref.Name
}

func duplicateNodeRefError(ref aggapi.NodeRef) error {
	return fmt.Errorf(
		"snapshot hierarchy contains duplicate or cyclic ref %s %s/%s",
		ref.APIVersion,
		ref.Kind,
		ref.Name,
	)
}

func (r *nodeResolver) matchingMissingRefs() []aggapi.NodeRef {
	matches := make([]aggapi.NodeRef, 0, len(r.missingRefs))
	for _, ref := range r.missingRefs {
		if ref.Kind != r.cfg.SelectedNodeKind || ref.Name != r.cfg.SelectedNodeName {
			continue
		}

		if r.cfg.SelectedNodeAPIVersion != "" && ref.APIVersion != r.cfg.SelectedNodeAPIVersion {
			continue
		}

		matches = append(matches, ref)
	}

	return matches
}

func (r *nodeResolver) matchingAPIVersions(
	obj *unstructured.Unstructured,
	ref aggapi.NodeRef,
) ([]string, bool, error) {
	sourceRef, hasSourceRef, err := snapshotSourceIdentity(obj, ref)
	if err != nil {
		return nil, false, err
	}

	importSourceRef, hasImportSourceRef, err := importSourceIdentity(obj, ref)
	if err != nil {
		return nil, false, err
	}

	identities := make([]nodeIdentity, 0, 3)
	identities = append(identities, nodeIdentity{
		apiVersion: ref.APIVersion,
		kind:       ref.Kind,
		name:       ref.Name,
		generated:  true,
	})

	if hasSourceRef {
		identities = append(identities, sourceRef)
	}

	if hasImportSourceRef {
		identities = append(identities, importSourceRef)
	}

	apiVersions := make([]string, 0, len(identities))
	seenAPIVersions := make(map[string]struct{}, len(identities))
	generated := false

	for _, identity := range identities {
		if !identity.matches(
			r.cfg.SelectedNodeKind,
			r.cfg.SelectedNodeName,
			r.cfg.SelectedNodeAPIVersion,
		) {
			continue
		}

		generated = generated || identity.generated

		if _, seen := seenAPIVersions[identity.apiVersion]; seen {
			continue
		}

		seenAPIVersions[identity.apiVersion] = struct{}{}
		apiVersions = append(apiVersions, identity.apiVersion)
	}

	return apiVersions, generated, nil
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
	resource, err := cfg.snapshotResource(ctx, ref)
	if err != nil {
		return nil, err
	}

	obj, err := controlPlaneRequest(
		ctx,
		cfg.controlPlaneTimeout(),
		fmt.Sprintf("getting snapshot node %s %s/%s", ref.Kind, ref.Namespace, ref.Name),
		func(requestCtx context.Context) (*unstructured.Unstructured, error) {
			return resource.Get(requestCtx, ref.Name, metav1.GetOptions{})
		},
	)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func (cfg Config) getSnapshotChild(
	ctx context.Context,
	parentRef aggapi.NodeRef,
	parent *unstructured.Unstructured,
	ref aggapi.NodeRef,
) (*unstructured.Unstructured, bool, error) {
	resource, err := cfg.snapshotResource(ctx, ref)
	if err != nil {
		return nil, false, err
	}

	obj, err := controlPlaneRequest(
		ctx,
		cfg.controlPlaneTimeout(),
		fmt.Sprintf("getting snapshot child %s %s/%s", ref.Kind, ref.Namespace, ref.Name),
		func(requestCtx context.Context) (*unstructured.Unstructured, error) {
			return resource.Get(requestCtx, ref.Name, metav1.GetOptions{})
		},
	)
	if err == nil {
		return obj, false, nil
	}

	if !kubeerrors.IsNotFound(err) {
		return nil, false, err
	}

	parentResourceVersion := parent.GetResourceVersion()
	if parentResourceVersion == "" {
		return nil, false, fmt.Errorf(
			"cannot prove absence of child ref %s %s/%s: parent %s %s/%s has no metadata.resourceVersion",
			ref.APIVersion,
			ref.Kind,
			ref.Name,
			parentRef.APIVersion,
			parentRef.Kind,
			parentRef.Name,
		)
	}

	if err := proveMissingSnapshotChild(
		ctx,
		cfg.controlPlaneTimeout(),
		resource,
		parentResourceVersion,
		ref,
	); err != nil {
		return nil, false, err
	}

	return nil, true, nil
}

func (cfg Config) snapshotResource(
	ctx context.Context,
	ref aggapi.NodeRef,
) (dynamic.ResourceInterface, error) {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("parse apiVersion %q: %w", ref.APIVersion, err)
	}

	gvk := schema.GroupVersionKind{
		Group:   gv.Group,
		Version: gv.Version,
		Kind:    ref.Kind,
	}

	mapping, err := cfg.restMapping(
		ctx,
		fmt.Sprintf("resolving snapshot hierarchy node %s %s/%s", ref.Kind, ref.Namespace, ref.Name),
		gvk.GroupKind(),
		gvk.Version,
	)
	if err != nil {
		return nil, fmt.Errorf("resolve resource for %s: %w", gvk.String(), err)
	}

	if mapping.GroupVersionKind != gvk {
		return nil, fmt.Errorf(
			"resolve resource for %s: REST mapping returned mismatched GVK %s",
			gvk.String(),
			mapping.GroupVersionKind.String(),
		)
	}

	if mapping.Scope == nil {
		return nil, fmt.Errorf("resolve resource for %s: REST mapping has no scope", gvk.String())
	}

	if mapping.Scope.Name() != meta.RESTScopeNameNamespace {
		return nil, fmt.Errorf(
			"snapshot hierarchy ref %s %s/%s violates the namespace-local contract: REST mapping for GVK %s is cluster-scoped",
			ref.APIVersion,
			ref.Kind,
			ref.Name,
			gvk.String(),
		)
	}

	return cfg.Dynamic.Resource(mapping.Resource).Namespace(cfg.Namespace), nil
}

func proveMissingSnapshotChild(
	ctx context.Context,
	controlPlaneTimeout time.Duration,
	resource dynamic.ResourceInterface,
	parentResourceVersion string,
	ref aggapi.NodeRef,
) error {
	selector := fields.OneTermEqualSelector("metadata.name", ref.Name).String()
	continueToken := ""
	seenContinueTokens := make(map[string]struct{})

	for pageNumber := 0; pageNumber < missingChildProofMaxPages; pageNumber++ {
		options := metav1.ListOptions{
			FieldSelector: selector,
			Limit:         missingChildProofPageLimit,
			Continue:      continueToken,
		}
		if continueToken == "" {
			options.ResourceVersion = parentResourceVersion
			options.ResourceVersionMatch = metav1.ResourceVersionMatchNotOlderThan
		}

		page, err := controlPlaneRequest(
			ctx,
			controlPlaneTimeout,
			fmt.Sprintf(
				"listing snapshot child %s %s/%s to prove absence",
				ref.Kind,
				ref.Namespace,
				ref.Name,
			),
			func(requestCtx context.Context) (*unstructured.UnstructuredList, error) {
				return resource.List(requestCtx, options)
			},
		)
		if err != nil {
			return fmt.Errorf(
				"list %s %s/%s at or after parent resourceVersion %s to prove absence: %w",
				ref.APIVersion,
				ref.Kind,
				ref.Name,
				parentResourceVersion,
				err,
			)
		}

		if page == nil {
			return fmt.Errorf(
				"list %s %s/%s returned no page while proving absence",
				ref.APIVersion,
				ref.Kind,
				ref.Name,
			)
		}

		for i := range page.Items {
			item := &page.Items[i]
			if item.GetName() != ref.Name {
				continue
			}

			return fmt.Errorf(
				"child ref %s %s/%s appeared in the collection after its GET returned NotFound; hierarchy changed while resolving, retry",
				ref.APIVersion,
				ref.Kind,
				ref.Name,
			)
		}

		next := page.GetContinue()
		if next == "" {
			if page.GetRemainingItemCount() != nil && *page.GetRemainingItemCount() != 0 {
				return fmt.Errorf(
					"list %s %s/%s returned an incomplete final page with remainingItemCount=%d",
					ref.APIVersion,
					ref.Kind,
					ref.Name,
					*page.GetRemainingItemCount(),
				)
			}

			return nil
		}

		if _, seen := seenContinueTokens[next]; seen {
			return fmt.Errorf(
				"list %s %s/%s repeated continue token %q while proving absence",
				ref.APIVersion,
				ref.Kind,
				ref.Name,
				next,
			)
		}

		seenContinueTokens[next] = struct{}{}
		continueToken = next
	}

	return fmt.Errorf(
		"list %s %s/%s exceeded %d pages while proving absence",
		ref.APIVersion,
		ref.Kind,
		ref.Name,
		missingChildProofMaxPages,
	)
}

func snapshotChildRefValues(obj *unstructured.Unstructured) ([]interface{}, error) {
	value, found, err := unstructured.NestedFieldNoCopy(obj.Object, "status", "childrenSnapshotRefs")
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, nil
	}

	rawRefs, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("has unexpected type %T", value)
	}

	return rawRefs, nil
}

func snapshotChildRefAt(rawRefs []interface{}, index int) (snapshotapi.SnapshotChildRef, error) {
	rawRef := rawRefs[index]

	m, ok := rawRef.(map[string]interface{})
	if !ok {
		return snapshotapi.SnapshotChildRef{}, fmt.Errorf("element %d has unexpected type %T", index, rawRef)
	}

	apiVersion, _ := m["apiVersion"].(string)
	kind, _ := m["kind"].(string)
	name, _ := m["name"].(string)

	if apiVersion == "" || kind == "" || name == "" {
		return snapshotapi.SnapshotChildRef{}, fmt.Errorf(
			"element %d is incomplete (apiVersion/kind/name required)",
			index,
		)
	}

	if err := ValidateNodeAPIVersion(apiVersion); err != nil {
		return snapshotapi.SnapshotChildRef{}, fmt.Errorf(
			"element %d has invalid apiVersion %q: %w",
			index,
			apiVersion,
			err,
		)
	}

	return snapshotapi.SnapshotChildRef{
		APIVersion: apiVersion,
		Kind:       kind,
		Name:       name,
	}, nil
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
