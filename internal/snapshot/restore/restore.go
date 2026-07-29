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
// Restore walks the selected namespaced snapshot hierarchy, fetches each node's
// manifests-with-data-restoration response with scope=node, and stages the complete
// post-order result before applying any object. This keeps every response bounded
// while preserving the server compiler's child-before-parent apply order.
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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
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
	snapshotKind  = "Snapshot"
	pvcKind       = "PersistentVolumeClaim"
	pvcResource   = "persistentvolumeclaims"
	pvResource    = "persistentvolumes"
	podResource   = "pods"
	eventResource = "events"

	// fieldManager is the SSA field manager name used for all restore applies.
	fieldManager = "d8-snapshot-restore"

	restoreEditRetryHint = "hint: retry with --edit to review and modify the resolved manifests before applying"

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
	selectedNodeAnnotation        = "volume.kubernetes.io/selected-node"

	eventReasonProvisioning          = "Provisioning"
	eventReasonExternalProvisioning  = "ExternalProvisioning"
	eventReasonProvisioningFailed    = "ProvisioningFailed"
	eventReasonProvisioningSucceeded = "ProvisioningSucceeded"
	eventReasonFailedBinding         = "FailedBinding"

	waitScanPageLimit  int64 = 100
	waitScanMaxPages   int   = 100
	waitScanMaxObjects int   = 10_000

	missingChildProofPageLimit int64 = 100
	missingChildProofMaxPages  int   = 100

	// A successful full walk processes at most maxNodes-1 child edges. It is
	// sequential, keeps at most maxDepth+1 parent frames, and issues at most one
	// GET plus missingChildProofMaxPages LISTs per child.
	restoreHierarchyMaxDepth = 64
	restoreHierarchyMaxNodes = 10_000

	defaultMaxStagedManifestBytes   int64 = 1 << 30
	defaultMaxStagedManifestObjects       = 1_000_000
	maxEditableManifestBytes        int64 = aggapi.DefaultMaxResponseBytes

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
	// AutoEdit opens the resolved manifests once when the pre-mutation DryRunAll
	// pass returns Kubernetes Invalid. Command callers enable it only when both
	// stdin and stdout are interactive terminals and --no-auto-edit is absent.
	// Edit and DryRun always suppress this automatic session.
	AutoEdit bool

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

	// maxStagedManifestBytes and maxStagedManifestObjects are test seams for
	// lowering the finite aggregate staging budgets.
	maxStagedManifestBytes   int64
	maxStagedManifestObjects int
	manifestStageOps         manifestStageOperations
	editManifests            func(context.Context, []unstructured.Unstructured) ([]unstructured.Unstructured, error)

	// newWaitContext is a test seam for controlling the shared wait boundary.
	newWaitContext func(context.Context, time.Duration) (context.Context, context.CancelFunc)
	// silenceApplyLog suppresses the per-object "would apply"/"applied" log line in
	// applyObject. Run sets it true on the implicit dry-run preflight config so operators
	// only see those messages on an explicit --dry-run request; it is never a CLI flag.
	silenceApplyLog bool
}

// pvcRef identifies the exact post-apply PVC and restore source to revalidate.
type pvcRef struct {
	namespace string
	name      string
	uid       types.UID
	// storageClassName is the PVC's staged spec.storageClassName (may be
	// empty; an empty value resolves to the cluster's default StorageClass,
	// not to Immediate binding, when the effective volumeBindingMode is
	// resolved in waitPVCsBound).
	storageClassName    string
	hasStorageClassName bool
	desiredSource       pvcSourceIdentity
	boundPV             *boundPVIdentity
}

type pvcMutationGuard struct {
	uid             types.UID
	resourceVersion string
	exists          bool
}

type pvcMutationGuards map[string]pvcMutationGuard

type pvcSourceIdentity struct {
	dataSourceRef *pvcDataSourceIdentity
	dataSource    *pvcDataSourceIdentity
}

type pvcDataSourceIdentity struct {
	apiGroup  string
	kind      string
	name      string
	namespace string
}

type boundPVIdentity struct {
	name string
	uid  types.UID
}

type applyResult struct {
	namespace string
	object    *unstructured.Unstructured
}

type automaticEditAbortedError struct {
	invalidErr error
	editErr    error
}

type existingBoundPVCError struct {
	namespace string
	name      string
	phase     string
}

func (e *existingBoundPVCError) Error() string {
	return fmt.Sprintf(
		"restore target PVC %s/%s already exists in phase %q; refusing to reuse it because it may contain stale data from an earlier operation; preserve its data and remove or rename the claim before retrying",
		e.namespace,
		e.name,
		e.phase,
	)
}

func (e *automaticEditAbortedError) Error() string {
	return fmt.Sprintf(
		"automatic edit aborted after Kubernetes Invalid response: %v; original response: %v",
		e.editErr,
		e.invalidErr,
	)
}

func (e *automaticEditAbortedError) Unwrap() []error {
	return []error{e.invalidErr, e.editErr}
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

	var targetObj *unstructured.Unstructured

	if cfg.SelectedNodeKind == "" {
		root, err := preflightRootSnapshotObject(ctx, cfg)
		if err != nil {
			return fmt.Errorf("preflight %s/%s: %w", cfg.Namespace, cfg.Snapshot, err)
		}

		targetObj = root
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
		targetObj = obj
	}

	targets, err := collectRestoreTargets(ctx, cfg, targetRef, targetObj)
	if err != nil {
		return fmt.Errorf("resolve restore subtree for %s/%s: %w", cfg.Namespace, cfg.Snapshot, err)
	}

	return runStagedRestore(ctx, cfg, targets)
}

func runStagedRestore(ctx context.Context, cfg Config, targets []restoreTarget) error {
	stages := make(manifestStages, 0, 2)
	runErr := runWithManifestStages(ctx, cfg, targets, &stages)

	return errors.Join(runErr, stages.cleanup())
}

func runWithManifestStages(ctx context.Context, cfg Config, targets []restoreTarget, stages *manifestStages) error {
	stage, err := newManifestStage(cfg)
	if err != nil {
		return fmt.Errorf("create restore manifest staging: %w", err)
	}

	stages.add(stage)

	for i := range targets {
		target := targets[i]

		opts := aggapi.RestoreScopeOptions{Scope: aggapi.RestoreScopeNode}
		if len(targets) == 1 {
			opts.FilterKind = cfg.FilterKind
			opts.FilterName = cfg.FilterName
			opts.FilterAPIVersion = cfg.FilterAPIVersion
		}

		raw, fetchErr := controlPlaneRequest(
			ctx,
			cfg.controlPlaneTimeout(),
			fmt.Sprintf("fetching restore manifests for %s %s/%s", target.ref.Kind, target.ref.Namespace, target.ref.Name),
			func(requestCtx context.Context) ([]byte, error) {
				return cfg.Source.RestoreManifestsScoped(requestCtx, target.ref, cfg.Namespace, opts)
			},
		)
		if fetchErr != nil {
			return fmt.Errorf(
				"fetch restore manifests for node %s %s/%s: %w",
				target.ref.APIVersion,
				target.ref.Kind,
				target.ref.Name,
				fetchErr,
			)
		}

		objs, decodeErr := decodeManifestArray(raw)
		if decodeErr != nil {
			return fmt.Errorf(
				"decode restore manifests for node %s %s/%s: %w",
				target.ref.APIVersion,
				target.ref.Kind,
				target.ref.Name,
				decodeErr,
			)
		}

		if addErr := stage.add(ctx, cfg, objs); addErr != nil {
			return fmt.Errorf(
				"preflight restore manifests for node %s %s/%s: %w",
				target.ref.APIVersion,
				target.ref.Kind,
				target.ref.Name,
				addErr,
			)
		}
	}

	if stage.objectCount == 0 {
		return fmt.Errorf("restore manifests for %s/%s are empty", cfg.Namespace, cfg.Snapshot)
	}

	if cfg.Edit {
		editedStage, editErr := restageEditedManifests(ctx, cfg, stage, stages)
		if editErr != nil {
			return fmt.Errorf("restore edit: %w", editErr)
		}

		stage = editedStage
	}

	editorSessionRan := cfg.Edit

	cfg.Log.Info("applying restore manifests",
		slog.String("namespace", cfg.Namespace),
		slog.String("snapshot", cfg.Snapshot),
		slog.Int("objects", stage.objectCount))

	automaticEditAttempted := false

	for {
		if _, err := preflightExistingBoundPVCs(ctx, cfg, stage); err != nil {
			return addRestoreEditHintForBoundPVC(err, editorSessionRan)
		}

		// Every candidate manifest set completes a full DryRunAll pass before the
		// first real mutation. An automatic edit restages the complete set and
		// returns here rather than continuing from the rejected object.
		dryRunCfg := cfg
		dryRunCfg.DryRun = true
		dryRunCfg.silenceApplyLog = !cfg.DryRun

		if _, _, dryRunErr := applyStaged(ctx, dryRunCfg, stage, nil); dryRunErr != nil {
			preflightErr := fmt.Errorf("dry-run preflight: %w", dryRunErr)

			autoEditEligible := cfg.AutoEdit &&
				!cfg.Edit &&
				!cfg.DryRun &&
				!automaticEditAttempted &&
				kubeerrors.IsInvalid(dryRunErr)
			if !autoEditEligible {
				if !editorSessionRan && isEditableDryRunRejection(dryRunErr) {
					return addRestoreEditHint(preflightErr)
				}

				return preflightErr
			}

			automaticEditAttempted = true
			editorSessionRan = true

			editedStage, editErr := restageEditedManifests(ctx, cfg, stage, stages)
			if editErr != nil {
				return &automaticEditAbortedError{
					invalidErr: preflightErr,
					editErr:    fmt.Errorf("automatic restore edit: %w", editErr),
				}
			}

			stage = editedStage

			continue
		}

		break
	}

	cfg.Log.Info("validated restore manifests (dry-run)",
		slog.String("namespace", cfg.Namespace),
		slog.String("snapshot", cfg.Snapshot),
		slog.Int("objects", stage.objectCount))

	// With --dry-run, only the validation pass runs; nothing has been mutated.
	if cfg.DryRun {
		return nil
	}

	// Close the admission window before the first real mutation. A target that was
	// absent or Pending before dry-run may have become Bound while validation ran.
	pvcGuards, err := preflightExistingBoundPVCs(ctx, cfg, stage)
	if err != nil {
		preflightErr := fmt.Errorf("post-dry-run PVC preflight: %w", err)

		return addRestoreEditHintForBoundPVC(preflightErr, editorSessionRan)
	}

	// Real apply pass: every object passed the dry-run, so we apply without DryRun.
	pvcs, applied, err := applyStaged(ctx, cfg, stage, pvcGuards)
	if err != nil {
		return fmt.Errorf(
			"restore apply stopped after %d of %d objects completed; the cluster may be partially applied and the active object's outcome is unknown: %w",
			applied,
			stage.objectCount,
			err,
		)
	}

	if cfg.Wait {
		return waitPVCsBound(ctx, cfg, pvcs)
	}

	return revalidatePVCsAfterApply(ctx, cfg, pvcs)
}

func isEditableDryRunRejection(err error) bool {
	return kubeerrors.IsInvalid(err) ||
		kubeerrors.IsConflict(err) ||
		kubeerrors.IsAlreadyExists(err)
}

func addRestoreEditHintForBoundPVC(err error, editorSessionRan bool) error {
	var boundErr *existingBoundPVCError

	if editorSessionRan || !errors.As(err, &boundErr) {
		return err
	}

	return addRestoreEditHint(err)
}

func addRestoreEditHint(err error) error {
	return fmt.Errorf("%w; %s", err, restoreEditRetryHint)
}

func restageEditedManifests(
	ctx context.Context,
	cfg Config,
	stage *manifestStage,
	stages *manifestStages,
) (*manifestStage, error) {
	if stage.bytesWritten > maxEditableManifestBytes {
		return nil, fmt.Errorf(
			"requires loading %d staged bytes, exceeding the bounded edit limit of %d bytes; select a smaller subtree",
			stage.bytesWritten,
			maxEditableManifestBytes,
		)
	}

	objs, err := stage.objects(ctx)
	if err != nil {
		return nil, fmt.Errorf("load staged manifests for editing: %w", err)
	}

	objs, err = cfg.editManifests(ctx, objs)
	if err != nil {
		return nil, err
	}

	editedStage, err := newManifestStage(cfg)
	if err != nil {
		return nil, fmt.Errorf("create edited restore manifest staging: %w", err)
	}

	stages.add(editedStage)

	if err = editedStage.add(ctx, cfg, objs); err != nil {
		return nil, fmt.Errorf("preflight edited restore manifests: %w", err)
	}

	if editedStage.objectCount == 0 {
		return nil, fmt.Errorf("edited restore manifests are empty")
	}

	return editedStage, nil
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

	if cfg.maxStagedManifestBytes <= 0 {
		cfg.maxStagedManifestBytes = defaultMaxStagedManifestBytes
	}

	if cfg.maxStagedManifestObjects <= 0 {
		cfg.maxStagedManifestObjects = defaultMaxStagedManifestObjects
	}

	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}

	if cfg.editManifests == nil {
		cfg.editManifests = editManifestsContext
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
	case cfg.Scope != "" &&
		cfg.Scope != aggapi.RestoreScopeSubtree &&
		cfg.Scope != aggapi.RestoreScopeNode:
		return fmt.Errorf("restore: Scope must be %q or %q", aggapi.RestoreScopeSubtree, aggapi.RestoreScopeNode)
	case (cfg.FilterKind == "") != (cfg.FilterName == ""):
		return fmt.Errorf("restore: FilterKind and FilterName must be set together")
	case cfg.FilterAPIVersion != "" && cfg.FilterKind == "":
		return fmt.Errorf("restore: FilterAPIVersion requires FilterKind and FilterName")
	case cfg.FilterKind != "" && cfg.Scope != aggapi.RestoreScopeNode:
		return fmt.Errorf("restore: object filter requires Scope=%q", aggapi.RestoreScopeNode)
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

		if !isNamespaced {
			return fmt.Errorf(
				"namespace restore does not support cluster-scoped object apiVersion=%q kind=%q name=%q; remove it from the snapshot or edited manifests before retrying",
				obj.GetAPIVersion(),
				obj.GetKind(),
				obj.GetName(),
			)
		}

		namespaced[i] = true

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

type restoreTarget struct {
	ref aggapi.NodeRef
	obj *unstructured.Unstructured
}

type restoreTargetFrame struct {
	target    restoreTarget
	depth     int
	childRefs []interface{}
	nextChild int
}

func collectRestoreTargets(
	ctx context.Context,
	cfg Config,
	rootRef aggapi.NodeRef,
	rootObj *unstructured.Unstructured,
) ([]restoreTarget, error) {
	if cfg.Scope == aggapi.RestoreScopeNode {
		return []restoreTarget{{ref: rootRef, obj: rootObj}}, nil
	}

	rootChildRefs, err := snapshotChildRefValues(rootObj)
	if err != nil {
		return nil, fmt.Errorf(
			"%s %s/%s: status.childrenSnapshotRefs: %w",
			rootRef.APIVersion,
			rootRef.Kind,
			rootRef.Name,
			err,
		)
	}

	seen := map[string]struct{}{nodeRefKey(rootRef): {}}
	stack := []restoreTargetFrame{{
		target:    restoreTarget{ref: rootRef, obj: rootObj},
		childRefs: rootChildRefs,
	}}
	targets := make([]restoreTarget, 0)

	for len(stack) != 0 {
		if err := hierarchyWalkContextError(ctx); err != nil {
			return nil, err
		}

		frame := &stack[len(stack)-1]
		if frame.nextChild == len(frame.childRefs) {
			targets = append(targets, frame.target)
			stack = stack[:len(stack)-1]

			continue
		}

		childIndex := frame.nextChild
		frame.nextChild++

		childRef, err := snapshotChildRefAt(frame.childRefs, childIndex)
		if err != nil {
			return nil, fmt.Errorf(
				"%s %s/%s: status.childrenSnapshotRefs: %w",
				frame.target.ref.APIVersion,
				frame.target.ref.Kind,
				frame.target.ref.Name,
				err,
			)
		}

		child := aggapi.NodeRef{
			APIVersion: childRef.APIVersion,
			Kind:       childRef.Kind,
			Name:       childRef.Name,
			Namespace:  cfg.Namespace,
		}
		childDepth := frame.depth + 1

		if len(seen) >= restoreHierarchyMaxNodes {
			return nil, fmt.Errorf(
				"snapshot hierarchy exceeds node budget of %d while adding %s %s/%s at depth %d",
				restoreHierarchyMaxNodes,
				child.APIVersion,
				child.Kind,
				child.Name,
				childDepth,
			)
		}

		if childDepth > restoreHierarchyMaxDepth {
			return nil, fmt.Errorf(
				"snapshot hierarchy exceeds depth budget of %d at %s %s/%s (depth %d; root depth is 0)",
				restoreHierarchyMaxDepth,
				child.APIVersion,
				child.Kind,
				child.Name,
				childDepth,
			)
		}

		key := nodeRefKey(child)
		if _, exists := seen[key]; exists {
			return nil, duplicateNodeRefError(child)
		}

		seen[key] = struct{}{}

		childObj, missing, err := cfg.getSnapshotChild(ctx, frame.target.ref, frame.target.obj, child)
		if err != nil {
			return nil, fmt.Errorf("get snapshot child %s %s/%s: %w", child.APIVersion, child.Kind, child.Name, err)
		}

		if missing {
			return nil, fmt.Errorf(
				"snapshot subtree is incomplete: child %s %s/%s is missing; reconcile the snapshot hierarchy before retrying",
				child.APIVersion,
				child.Kind,
				child.Name,
			)
		}

		if err := preflightSelectedNode(child, childObj); err != nil {
			return nil, fmt.Errorf("preflight snapshot child %s %s/%s: %w", child.APIVersion, child.Kind, child.Name, err)
		}

		childRefs, err := snapshotChildRefValues(childObj)
		if err != nil {
			return nil, fmt.Errorf(
				"%s %s/%s: status.childrenSnapshotRefs: %w",
				child.APIVersion,
				child.Kind,
				child.Name,
				err,
			)
		}

		stack = append(stack, restoreTargetFrame{
			target:    restoreTarget{ref: child, obj: childObj},
			depth:     childDepth,
			childRefs: childRefs,
		})
	}

	return targets, nil
}

type manifestIdentity struct {
	group     string
	resource  string
	namespace string
	name      string
}

type manifestStage struct {
	file         *os.File
	path         string
	closeFile    func(*os.File) error
	removeFile   func(string) error
	maxBytes     int64
	maxObjects   int
	bytesWritten int64
	objectCount  int
	seen         map[manifestIdentity][sha256.Size]byte
}

type manifestStageOperations struct {
	closeFile  func(*os.File) error
	removeFile func(string) error
	created    func(*manifestStage)
}

type manifestStages []*manifestStage

func newManifestStage(cfg Config) (*manifestStage, error) {
	file, err := os.CreateTemp("", "d8-restore-stage-*.jsonl")
	if err != nil {
		return nil, err
	}

	closeFile := cfg.manifestStageOps.closeFile
	if closeFile == nil {
		closeFile = func(file *os.File) error {
			return file.Close()
		}
	}

	removeFile := cfg.manifestStageOps.removeFile
	if removeFile == nil {
		removeFile = os.Remove
	}

	stage := &manifestStage{
		file:       file,
		path:       file.Name(),
		closeFile:  closeFile,
		removeFile: removeFile,
		maxBytes:   cfg.maxStagedManifestBytes,
		maxObjects: cfg.maxStagedManifestObjects,
		seen:       make(map[manifestIdentity][sha256.Size]byte),
	}

	if cfg.manifestStageOps.created != nil {
		cfg.manifestStageOps.created(stage)
	}

	return stage, nil
}

func (s *manifestStage) cleanup() error {
	var closeErr error

	if s.file != nil {
		file := s.file
		s.file = nil

		if err := s.closeFile(file); err != nil {
			closeErr = fmt.Errorf("close restore manifest staging %q: %w", s.path, err)
		}
	}

	var removeErr error

	if s.path != "" {
		if err := s.removeFile(s.path); err != nil {
			removeErr = fmt.Errorf("remove restore manifest staging %q: %w", s.path, err)
		} else {
			s.path = ""
		}
	}

	return errors.Join(closeErr, removeErr)
}

func (s *manifestStages) add(stage *manifestStage) {
	*s = append(*s, stage)
}

func (s manifestStages) cleanup() error {
	cleanupErrs := make([]error, 0, len(s))

	for i := len(s) - 1; i >= 0; i-- {
		if err := s[i].cleanup(); err != nil {
			cleanupErrs = append(cleanupErrs, err)
		}
	}

	return errors.Join(cleanupErrs...)
}

func (s *manifestStage) add(ctx context.Context, cfg Config, objs []unstructured.Unstructured) error {
	if err := preflightManifestNamespaces(ctx, cfg, objs); err != nil {
		return err
	}

	if err := preflightLeaves(ctx, cfg, objs); err != nil {
		return err
	}

	for i := range objs {
		if err := hierarchyWalkContextError(ctx); err != nil {
			return err
		}

		obj := &objs[i]

		identity, err := cfg.manifestIdentity(ctx, obj)
		if err != nil {
			return err
		}

		raw, err := json.Marshal(obj.Object)
		if err != nil {
			return fmt.Errorf(
				"marshal restore manifest apiVersion=%q kind=%q name=%q for staging: %w",
				obj.GetAPIVersion(),
				obj.GetKind(),
				obj.GetName(),
				err,
			)
		}

		digest := sha256.Sum256(raw)
		// The first post-order occurrence wins only when canonical JSON is
		// identical. Any semantic difference for the same REST resource,
		// namespace, and name is a conflict rather than an order-dependent merge.
		if previous, exists := s.seen[identity]; exists {
			if previous == digest {
				continue
			}

			return fmt.Errorf(
				"conflicting duplicate restore object apiVersion=%q kind=%q namespace=%q name=%q resolves to %s/%s; remove one conflicting copy before retrying",
				obj.GetAPIVersion(),
				obj.GetKind(),
				obj.GetNamespace(),
				obj.GetName(),
				identity.group,
				identity.resource,
			)
		}

		if s.objectCount >= s.maxObjects {
			return fmt.Errorf("restore manifest staging exceeds object budget of %d", s.maxObjects)
		}

		recordBytes := int64(len(raw) + 1)
		if recordBytes > s.maxBytes-s.bytesWritten {
			return fmt.Errorf(
				"restore manifest staging exceeds temporary-disk byte budget of %d (would require at least %d); select a smaller subtree",
				s.maxBytes,
				s.bytesWritten+recordBytes,
			)
		}

		if _, err := s.file.Write(raw); err != nil {
			return fmt.Errorf("write staged restore manifest: %w", err)
		}

		if _, err := s.file.Write([]byte{'\n'}); err != nil {
			return fmt.Errorf("terminate staged restore manifest: %w", err)
		}

		s.seen[identity] = digest
		s.bytesWritten += recordBytes
		s.objectCount++
	}

	return nil
}

func (cfg Config) manifestIdentity(
	ctx context.Context,
	obj *unstructured.Unstructured,
) (manifestIdentity, error) {
	if obj.GetAPIVersion() == "" || obj.GetKind() == "" || obj.GetName() == "" {
		return manifestIdentity{}, fmt.Errorf(
			"restore manifest identity is incomplete: apiVersion=%q kind=%q name=%q are all required",
			obj.GetAPIVersion(),
			obj.GetKind(),
			obj.GetName(),
		)
	}

	gvk := obj.GroupVersionKind()

	mapping, err := cfg.restMapping(
		ctx,
		fmt.Sprintf(
			"resolving canonical identity for restore manifest apiVersion=%q kind=%q name=%q",
			obj.GetAPIVersion(),
			obj.GetKind(),
			obj.GetName(),
		),
		gvk.GroupKind(),
		gvk.Version,
	)
	if err != nil {
		return manifestIdentity{}, fmt.Errorf(
			"resolve canonical identity for restore manifest apiVersion=%q kind=%q name=%q: %w",
			obj.GetAPIVersion(),
			obj.GetKind(),
			obj.GetName(),
			err,
		)
	}

	return manifestIdentity{
		group:     mapping.Resource.Group,
		resource:  mapping.Resource.Resource,
		namespace: obj.GetNamespace(),
		name:      obj.GetName(),
	}, nil
}

func (s *manifestStage) forEach(
	ctx context.Context,
	visit func(*unstructured.Unstructured) error,
) error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind restore manifest staging: %w", err)
	}

	decoder := json.NewDecoder(s.file)

	for {
		if err := hierarchyWalkContextError(ctx); err != nil {
			return err
		}

		var obj unstructured.Unstructured

		err := decoder.Decode(&obj.Object)
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("decode staged restore manifest: %w", err)
		}

		if err := visit(&obj); err != nil {
			return err
		}
	}
}

func (s *manifestStage) objects(ctx context.Context) ([]unstructured.Unstructured, error) {
	objs := make([]unstructured.Unstructured, 0, s.objectCount)

	err := s.forEach(ctx, func(obj *unstructured.Unstructured) error {
		objs = append(objs, *obj)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return objs, nil
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
	_, err := preflightRootSnapshotObject(ctx, cfg)

	return err
}

func preflightRootSnapshotObject(
	ctx context.Context,
	cfg Config,
) (*unstructured.Unstructured, error) {
	ref := aggapi.NodeRef{
		APIVersion: snapshotapi.StorageGroup + "/" + snapshotapi.Version,
		Kind:       snapshotKind,
		Name:       cfg.Snapshot,
		Namespace:  cfg.Namespace,
	}

	snap, err := cfg.getSnapshotNode(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("get Snapshot: %w", err)
	}

	if !isConditionTrue(snap) {
		status, reason, message := readyConditionDetail(snap, readyConditionType)
		if status == conditionFalse && source.IsDegradedReason(reason) {
			return nil, fmt.Errorf("snapshot is DEGRADED (reason=%s: %s): a namespaced child was deleted but "+
				"its data is intact in the content-layer trash; a full-subtree restore of the root is "+
				"blocked, but --node <ready child> can restore an intact subtree", reason, message)
		}

		return nil, fmt.Errorf("snapshot is not Ready=True (cannot restore an incomplete snapshot)")
	}

	bound, _, _ := unstructured.NestedString(snap.Object, "status", "boundSnapshotContentName")
	if bound == "" {
		return nil, fmt.Errorf("snapshot has no status.boundSnapshotContentName (not yet bound)")
	}

	return snap, nil
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

// preflightExistingBoundPVCs rejects stale successful state and records the
// API-enforced conditions for the real PVC mutation.
func preflightExistingBoundPVCs(
	ctx context.Context,
	cfg Config,
	stage *manifestStage,
) (pvcMutationGuards, error) {
	gvr := schema.GroupVersionResource{Version: "v1", Resource: pvcResource}
	guards := make(pvcMutationGuards)

	err := stage.forEach(ctx, func(obj *unstructured.Unstructured) error {
		if obj.GetAPIVersion() != "v1" || obj.GetKind() != pvcKind {
			return nil
		}

		namespace := obj.GetNamespace()

		pvc, err := controlPlaneRequest(
			ctx,
			cfg.controlPlaneTimeout(),
			fmt.Sprintf("getting restore target PVC %s/%s", namespace, obj.GetName()),
			func(requestCtx context.Context) (*unstructured.Unstructured, error) {
				return cfg.Dynamic.Resource(gvr).Namespace(namespace).Get(
					requestCtx,
					obj.GetName(),
					metav1.GetOptions{},
				)
			},
		)
		if kubeerrors.IsNotFound(err) {
			guards[pvcMutationGuardKey(namespace, obj.GetName())] = pvcMutationGuard{}

			return nil
		}

		if err != nil {
			return fmt.Errorf("preflight restore target PVC %s/%s: %w", namespace, obj.GetName(), err)
		}

		phase, _, err := unstructured.NestedString(pvc.Object, "status", "phase")
		if err != nil {
			return fmt.Errorf(
				"preflight restore target PVC %s/%s: read status.phase: %w",
				namespace,
				obj.GetName(),
				err,
			)
		}

		if phase == pvcPhaseBound {
			return &existingBoundPVCError{
				namespace: namespace,
				name:      obj.GetName(),
				phase:     phase,
			}
		}

		if pvc.GetUID() == "" {
			return fmt.Errorf(
				"preflight restore target PVC %s/%s: API server returned an empty metadata.uid",
				namespace,
				obj.GetName(),
			)
		}

		if pvc.GetResourceVersion() == "" {
			return fmt.Errorf(
				"preflight restore target PVC %s/%s: API server returned an empty metadata.resourceVersion",
				namespace,
				obj.GetName(),
			)
		}

		guards[pvcMutationGuardKey(namespace, obj.GetName())] = pvcMutationGuard{
			uid:             pvc.GetUID(),
			resourceVersion: pvc.GetResourceVersion(),
			exists:          true,
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return guards, nil
}

func pvcMutationGuardKey(namespace, name string) string {
	return namespace + "\x00" + name
}

func applyStaged(
	ctx context.Context,
	cfg Config,
	stage *manifestStage,
	pvcGuards pvcMutationGuards,
) ([]pvcRef, int, error) {
	pvcs := make([]pvcRef, 0)
	applied := 0

	err := stage.forEach(ctx, func(obj *unstructured.Unstructured) error {
		isPVC := obj.GetAPIVersion() == "v1" && obj.GetKind() == pvcKind

		var (
			desiredSource              pvcSourceIdentity
			desiredStorageClassName    string
			hasDesiredStorageClassName bool
			pvcGuard                   *pvcMutationGuard
		)

		if isPVC {
			var sourceErr error

			desiredSource, sourceErr = pvcSourceIdentityFromObject(obj)
			if sourceErr != nil {
				return fmt.Errorf(
					"read desired restore source for PVC %s/%s: %w",
					obj.GetNamespace(),
					obj.GetName(),
					sourceErr,
				)
			}

			desiredStorageClassName, hasDesiredStorageClassName, sourceErr = unstructured.NestedString(
				obj.Object,
				"spec",
				"storageClassName",
			)
			if sourceErr != nil {
				return fmt.Errorf(
					"read desired storageClassName for PVC %s/%s: %w",
					obj.GetNamespace(),
					obj.GetName(),
					sourceErr,
				)
			}

			if !cfg.DryRun {
				guard, found := pvcGuards[pvcMutationGuardKey(obj.GetNamespace(), obj.GetName())]
				if !found {
					return fmt.Errorf(
						"missing real-apply mutation guard for PVC %s/%s",
						obj.GetNamespace(),
						obj.GetName(),
					)
				}

				pvcGuard = &guard
			}
		}

		result, err := applyObject(ctx, cfg, obj, pvcGuard)
		if err != nil {
			return fmt.Errorf(
				"apply %s/%s %q: %w",
				obj.GetAPIVersion(),
				obj.GetKind(),
				obj.GetName(),
				err,
			)
		}

		if isPVC {
			if err := validateAppliedPVCIdentity(
				result,
				desiredSource,
				desiredStorageClassName,
				hasDesiredStorageClassName,
				pvcGuard,
				obj.GetName(),
				cfg.DryRun,
			); err != nil {
				return err
			}

			if !cfg.DryRun {
				ref, refErr := pvcRefFromApplied(
					ctx,
					cfg,
					result,
					desiredSource,
					desiredStorageClassName,
					hasDesiredStorageClassName,
					obj.GetName(),
				)
				if refErr != nil {
					return fmt.Errorf(
						"record post-apply identity for PVC %s/%s: %w",
						result.namespace,
						obj.GetName(),
						refErr,
					)
				}

				pvcs = append(pvcs, ref)
			}
		}

		applied++

		return nil
	})
	if err != nil {
		return nil, applied, err
	}

	return pvcs, applied, nil
}

func validateAppliedPVCIdentity(
	result applyResult,
	desiredSource pvcSourceIdentity,
	desiredStorageClassName string,
	hasDesiredStorageClassName bool,
	guard *pvcMutationGuard,
	expectedName string,
	dryRun bool,
) error {
	if result.object == nil {
		return fmt.Errorf("API server returned no object for PVC %s/%s", result.namespace, expectedName)
	}

	if result.object.GetName() != expectedName {
		return fmt.Errorf(
			"API server returned PVC name %q for requested PVC %s/%s",
			result.object.GetName(),
			result.namespace,
			expectedName,
		)
	}

	observedSource, err := pvcSourceIdentityFromObject(result.object)
	if err != nil {
		return fmt.Errorf(
			"read API server response restore source for PVC %s/%s: %w",
			result.namespace,
			expectedName,
			err,
		)
	}

	phase := "real apply"
	if dryRun {
		phase = "dry-run apply"
	}

	if !desiredSource.matchesObserved(observedSource) {
		return fmt.Errorf(
			"%s response for PVC %s/%s changed desired dataSource/dataSourceRef identity",
			phase,
			result.namespace,
			expectedName,
		)
	}

	observedStorageClassName, hasObservedStorageClassName, err := unstructured.NestedString(
		result.object.Object,
		"spec",
		"storageClassName",
	)
	if err != nil {
		return fmt.Errorf(
			"read %s response storageClassName for PVC %s/%s: %w",
			phase,
			result.namespace,
			expectedName,
			err,
		)
	}

	if hasObservedStorageClassName != hasDesiredStorageClassName ||
		observedStorageClassName != desiredStorageClassName {
		return fmt.Errorf(
			"%s response for PVC %s/%s changed desired storageClassName (expected present=%t value=%q, observed present=%t value=%q)",
			phase,
			result.namespace,
			expectedName,
			hasDesiredStorageClassName,
			desiredStorageClassName,
			hasObservedStorageClassName,
			observedStorageClassName,
		)
	}

	if guard != nil && guard.exists && result.object.GetUID() != guard.uid {
		return fmt.Errorf(
			"%s response for PVC %s/%s changed preflight UID (expected %q, observed %q)",
			phase,
			result.namespace,
			expectedName,
			guard.uid,
			result.object.GetUID(),
		)
	}

	return nil
}

func pvcRefFromApplied(
	ctx context.Context,
	cfg Config,
	result applyResult,
	desiredSource pvcSourceIdentity,
	desiredStorageClassName string,
	hasDesiredStorageClassName bool,
	expectedName string,
) (pvcRef, error) {
	if result.object == nil {
		return pvcRef{}, fmt.Errorf("API server returned no object")
	}

	uid := result.object.GetUID()
	if uid == "" {
		return pvcRef{}, fmt.Errorf("API server returned an empty metadata.uid")
	}

	ref := pvcRef{
		namespace:           result.namespace,
		name:                expectedName,
		uid:                 uid,
		storageClassName:    desiredStorageClassName,
		hasStorageClassName: hasDesiredStorageClassName,
		desiredSource:       desiredSource,
	}

	phase, _, err := unstructured.NestedString(result.object.Object, "status", "phase")
	if err != nil {
		return pvcRef{}, fmt.Errorf("read applied status.phase: %w", err)
	}

	if phase == pvcPhaseBound {
		boundPV, pvErr := getBoundPVIdentity(ctx, cfg, result.object, ref)
		if pvErr != nil {
			return pvcRef{}, pvErr
		}

		ref.boundPV = &boundPV
	}

	return ref, nil
}

func pvcSourceIdentityFromObject(obj *unstructured.Unstructured) (pvcSourceIdentity, error) {
	dataSourceRef, err := pvcDataSourceIdentityFromObject(obj, "dataSourceRef")
	if err != nil {
		return pvcSourceIdentity{}, fmt.Errorf("read spec.dataSourceRef: %w", err)
	}

	dataSource, err := pvcDataSourceIdentityFromObject(obj, "dataSource")
	if err != nil {
		return pvcSourceIdentity{}, fmt.Errorf("read spec.dataSource: %w", err)
	}

	identity := pvcSourceIdentity{
		dataSourceRef: dataSourceRef,
		dataSource:    dataSource,
	}

	if dataSourceRef != nil && dataSource != nil && *dataSourceRef != *dataSource {
		return pvcSourceIdentity{}, fmt.Errorf("spec.dataSourceRef and spec.dataSource identify different volume sources")
	}

	return identity, nil
}

func pvcDataSourceIdentityFromObject(
	obj *unstructured.Unstructured,
	field string,
) (*pvcDataSourceIdentity, error) {
	value, found, err := unstructured.NestedMap(obj.Object, "spec", field)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, nil
	}

	identity := &pvcDataSourceIdentity{}

	for fieldName, destination := range map[string]*string{
		"apiGroup":  &identity.apiGroup,
		"kind":      &identity.kind,
		"name":      &identity.name,
		"namespace": &identity.namespace,
	} {
		fieldValue, _, fieldErr := unstructured.NestedString(value, fieldName)
		if fieldErr != nil {
			return nil, fmt.Errorf("%s.%s: %w", field, fieldName, fieldErr)
		}

		*destination = fieldValue
	}

	if identity.kind == "" || identity.name == "" {
		return nil, fmt.Errorf("%s is malformed: kind and name are required", field)
	}

	return identity, nil
}

func (desired pvcSourceIdentity) matchesObserved(observed pvcSourceIdentity) bool {
	expected, hasExpected := desired.canonical()
	if !hasExpected {
		return observed.dataSourceRef == nil && observed.dataSource == nil
	}

	if desired.dataSourceRef != nil &&
		(observed.dataSourceRef == nil || *observed.dataSourceRef != *desired.dataSourceRef) {
		return false
	}

	if desired.dataSource != nil &&
		(observed.dataSource == nil || *observed.dataSource != *desired.dataSource) {
		return false
	}

	if observed.dataSourceRef != nil && *observed.dataSourceRef != expected {
		return false
	}

	if observed.dataSource != nil && *observed.dataSource != expected {
		return false
	}

	return true
}

func (desired pvcSourceIdentity) canonical() (pvcDataSourceIdentity, bool) {
	if desired.dataSourceRef != nil {
		return *desired.dataSourceRef, true
	}

	if desired.dataSource != nil {
		return *desired.dataSource, true
	}

	return pvcDataSourceIdentity{}, false
}

// applyObject applies a single object to the cluster. It uses Server-Side Apply
// except when an absent PVC must be created atomically to prevent same-name
// adoption. Namespaced objects without a namespace inherit the target namespace.
// It returns the effective namespace and API-server response.
func applyObject(
	ctx context.Context,
	cfg Config,
	obj *unstructured.Unstructured,
	pvcGuard *pvcMutationGuard,
) (applyResult, error) {
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
		return applyResult{}, err
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

	if pvcGuard != nil && pvcGuard.exists {
		obj.SetResourceVersion(pvcGuard.resourceVersion)
	}

	jsonBytes, err := json.Marshal(obj.Object)
	if err != nil {
		return applyResult{}, fmt.Errorf("marshal object for apply: %w", err)
	}

	force := true

	patchOpts := metav1.PatchOptions{
		FieldManager: fieldManager,
		Force:        &force,
	}

	if cfg.DryRun {
		patchOpts.DryRun = []string{metav1.DryRunAll}
	}

	var (
		applied  *unstructured.Unstructured
		applyErr error
	)

	if pvcGuard != nil && !pvcGuard.exists {
		createOpts := metav1.CreateOptions{FieldManager: fieldManager}
		applied, applyErr = controlPlaneRequest(
			ctx,
			cfg.controlPlaneTimeout(),
			fmt.Sprintf("creating absent PVC %s/%s", ns, obj.GetName()),
			func(requestCtx context.Context) (*unstructured.Unstructured, error) {
				return ri.Create(requestCtx, obj, createOpts)
			},
		)
	} else {
		applied, applyErr = controlPlaneRequest(
			ctx,
			cfg.controlPlaneTimeout(),
			fmt.Sprintf("%s %s/%s %q", phase, obj.GetAPIVersion(), obj.GetKind(), obj.GetName()),
			func(requestCtx context.Context) (*unstructured.Unstructured, error) {
				return ri.Patch(requestCtx, obj.GetName(), types.ApplyPatchType, jsonBytes, patchOpts)
			},
		)
	}

	if applyErr != nil {
		if pvcGuard != nil && kubeerrors.IsConflict(applyErr) {
			return applyResult{}, fmt.Errorf(
				"restore target PVC %s/%s changed after preflight; refusing stale mutation: %w",
				ns,
				obj.GetName(),
				applyErr,
			)
		}

		if pvcGuard != nil && !pvcGuard.exists && kubeerrors.IsAlreadyExists(applyErr) {
			return applyResult{}, fmt.Errorf(
				"restore target PVC %s/%s appeared after preflight; refusing to mutate or reuse it: %w",
				ns,
				obj.GetName(),
				applyErr,
			)
		}

		if kubeerrors.IsInvalid(applyErr) {
			return applyResult{}, classifyInvalidApplyError(ctx, cfg, ri, ns, obj, applyErr)
		}

		if kubeerrors.IsConflict(applyErr) {
			return applyResult{}, fmt.Errorf("Kubernetes API reported an apply conflict: %w", applyErr)
		}

		if kubeerrors.IsAlreadyExists(applyErr) {
			return applyResult{}, fmt.Errorf("Kubernetes API reported that the object already exists: %w", applyErr)
		}

		return applyResult{}, fmt.Errorf("apply: %w", applyErr)
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

	return applyResult{namespace: ns, object: applied}, nil
}

func classifyInvalidApplyError(
	ctx context.Context,
	cfg Config,
	resource dynamic.ResourceInterface,
	namespace string,
	obj *unstructured.Unstructured,
	invalidErr error,
) error {
	if invalidResponseHasImmutableCause(invalidErr) &&
		applyTargetExists(ctx, cfg, resource, namespace, obj.GetName()) {
		return fmt.Errorf(
			"%s/%s %s/%s already exists and the API server rejected an immutable field that differs from the snapshot; delete it and re-run restore: %w",
			obj.GetAPIVersion(),
			obj.GetKind(),
			namespace,
			obj.GetName(),
			invalidErr,
		)
	}

	return fmt.Errorf("Kubernetes API rejected the restore object as invalid: %w", invalidErr)
}

func invalidResponseHasImmutableCause(err error) bool {
	var statusErr kubeerrors.APIStatus
	if !errors.As(err, &statusErr) {
		return false
	}

	status := statusErr.Status()
	if status.Details == nil {
		return false
	}

	for _, cause := range status.Details.Causes {
		message := strings.ToLower(cause.Message)
		if strings.Contains(message, "immutable") || strings.Contains(message, "may not be changed") {
			return true
		}
	}

	return false
}

func applyTargetExists(
	ctx context.Context,
	cfg Config,
	resource dynamic.ResourceInterface,
	namespace string,
	name string,
) bool {
	_, err := controlPlaneRequest(
		ctx,
		cfg.controlPlaneTimeout(),
		fmt.Sprintf("checking whether invalid restore target %s/%s exists", namespace, name),
		func(requestCtx context.Context) (*unstructured.Unstructured, error) {
			return resource.Get(requestCtx, name, metav1.GetOptions{})
		},
	)

	return err == nil
}

// waitPVCsBound blocks until every restored PVC reports status.phase == Bound or the
// configured timeout elapses.
//
// Scope: only PVCs that appear in the applied manifest set are awaited. Disk-backed PVCs
// for domain objects are recreated asynchronously by the domain controller (they are not
// part of manifests-with-data-restoration output), so they are intentionally not tracked
// here; awaiting them would require knowledge of the domain controller's naming/labeling.
//
// A PVC whose effective StorageClass volumeBindingMode is WaitForFirstConsumer is inspected
// before polling: a Pending claim with no selected node, live consumer, or provisioning
// observation is a normal, non-blocking state. Once provisioning is active, its PVC and
// bounded Event history are both rechecked until Bound or a terminal result.
func waitPVCsBound(ctx context.Context, cfg Config, pvcs []pvcRef) error {
	if len(pvcs) == 0 {
		return nil
	}

	gvr := schema.GroupVersionResource{Version: "v1", Resource: pvcResource}
	scGVR := schema.GroupVersionResource{Group: storageClassGroup, Version: "v1", Resource: storageClassResource}

	newWaitContext := context.WithTimeout
	if cfg.newWaitContext != nil {
		newWaitContext = cfg.newWaitContext
	}

	waitCtx, cancel := newWaitContext(ctx, cfg.Timeout)
	defer cancel()

	cfg.Log.Info("waiting for restored PVCs to bind", slog.Int("count", len(pvcs)))

	bindingModes := make(map[string]string)

	type waitRef struct {
		pvc                 pvcRef
		recheckProvisioning bool
	}

	activeRefs := make([]waitRef, 0, len(pvcs))

	var (
		boundCount   int
		skippedCount int
	)

	for _, ref := range pvcs {
		mode, err := resolveVolumeBindingMode(waitCtx, cfg, scGVR, ref.storageClassName, bindingModes)
		if err != nil {
			return fmt.Errorf("resolve volume binding mode for PVC %s/%s: %w", ref.namespace, ref.name, err)
		}

		recheckProvisioning := mode == volumeBindingModeWFC
		if recheckProvisioning {
			active, bound, err := inspectWFFCPVC(waitCtx, cfg, gvr, ref)
			if err != nil {
				return err
			}

			if bound {
				boundCount++

				continue
			}

			if !active {
				skippedCount++

				continue
			}
		}

		activeRefs = append(activeRefs, waitRef{pvc: ref, recheckProvisioning: recheckProvisioning})
	}

	for len(activeRefs) > 0 {
		unresolvedRefs := make([]waitRef, 0, len(activeRefs))
		unresolvedPhases := make([]string, 0, len(activeRefs))

		for _, activeRef := range activeRefs {
			bound, phase, err := observePVCBound(
				waitCtx,
				cfg,
				gvr,
				activeRef.pvc,
				activeRef.recheckProvisioning,
			)
			if err != nil {
				return err
			}

			if bound {
				boundCount++

				continue
			}

			unresolvedRefs = append(unresolvedRefs, activeRef)
			unresolvedPhases = append(unresolvedPhases, phase)
		}

		if len(unresolvedRefs) == 0 {
			break
		}

		activeRefs = unresolvedRefs

		if !sleepCtx(waitCtx, cfg.PollInterval) {
			firstRef := unresolvedRefs[0].pvc

			return waitContextError(
				waitCtx,
				fmt.Sprintf(
					"waiting for PVC %s/%s to become Bound; observed phase %q",
					firstRef.namespace,
					firstRef.name,
					unresolvedPhases[0],
				),
			)
		}
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

// inspectWFFCPVC returns dormant Pending claims without starting the shared wait
// deadline. Selected, consumed, or actively provisioning claims must be polled.
func inspectWFFCPVC(
	ctx context.Context,
	cfg Config,
	gvr schema.GroupVersionResource,
	ref pvcRef,
) (bool, bool, error) {
	pvc, phase, err := getPVCWaitState(ctx, cfg, gvr, ref)
	if err != nil {
		return false, false, err
	}

	if phase == pvcPhaseBound {
		cfg.Log.Info("PVC bound",
			slog.String("namespace", ref.namespace),
			slog.String("name", ref.name),
			slog.String("phase", phase))

		return false, true, nil
	}

	selected := pvc.GetAnnotations()[selectedNodeAnnotation] != ""

	consumed, err := hasLivePVCConsumer(ctx, cfg, ref)
	if err != nil {
		return false, false, err
	}

	eventState, err := currentPVCProvisioningEvent(ctx, cfg, ref)
	if err != nil {
		return false, false, err
	}

	if eventState.terminal {
		return false, false, fmt.Errorf(
			"restored PVC %s/%s has terminal provisioning event %q: %s",
			ref.namespace,
			ref.name,
			eventState.reason,
			eventState.message,
		)
	}

	if selected || consumed || eventState.active {
		return true, false, nil
	}

	cfg.Log.Info("PVC is WaitForFirstConsumer and Pending with no consumer yet; not waiting for Bound",
		slog.String("namespace", ref.namespace),
		slog.String("name", ref.name),
		slog.String("phase", phase))

	return false, false, nil
}

// observePVCBound performs one bounded observation in a shared wait round. Active WFFC
// claims also recheck bounded Event history before the scheduler advances to the next
// claim, so manifest order cannot starve later terminal failures.
func observePVCBound(
	ctx context.Context,
	cfg Config,
	gvr schema.GroupVersionResource,
	ref pvcRef,
	recheckProvisioning bool,
) (bool, string, error) {
	phase, err := getPVCWaitPhase(ctx, cfg, gvr, ref)
	if err != nil {
		return false, "", err
	}

	if phase == pvcPhaseBound {
		cfg.Log.Info("PVC bound",
			slog.String("namespace", ref.namespace),
			slog.String("name", ref.name),
			slog.String("phase", phase))

		return true, phase, nil
	}

	if !recheckProvisioning {
		return false, phase, nil
	}

	eventState, err := currentPVCProvisioningEvent(ctx, cfg, ref)
	if err != nil {
		return false, "", err
	}

	if eventState.terminal {
		return false, "", fmt.Errorf(
			"restored PVC %s/%s has terminal provisioning event %q: %s",
			ref.namespace,
			ref.name,
			eventState.reason,
			eventState.message,
		)
	}

	return false, phase, nil
}

func getPVCWaitPhase(ctx context.Context, cfg Config, gvr schema.GroupVersionResource, ref pvcRef) (string, error) {
	_, phase, err := getPVCWaitState(ctx, cfg, gvr, ref)

	return phase, err
}

func getPVCWaitState(
	ctx context.Context,
	cfg Config,
	gvr schema.GroupVersionResource,
	ref pvcRef,
) (*unstructured.Unstructured, string, error) {
	pvc, err := controlPlaneRequest(
		ctx,
		cfg.controlPlaneTimeout(),
		fmt.Sprintf("getting restored PVC %s/%s", ref.namespace, ref.name),
		func(requestCtx context.Context) (*unstructured.Unstructured, error) {
			return cfg.Dynamic.Resource(gvr).Namespace(ref.namespace).Get(requestCtx, ref.name, metav1.GetOptions{})
		},
	)
	if kubeerrors.IsNotFound(err) {
		return nil, "", fmt.Errorf("restored PVC %s/%s was not found after apply: %w", ref.namespace, ref.name, err)
	}

	if err != nil {
		if ctxErr := waitContextError(ctx, fmt.Sprintf("getting restored PVC %s/%s", ref.namespace, ref.name)); ctxErr != nil {
			err = errors.Join(err, ctxErr)
		}

		return nil, "", fmt.Errorf("get restored PVC %s/%s: %w", ref.namespace, ref.name, err)
	}

	phase, found, err := unstructured.NestedString(pvc.Object, "status", "phase")
	if err != nil {
		return nil, "", fmt.Errorf("read status.phase of restored PVC %s/%s: %w", ref.namespace, ref.name, err)
	}

	observedPhase := phase
	if !found || phase == "" {
		observedPhase = "<missing>"
	}

	if deletionTimestamp := pvc.GetDeletionTimestamp(); deletionTimestamp != nil {
		return nil, "", fmt.Errorf(
			"restored PVC %s/%s is terminating with deletionTimestamp %s; observed phase %q",
			ref.namespace,
			ref.name,
			deletionTimestamp.UTC().Format(time.RFC3339),
			observedPhase,
		)
	}

	if err := validateCurrentPVCIdentity(pvc, ref, observedPhase); err != nil {
		return nil, "", err
	}

	switch phase {
	case pvcPhaseBound:
		if err := validateBoundPV(ctx, cfg, pvc, ref); err != nil {
			return nil, "", err
		}

		return pvc, phase, nil
	case pvcPhasePending:
		return pvc, phase, nil
	case pvcPhaseLost:
		return nil, "", fmt.Errorf("restored PVC %s/%s is in terminal phase %q", ref.namespace, ref.name, phase)
	case "":
		return nil, "", fmt.Errorf("restored PVC %s/%s has missing status.phase", ref.namespace, ref.name)
	default:
		return nil, "", fmt.Errorf("restored PVC %s/%s has unrecognized phase %q", ref.namespace, ref.name, phase)
	}
}

func revalidatePVCsAfterApply(ctx context.Context, cfg Config, pvcs []pvcRef) error {
	gvr := schema.GroupVersionResource{Version: "v1", Resource: pvcResource}

	for _, ref := range pvcs {
		pvc, err := controlPlaneRequest(
			ctx,
			cfg.controlPlaneTimeout(),
			fmt.Sprintf("revalidating restored PVC %s/%s", ref.namespace, ref.name),
			func(requestCtx context.Context) (*unstructured.Unstructured, error) {
				return cfg.Dynamic.Resource(gvr).Namespace(ref.namespace).Get(
					requestCtx,
					ref.name,
					metav1.GetOptions{},
				)
			},
		)
		if err != nil {
			return fmt.Errorf("revalidate PVC %s/%s after apply: %w", ref.namespace, ref.name, err)
		}

		phase, _, phaseErr := unstructured.NestedString(pvc.Object, "status", "phase")
		if phaseErr != nil {
			return fmt.Errorf("read status.phase while revalidating PVC %s/%s: %w", ref.namespace, ref.name, phaseErr)
		}

		observedPhase := phase
		if observedPhase == "" {
			observedPhase = "<missing>"
		}

		if err := validateCurrentPVCIdentity(pvc, ref, observedPhase); err != nil {
			return fmt.Errorf("revalidate PVC after apply: %w", err)
		}

		if phase == pvcPhaseBound {
			if err := validateBoundPV(ctx, cfg, pvc, ref); err != nil {
				return fmt.Errorf("revalidate PVC after apply: %w", err)
			}
		}
	}

	return nil
}

func validateCurrentPVCIdentity(
	pvc *unstructured.Unstructured,
	ref pvcRef,
	observedPhase string,
) error {
	if pvc.GetUID() != ref.uid {
		return fmt.Errorf(
			"restored PVC %s/%s changed UID after apply (expected %q, observed %q); observed phase %q",
			ref.namespace,
			ref.name,
			ref.uid,
			pvc.GetUID(),
			observedPhase,
		)
	}

	observedSource, err := pvcSourceIdentityFromObject(pvc)
	if err != nil {
		return fmt.Errorf(
			"read current restore source of PVC %s/%s; observed phase %q: %w",
			ref.namespace,
			ref.name,
			observedPhase,
			err,
		)
	}

	if !ref.desiredSource.matchesObserved(observedSource) {
		return fmt.Errorf(
			"restored PVC %s/%s dataSource/dataSourceRef identity changed after apply; observed phase %q",
			ref.namespace,
			ref.name,
			observedPhase,
		)
	}

	observedStorageClassName, hasObservedStorageClassName, err := unstructured.NestedString(
		pvc.Object,
		"spec",
		"storageClassName",
	)
	if err != nil {
		return fmt.Errorf(
			"read current storageClassName of restored PVC %s/%s; observed phase %q: %w",
			ref.namespace,
			ref.name,
			observedPhase,
			err,
		)
	}

	if hasObservedStorageClassName != ref.hasStorageClassName ||
		observedStorageClassName != ref.storageClassName {
		return fmt.Errorf(
			"restored PVC %s/%s storageClassName changed after apply; observed phase %q",
			ref.namespace,
			ref.name,
			observedPhase,
		)
	}

	return nil
}

func validateBoundPV(
	ctx context.Context,
	cfg Config,
	pvc *unstructured.Unstructured,
	ref pvcRef,
) error {
	current, err := getBoundPVIdentity(ctx, cfg, pvc, ref)
	if err != nil {
		return err
	}

	if ref.boundPV != nil && current != *ref.boundPV {
		return fmt.Errorf(
			"restored PVC %s/%s bound PersistentVolume identity changed after apply (expected %s UID %q, observed %s UID %q)",
			ref.namespace,
			ref.name,
			ref.boundPV.name,
			ref.boundPV.uid,
			current.name,
			current.uid,
		)
	}

	return nil
}

func getBoundPVIdentity(
	ctx context.Context,
	cfg Config,
	pvc *unstructured.Unstructured,
	ref pvcRef,
) (boundPVIdentity, error) {
	volumeName, _, err := unstructured.NestedString(pvc.Object, "spec", "volumeName")
	if err != nil {
		return boundPVIdentity{}, fmt.Errorf(
			"read spec.volumeName of Bound PVC %s/%s: %w",
			ref.namespace,
			ref.name,
			err,
		)
	}

	if volumeName == "" {
		return boundPVIdentity{}, fmt.Errorf(
			"restored PVC %s/%s is Bound but has no spec.volumeName",
			ref.namespace,
			ref.name,
		)
	}

	gvr := schema.GroupVersionResource{Version: "v1", Resource: pvResource}

	pv, err := controlPlaneRequest(
		ctx,
		cfg.controlPlaneTimeout(),
		fmt.Sprintf("getting bound PersistentVolume %s for PVC %s/%s", volumeName, ref.namespace, ref.name),
		func(requestCtx context.Context) (*unstructured.Unstructured, error) {
			return cfg.Dynamic.Resource(gvr).Get(requestCtx, volumeName, metav1.GetOptions{})
		},
	)
	if err != nil {
		return boundPVIdentity{}, fmt.Errorf(
			"get bound PersistentVolume %s for restored PVC %s/%s: %w",
			volumeName,
			ref.namespace,
			ref.name,
			err,
		)
	}

	if pv.GetUID() == "" {
		return boundPVIdentity{}, fmt.Errorf("bound PersistentVolume %s has empty metadata.uid", volumeName)
	}

	claimNamespace, _, namespaceErr := unstructured.NestedString(pv.Object, "spec", "claimRef", "namespace")
	claimName, _, nameErr := unstructured.NestedString(pv.Object, "spec", "claimRef", "name")

	claimUID, _, uidErr := unstructured.NestedString(pv.Object, "spec", "claimRef", "uid")
	if err := errors.Join(namespaceErr, nameErr, uidErr); err != nil {
		return boundPVIdentity{}, fmt.Errorf("read claimRef of bound PersistentVolume %s: %w", volumeName, err)
	}

	if claimNamespace != ref.namespace || claimName != ref.name || types.UID(claimUID) != ref.uid {
		return boundPVIdentity{}, fmt.Errorf(
			"bound PersistentVolume %s claimRef does not identify restored PVC %s/%s UID %q (observed %s/%s UID %q)",
			volumeName,
			ref.namespace,
			ref.name,
			ref.uid,
			claimNamespace,
			claimName,
			claimUID,
		)
	}

	return boundPVIdentity{name: volumeName, uid: pv.GetUID()}, nil
}

type pvcProvisioningEvent struct {
	active   bool
	terminal bool
	name     string
	uid      types.UID
	reason   string
	message  string
	stamp    time.Time
}

func currentPVCProvisioningEvent(
	ctx context.Context,
	cfg Config,
	ref pvcRef,
) (pvcProvisioningEvent, error) {
	gvr := schema.GroupVersionResource{Version: "v1", Resource: eventResource}
	selector := fields.AndSelectors(
		fields.OneTermEqualSelector("involvedObject.kind", pvcKind),
		fields.OneTermEqualSelector("involvedObject.namespace", ref.namespace),
		fields.OneTermEqualSelector("involvedObject.name", ref.name),
		fields.OneTermEqualSelector("involvedObject.uid", string(ref.uid)),
	).String()

	var latest pvcProvisioningEvent

	err := scanResourcePages(ctx, cfg, gvr, ref.namespace, selector, func(obj *unstructured.Unstructured) error {
		if !eventInvolvesPVC(obj, ref) {
			return nil
		}

		reason, _, _ := unstructured.NestedString(obj.Object, "reason")

		candidate := pvcProvisioningEvent{
			name:   obj.GetName(),
			uid:    obj.GetUID(),
			reason: reason,
			stamp:  eventTimestamp(obj),
		}

		switch reason {
		case eventReasonProvisioning, eventReasonExternalProvisioning, eventReasonProvisioningSucceeded:
			candidate.active = true
		case eventReasonProvisioningFailed, eventReasonFailedBinding:
			candidate.terminal = true
		default:
			return nil
		}

		candidate.message, _, _ = unstructured.NestedString(obj.Object, "message")
		if provisioningEventLater(candidate, latest) {
			latest = candidate
		}

		return nil
	})
	if err != nil {
		return pvcProvisioningEvent{}, fmt.Errorf(
			"inspect provisioning events for restored PVC %s/%s: %w",
			ref.namespace,
			ref.name,
			err,
		)
	}

	return latest, nil
}

func eventInvolvesPVC(event *unstructured.Unstructured, ref pvcRef) bool {
	kind, _, _ := unstructured.NestedString(event.Object, "involvedObject", "kind")
	namespace, _, _ := unstructured.NestedString(event.Object, "involvedObject", "namespace")
	name, _, _ := unstructured.NestedString(event.Object, "involvedObject", "name")
	uid, _, _ := unstructured.NestedString(event.Object, "involvedObject", "uid")

	return kind == pvcKind &&
		namespace == ref.namespace &&
		name == ref.name &&
		types.UID(uid) == ref.uid
}

func provisioningEventLater(candidate, latest pvcProvisioningEvent) bool {
	if latest.reason == "" || candidate.stamp.After(latest.stamp) {
		return true
	}

	if candidate.stamp.Before(latest.stamp) {
		return false
	}

	candidatePriority := provisioningEventPriority(candidate)

	latestPriority := provisioningEventPriority(latest)
	if candidatePriority != latestPriority {
		return candidatePriority > latestPriority
	}

	if candidate.name != latest.name {
		return candidate.name > latest.name
	}

	if candidate.uid != latest.uid {
		return candidate.uid > latest.uid
	}

	if candidate.reason != latest.reason {
		return candidate.reason > latest.reason
	}

	return candidate.message > latest.message
}

func provisioningEventPriority(event pvcProvisioningEvent) int {
	switch {
	case event.terminal:
		return 3
	case event.reason == eventReasonProvisioningSucceeded:
		return 2
	case event.active:
		return 1
	default:
		return 0
	}
}

func eventTimestamp(event *unstructured.Unstructured) time.Time {
	for _, path := range [][]string{
		{"series", "lastObservedTime"},
		{"eventTime"},
		{"lastTimestamp"},
		{"firstTimestamp"},
	} {
		value, _, _ := unstructured.NestedString(event.Object, path...)
		if value == "" {
			continue
		}

		stamp, err := time.Parse(time.RFC3339Nano, value)
		if err == nil {
			return stamp
		}
	}

	return event.GetCreationTimestamp().Time
}

func hasLivePVCConsumer(ctx context.Context, cfg Config, ref pvcRef) (bool, error) {
	gvr := schema.GroupVersionResource{Version: "v1", Resource: podResource}
	consumed := false

	err := scanResourcePages(ctx, cfg, gvr, ref.namespace, "", func(obj *unstructured.Unstructured) error {
		if podConsumesPVC(obj, ref.name) {
			consumed = true
		}

		return nil
	})
	if err != nil {
		return false, fmt.Errorf(
			"inspect live Pod consumers of restored PVC %s/%s: %w",
			ref.namespace,
			ref.name,
			err,
		)
	}

	return consumed, nil
}

func podConsumesPVC(pod *unstructured.Unstructured, claimName string) bool {
	if pod.GetDeletionTimestamp() != nil {
		return false
	}

	phase, _, _ := unstructured.NestedString(pod.Object, "status", "phase")
	if phase == "Succeeded" || phase == "Failed" {
		return false
	}

	volumes, _, _ := unstructured.NestedSlice(pod.Object, "spec", "volumes")
	for _, rawVolume := range volumes {
		volume, ok := rawVolume.(map[string]interface{})
		if !ok {
			continue
		}

		referencedClaim, _, _ := unstructured.NestedString(volume, "persistentVolumeClaim", "claimName")
		if referencedClaim == claimName {
			return true
		}

		volumeName, _, _ := unstructured.NestedString(volume, "name")
		if _, found, _ := unstructured.NestedMap(volume, "ephemeral"); found &&
			pod.GetName()+"-"+volumeName == claimName {
			return true
		}
	}

	return false
}

func scanResourcePages(
	ctx context.Context,
	cfg Config,
	gvr schema.GroupVersionResource,
	namespace string,
	fieldSelector string,
	visit func(*unstructured.Unstructured) error,
) error {
	continueToken := ""
	seenTokens := make(map[string]struct{})
	objects := 0

	for pageNumber := 0; pageNumber < waitScanMaxPages; pageNumber++ {
		options := metav1.ListOptions{
			FieldSelector: fieldSelector,
			Limit:         waitScanPageLimit,
			Continue:      continueToken,
		}

		var resource dynamic.ResourceInterface = cfg.Dynamic.Resource(gvr)
		if namespace != "" {
			resource = cfg.Dynamic.Resource(gvr).Namespace(namespace)
		}

		page, err := controlPlaneRequest(
			ctx,
			cfg.controlPlaneTimeout(),
			fmt.Sprintf("listing %s in namespace %q", gvr.Resource, namespace),
			func(requestCtx context.Context) (*unstructured.UnstructuredList, error) {
				return resource.List(requestCtx, options)
			},
		)
		if err != nil {
			return err
		}

		if page == nil {
			return fmt.Errorf("list %s returned no page", gvr.Resource)
		}

		for i := range page.Items {
			objects++
			if objects > waitScanMaxObjects {
				return fmt.Errorf(
					"list %s exceeds object budget of %d",
					gvr.Resource,
					waitScanMaxObjects,
				)
			}

			if err := visit(&page.Items[i]); err != nil {
				return err
			}
		}

		next := page.GetContinue()
		if next == "" {
			return nil
		}

		if _, seen := seenTokens[next]; seen {
			return fmt.Errorf("list %s repeated continue token %q", gvr.Resource, next)
		}

		seenTokens[next] = struct{}{}
		continueToken = next
	}

	return fmt.Errorf("list %s exceeds page budget of %d", gvr.Resource, waitScanMaxPages)
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

	if mapping == nil {
		return schema.GroupVersionResource{}, false, fmt.Errorf("resolve resource for %s: REST mapping is nil", gvk.String())
	}

	if mapping.Scope == nil {
		return schema.GroupVersionResource{}, false, fmt.Errorf("resolve resource for %s: REST mapping has no scope", gvk.String())
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
