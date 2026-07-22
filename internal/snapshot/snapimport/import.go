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
	"log/slog"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/progress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
)

const (
	defaultTimeout      = 20 * time.Minute
	defaultPollInterval = 3 * time.Second
	defaultWorkers      = 5
)

// ManifestUploader posts a node's manifests-and-children-refs-upload payload. It is
// satisfied by *aggapi.Client and stubbed in tests.
type ManifestUploader interface {
	UploadManifests(ctx context.Context, ref aggapi.NodeRef, body []byte) ([]byte, error)
}

// uploadPayload is the manifests-and-children-refs-upload request body.
type uploadPayload struct {
	Manifests json.RawMessage `json:"manifests"`
	ChildRefs []ChildRef      `json:"childRefs"`
}

// Config holds all parameters for one import run.
type Config struct {
	// Namespace is the target namespace the snapshot tree is reconstructed into.
	Namespace string
	// InputDir is the root archive directory produced by `d8 snapshot download`.
	InputDir string
	// TTL is the DataImport TTL used for data-leaf imports.
	TTL string
	// Timeout bounds the per-node readiness/completion waits.
	Timeout time.Duration
	// PollInterval is the readiness polling cadence.
	PollInterval time.Duration

	// SelectedNodeKind restricts the import to a single node subtree when non-empty.
	// After BuildPlan the plan is filtered to the selected node and its descendants.
	// The selected node becomes the import root for waitRootReady; it must be a core
	// Snapshot or a CSI VolumeSnapshot data leaf (domain aggregators are rejected).
	SelectedNodeKind string
	// SelectedNodeName is the name of the selected node. Required when SelectedNodeKind is set.
	SelectedNodeName string

	// Uploader posts manifests-and-children-refs-upload (aggregated API).
	Uploader ManifestUploader
	// Volumes imports data-leaf volume bytes (DataImport + HTTP upload).
	Volumes VolumeImporter
	// Dynamic creates import-mode CRs and reads readiness status.
	Dynamic dynamic.Interface
	// Workers is the maximum number of data-leaf volume uploads to run concurrently in
	// pass 2b. Defaults to 5 when zero. Block-volume uploads stream-decode directly into
	// the PUT (see snapimport.putBlock), so raising Workers no longer multiplies temporary
	// disk usage — only per-worker in-memory codec buffers.
	Workers int
	// AllowExisting, when true, downgrades the namespace preflight conflict check to a
	// warning instead of an error. Import-mode markers from a prior run of this import
	// are never treated as conflicts regardless of this flag. When false (default), the
	// run aborts before any cluster mutation if conflicting non-import-mode objects exist.
	AllowExisting bool
	// Mapper resolves node GVKs to resources.
	Mapper meta.RESTMapper
	// Log receives progress output.
	Log *slog.Logger
	// Progress, when non-nil, receives per-stream byte increments for each data-leaf
	// volume upload. Each leaf gets its own Stream; nil disables progress reporting
	// and leaves upload behaviour unchanged.
	Progress progress.Sink
}

// Run imports a local snapshot archive into the target namespace. It plans the tree
// bottom-up, then:
//  1. creates every import-mode CR TOP-DOWN (parents first) so each child carries a
//     child->parent ownerRef stamped with the parent's server-assigned UID (the API
//     server requires a non-empty uid on ownerReferences, and the state-snapshotter
//     import binders resolve a leaf's parent SnapshotContent through that ownerRef);
//  2. waits for the bind-first contract: the manifests upload is refused with 409
//     ImportContentNotBound until a node's status.boundSnapshotContentName is set, so Run
//     blocks (waitForBinds) until EVERY planned node reports a bound SnapshotContent before
//     uploading anything. The binder creates and binds contents top-down from the markers of
//     pass 1, independent of the upload, so the CLI can wait for all nodes collectively;
//  3. uploads EVERY node's manifests plus its direct child refs (pass 2a) BEFORE importing
//     any volume bytes (pass 2b). The two are sequenced, not interleaved, because a data
//     leaf's SVDM DataImport stays Pending ("awaiting target leaf bound SnapshotContent")
//     until the leaf VolumeSnapshot is bound, which needs the parent SnapshotContent, which
//     needs the parent's manifests upload — so finishing a leaf (including waiting on its
//     DataImport) before its ancestors' manifests are up would deadlock until timeout;
//  4. pass 2b runs data-leaf uploads with bounded concurrency (cfg.Workers goroutines via
//     errgroup.SetLimit); the first leaf error cancels all in-flight siblings via the
//     derived ctx;
//
// finally waiting for the root Snapshot and its bound SnapshotContent to become Ready.
func Run(ctx context.Context, cfg Config) error {
	cfg = applyDefaults(cfg)

	if err := validate(cfg); err != nil {
		return err
	}

	plan, err := BuildPlan(cfg.InputDir)
	if err != nil {
		return fmt.Errorf("build import plan: %w", err)
	}

	if len(plan) == 0 {
		return fmt.Errorf("archive %q contains no snapshot nodes", cfg.InputDir)
	}

	if cfg.SelectedNodeKind != "" {
		plan, err = filterPlanToSubtree(plan, cfg.SelectedNodeKind, cfg.SelectedNodeName)
		if err != nil {
			return fmt.Errorf("filter archive to selected node %s/%s: %w", cfg.SelectedNodeKind, cfg.SelectedNodeName, err)
		}
	}

	root := plan[len(plan)-1]

	if cfg.SelectedNodeKind == "" {
		// Full import: the archive root must be a core Snapshot.
		if !root.isStructural() {
			return fmt.Errorf("archive root %s/%s is not a core Snapshot; only core Snapshot trees can be imported", root.Kind, root.Name)
		}
	} else {
		// Subtree import: the selected root must be independently importable — a core
		// Snapshot, a CSI VolumeSnapshot data leaf, or a domain data leaf. A domain
		// aggregator or a manifest-only domain node is importable too, but only as a non-root
		// (it needs a parent SnapshotContent to attach to), so neither can be selected as a
		// standalone --node root; both are reconstructed as part of a full-archive import.
		switch {
		case root.isDomainAggregator():
			return fmt.Errorf(
				"selected node %s/%s is a domain aggregator and cannot be selected as a standalone --node root: "+
					"it has no parent SnapshotContent to attach to. Import the full archive (omit --node) to "+
					"reconstruct it as part of its parent tree, or select a supported leaf with "+
					"--node <Kind>/<name> (e.g. a CSI VolumeSnapshot or domain data leaf)",
				root.Kind, root.Name)
		case !root.canBeImportRoot():
			return fmt.Errorf(
				"selected node %s/%s is a manifest-only domain node and cannot be imported as a standalone root: "+
					"it carries no own volume data and is materialised only as part of its parent tree; "+
					"import the full archive (omit --node) or select its ancestor Snapshot",
				root.Kind, root.Name)
		}
	}

	if err := preflight(plan); err != nil {
		return err
	}

	if err := preflightNamespace(ctx, cfg, plan); err != nil {
		return err
	}

	cfg.Log.Info("importing snapshot archive",
		slog.String("namespace", cfg.Namespace),
		slog.String("input", cfg.InputDir),
		slog.String("root", root.Name),
		slog.Int("nodes", len(plan)))

	parents := buildParentIndex(plan)

	// Pass 1 (top-down): create import-mode markers so child->parent ownerRefs can carry
	// the parent UID.
	if err := cfg.createMarkers(ctx, plan, parents); err != nil {
		return err
	}

	// Bind gate (bind-first contract): wait until every planned node reports a bound
	// SnapshotContent before uploading. The namespaced upload subresource returns 409
	// ImportContentNotBound until status.boundSnapshotContentName is set, so gating the whole
	// pass here — rather than discovering the 409 mid-upload — turns a distributed race into a
	// single deterministic wait. The aggregated client still retries a stray ImportContentNotBound
	// per request as a safety net (see aggapi.UploadManifests).
	if err := waitForBinds(ctx, cfg, plan); err != nil {
		return err
	}

	// Pass 2a: upload every node's manifests + child refs. By this point the bind gate above has
	// ensured every node (including every data-leaf VolumeSnapshot) has a bound SnapshotContent,
	// the precondition for the bind-first upload to be accepted and for a leaf's DataImport to
	// leave Pending. Bottom-up keeps a parent's upload after its children's, matching capture order.
	for i := range plan {
		if err := uploadNodeManifests(ctx, cfg, plan[i]); err != nil {
			return err
		}
	}

	// Pass 2b: import each data leaf's volume bytes with bounded concurrency. The DataImport
	// is created immediately before the upload so its idle TTL window stays minimal.
	// The first leaf error cancels all in-flight siblings via gctx.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(cfg.Workers)

	for i := range plan {
		node := plan[i]

		g.Go(func() error {
			return importNodeData(gctx, cfg, node)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return waitRootReady(ctx, cfg, root)
}

// preflight rejects, before any cluster mutation, archives the CLI cannot client-drive.
// A CSI VolumeSnapshot data leaf missing its volume data file (data.bin or data.tar) is the
// only such case: every node kind — including domain aggregators — is importable as part of
// a tree (aggregators are reconstructed server-side by the genericbinder from their uploaded
// manifests + child refs). The standalone --node root restriction is enforced separately in
// Run, not here.
func preflight(plan []PlannedNode) error {
	for _, node := range plan {
		if node.isVolumeSnapshotLeaf() && !node.HasBlockData() && !node.FilesystemData {
			return fmt.Errorf("data leaf %s/%s has no volume data file (data.bin or data.tar) in the archive", node.Kind, node.Name)
		}
	}

	return nil
}

// preflightNamespace checks, before any cluster mutation, that the target namespace
// contains no foreign (non-import-mode) objects sharing a name with a planned node.
// Pre-existing import-mode markers from a prior run of this import are never conflicts.
// Conflicts are aggregated into a single actionable error. With cfg.AllowExisting true,
// conflicts are downgraded to a warning; reconcileExistingMarker protection is unaffected.
func preflightNamespace(ctx context.Context, cfg Config, plan []PlannedNode) error {
	var conflicts []string

	for _, node := range plan {
		mapping, mapErr := cfg.mappingForNode(node)
		if mapErr != nil {
			return mapErr
		}

		obj, getErr := cfg.resourceInterface(mapping).Get(ctx, node.Name, metav1.GetOptions{})
		if kubeerrors.IsNotFound(getErr) {
			continue
		}

		if getErr != nil {
			return fmt.Errorf("checking namespace for %s/%s: %w", node.Kind, node.Name, getErr)
		}

		imp, markerErr := isImportModeMarker(obj)
		if markerErr != nil {
			return markerErr
		}

		if !imp {
			conflicts = append(conflicts, fmt.Sprintf("%s %s", node.Kind, node.Name))
		}
	}

	if len(conflicts) == 0 {
		return nil
	}

	if cfg.AllowExisting {
		cfg.Log.Warn("namespace preflight: conflicting objects in target namespace; proceeding due to --allow-existing",
			slog.String("namespace", cfg.Namespace),
			slog.String("conflicts", strings.Join(conflicts, ", ")))

		return nil
	}

	return fmt.Errorf("target namespace %q contains %d object(s) not in import mode; "+
		"import into a fresh namespace or use --allow-existing to skip this check: %s",
		cfg.Namespace, len(conflicts), strings.Join(conflicts, ", "))
}

// resourceInterface returns the dynamic resource interface scoped for the given REST
// mapping: namespace-scoped resources are narrowed to cfg.Namespace; cluster-scoped
// resources use the namespaceable interface directly (which implements ResourceInterface).
func (cfg Config) resourceInterface(mapping *meta.RESTMapping) dynamic.ResourceInterface {
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		return cfg.Dynamic.Resource(mapping.Resource).Namespace(cfg.Namespace)
	}

	return cfg.Dynamic.Resource(mapping.Resource)
}

// mappingForNode resolves the REST mapping (GVR + scope) for a planned node's GVK via
// cfg.Mapper. It is the single GVK->resource resolver shared by the namespace preflight and
// the bind gate.
func (cfg Config) mappingForNode(node PlannedNode) (*meta.RESTMapping, error) {
	gv, err := schema.ParseGroupVersion(node.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("parse apiVersion %q for %s/%s: %w", node.APIVersion, node.Kind, node.Name, err)
	}

	gvk := gv.WithKind(node.Kind)

	mapping, err := cfg.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, fmt.Errorf("resolve resource for %s: %w", gvk.String(), err)
	}

	return mapping, nil
}

// boundContentName returns status.boundSnapshotContentName, or "" when unset. A non-empty
// value is the signal the state-snapshotter binder has bound the node's SnapshotContent —
// the precondition the bind-first upload contract enforces.
func boundContentName(obj *unstructured.Unstructured) string {
	name, _, _ := unstructured.NestedString(obj.Object, "status", "boundSnapshotContentName")

	return name
}

// waitForBinds blocks until every planned node's namespaced CR reports a non-empty
// status.boundSnapshotContentName. This is the PRIMARY bind gate for the bind-first upload
// contract: the namespaced manifests upload subresource is refused with 409 ImportContentNotBound
// until a node is bound. The state-snapshotter binder creates and binds SnapshotContents top-down
// from the import-mode markers created in pass 1, independent of the manifest upload (no deadlock),
// so Run waits for all nodes collectively before pass 2a rather than racing the 409 per upload.
//
// The poll cadence is cfg.PollInterval and the whole wait is bounded by cfg.Timeout (derived from
// --timeout). A node whose Get fails (other than not-yet-bound) aborts the wait immediately; ctx
// cancellation returns ctx.Err(). The aggregated client additionally retries a stray
// ImportContentNotBound 409 per upload as a safety net (aggapi.UploadManifests).
func waitForBinds(ctx context.Context, cfg Config, plan []PlannedNode) error {
	type pendingNode struct {
		node PlannedNode
		gvr  schema.GroupVersionResource
	}

	pending := make([]pendingNode, 0, len(plan))

	for i := range plan {
		mapping, err := cfg.mappingForNode(plan[i])
		if err != nil {
			return err
		}

		pending = append(pending, pendingNode{node: plan[i], gvr: mapping.Resource})
	}

	cfg.Log.Info("waiting for import nodes to bind their SnapshotContents", slog.Int("nodes", len(pending)))

	deadline := time.Now().Add(cfg.Timeout)

	for {
		var stillPending []pendingNode

		for _, p := range pending {
			obj, err := cfg.Dynamic.Resource(p.gvr).Namespace(cfg.Namespace).Get(ctx, p.node.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("get %s %s/%s while waiting for bind: %w", p.node.Kind, cfg.Namespace, p.node.Name, err)
			}

			if boundContentName(obj) == "" {
				stillPending = append(stillPending, p)
			}
		}

		if len(stillPending) == 0 {
			cfg.Log.Info("all import nodes bound their SnapshotContents")

			return nil
		}

		if time.Now().After(deadline) {
			names := make([]string, 0, len(stillPending))
			for _, p := range stillPending {
				names = append(names, p.node.Kind+"/"+p.node.Name)
			}

			return fmt.Errorf("timeout waiting for %d import node(s) to bind a SnapshotContent "+
				"(status.boundSnapshotContentName still empty): %s", len(stillPending), strings.Join(names, ", "))
		}

		pending = stillPending

		if !sleepCtx(ctx, cfg.PollInterval) {
			return ctx.Err()
		}
	}
}

// createMarkers creates every import-mode CR top-down (reverse post-order = parents before
// children) so a child's parent already exists and exposes a UID. Every marker is the same
// minimal spec.mode: Import CR; for data leaves the DataImport itself is created bottom-up
// in pass 2 immediately before the upload (so its idle TTL does not start ticking while earlier
// siblings are still uploading) and is later matched to the leaf by its spec.snapshotRef
// (apiVersion/kind/name).
func (cfg Config) createMarkers(ctx context.Context, plan []PlannedNode, parents map[string]int) error {
	uids := make(map[string]types.UID, len(plan))

	for i := len(plan) - 1; i >= 0; i-- {
		node := plan[i]

		marker, err := importMarkerCR(node, cfg.Namespace)
		if err != nil {
			return err
		}

		if pi, ok := parents[nodeKey(node)]; ok {
			parent := plan[pi]

			puid, known := uids[nodeKey(parent)]
			if !known {
				return fmt.Errorf("internal: parent %s/%s of %s/%s was not created before it",
					parent.Kind, parent.Name, node.Kind, node.Name)
			}

			marker.SetOwnerReferences([]metav1.OwnerReference{parentOwnerReference(node, parent, puid)})
		}

		uid, err := cfg.ensureMarker(ctx, marker)
		if err != nil {
			return fmt.Errorf("create import CR %s/%s: %w", node.Kind, node.Name, err)
		}

		uids[nodeKey(node)] = uid
	}

	return nil
}

// nodeDisplayLabel returns the human-readable "<Kind>/<Name>" label for node, for
// user-facing output only (progress stream names) — mirroring source.Node.DisplayLabel
// on the download side, restricted to the fields the archive actually persists for an
// import-side PlannedNode.
//
// It prefers the original captured source object's identity (node.SourceObjectRef.Kind/
// Name) when the archive recorded one: domain snapshot nodes carry their spec.sourceRef
// this way. Core Snapshot nodes and CSI VolumeSnapshot data leaves have no
// SourceObjectRef (see archive.SnapshotYAML.SourceObjectRef's doc comment) and fall back
// to the snapshot CR's own Kind/Name.
func nodeDisplayLabel(node PlannedNode) string {
	if node.SourceObjectRef != nil && node.SourceObjectRef.Kind != "" && node.SourceObjectRef.Name != "" {
		return fmt.Sprintf("%s/%s", node.SourceObjectRef.Kind, node.SourceObjectRef.Name)
	}

	return fmt.Sprintf("%s/%s", node.Kind, node.Name)
}

// uploadNode uploads one node's manifests + direct child refs, then, for a data leaf,
// creates its DataImport and imports the volume bytes back-to-back so the importer's idle
// TTL window stays minimal.
// importNodeData imports a data leaf's volume bytes (no-op for non-leaf nodes). It creates
// the leaf's DataImport and streams its block data back-to-back so the importer's idle TTL
// window stays minimal. It must run only after every node's manifests are uploaded (pass 2a)
// so the leaf VolumeSnapshot already has a bound SnapshotContent.
func importNodeData(ctx context.Context, cfg Config, node PlannedNode) error {
	if !node.isVolumeSnapshotLeaf() && !node.isDomainDataLeaf() {
		return nil
	}

	cfg.Log.Info("importing volume data",
		slog.String("kind", node.Kind),
		slog.String("name", node.Name))

	diName, err := cfg.Volumes.EnsureDataImport(ctx, node, cfg.Namespace)
	if err != nil {
		return fmt.Errorf("ensure DataImport for %s/%s: %w", node.Kind, node.Name, err)
	}

	// One Stream per data-leaf upload; nil hooks when no Sink is configured so the
	// upload path is completely unchanged when Progress is not set.
	var (
		onProgress func(int)
		setTotal   func(int64)
		activate   func()
		stream     progress.Stream
	)

	if cfg.Progress != nil {
		stream = cfg.Progress.NewStream(nodeDisplayLabel(node), 0)
		onProgress = stream.IncrBy
		setTotal = stream.SetTotal
		activate = stream.Activate
	}

	// uploadErr (a plain local, not a named return — nonamedreturns is enforced
	// repo-wide) decides the stream's terminal outcome below: Done on success,
	// Fail on error, so a failed/cancelled upload is never counted as complete.
	uploadErr := cfg.Volumes.UploadVolumeData(ctx, node, diName, cfg.Namespace, setTotal, onProgress, activate)

	if stream != nil {
		if uploadErr != nil {
			stream.Fail()
		} else {
			stream.Done()
		}
	}

	if uploadErr != nil {
		return fmt.Errorf("import volume data for %s/%s: %w", node.Kind, node.Name, uploadErr)
	}

	return nil
}

// buildParentIndex maps each child node key to the plan index of its parent node, derived
// from each node's recorded direct child refs.
func buildParentIndex(plan []PlannedNode) map[string]int {
	idx := make(map[string]int)

	for i := range plan {
		for _, c := range plan[i].Children {
			idx[refKey(c.APIVersion, c.Kind, c.Name)] = i
		}
	}

	return idx
}

// nodeKey is the stable identity (apiVersion/kind/name) of a planned node.
func nodeKey(n PlannedNode) string {
	return refKey(n.APIVersion, n.Kind, n.Name)
}

// refKey builds the stable identity key shared by nodeKey and buildParentIndex.
func refKey(apiVersion, kind, name string) string {
	return apiVersion + "|" + kind + "|" + name
}

// parentOwnerReference builds the child->parent Snapshot ownerRef the state-snapshotter
// import binders resolve to discover a leaf's parent SnapshotContent. It mirrors the
// capture-path semantics: a structural child Snapshot is controller-owned by its parent,
// while a CSI VolumeSnapshot leaf is a visibility leaf owned for lifecycle/GC only
// (Controller intentionally unset).
func parentOwnerReference(child, parent PlannedNode, parentUID types.UID) metav1.OwnerReference {
	ref := metav1.OwnerReference{
		APIVersion: parent.APIVersion,
		Kind:       parent.Kind,
		Name:       parent.Name,
		UID:        parentUID,
	}

	if !child.isVolumeSnapshotLeaf() {
		controller := true
		ref.Controller = &controller
	}

	return ref
}

// uploadNodeManifests POSTs the node's own manifests plus its direct child refs.
func uploadNodeManifests(ctx context.Context, cfg Config, node PlannedNode) error {
	manifestsJSON, err := marshalManifests(node.Manifests)
	if err != nil {
		return fmt.Errorf("marshal manifests for %s/%s: %w", node.Kind, node.Name, err)
	}

	childRefs := node.Children
	if childRefs == nil {
		childRefs = []ChildRef{}
	}

	body, err := json.Marshal(uploadPayload{Manifests: manifestsJSON, ChildRefs: childRefs})
	if err != nil {
		return fmt.Errorf("marshal upload payload for %s/%s: %w", node.Kind, node.Name, err)
	}

	if _, err := cfg.Uploader.UploadManifests(ctx, node.Ref(cfg.Namespace), body); err != nil {
		return fmt.Errorf("upload manifests for %s/%s: %w", node.Kind, node.Name, err)
	}

	return nil
}

// marshalManifests renders the node's own manifests as a JSON array (the shape the
// server expects in the upload payload's "manifests" field).
func marshalManifests(manifests []unstructured.Unstructured) (json.RawMessage, error) {
	items := make([]map[string]interface{}, 0, len(manifests))
	for i := range manifests {
		items = append(items, manifests[i].Object)
	}

	return json.Marshal(items)
}

// ensureMarker creates the import-mode CR when absent and returns its UID. On idempotent
// re-runs (the CR already exists) it reuses the live UID and patches in any desired
// child->parent ownerRef that a previous partial run did not yet stamp. It never clobbers
// the live spec.
func (cfg Config) ensureMarker(ctx context.Context, obj *unstructured.Unstructured) (types.UID, error) {
	gvk := obj.GroupVersionKind()

	mapping, err := cfg.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return "", fmt.Errorf("resolve resource for %s: %w", gvk.String(), err)
	}

	ri := cfg.Dynamic.Resource(mapping.Resource).Namespace(obj.GetNamespace())

	existing, err := ri.Get(ctx, obj.GetName(), metav1.GetOptions{})
	if err == nil {
		return reconcileExistingMarker(ctx, ri, existing, obj.GetOwnerReferences())
	} else if !kubeerrors.IsNotFound(err) {
		return "", fmt.Errorf("get: %w", err)
	}

	created, err := ri.Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		if !kubeerrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("create: %w", err)
		}

		got, gErr := ri.Get(ctx, obj.GetName(), metav1.GetOptions{})
		if gErr != nil {
			return "", fmt.Errorf("get after AlreadyExists: %w", gErr)
		}

		return reconcileExistingMarker(ctx, ri, got, obj.GetOwnerReferences())
	}

	return created.GetUID(), nil
}

// reconcileExistingMarker handles a pre-existing CR sharing the marker's name. It first
// refuses to touch an object that is NOT in import mode (a capture-mode Snapshot/VolumeSnapshot
// that merely shares the name) so importing into a populated namespace cannot mutate live
// production objects; only then does it reconcile the child->parent ownerRefs.
func reconcileExistingMarker(ctx context.Context, ri dynamic.ResourceInterface, existing *unstructured.Unstructured, desired []metav1.OwnerReference) (types.UID, error) {
	imp, err := isImportModeMarker(existing)
	if err != nil {
		return "", err
	}

	if !imp {
		return "", fmt.Errorf("%s %s/%s already exists and is not in import mode "+
			"(spec.mode is not %q); refusing to mutate a pre-existing "+
			"object — import into a fresh namespace",
			existing.GetKind(), existing.GetNamespace(), existing.GetName(), snapshotapi.SnapshotModeImport)
	}

	return reconcileMarkerOwnerRefs(ctx, ri, existing, desired)
}

// isImportModeMarker reports whether a live CR is an import-mode marker. Import mode is keyed
// off spec.mode: absent or "Capture" is capture mode (not an import marker); "Import" is an
// import marker. It is fail-closed: any other value, or a non-string spec.mode, is a malformed
// object and returns an error rather than being silently classified as capture mode.
func isImportModeMarker(obj *unstructured.Unstructured) (bool, error) {
	mode, found, err := unstructured.NestedString(obj.Object, "spec", "mode")
	if err != nil {
		return false, fmt.Errorf("%s %s/%s: spec.mode is not a string: %w",
			obj.GetKind(), obj.GetNamespace(), obj.GetName(), err)
	}

	if !found || mode == "" {
		return false, nil
	}

	switch snapshotapi.SnapshotMode(mode) {
	case snapshotapi.SnapshotModeCapture:
		return false, nil
	case snapshotapi.SnapshotModeImport:
		return true, nil
	default:
		return false, fmt.Errorf("%s %s/%s: spec.mode %q is invalid (want %q or %q)",
			obj.GetKind(), obj.GetNamespace(), obj.GetName(), mode,
			snapshotapi.SnapshotModeCapture, snapshotapi.SnapshotModeImport)
	}
}

// reconcileMarkerOwnerRefs patches any desired child->parent ownerRef a previous partial
// run did not yet stamp onto the live CR, then returns its UID. It never clobbers the spec.
func reconcileMarkerOwnerRefs(ctx context.Context, ri dynamic.ResourceInterface, existing *unstructured.Unstructured, desired []metav1.OwnerReference) (types.UID, error) {
	if addOwnerRefs(existing, desired) {
		updated, err := ri.Update(ctx, existing, metav1.UpdateOptions{})
		if err != nil {
			return "", fmt.Errorf("ensure ownerRefs: %w", err)
		}

		existing = updated
	}

	return existing.GetUID(), nil
}

// addOwnerRefs reconciles obj's ownerReferences toward desired: it appends any missing ref
// and, for a ref already present (matched by apiVersion/kind/name), refreshes its
// UID/Controller/BlockOwnerDeletion when they drifted. The UID refresh matters on a retried
// import where the parent was deleted and recreated with a new server-assigned UID: the
// state-snapshotter import binders resolve a leaf's parent through this ownerRef and reject
// a stale UID, so a matching-but-outdated ref must be updated, not left as-is. It never
// removes refs and reports whether obj changed.
func addOwnerRefs(obj *unstructured.Unstructured, desired []metav1.OwnerReference) bool {
	if len(desired) == 0 {
		return false
	}

	refs := obj.GetOwnerReferences()
	changed := false

	for _, d := range desired {
		idx := indexOwnerRef(refs, d)
		if idx < 0 {
			refs = append(refs, d)
			changed = true

			continue
		}

		if !ownerRefEquivalent(refs[idx], d) {
			refs[idx] = d
			changed = true
		}
	}

	if changed {
		obj.SetOwnerReferences(refs)
	}

	return changed
}

// indexOwnerRef returns the index of the ref matching want by apiVersion/kind/name, or -1.
func indexOwnerRef(refs []metav1.OwnerReference, want metav1.OwnerReference) int {
	for i, r := range refs {
		if r.APIVersion == want.APIVersion && r.Kind == want.Kind && r.Name == want.Name {
			return i
		}
	}

	return -1
}

// ownerRefEquivalent reports whether two ownerRefs already sharing apiVersion/kind/name also
// agree on the fields the import binders depend on: the parent UID and the controller and
// blockOwnerDeletion flags.
func ownerRefEquivalent(a, b metav1.OwnerReference) bool {
	return a.UID == b.UID &&
		boolPtrEqual(a.Controller, b.Controller) &&
		boolPtrEqual(a.BlockOwnerDeletion, b.BlockOwnerDeletion)
}

func boolPtrEqual(a, b *bool) bool {
	if a == nil || b == nil {
		return a == b
	}

	return *a == *b
}

// waitRootReady blocks until the import root reports its namespaced Ready condition True.
// Readiness is observed solely on the namespaced snapshot CR (status.conditions Ready) — the
// cluster-scoped SnapshotContent is never read. The Ready condition on the root aggregates
// the whole imported subtree, so a single namespaced Get per poll is sufficient:
//   - core Snapshot: wait for the Snapshot's Ready=True (aggregates the full tree).
//   - CSI VolumeSnapshot leaf (single-leaf subtree import): wait for the leaf's Ready=True.
//   - domain data leaf: wait for the leaf's Ready=True (uses the domain leaf's own GVR
//     resolved via cfg.Mapper).
func waitRootReady(ctx context.Context, cfg Config, root PlannedNode) error {
	if root.isVolumeSnapshotLeaf() {
		return waitLeafReady(ctx, cfg, root)
	}

	if root.isDomainDataLeaf() {
		return waitDomainLeafReady(ctx, cfg, root)
	}

	gvr, err := cfg.snapshotResource()
	if err != nil {
		return err
	}

	return waitNamespacedReady(ctx, cfg, gvr, root.Name, snapshotKind)
}

// waitLeafReady waits for a CSI VolumeSnapshot leaf to report its namespaced Ready=True.
// Used when the import root is a single data leaf. The cluster-scoped SnapshotContent is
// never read.
func waitLeafReady(ctx context.Context, cfg Config, leaf PlannedNode) error {
	gvr, err := cfg.volumeSnapshotResource()
	if err != nil {
		return err
	}

	return waitNamespacedReady(ctx, cfg, gvr, leaf.Name, volumeSnapshotKind)
}

// waitDomainLeafReady waits for a domain data leaf to report its namespaced Ready=True.
// It resolves the leaf's GVR via cfg.Mapper. The cluster-scoped SnapshotContent is never read.
func waitDomainLeafReady(ctx context.Context, cfg Config, leaf PlannedNode) error {
	gvr, err := cfg.domainLeafResource(leaf)
	if err != nil {
		return err
	}

	return waitNamespacedReady(ctx, cfg, gvr, leaf.Name, leaf.Kind)
}

// domainLeafResource resolves the namespaced GVR for a domain data leaf using cfg.Mapper.
func (cfg Config) domainLeafResource(leaf PlannedNode) (schema.GroupVersionResource, error) {
	gv, err := schema.ParseGroupVersion(leaf.APIVersion)
	if err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("parse apiVersion %q for domain leaf %s/%s: %w", leaf.APIVersion, leaf.Kind, leaf.Name, err)
	}

	mapping, err := cfg.Mapper.RESTMapping(schema.GroupKind{Group: gv.Group, Kind: leaf.Kind}, gv.Version)
	if err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("resolve resource for domain leaf %s/%s: %w", leaf.Kind, leaf.Name, err)
	}

	return mapping.Resource, nil
}

// waitNamespacedReady polls the namespaced CR gvr/name and applies the readiness contract
// established by read-only analysis of state-snapshotter / storage-foundation (see
// docs/2026-06-29-unified-snapshots-overview.md and the stage-2 plan, §5.B / Appendix A):
//
//   - Ready.status == True                                   -> success.
//   - captureState.domainSpecificController.phase == Failed  -> immediate error (monotonic
//     terminal sink on the capture path; the domain reason is free-form and shown as-is).
//   - Ready.status == False AND reason is terminal
//     (terminalReadyReasons: enum + import-leaf terminals)   -> immediate error.
//   - anything else (Ready absent / False with a non-terminal reason such as ImportPending,
//     ChildrenPending, DataCapturePending, … / Unknown)      -> keep waiting until timeout.
//
// Ready=False is NOT an error by itself: the controller uses it for both in-progress and
// terminal states, distinguished by reason/phase. The aggregated Ready on the snapshot CR
// reflects the whole imported (sub)tree, so a single namespaced Get per poll is sufficient
// and no cluster-scoped SnapshotContent is ever read. kind is used only for log/error text.
func waitNamespacedReady(ctx context.Context, cfg Config, gvr schema.GroupVersionResource, name, kind string) error {
	cfg.Log.Info("waiting for resource to become Ready",
		slog.String("kind", kind),
		slog.String("name", name))

	deadline := time.Now().Add(cfg.Timeout)

	for {
		obj, err := cfg.Dynamic.Resource(gvr).Namespace(cfg.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get %s %s/%s: %w", kind, cfg.Namespace, name, err)
		}

		status, reason, message := readyConditionState(obj)

		if status == string(metav1.ConditionTrue) {
			cfg.Log.Info("resource is Ready", slog.String("kind", kind), slog.String("name", name))

			return nil
		}

		// Terminal capture sink: fail fast instead of waiting out the timeout. Absent on
		// import-mode objects (no captureState), so this is a no-op for the import path.
		if domainCapturePhase(obj) == capturePhaseFailed {
			return fmt.Errorf("%s %s/%s failed: captureState.domainSpecificController.phase=Failed (Ready reason=%q: %s)",
				kind, cfg.Namespace, name, reason, message)
		}

		// Terminal Ready reason (enum + import-leaf terminals): fail fast.
		if status == string(metav1.ConditionFalse) && isTerminalReadyReason(reason) {
			return fmt.Errorf("%s %s/%s failed: Ready=False reason=%q: %s",
				kind, cfg.Namespace, name, reason, message)
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for %s %s/%s to become Ready (last Ready status=%q reason=%q: %s)",
				kind, cfg.Namespace, name, status, reason, message)
		}

		if !sleepCtx(ctx, cfg.PollInterval) {
			return ctx.Err()
		}
	}
}

// snapshotResource resolves the core Snapshot resource.
func (cfg Config) snapshotResource() (schema.GroupVersionResource, error) {
	mapping, err := cfg.Mapper.RESTMapping(schema.GroupKind{Group: aggapi.StorageGroup, Kind: snapshotKind}, "v1alpha1")
	if err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("resolve Snapshot resource: %w", err)
	}

	return mapping.Resource, nil
}

// volumeSnapshotResource resolves the CSI VolumeSnapshot resource.
func (cfg Config) volumeSnapshotResource() (schema.GroupVersionResource, error) {
	mapping, err := cfg.Mapper.RESTMapping(
		schema.GroupKind{Group: aggapi.VolumeSnapshotGroup, Kind: volumeSnapshotKind},
		aggapi.VSConnectorVersion)
	if err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("resolve VolumeSnapshot resource: %w", err)
	}

	return mapping.Resource, nil
}

// filterPlanToSubtree finds the node with the given kind and name in plan and returns the
// post-ordered subtree rooted at that node (selected node + all descendants). The plan's
// relative order is preserved so the result remains in bottom-up (post-order) order.
func filterPlanToSubtree(plan []PlannedNode, kind, name string) ([]PlannedNode, error) {
	// Index nodes by their identity key for O(1) child lookup.
	byKey := make(map[string]PlannedNode, len(plan))

	for _, n := range plan {
		byKey[nodeKey(n)] = n
	}

	// Find the selected node (match by kind+name; apiVersion may vary across domains).
	var selected PlannedNode

	found := false

	for _, n := range plan {
		if n.Kind == kind && n.Name == name {
			selected = n
			found = true

			break
		}
	}

	if !found {
		return nil, fmt.Errorf("node %s/%s not found in archive", kind, name)
	}

	// BFS from the selected node to collect all descendant keys.
	inSubtree := make(map[string]struct{})
	queue := []PlannedNode{selected}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		inSubtree[nodeKey(cur)] = struct{}{}

		for _, c := range cur.Children {
			key := refKey(c.APIVersion, c.Kind, c.Name)
			if _, visited := inSubtree[key]; !visited {
				if child, ok := byKey[key]; ok {
					queue = append(queue, child)
				}
			}
		}
	}

	// Filter the plan preserving post-order.
	filtered := make([]PlannedNode, 0, len(inSubtree))

	for _, n := range plan {
		if _, ok := inSubtree[nodeKey(n)]; ok {
			filtered = append(filtered, n)
		}
	}

	return filtered, nil
}

// applyDefaults fills zero-valued optional fields with their defaults.
func applyDefaults(cfg Config) Config {
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}

	if cfg.Workers <= 0 {
		cfg.Workers = defaultWorkers
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
		return fmt.Errorf("import: Namespace must be set")
	case cfg.InputDir == "":
		return fmt.Errorf("import: InputDir must be set")
	case cfg.Uploader == nil:
		return fmt.Errorf("import: Uploader must be set")
	case cfg.Volumes == nil:
		return fmt.Errorf("import: Volumes importer must be set")
	case cfg.Dynamic == nil:
		return fmt.Errorf("import: Dynamic client must be set")
	case cfg.Mapper == nil:
		return fmt.Errorf("import: Mapper must be set")
	default:
		return nil
	}
}
