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

// Package aggapi is a thin client for the state-snapshotter aggregated subresource
// API exposed through the kube-apiserver aggregation layer.
//
// After C9 there is no whole-subtree server-side aggregation: each snapshot node
// exposes three per-CR subresources, addressed by the node's resource plural:
//   - manifests-download: the node's own captured manifests (status preserved,
//     namespace made relative). Served by the node's OWN subresource group (core
//     group for the core Snapshot, the domain-prefixed group for domain snapshot CRs
//     — the domain apiserver proxies to the core content layer); CSI VolumeSnapshot
//     leaves use the dedicated VS-connector group instead.
//   - manifests-with-data-restoration: a ready-to-apply manifest array for the
//     node's whole subtree (the server delegates domain subtrees internally).
//     Served by the node's OWN subresource group (core group for the core
//     Snapshot, the domain-prefixed group for domain snapshot CRs).
//   - manifests-and-children-refs-upload: import one node's manifests plus its
//     direct child refs. Served by the node's OWN subresource group (core group for
//     the core Snapshot, the domain-prefixed group for domain snapshot CRs — the
//     domain apiserver's upload facade records the node's own childrenSnapshotRefs and
//     forwards the manifests to the core content layer); CSI VolumeSnapshot leaves use
//     the dedicated VS-connector group instead. The upload is bind-first: it returns
//     409 ImportContentNotBound until the node's SnapshotContent is bound.
//
// All three subresources now route the same way — by the node's own group — so there is
// no longer any download/upload group asymmetry. Every subresource is addressed by the
// node's own namespaced CR (Snapshot, domain snapshot CR, or CSI VolumeSnapshot leaf).
// The client never reads cluster-scoped SnapshotContent objects.
package aggapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Aggregated subresource API groups and versions.
const (
	// CoreSubresourcesGroup is the core controller's aggregated subresources API group.
	// It serves manifests-download for every node kind and the core Snapshot's
	// restore/upload subresources.
	CoreSubresourcesGroup = "subresources.state-snapshotter.deckhouse.io"
	// CoreSubresourcesVersion is the version served under CoreSubresourcesGroup.
	CoreSubresourcesVersion = "v1alpha1"

	// DomainSubresourcesGroupPrefix is prepended to a domain snapshot's API group to
	// address its aggregated subresources group (e.g. "sds-unified-snapshots-poc.deckhouse.io"
	// -> "subresources.sds-unified-snapshots-poc.deckhouse.io").
	DomainSubresourcesGroupPrefix = "subresources."

	// VSConnectorGroup is the generic-PVC extended VolumeSnapshot connector subresource group.
	VSConnectorGroup = "subresources.snapshot.storage.k8s.io"
	// VSConnectorVersion is the version served under VSConnectorGroup.
	VSConnectorVersion = "v1"

	// StorageGroup is the API group of the core Snapshot / SnapshotContent CRDs.
	StorageGroup = "state-snapshotter.deckhouse.io"
	// VolumeSnapshotGroup is the CSI external-snapshotter API group of VolumeSnapshot leaves.
	VolumeSnapshotGroup = "snapshot.storage.k8s.io"
	// VolumeSnapshotResource is the resource plural of CSI VolumeSnapshot objects.
	VolumeSnapshotResource = "volumesnapshots"
	// VolumeSnapshotKind is the kind of CSI VolumeSnapshot leaf nodes.
	VolumeSnapshotKind = "VolumeSnapshot"
)

// Subresource names.
const (
	// SubManifestsDownload reads one node's own captured manifests.
	SubManifestsDownload = "manifests-download"
	// SubManifestsRestore reads a ready-to-apply manifest array for a node's subtree.
	SubManifestsRestore = "manifests-with-data-restoration"
	// SubManifestsUpload imports one node's manifests plus its direct child refs.
	SubManifestsUpload = "manifests-and-children-refs-upload"
)

// NodeRef uniquely identifies a snapshot node for aggregated subresource calls.
// It is intentionally string-based (no typed objects) so that domain-specific
// kinds can be addressed without registering their Go types.
type NodeRef struct {
	APIVersion string
	Kind       string
	Name       string
	Namespace  string
}

// IsVolumeSnapshotLeaf reports whether the ref addresses a CSI VolumeSnapshot leaf,
// which is served by the VS-connector subresource group instead of the core group.
func (r NodeRef) IsVolumeSnapshotLeaf() bool {
	gv, err := schema.ParseGroupVersion(r.APIVersion)
	if err != nil {
		return false
	}

	return gv.Group == VolumeSnapshotGroup && r.Kind == VolumeSnapshotKind
}

// Client performs aggregated-apiserver calls over an absolute-path REST interface
// (typically a discovery REST client) and resolves a node's GVK to its resource
// plural via a RESTMapper.
type Client struct {
	rest   rest.Interface
	mapper meta.RESTMapper
}

// NewClient builds a Client from a raw REST interface (e.g. the discovery REST client)
// and a RESTMapper used to resolve GVK -> resource plural.
func NewClient(restClient rest.Interface, mapper meta.RESTMapper) *Client {
	return &Client{rest: restClient, mapper: mapper}
}

// NewClientForConfig builds a Client from a rest.Config, constructing the discovery
// REST client internally. mapper resolves GVK -> resource plural.
func NewClientForConfig(cfg *rest.Config, mapper meta.RESTMapper) (*Client, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build clientset for aggregated API: %w", err)
	}

	return NewClient(cs.Discovery().RESTClient(), mapper), nil
}

// NodeManifestsDownload performs GET <node>/manifests-download and returns the raw
// JSON array body (the node's own captured manifests).
//
// The call is retried with bounded exponential backoff on a transient aggregated-API
// error (see isTransientManifestsDownloadError) — observed in practice as the
// aggregated APIService backend briefly restarting/overloading and returning "the
// server is currently unable to handle the request" (HTTP 503).
func (c *Client) NodeManifestsDownload(ctx context.Context, ref NodeRef) ([]byte, error) {
	path, err := c.downloadPath(ref)
	if err != nil {
		return nil, err
	}

	return c.getManifestsDownload(ctx, path)
}

// manifestsDownloadBackoff bounds retries of the manifests-download aggregated-API call
// on a transient backend-unavailable error. 5 attempts starting at 500ms and doubling
// (capped at 8s) bound the worst-case wall-clock to single-digit seconds: enough for a
// momentarily-restarting aggregated APIService backend to recover, but short enough that
// a genuinely-down backend still fails fast instead of hanging.
var manifestsDownloadBackoff = wait.Backoff{
	Steps:    5,
	Duration: 500 * time.Millisecond,
	Factor:   2.0,
	Jitter:   0.1,
	Cap:      8 * time.Second,
}

// isTransientManifestsDownloadError reports whether err is worth retrying: the
// aggregated APIService backend is momentarily unavailable, timed out, or asked the
// client to back off. Any other error (NotFound, Forbidden, invalid request, ...) is a
// genuine failure and must surface on the first attempt, not be masked by a retry loop.
func isTransientManifestsDownloadError(err error) bool {
	return apierrors.IsServiceUnavailable(err) || apierrors.IsServerTimeout(err) || apierrors.IsTooManyRequests(err)
}

// getManifestsDownload performs GET path, retrying with bounded exponential backoff
// (manifestsDownloadBackoff) on a transient aggregated-API error. Scope is intentionally
// narrow to the manifests-download subresource; other aggregated calls (restore, upload)
// are unaffected.
//
// ctx cancellation aborts the retry loop immediately, including mid-backoff sleep, and
// returns ctx.Err(). A backend that stays unavailable for the whole retry budget
// exhausts manifestsDownloadBackoff's attempts and returns the last transient error seen,
// so callers get a clear, actionable failure instead of an indefinite hang.
func (c *Client) getManifestsDownload(ctx context.Context, path string) ([]byte, error) {
	var (
		body    []byte
		lastErr error
	)

	backoffErr := wait.ExponentialBackoffWithContext(ctx, manifestsDownloadBackoff, func(stepCtx context.Context) (bool, error) {
		var doErr error

		body, doErr = c.rest.Get().AbsPath(path).DoRaw(stepCtx)

		switch {
		case doErr == nil:
			return true, nil
		case isTransientManifestsDownloadError(doErr):
			lastErr = doErr
			return false, nil
		default:
			return false, doErr
		}
	})

	switch {
	case backoffErr == nil:
		return body, nil
	case ctx.Err() != nil:
		return nil, fmt.Errorf("GET %s: %w", path, ctx.Err())
	case wait.Interrupted(backoffErr):
		return nil, fmt.Errorf("GET %s: exhausted retries on transient error: %w", path, lastErr)
	default:
		return nil, fmt.Errorf("GET %s: %w", path, backoffErr)
	}
}

// RestoreScope selects the compilation depth of a RestoreManifestsScoped call, mirroring
// state-snapshotter's usecase/restore.Scope wire values verbatim (restore_handler.go
// parseRestoreQueryOptions) — these literals go on the wire as the "scope" query param, so they
// must match the server exactly, not just read similarly.
type RestoreScope string

const (
	// RestoreScopeSubtree compiles the addressed node and its whole subtree, recursively. It is
	// the server's default when the scope query param is omitted entirely.
	RestoreScopeSubtree RestoreScope = "subtree"
	// RestoreScopeNode compiles ONLY the addressed node, with no descendants. Required before an
	// object filter (FilterKind/FilterName/FilterAPIVersion) is accepted.
	RestoreScopeNode RestoreScope = "node"
)

// RestoreScopeOptions narrows a RestoreManifestsScoped call to one node (Scope ==
// RestoreScopeNode) and, optionally, a single captured object within that node (FilterKind +
// FilterName, with FilterAPIVersion further disambiguating). The server accepts the object filter
// ONLY together with Scope == RestoreScopeNode and rejects any other combination with a 400
// (restore_handler.go parseRestoreQueryOptions) — RestoreManifestsScoped does not pre-validate
// this client-side, it lets the server enforce its own contract and surfaces the resulting
// ErrRestoreBadRequest. A zero-value RestoreScopeOptions reproduces today's default (full subtree,
// no filter) byte-for-byte: no scope/kind/name/apiVersion query params are sent at all.
type RestoreScopeOptions struct {
	Scope            RestoreScope
	FilterKind       string
	FilterName       string
	FilterAPIVersion string
}

// RestoreManifests performs GET <node>/manifests-with-data-restoration?targetNamespace=<ns>
// and returns the raw apply-ready JSON array body for the node's whole subtree.
//
// It is a thin wrapper over RestoreManifestsScoped with a zero-value RestoreScopeOptions; callers
// that need scope=node or an object filter call RestoreManifestsScoped directly.
func (c *Client) RestoreManifests(ctx context.Context, ref NodeRef, targetNamespace string) ([]byte, error) {
	return c.RestoreManifestsScoped(ctx, ref, targetNamespace, RestoreScopeOptions{})
}

// RestoreManifestsScoped performs GET <node>/manifests-with-data-restoration with targetNamespace
// and, when set, the server's scope/kind/name/apiVersion query params (restore_handler.go
// parseRestoreQueryOptions). Each of opts.Scope/FilterKind/FilterName/FilterAPIVersion is sent
// ONLY when non-empty — an empty Scope omits the scope param entirely rather than sending
// scope=subtree explicitly, reproducing the server's own default byte-for-byte.
func (c *Client) RestoreManifestsScoped(ctx context.Context, ref NodeRef, targetNamespace string, opts RestoreScopeOptions) ([]byte, error) {
	path, err := c.subresourcePath(ref, SubManifestsRestore)
	if err != nil {
		return nil, err
	}

	req := c.rest.Get().AbsPath(path).Param("targetNamespace", targetNamespace)
	if opts.Scope != "" {
		req = req.Param("scope", string(opts.Scope))
	}

	if opts.FilterKind != "" {
		req = req.Param("kind", opts.FilterKind)
	}

	if opts.FilterName != "" {
		req = req.Param("name", opts.FilterName)
	}

	if opts.FilterAPIVersion != "" {
		req = req.Param("apiVersion", opts.FilterAPIVersion)
	}

	body, err := req.DoRaw(ctx)
	if err != nil {
		return nil, classifyRestoreError(path, err, body)
	}

	return body, nil
}

// ErrRestoreBadRequest is returned by RestoreManifestsScoped when the server rejects the request
// as invalid (state-snapshotter restore.ErrBadRequest wire equivalent: an unknown scope value, an
// object filter missing kind or name, or an object filter used with a scope other than
// RestoreScopeNode). Distinguish it from other failures with errors.Is.
var ErrRestoreBadRequest = errors.New("restore request rejected as invalid")

// ErrRestoreNotFound is returned by RestoreManifestsScoped when the addressed node, or — with an
// object filter — the requested object within it, does not exist. Distinguish it from other
// failures with errors.Is.
var ErrRestoreNotFound = errors.New("restore target not found")

// classifyRestoreError maps a RestoreManifestsScoped DoRaw failure to a wrapped, distinguishable
// CLI-facing error instead of a bare generic failure the caller would have to inspect an HTTP
// status code to understand.
//
// DoRaw derives err's k8s error TYPE purely from the HTTP status code (errors.NewGenericServerResponse
// — see isImportContentNotBound's doc comment for the same mechanism), so apierrors.IsBadRequest /
// apierrors.IsNotFound already classify 400/404 correctly. But that generic error's MESSAGE is a
// fixed placeholder string for a JSON body (DoRaw's isTextResponse check is false for
// "application/json"), discarding the server's actual explanation (e.g. which validation rule in
// parseRestoreQueryOptions failed). body is the raw metav1.Status JSON DoRaw returns alongside the
// error on every failure; decode it best-effort to recover that explanation for the caller.
func classifyRestoreError(path string, err error, body []byte) error {
	message := restoreErrorMessage(body)

	switch {
	case apierrors.IsBadRequest(err):
		return fmt.Errorf("GET %s: %w: %s: %w", path, ErrRestoreBadRequest, message, err)
	case apierrors.IsNotFound(err):
		return fmt.Errorf("GET %s: %w: %s: %w", path, ErrRestoreNotFound, message, err)
	default:
		return fmt.Errorf("GET %s: %w", path, err)
	}
}

// restoreErrorMessage best-effort decodes body as a metav1.Status and returns its Message, or a
// fixed fallback when body is not a well-formed Status with a message (e.g. empty body, or a
// transport-level failure with no server response at all).
func restoreErrorMessage(body []byte) string {
	var status metav1.Status
	if err := json.Unmarshal(body, &status); err != nil || status.Message == "" {
		return "no further detail from the server"
	}

	return status.Message
}

// UploadManifests performs POST <node>/manifests-and-children-refs-upload with the
// given JSON body ({"manifests": <array>, "childRefs": [...]}) and returns the raw body.
//
// The upload is bind-first: the aggregated apiserver refuses it with 409
// ImportContentNotBound until the addressed node's status.boundSnapshotContentName is set.
// The import orchestrator (see snapimport.Run) already gates the whole upload pass behind a
// collective wait-for-bind, so this per-request retry is only a safety net for the narrow
// read-after-write race where the CLI observed a node bound but this upload endpoint has not
// yet — see postManifestsUpload.
func (c *Client) UploadManifests(ctx context.Context, ref NodeRef, body []byte) ([]byte, error) {
	path, err := c.uploadPath(ref)
	if err != nil {
		return nil, err
	}

	return c.postManifestsUpload(ctx, path, body)
}

// ReasonImportContentNotBound is the canonical status.reason of the bind-first 409 the
// namespaced (core / domain / VS-connector) manifests-and-children-refs-upload returns while a
// node's status.boundSnapshotContentName is still empty. It mirrors state-snapshotter
// usecase.ReasonImportContentNotBound verbatim — the wire contract d8 retries on until the
// binder binds the node's SnapshotContent.
const ReasonImportContentNotBound = "ImportContentNotBound"

// manifestsUploadBackoff bounds the safety-net retries of manifests-and-children-refs-upload on
// the transient bind-first 409 (ImportContentNotBound): the node's import-mode marker exists but
// the state-snapshotter binder has not yet stamped status.boundSnapshotContentName. Same shape as
// manifestsDownloadBackoff (5 attempts, 500ms doubling, capped at 8s): enough to absorb the
// bind read-after-write race, short enough that a genuinely-unbound node still fails fast instead
// of hanging (the primary bind gate is snapimport.Run's waitForBinds, not this loop).
var manifestsUploadBackoff = wait.Backoff{
	Steps:    5,
	Duration: 500 * time.Millisecond,
	Factor:   2.0,
	Jitter:   0.1,
	Cap:      8 * time.Second,
}

// isImportContentNotBound reports whether an upload attempt is the transient bind-first 409 worth
// retrying: the aggregated apiserver refused it with status.reason=ImportContentNotBound because
// the addressed node is not yet bound. Any other outcome (Forbidden back-ref mismatch, NotFound
// addressing error, invalid request, ...) is a genuine failure that must surface on the first
// attempt, not be masked by the retry loop.
//
// The check parses the RESPONSE BODY, not the error: rest.Request.DoRaw derives its error from the
// HTTP status code alone (errors.NewGenericServerResponse maps 409 -> the generic "Conflict"
// reason), discarding the server's custom status.reason. DoRaw still returns the raw metav1.Status
// body alongside that error, so the custom "ImportContentNotBound" reason — the wire signal the
// contract defines — is recovered by decoding the body and confirming both code 409 and the reason.
func isImportContentNotBound(err error, body []byte) bool {
	if err == nil {
		return false
	}

	var status metav1.Status
	if json.Unmarshal(body, &status) != nil {
		return false
	}

	return status.Code == int32(http.StatusConflict) && string(status.Reason) == ReasonImportContentNotBound
}

// postManifestsUpload performs POST path with body, retrying with bounded exponential backoff
// (manifestsUploadBackoff) on the transient bind-first 409 (ImportContentNotBound). Scope is
// intentionally narrow to that one reason; every other error surfaces on the first attempt.
//
// ctx cancellation aborts the retry loop immediately, including mid-backoff sleep, and returns
// ctx.Err(). A node that stays unbound for the whole retry budget exhausts manifestsUploadBackoff's
// attempts and returns the last ImportContentNotBound error seen, so callers get a clear,
// actionable failure instead of an indefinite hang.
func (c *Client) postManifestsUpload(ctx context.Context, path string, body []byte) ([]byte, error) {
	var (
		out     []byte
		lastErr error
	)

	backoffErr := wait.ExponentialBackoffWithContext(ctx, manifestsUploadBackoff, func(stepCtx context.Context) (bool, error) {
		var doErr error

		out, doErr = c.rest.Post().
			AbsPath(path).
			SetHeader("Content-Type", "application/json").
			Body(body).
			DoRaw(stepCtx)

		switch {
		case doErr == nil:
			return true, nil
		case isImportContentNotBound(doErr, out):
			lastErr = doErr
			return false, nil
		default:
			return false, doErr
		}
	})

	switch {
	case backoffErr == nil:
		return out, nil
	case ctx.Err() != nil:
		return nil, fmt.Errorf("POST %s: %w", path, ctx.Err())
	case wait.Interrupted(backoffErr):
		return nil, fmt.Errorf("POST %s: exhausted retries on bind-first 409 (ImportContentNotBound): %w", path, lastErr)
	default:
		return nil, fmt.Errorf("POST %s: %w", path, backoffErr)
	}
}

// downloadPath builds the manifests-download absolute path for ref, addressed through the
// node's OWN aggregated subresource group (symmetric to restore's subresourcePath): the core
// Snapshot -> the core group, CSI VolumeSnapshot leaves -> the VS-connector group, and any
// domain snapshot CR -> the domain-prefixed group. A domain node's manifests-download is served
// by the domain apiserver as a proxy to the core content layer, so it is NOT the core group.
func (c *Client) downloadPath(ref NodeRef) (string, error) {
	return c.subresourcePath(ref, SubManifestsDownload)
}

// uploadPath builds the manifests-and-children-refs-upload absolute path for ref, addressed
// through the node's OWN aggregated subresource group — identical routing to downloadPath and
// restore's subresourcePath: the core Snapshot -> the core group, CSI VolumeSnapshot leaves ->
// the VS-connector group, and any domain snapshot CR -> the domain-prefixed group (served by the
// domain apiserver's upload facade, which records the node's own childrenSnapshotRefs and forwards
// the manifests to the core content layer). The former asymmetry — upload going to the core group
// while download/restore routed by node group — is gone.
func (c *Client) uploadPath(ref NodeRef) (string, error) {
	return c.subresourcePath(ref, SubManifestsUpload)
}

// subresourcePath builds the absolute path for a per-CR aggregated subresource (sub) of ref,
// addressed through the node's OWN aggregated subresource group: the core group for the core
// Snapshot, the domain-prefixed group for domain snapshot CRs, and the VS-connector group for CSI
// VolumeSnapshot leaves. All three per-CR subresources — manifests-download,
// manifests-with-data-restoration, and manifests-and-children-refs-upload — route the same way.
func (c *Client) subresourcePath(ref NodeRef, sub string) (string, error) {
	group, version, err := subresourceGroupVersion(ref)
	if err != nil {
		return "", err
	}

	resource, err := c.resourceFor(ref)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("/apis/%s/%s/namespaces/%s/%s/%s/%s",
		group, version, ref.Namespace, resource, ref.Name, sub), nil
}

// resourceFor resolves the resource plural for ref. CSI VolumeSnapshot leaves use a
// fixed plural; all other kinds are resolved via the RESTMapper.
func (c *Client) resourceFor(ref NodeRef) (string, error) {
	if ref.IsVolumeSnapshotLeaf() {
		return VolumeSnapshotResource, nil
	}

	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return "", fmt.Errorf("parse apiVersion %q: %w", ref.APIVersion, err)
	}

	if c.mapper == nil {
		return "", fmt.Errorf("no RESTMapper configured to resolve %s/%s", ref.APIVersion, ref.Kind)
	}

	mapping, err := c.mapper.RESTMapping(schema.GroupKind{Group: gv.Group, Kind: ref.Kind}, gv.Version)
	if err != nil {
		return "", fmt.Errorf("resolve resource for %s/%s: %w", ref.APIVersion, ref.Kind, err)
	}

	return mapping.Resource.Resource, nil
}

// LeafDataExportTarget resolves the DataExport targetRef {group, resource, kind} for a
// snapshot leaf node. CSI VolumeSnapshot leaves use the fixed constants VolumeSnapshotGroup /
// VolumeSnapshotResource / VolumeSnapshotKind; all other kinds derive group from the leaf's
// apiVersion, use its own kind directly, and resolve the resource plural via the RESTMapper.
//
// This is used to build a DataExport that the storage-volume-data-manager controller
// can route through its kind-agnostic snapshot export path (categorySnapshot):
// group/kind must identify a namespaced snapshot CR so the controller can read
// its status.boundSnapshotContentName → SnapshotContent → status.dataRef.
//
// TEMP REVERTME: resource (plural) is populated in addition to kind so the DataExport
// satisfies the deployed storage-volume-data-manager (mr135) GVR-based CRD, which requires
// spec.targetRef.resource. The kind-based contract is not yet in SVDM main. Sending both
// resource and kind is safe because each CRD prunes the field it doesn't know.
func (c *Client) LeafDataExportTarget(ref NodeRef) (string, string, string, error) {
	if ref.IsVolumeSnapshotLeaf() {
		return VolumeSnapshotGroup, VolumeSnapshotResource, VolumeSnapshotKind, nil
	}

	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return "", "", "", fmt.Errorf("parse apiVersion %q: %w", ref.APIVersion, err)
	}

	resource, err := c.resourceFor(ref)
	if err != nil {
		return "", "", "", err
	}

	return gv.Group, resource, ref.Kind, nil
}

// subresourceGroupVersion returns the aggregated subresource group and version that
// serves restore/upload for a node of the given GVK:
//   - CSI VolumeSnapshot leaves -> the VS-connector group.
//   - the core Snapshot (state-snapshotter.deckhouse.io) -> the core subresources group.
//   - any domain snapshot CR -> "subresources." + its API group, same version.
func subresourceGroupVersion(ref NodeRef) (string, string, error) {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return "", "", fmt.Errorf("parse apiVersion %q: %w", ref.APIVersion, err)
	}

	switch {
	case gv.Group == VolumeSnapshotGroup && ref.Kind == VolumeSnapshotKind:
		return VSConnectorGroup, VSConnectorVersion, nil
	case gv.Group == StorageGroup:
		return CoreSubresourcesGroup, CoreSubresourcesVersion, nil
	default:
		return DomainSubresourcesGroupPrefix + gv.Group, gv.Version, nil
	}
}
