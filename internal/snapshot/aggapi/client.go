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
//     namespace made relative). Always served by the CORE aggregated apiserver
//     for every node kind (core Snapshot and domain snapshot CRs alike); CSI
//     VolumeSnapshot leaves use the dedicated VS-connector group instead.
//   - manifests-with-data-restoration: a ready-to-apply manifest array for the
//     node's whole subtree (the server delegates domain subtrees internally).
//     Served by the node's OWN subresource group (core group for the core
//     Snapshot, the domain-prefixed group for domain snapshot CRs).
//   - manifests-and-children-refs-upload: import one node's manifests plus its
//     direct child refs. Served by the node's own subresource group.
//
// The cluster-scoped snapshotcontents/<name>/manifests-download surface backs the
// DataImport path (reading an original PVC manifest before any namespaced CR binds).
package aggapi

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	// address its aggregated subresources group (e.g. "demo.state-snapshotter.deckhouse.io"
	// -> "subresources.demo.state-snapshotter.deckhouse.io").
	DomainSubresourcesGroupPrefix = "subresources."

	// VSConnectorGroup is the generic-PVC extended VolumeSnapshot connector subresource group.
	VSConnectorGroup = "subresources.snapshot.storage.k8s.io"
	// VSConnectorVersion is the version served under VSConnectorGroup.
	VSConnectorVersion = "v1"

	// StorageGroup is the API group of the core Snapshot / SnapshotContent CRDs.
	StorageGroup = "storage.deckhouse.io"
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
func (c *Client) NodeManifestsDownload(ctx context.Context, ref NodeRef) ([]byte, error) {
	path, err := c.downloadPath(ref)
	if err != nil {
		return nil, err
	}

	body, err := c.rest.Get().AbsPath(path).DoRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}

	return body, nil
}

// ContentManifestsDownload performs GET snapshotcontents/<name>/manifests-download
// (cluster-scoped) and returns the raw JSON array body. Used by the DataImport path
// to read the original PVC manifest before any namespaced CR binds.
func (c *Client) ContentManifestsDownload(ctx context.Context, contentName string) ([]byte, error) {
	path := fmt.Sprintf("/apis/%s/%s/snapshotcontents/%s/%s",
		CoreSubresourcesGroup, CoreSubresourcesVersion, contentName, SubManifestsDownload)

	body, err := c.rest.Get().AbsPath(path).DoRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}

	return body, nil
}

// RestoreManifests performs GET <node>/manifests-with-data-restoration?targetNamespace=<ns>
// and returns the raw apply-ready JSON array body for the node's whole subtree.
func (c *Client) RestoreManifests(ctx context.Context, ref NodeRef, targetNamespace string) ([]byte, error) {
	path, err := c.subresourcePath(ref, SubManifestsRestore)
	if err != nil {
		return nil, err
	}

	body, err := c.rest.Get().AbsPath(path).Param("targetNamespace", targetNamespace).DoRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("GET %s?targetNamespace=%s: %w", path, targetNamespace, err)
	}

	return body, nil
}

// UploadManifests performs POST <node>/manifests-and-children-refs-upload with the
// given JSON body ({"manifests": <array>, "childRefs": [...]}) and returns the raw body.
func (c *Client) UploadManifests(ctx context.Context, ref NodeRef, body []byte) ([]byte, error) {
	path, err := c.subresourcePath(ref, SubManifestsUpload)
	if err != nil {
		return nil, err
	}

	out, err := c.rest.Post().
		AbsPath(path).
		SetHeader("Content-Type", "application/json").
		Body(body).
		DoRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", path, err)
	}

	return out, nil
}

// downloadPath builds the manifests-download absolute path for ref. manifests-download
// is served by the core aggregated apiserver for every node kind except CSI
// VolumeSnapshot leaves, which use the VS-connector group.
func (c *Client) downloadPath(ref NodeRef) (string, error) {
	if ref.IsVolumeSnapshotLeaf() {
		return fmt.Sprintf("/apis/%s/%s/namespaces/%s/%s/%s/%s",
			VSConnectorGroup, VSConnectorVersion, ref.Namespace, VolumeSnapshotResource, ref.Name, SubManifestsDownload), nil
	}

	resource, err := c.resourceFor(ref)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("/apis/%s/%s/namespaces/%s/%s/%s/%s",
		CoreSubresourcesGroup, CoreSubresourcesVersion, ref.Namespace, resource, ref.Name, SubManifestsDownload), nil
}

// subresourcePath builds the absolute path for a restore/upload subresource of ref,
// addressed through the node's OWN aggregated subresource group.
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

// LeafDataExportTarget resolves the DataExport targetRef {group, resource} for a snapshot
// leaf node. CSI VolumeSnapshot leaves use the fixed constants VolumeSnapshotGroup /
// VolumeSnapshotResource. All other kinds are resolved via the RESTMapper.
//
// This is used to build a DataExport that the storage-volume-data-manager controller
// can route through its resource-agnostic snapshot export path (categorySnapshot):
// group/resource must identify a namespaced snapshot CR so the controller can read
// its status.boundSnapshotContentName → SnapshotContent → status.dataRef.
func (c *Client) LeafDataExportTarget(ref NodeRef) (group, resource string, err error) {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return "", "", fmt.Errorf("parse apiVersion %q: %w", ref.APIVersion, err)
	}

	if ref.IsVolumeSnapshotLeaf() {
		return VolumeSnapshotGroup, VolumeSnapshotResource, nil
	}

	res, err := c.resourceFor(ref)
	if err != nil {
		return "", "", err
	}

	return gv.Group, res, nil
}

// subresourceGroupVersion returns the aggregated subresource group and version that
// serves restore/upload for a node of the given GVK:
//   - CSI VolumeSnapshot leaves -> the VS-connector group.
//   - the core Snapshot (storage.deckhouse.io) -> the core subresources group.
//   - any domain snapshot CR -> "subresources." + its API group, same version.
func subresourceGroupVersion(ref NodeRef) (group, version string, err error) {
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
