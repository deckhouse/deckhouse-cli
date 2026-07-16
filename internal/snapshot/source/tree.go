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

package source

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
)

// ErrCycle is returned when a cycle or duplicate reference is detected in the snapshot tree.
var ErrCycle = errors.New("cycle detected in snapshot tree")

// ErrLeafNotBound is returned when a VolumeSnapshot visibility-leaf has no namespaced
// status.data; the leaf is not yet captured and is not ready for download.
var ErrLeafNotBound = errors.New("VolumeSnapshot leaf not yet captured (no status.data)")

// rootAPIVersion is the apiVersion of the root Snapshot CR.
const rootAPIVersion = snapshotapi.StorageGroup + "/" + snapshotapi.Version

// volumeSnapshotAPIVersion is the apiVersion of CSI VolumeSnapshot visibility-leaf nodes.
const volumeSnapshotAPIVersion = "snapshot.storage.k8s.io/v1"

// BuildTree fetches the root Snapshot by name and recursively resolves the full snapshot
// tree by following status.childrenSnapshotRefs.
//
// Each node is built solely from its own namespaced status via ParseNodeStatus:
// status.sourceRef (the captured source identity) and status.data (the single captured
// volume, Variant A cardinality ≤1). The tree builder NEVER reads cluster-scoped
// SnapshotContent. Node manifests are fetched separately via the aggregated
// manifests-download subresource.
//
// All snapshot nodes are namespace-local: child refs carry no namespace field and are
// always fetched in the same namespace as the root. The function does one Get per node
// and never lists.
//
// childrenSnapshotRefs are partitioned into two sets:
//   - Domain refs (apiVersion != "snapshot.storage.k8s.io/v1" or kind != "VolumeSnapshot")
//     are recursed normally.
//   - VolumeSnapshot visibility-leaf refs (apiVersion == "snapshot.storage.k8s.io/v1" and
//     kind == "VolumeSnapshot") signal that this node is an aggregator. Each leaf is resolved
//     via visitVisibilityLeaf: Get the VolumeSnapshot and read its own namespaced
//     status.sourceRef/status.data. The leaf node's Name is the VS CR name (for
//     ManifestScopeRef); its readable directory base comes from status.sourceRef.name.
//
// A non-aggregator node's captured volume (if any) is its own status.data. An aggregator
// node has status.data == nil and exposes data through its leaf children.
//
// Returns ErrCycle if a duplicate snapshot ref is encountered.
// Returns ErrLeafNotBound if a VolumeSnapshot leaf has no status.data.
func BuildTree(ctx context.Context, c client.Client, namespace, rootName string) (*Node, error) {
	v := &treeBuilder{
		client:    c,
		namespace: namespace,
		seen:      make(map[string]struct{}),
	}

	return v.visit(ctx, rootAPIVersion, "Snapshot", rootName, nil)
}

// treeBuilder accumulates per-traversal state: the client, the fixed namespace, and
// the set of already-visited apiVersion/kind/name keys for cycle detection.
type treeBuilder struct {
	client    client.Client
	namespace string
	seen      map[string]struct{}
}

func (b *treeBuilder) visit(ctx context.Context, apiVersion, kind, name string, parent *Node) (*Node, error) {
	nodeKey := apiVersion + "/" + kind + "/" + name
	if _, dup := b.seen[nodeKey]; dup {
		return nil, fmt.Errorf("ref %s %s/%s: %w", apiVersion, kind, name, ErrCycle)
	}

	b.seen[nodeKey] = struct{}{}

	obj, err := fetchUnstructured(ctx, b.client, b.namespace, apiVersion, kind, name)
	if err != nil {
		return nil, fmt.Errorf("fetch %s %s/%s: %w", apiVersion, kind, name, err)
	}

	ident, sourceRef, data, err := ParseNodeStatus(obj)
	if err != nil {
		return nil, err
	}

	node := &Node{
		APIVersion: ident.APIVersion,
		Kind:       ident.Kind,
		Name:       ident.Name,
		Namespace:  ident.Namespace,
		UID:        ident.UID,
		SourceRef:  sourceRef,
		Data:       data,
		Parent:     parent,
	}

	allChildRefs, err := extractChildRefs(obj)
	if err != nil {
		return nil, fmt.Errorf("%s %s/%s: status.childrenSnapshotRefs: %w", apiVersion, kind, name, err)
	}

	// Partition childRefs into domain refs (to recurse) and visibility-leaf refs.
	domainRefs, leafRefs := partitionChildRefs(allChildRefs)

	node.Children = make([]*Node, 0, len(domainRefs)+len(leafRefs))

	for _, ref := range domainRefs {
		child, err := b.visit(ctx, ref.APIVersion, ref.Kind, ref.Name, node)
		if err != nil {
			return nil, err
		}

		node.Children = append(node.Children, child)
	}

	if len(leafRefs) > 0 {
		// Aggregator: resolve each VolumeSnapshot visibility-leaf from its own namespaced
		// status. The aggregator itself carries no own data (Data stays nil); each leaf owns
		// its captured volume and the PVC manifest (each leaf's own ManifestCheckpoint).
		for _, leafRef := range leafRefs {
			leaf, err := b.visitVisibilityLeaf(ctx, leafRef.Name, node)
			if err != nil {
				return nil, fmt.Errorf("resolve VolumeSnapshot leaf %q: %w", leafRef.Name, err)
			}

			node.Children = append(node.Children, leaf)
		}

		return node, nil
	}

	// Non-aggregator: the captured volume (if any) is the node's own status.data (already set).
	return node, nil
}

// visitVisibilityLeaf resolves one VolumeSnapshot visibility-leaf into a Node.
//
// It fetches the VolumeSnapshot (snapshot.storage.k8s.io/v1) by vsName in the tree's
// namespace and builds the leaf entirely from the VS's own namespaced status via
// ParseNodeStatus (status.sourceRef + status.data) — no child SnapshotContent read.
//
// The leaf node Name is the VS CR name (used by ManifestScopeRef to address the
// VolumeSnapshot connector subresource). Its readable directory base comes from
// status.sourceRef.name (the captured PVC name).
//
// Returns ErrLeafNotBound when the VolumeSnapshot has no status.data (not yet captured).
func (b *treeBuilder) visitVisibilityLeaf(ctx context.Context, vsName string, parent *Node) (*Node, error) {
	vs, err := fetchUnstructured(ctx, b.client, b.namespace, volumeSnapshotAPIVersion, "VolumeSnapshot", vsName)
	if err != nil {
		return nil, fmt.Errorf("fetch VolumeSnapshot %s/%s: %w", b.namespace, vsName, err)
	}

	ident, sourceRef, data, err := ParseNodeStatus(vs)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, fmt.Errorf("VolumeSnapshot %s/%s: %w", b.namespace, vsName, ErrLeafNotBound)
	}

	return &Node{
		APIVersion: ident.APIVersion,
		Kind:       ident.Kind,
		Name:       ident.Name,
		Namespace:  ident.Namespace,
		UID:        ident.UID,
		SourceRef:  sourceRef,
		Data:       data,
		Parent:     parent,
	}, nil
}

// partitionChildRefs splits childRefs into domain refs (to recurse) and VolumeSnapshot
// visibility-leaf refs. Visibility-leaf refs have apiVersion == "snapshot.storage.k8s.io/v1"
// and kind == "VolumeSnapshot".
func partitionChildRefs(refs []snapshotapi.SnapshotChildRef) ([]snapshotapi.SnapshotChildRef, []snapshotapi.SnapshotChildRef) {
	domain := make([]snapshotapi.SnapshotChildRef, 0, len(refs))
	leaves := make([]snapshotapi.SnapshotChildRef, 0, len(refs))

	for _, ref := range refs {
		if isVisibilityLeaf(ref) {
			leaves = append(leaves, ref)
		} else {
			domain = append(domain, ref)
		}
	}

	return domain, leaves
}

// isVisibilityLeaf reports whether a child ref is a CSI VolumeSnapshot visibility-leaf.
// Mirrors state-snapshotter pkg/snapshot.IsVolumeSnapshotVisibilityLeaf.
func isVisibilityLeaf(ref snapshotapi.SnapshotChildRef) bool {
	return ref.APIVersion == volumeSnapshotAPIVersion && ref.Kind == "VolumeSnapshot"
}

// fetchUnstructured fetches any namespaced Kubernetes object by apiVersion/kind/name
// using an unstructured.Unstructured so that the client scheme does not need to know
// the domain-specific type (e.g. DemoVirtualMachineSnapshot).
func fetchUnstructured(ctx context.Context, c client.Client, namespace, apiVersion, kind, name string) (*unstructured.Unstructured, error) {
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return nil, fmt.Errorf("parse apiVersion %q: %w", apiVersion, err)
	}

	obj := new(unstructured.Unstructured)
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   gv.Group,
		Version: gv.Version,
		Kind:    kind,
	})

	nn := types.NamespacedName{Namespace: namespace, Name: name}
	if err := c.Get(ctx, nn, obj); err != nil {
		return nil, err
	}

	return obj, nil
}

// extractChildRefs returns the status.childrenSnapshotRefs array from an
// unstructured snapshot object. Returns nil (not an error) if the field is absent.
func extractChildRefs(obj *unstructured.Unstructured) ([]snapshotapi.SnapshotChildRef, error) {
	rawRefs, found, err := unstructured.NestedSlice(obj.Object, "status", "childrenSnapshotRefs")
	if err != nil {
		return nil, err
	}

	if !found || len(rawRefs) == 0 {
		return nil, nil
	}

	refs := make([]snapshotapi.SnapshotChildRef, 0, len(rawRefs))

	for i, raw := range rawRefs {
		m, ok := raw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("element %d has unexpected type %T", i, raw)
		}

		ref := snapshotapi.SnapshotChildRef{
			APIVersion: mapString(m, "apiVersion"),
			Kind:       mapString(m, "kind"),
			Name:       mapString(m, "name"),
		}

		if ref.APIVersion == "" || ref.Kind == "" || ref.Name == "" {
			return nil, fmt.Errorf("element %d is incomplete: %v", i, m)
		}

		refs = append(refs, ref)
	}

	return refs, nil
}

// mapString extracts a string value from a map[string]interface{} by key.
// Returns "" if the key is absent or the value is not a string.
func mapString(m map[string]interface{}, key string) string {
	v, _ := m[key].(string)

	return v
}
