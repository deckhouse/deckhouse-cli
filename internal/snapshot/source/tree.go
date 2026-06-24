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

// rootAPIVersion is the apiVersion of the root Snapshot CR.
const rootAPIVersion = snapshotapi.StorageGroup + "/" + snapshotapi.Version

// volumeSnapshotAPIVersion is the apiVersion of CSI VolumeSnapshot visibility-leaf nodes.
const volumeSnapshotAPIVersion = "snapshot.storage.k8s.io/v1"

// BuildTree fetches the root Snapshot by name and recursively resolves the full snapshot
// tree by following status.childrenSnapshotRefs.
//
// Each node's SnapshotContent is resolved via status.boundSnapshotContentName; the
// single DataRef (Variant A, cardinality ≤1) is taken from the content's status.DataRef
// via DataRefList(). Node manifests are fetched separately via the aggregated
// manifests-download subresource.
//
// All snapshot nodes are namespace-local: child refs carry no namespace field and are
// always fetched in the same namespace as the root. The function does one typed Get per
// node and never lists.
//
// childrenSnapshotRefs are partitioned into two sets:
//   - Domain refs (apiVersion != "snapshot.storage.k8s.io/v1" or kind != "VolumeSnapshot")
//     are recursed normally via boundSnapshotContentName.
//   - VolumeSnapshot visibility-leaf refs (apiVersion == "snapshot.storage.k8s.io/v1" and
//     kind == "VolumeSnapshot") are NOT fetched or recursed. Their presence signals that
//     this node is an aggregator: content.DataRefList() (0 or 1 binding) produces the
//     orphan leaf volume node(s). Under Variant A the aggregator content keeps DataRef=nil,
//     so no orphan leaves are produced here; C2 (datarefs-leaf-resolve) replaces this
//     aggregator branch with the VS-lookup model.
//
// When a node has no visibility-leaf children, its content.DataRefList() (0 or 1 binding)
// is stored in OwnDataRefs and no leaf children are created (data lives in the node's dir).
//
// Returns ErrCycle if a duplicate snapshot ref is encountered.
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

	boundContentName, err := nestedStringField(obj, "status", "boundSnapshotContentName")
	if err != nil {
		return nil, fmt.Errorf("%s %s/%s: status.boundSnapshotContentName: %w", apiVersion, kind, name, err)
	}

	if boundContentName == "" {
		return nil, fmt.Errorf("%s %s/%s: status.boundSnapshotContentName is empty (not yet bound)", apiVersion, kind, name)
	}

	content := new(snapshotapi.SnapshotContent)
	if err := b.client.Get(ctx, types.NamespacedName{Name: boundContentName}, content); err != nil {
		return nil, fmt.Errorf("fetch SnapshotContent %q for %s %s/%s: %w", boundContentName, apiVersion, kind, name, err)
	}

	sourceRef := obj.GetAnnotations()[snapshotapi.AnnotationSourceRef]

	// Parse the source-ref annotation to extract the source object name (best-effort).
	var sourceName string
	if id, err := ParseSourceRef(sourceRef); err == nil {
		sourceName = id.Name
	}

	node := &Node{
		APIVersion: apiVersion,
		Kind:       kind,
		Name:       name,
		Namespace:  b.namespace,
		SourceRef:  sourceRef,
		SourceName: sourceName,
		Parent:     parent,
	}

	allChildRefs, err := extractChildRefs(obj)
	if err != nil {
		return nil, fmt.Errorf("%s %s/%s: status.childrenSnapshotRefs: %w", apiVersion, kind, name, err)
	}

	// Partition childRefs into domain refs (to recurse) and visibility-leaf refs (discriminator only).
	domainRefs, hasVisibilityLeaves := partitionChildRefs(allChildRefs)

	dataRefs := content.DataRefList()
	node.Children = make([]*Node, 0, len(domainRefs)+len(dataRefs))

	for _, ref := range domainRefs {
		child, err := b.visit(ctx, ref.APIVersion, ref.Kind, ref.Name, node)
		if err != nil {
			return nil, err
		}

		node.Children = append(node.Children, child)
	}

	if hasVisibilityLeaves {
		// Aggregator: expose each dataRef (0 or 1 under Variant A) as an orphan leaf
		// volume node. OwnDataRefs stays nil; volume data is addressed via leaf.Binding.
		// Under the real Variant A contract the aggregator content keeps DataRef=nil, so
		// dataRefs is empty here and no orphan leaves are created. The datarefs-leaf-resolve
		// task will replace this branch with the VS-lookup model (C2).
		for i := range dataRefs {
			binding := dataRefs[i]

			leafNode := &Node{
				APIVersion: volumeSnapshotAPIVersion,
				Kind:       "VolumeSnapshot",
				Name:       binding.Target.Name,
				Namespace:  b.namespace,
				SourceRef:  binding.TargetUID,
				SourceName: binding.Target.Name,
				Parent:     node,
				Binding:    &binding,
			}

			node.Children = append(node.Children, leafNode)
		}

		return node, nil
	}

	// Non-aggregator: data lives directly in this node.
	// Copy the slice so callers cannot alias the DataRefList result.
	if len(dataRefs) > 0 {
		own := make([]snapshotapi.SnapshotDataBinding, len(dataRefs))
		copy(own, dataRefs)
		node.OwnDataRefs = own
	}

	return node, nil
}

// partitionChildRefs splits childRefs into domain refs (to recurse) and reports whether
// any VolumeSnapshot visibility-leaf refs were found. Visibility-leaf refs are identified
// by apiVersion == "snapshot.storage.k8s.io/v1" and kind == "VolumeSnapshot".
// They are NOT included in the returned slice and are never fetched.
func partitionChildRefs(refs []snapshotapi.SnapshotChildRef) ([]snapshotapi.SnapshotChildRef, bool) {
	domain := make([]snapshotapi.SnapshotChildRef, 0, len(refs))
	hasLeaves := false

	for _, ref := range refs {
		if isVisibilityLeaf(ref) {
			hasLeaves = true
		} else {
			domain = append(domain, ref)
		}
	}

	return domain, hasLeaves
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

// nestedStringField returns the string value at the given field path from an
// unstructured object, or "" if the path does not exist.
func nestedStringField(obj *unstructured.Unstructured, fields ...string) (string, error) {
	val, found, err := unstructured.NestedString(obj.Object, fields...)
	if err != nil {
		return "", err
	}

	if !found {
		return "", nil
	}

	return val, nil
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
