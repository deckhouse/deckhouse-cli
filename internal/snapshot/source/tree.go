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

// ErrMultipleVolumes is returned when a snapshot node has more than one dataRefs entry.
// The one-volume-per-node invariant is enforced at tree-build time.
var ErrMultipleVolumes = errors.New("node has more than one data volume")

// rootAPIVersion is the apiVersion of the root Snapshot CR.
const rootAPIVersion = snapshotapi.StorageGroup + "/" + snapshotapi.Version

// BuildTree fetches the root Snapshot by name and recursively resolves the full snapshot
// tree by following status.childrenSnapshotRefs.
//
// Each node's SnapshotContent is resolved via status.boundSnapshotContentName; the
// ManifestCheckpointName and DataRefs are taken from the content's status.
//
// All nodes are namespace-local: child refs carry no namespace field and are always
// fetched in the same namespace as the root. The function does one typed Get per node
// and never lists.
//
// Returns ErrCycle if a duplicate ref is encountered, ErrMultipleVolumes if any node
// has more than one dataRefs entry.
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

	content := &snapshotapi.SnapshotContent{}
	if err := b.client.Get(ctx, types.NamespacedName{Name: boundContentName}, content); err != nil {
		return nil, fmt.Errorf("fetch SnapshotContent %q for %s %s/%s: %w", boundContentName, apiVersion, kind, name, err)
	}

	if len(content.Status.DataRefs) > 1 {
		return nil, fmt.Errorf("node %s/%s (content %q): %w", kind, name, boundContentName, ErrMultipleVolumes)
	}

	node := &Node{
		APIVersion:             apiVersion,
		Kind:                   kind,
		Name:                   name,
		Namespace:              b.namespace,
		ManifestCheckpointName: content.Status.ManifestCheckpointName,
		DataRefs:               content.Status.DataRefs,
		Parent:                 parent,
	}

	childRefs, err := extractChildRefs(obj)
	if err != nil {
		return nil, fmt.Errorf("%s %s/%s: status.childrenSnapshotRefs: %w", apiVersion, kind, name, err)
	}

	node.Children = make([]*Node, 0, len(childRefs))

	for _, ref := range childRefs {
		child, err := b.visit(ctx, ref.APIVersion, ref.Kind, ref.Name, node)
		if err != nil {
			return nil, err
		}

		node.Children = append(node.Children, child)
	}

	return node, nil
}

// fetchUnstructured fetches any namespaced Kubernetes object by apiVersion/kind/name
// using an unstructured.Unstructured so that the client scheme does not need to know
// the domain-specific type (e.g. DemoVirtualMachineSnapshot).
func fetchUnstructured(ctx context.Context, c client.Client, namespace, apiVersion, kind, name string) (*unstructured.Unstructured, error) {
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return nil, fmt.Errorf("parse apiVersion %q: %w", apiVersion, err)
	}

	obj := &unstructured.Unstructured{}
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
