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
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

const (
	snapshotGroup           = "storage.deckhouse.io"
	snapshotVersion         = "v1alpha1"
	snapshotKind            = "Snapshot"
	snapshotResource        = "snapshots"
	snapshotContentResource = "snapshotcontents"
	snapshotContentKind     = "SnapshotContent"
)

// DataRef describes one volume data binding from SnapshotContent.status.dataRefs[].
type DataRef struct {
	// VSCName is the name of the cluster-scoped VolumeSnapshotContent holding the volume data.
	VSCName string
	// PVCName is the name of the original PVC that was snapshotted.
	PVCName string
	// PVCNamespace is the namespace of the original PVC.
	PVCNamespace string
}

// Node is one entry in the in-memory snapshot tree built from Kubernetes CR statuses.
type Node struct {
	// ID is the stable node identifier (<Kind>--<name>).
	ID string

	APIVersion string
	Kind       string
	// Resource is the plural lowercase resource name (e.g. "snapshots").
	Resource  string
	Name      string
	Namespace string

	// UID and ResourceVersion are populated from the object metadata.
	UID             string
	ResourceVersion string

	// BoundSnapshotContentName is status.boundSnapshotContentName of the snapshot CR.
	BoundSnapshotContentName string

	// DataRefs holds the volume data bindings from SnapshotContent.status.dataRefs[].
	// HasData is true when len(DataRefs) > 0.
	DataRefs []DataRef
	HasData  bool

	ParentID string
	Children []*Node
}

// TreeOptions controls which part of the tree is selected for download.
type TreeOptions struct {
	// NodeFilter, when non-empty, selects only the subtree rooted at this node ID.
	NodeFilter string
}

// ErrNotFound is returned when the root Snapshot CR does not exist.
type ErrNotFound struct{ Name, Namespace string }

func (e *ErrNotFound) Error() string {
	return fmt.Sprintf("Snapshot %s/%s not found; create a Snapshot first or check the name",
		e.Namespace, e.Name)
}

// ErrNotReady is returned when the Snapshot exists but is not in Ready state.
type ErrNotReady struct {
	Name, Namespace string
	Reason          string
}

func (e *ErrNotReady) Error() string {
	return fmt.Sprintf(
		"Snapshot %s/%s is not Ready (%s)\n\nTo inspect its status run:\n  d8 k -n %s get snapshot %s -o yaml",
		e.Namespace, e.Name, e.Reason, e.Namespace, e.Name,
	)
}

// BuildTree reads the root Snapshot CR and constructs the full node tree by
// recursively following status.childrenSnapshotRefs.
// Returns ErrNotFound or ErrNotReady when the snapshot is absent or unready.
func BuildTree(ctx context.Context, client ctrlrtclient.Client, namespace, snapshotName string) (*Node, error) {
	root, err := fetchSnapshotNode(ctx, client, namespace, snapshotResource, snapshotKind,
		schema.GroupVersionKind{Group: snapshotGroup, Version: snapshotVersion, Kind: snapshotKind},
		snapshotName, "")
	if err != nil {
		return nil, err
	}

	if err := populateChildren(ctx, client, root, namespace); err != nil {
		return nil, err
	}

	return root, nil
}

// SelectSubtree returns the subtree rooted at opts.NodeFilter, or the full tree when it is empty.
func SelectSubtree(root *Node, opts TreeOptions) (*Node, error) {
	if opts.NodeFilter == "" {
		return root, nil
	}

	found := findNode(root, opts.NodeFilter)
	if found == nil {
		return nil, fmt.Errorf("node %q not found in snapshot tree; run `d8 snapshot list` to see available nodes", opts.NodeFilter)
	}

	return found, nil
}

// FlatNodes returns all nodes in the subtree rooted at n in breadth-first order.
func FlatNodes(n *Node) []*Node {
	var result []*Node

	queue := []*Node{n}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		result = append(result, cur)
		queue = append(queue, cur.Children...)
	}

	return result
}

// ToNodeRecords converts a slice of Nodes to archive.NodeRecords.
func ToNodeRecords(nodes []*Node) []archive.NodeRecord {
	recs := make([]archive.NodeRecord, len(nodes))

	for i, n := range nodes {
		recs[i] = ToNodeRecord(n)
	}

	return recs
}

// ToNodeRecord converts a Node to an archive.NodeRecord for writing to nodes.jsonl.
func ToNodeRecord(n *Node) archive.NodeRecord {
	children := make([]string, 0, len(n.Children))

	for _, c := range n.Children {
		children = append(children, c.ID)
	}

	dataRefs := make([]archive.VolumeDataRef, 0, len(n.DataRefs))
	for _, dr := range n.DataRefs {
		dataRefs = append(dataRefs, archive.VolumeDataRef{
			VSCName:      dr.VSCName,
			PVCName:      dr.PVCName,
			PVCNamespace: dr.PVCNamespace,
		})
	}

	return archive.NodeRecord{
		ID:                       n.ID,
		APIVersion:               n.APIVersion,
		Kind:                     n.Kind,
		Name:                     n.Name,
		Namespace:                n.Namespace,
		ParentID:                 n.ParentID,
		Children:                 children,
		BoundSnapshotContentName: n.BoundSnapshotContentName,
		DataRefs:                 dataRefs,
		HasData:                  n.HasData,
	}
}

// fetchSnapshotNode loads a single snapshot-like CR and builds a Node.
func fetchSnapshotNode(ctx context.Context, client ctrlrtclient.Client,
	namespace, resource, kind string, gvk schema.GroupVersionKind, name, parentID string,
) (*Node, error) {
	obj := new(unstructured.Unstructured)
	obj.SetGroupVersionKind(gvk)

	err := client.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: name}, obj)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, &ErrNotFound{Name: name, Namespace: namespace}
		}

		return nil, fmt.Errorf("get %s %s/%s: %w", kind, namespace, name, err)
	}

	// Only the root snapshot must be Ready; children are informational.
	if parentID == "" {
		if reason, ready := checkReady(obj); !ready {
			return nil, &ErrNotReady{Name: name, Namespace: namespace, Reason: reason}
		}
	}

	boundContent, _, _ := unstructured.NestedString(obj.Object, "status", "boundSnapshotContentName")

	var dataRefs []DataRef
	if boundContent != "" {
		dataRefs, _ = fetchDataRefs(ctx, client, boundContent)
	}

	node := &Node{
		ID:                       archive.NodeID(kind, name),
		APIVersion:               gvk.GroupVersion().String(),
		Kind:                     kind,
		Resource:                 resource,
		Name:                     name,
		Namespace:                namespace,
		UID:                      string(obj.GetUID()),
		ResourceVersion:          obj.GetResourceVersion(),
		BoundSnapshotContentName: boundContent,
		DataRefs:                 dataRefs,
		HasData:                  len(dataRefs) > 0,
		ParentID:                 parentID,
	}

	return node, nil
}

// fetchDataRefs reads SnapshotContent.status.dataRefs[] and returns the bindings.
// Errors are silently suppressed so a missing or unready SnapshotContent does not
// block manifest download.
func fetchDataRefs(ctx context.Context, client ctrlrtclient.Client, snapshotContentName string) ([]DataRef, error) {
	contentGVK := schema.GroupVersionKind{
		Group:   snapshotGroup,
		Version: snapshotVersion,
		Kind:    snapshotContentKind,
	}

	obj := new(unstructured.Unstructured)
	obj.SetGroupVersionKind(contentGVK)

	if err := client.Get(ctx, ctrlrtclient.ObjectKey{Name: snapshotContentName}, obj); err != nil {
		return nil, fmt.Errorf("get SnapshotContent %s: %w", snapshotContentName, err)
	}

	rawRefs, _, _ := unstructured.NestedSlice(obj.Object, "status", "dataRefs")
	if len(rawRefs) == 0 {
		return nil, nil
	}

	refs := make([]DataRef, 0, len(rawRefs))
	for _, raw := range rawRefs {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		targetMap, _, _ := unstructured.NestedMap(m, "target")

		artifactMap, _, _ := unstructured.NestedMap(m, "artifact")
		if targetMap == nil || artifactMap == nil {
			continue
		}

		artifactKind, _ := artifactMap["kind"].(string)
		if artifactKind != "VolumeSnapshotContent" {
			continue
		}

		vscName, _ := artifactMap["name"].(string)
		pvcName, _ := targetMap["pvcName"].(string)
		pvcNS, _ := targetMap["pvcNamespace"].(string)

		if vscName == "" {
			continue
		}

		refs = append(refs, DataRef{
			VSCName:      vscName,
			PVCName:      pvcName,
			PVCNamespace: pvcNS,
		})
	}

	return refs, nil
}

// populateChildren recursively fetches children listed in status.childrenSnapshotRefs.
func populateChildren(ctx context.Context, client ctrlrtclient.Client, parent *Node, namespace string) error {
	refs, found, err := unstructured.NestedSlice(getUnstructuredObject(ctx, client, parent), "status", "childrenSnapshotRefs")
	if err != nil || !found {
		return nil
	}

	for _, raw := range refs {
		refMap, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		childAPIVersion, _ := refMap["apiVersion"].(string)
		childKind, _ := refMap["kind"].(string)
		childName, _ := refMap["name"].(string)

		if childKind == "" || childName == "" {
			continue
		}

		childGVK, childResource, err := resolveGVK(client, childAPIVersion, childKind)
		if err != nil {
			// Unknown type: record a minimal node without descending.
			child := &Node{
				ID:         archive.NodeID(childKind, childName),
				APIVersion: childAPIVersion,
				Kind:       childKind,
				Resource:   strings.ToLower(childKind) + "s",
				Name:       childName,
				Namespace:  namespace,
				ParentID:   parent.ID,
			}

			parent.Children = append(parent.Children, child)

			continue
		}

		child, err := fetchSnapshotNode(ctx, client, namespace, childResource, childKind, childGVK, childName, parent.ID)
		if err != nil {
			return err
		}

		parent.Children = append(parent.Children, child)

		if err := populateChildren(ctx, client, child, namespace); err != nil {
			return err
		}
	}

	return nil
}

// getUnstructuredObject re-fetches the CR as a raw map for status field access.
func getUnstructuredObject(ctx context.Context, client ctrlrtclient.Client, n *Node) map[string]any {
	gv, err := schema.ParseGroupVersion(n.APIVersion)
	if err != nil {
		return nil
	}

	obj := new(unstructured.Unstructured)
	obj.SetGroupVersionKind(gv.WithKind(n.Kind))

	if err := client.Get(ctx, ctrlrtclient.ObjectKey{Namespace: n.Namespace, Name: n.Name}, obj); err != nil {
		return nil
	}

	return obj.Object
}

// resolveGVK uses the REST mapper to resolve Kind -> GVK + plural resource name.
func resolveGVK(client ctrlrtclient.Client, apiVersion, kind string) (schema.GroupVersionKind, string, error) {
	mapper := client.RESTMapper()
	if mapper == nil {
		return schema.GroupVersionKind{}, "", fmt.Errorf("no REST mapper available")
	}

	var gvk schema.GroupVersionKind

	if apiVersion != "" {
		gv, err := schema.ParseGroupVersion(apiVersion)
		if err != nil {
			return schema.GroupVersionKind{}, "", fmt.Errorf("parse group version %q: %w", apiVersion, err)
		}

		gvk = gv.WithKind(kind)
	}

	mappings, err := mapper.RESTMappings(schema.GroupKind{Group: gvk.Group, Kind: kind})
	if err != nil || len(mappings) == 0 {
		return schema.GroupVersionKind{}, "", fmt.Errorf("no REST mapping for %s/%s: %w", apiVersion, kind, err)
	}

	// Prefer the mapping whose version matches the ref.
	var chosen *meta.RESTMapping

	for _, m := range mappings {
		if gvk.Version == "" || m.GroupVersionKind.Version == gvk.Version {
			chosen = m

			break
		}
	}

	if chosen == nil {
		chosen = mappings[0]
	}

	return chosen.GroupVersionKind, chosen.Resource.Resource, nil
}

// checkReady returns (reason, true) when the object has condition Ready=True
// and a non-empty status.boundSnapshotContentName.
func checkReady(obj *unstructured.Unstructured) (string, bool) {
	bound, _, _ := unstructured.NestedString(obj.Object, "status", "boundSnapshotContentName")
	if bound == "" {
		return "boundSnapshotContentName is empty", false
	}

	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")

	for _, raw := range conditions {
		c, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		condType, _ := c["type"].(string)
		if condType != "Ready" {
			continue
		}

		status, _ := c["status"].(string)
		if status == "True" {
			return "", true
		}

		msg, _ := c["message"].(string)
		reason2, _ := c["reason"].(string)

		if msg != "" {
			return msg, false
		}

		return reason2, false
	}

	return "Ready condition not found", false
}

// findNode does a DFS to locate the node with the given ID.
func findNode(n *Node, id string) *Node {
	if n.ID == id {
		return n
	}

	for _, c := range n.Children {
		if found := findNode(c, id); found != nil {
			return found
		}
	}

	return nil
}
