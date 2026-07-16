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
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// ArtifactRef points to a durable data artifact (e.g. a VolumeSnapshotContent). It mirrors
// state-snapshotter api/storage/v1alpha1 SnapshotDataArtifactRef. UID is best-effort: the core
// fills it once known, so it is optional in the wire form and validated only on the data path.
type ArtifactRef struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid,omitempty"`
}

// NodeData is the decoded namespaced status.data descriptor: a self-contained
// {source, artifact, volume metadata} block the core mirrors onto every snapshot node.
// Variant A (cardinality ≤1): a node carries at most one data binding; multiple volumes are
// modeled as child volume nodes.
//
// It mirrors state-snapshotter api/storage/v1alpha1 SnapshotDataBinding (source-based). This is
// the correct, current contract; the legacy target/targetUID/dataRef shape in
// internal/snapshot/api/v1alpha1 is outdated and is retired during Stage 2b when the tree
// builder switches to ParseNodeStatus.
type NodeData struct {
	// Source identifies the captured PersistentVolumeClaim backing this node's data. Its uid is
	// the single volume identity (state-snapshotter dropped the standalone targetUID).
	Source SourceRefIdentity `json:"source"`
	// Artifact references the cluster-scoped durable data artifact.
	Artifact ArtifactRef `json:"artifact"`
	// VolumeMode is the source volume mode (Block or Filesystem).
	VolumeMode string `json:"volumeMode,omitempty"`
	// FsType is the source filesystem type (Filesystem volumes only).
	FsType string `json:"fsType,omitempty"`
	// AccessModes records the source PVC access modes.
	AccessModes []string `json:"accessModes,omitempty"`
	// StorageClassName records the source StorageClass of the captured volume.
	StorageClassName string `json:"storageClassName,omitempty"`
	// Size is the allocated size of the captured volume as a resource.Quantity string (e.g. "10Gi").
	Size string `json:"size,omitempty"`
}

// ParseNodeStatus decodes a snapshot node's identity plus its self-contained namespaced status
// fragments (status.sourceRef and status.data) directly from the unstructured object, without
// ever reading cluster-scoped SnapshotContent. It is the single reader d8 uses to build the
// tree from the namespaced API (see docs/2026-06-29-unified-snapshots-overview.md).
//
// It is fail-closed: an absent status.sourceRef or status.data is allowed (returns nil), but a
// present-yet-malformed fragment is a hard error (never silently treated as "no data"). Both
// fragments must be JSON objects with their required identity fields set; status.data.size, when
// present, must parse as a quantity. status.sourceRef is a full provenance identity, so its uid is
// REQUIRED here; its namespace is required for namespaced source kinds but intentionally absent
// for the cluster-scoped root source (v1/Namespace), so it is validated per source scope (see
// parseStatusSourceRef) rather than unconditionally. The data leg's completeness
// (status.data.source.uid, artifact identity) is enforced separately by the data path
// (RequireNodeData) on a Ready node.
//
// The node's own SnapshotIdentity is validated up front: it feeds the resume key, checksum/index
// and the collision discriminator, so a weak (partially empty) identity would silently corrupt
// those. Every snapshot node is namespaced in Stage 2, hence metadata.namespace is required here.
func ParseNodeStatus(obj *unstructured.Unstructured) (SnapshotIdentity, *SourceRefIdentity, *NodeData, error) {
	ident := SnapshotIdentity{
		APIVersion: obj.GetAPIVersion(),
		Kind:       obj.GetKind(),
		Namespace:  obj.GetNamespace(),
		Name:       obj.GetName(),
		UID:        obj.GetUID(),
	}

	if ident.APIVersion == "" || ident.Kind == "" || ident.Namespace == "" || ident.Name == "" || ident.UID == "" {
		return ident, nil, nil, fmt.Errorf("%s: snapshot identity is incomplete (apiVersion/kind/namespace/name/uid required)", objRefString(obj))
	}

	src, err := parseStatusSourceRef(obj)
	if err != nil {
		return ident, nil, nil, err
	}

	data, err := parseStatusData(obj)
	if err != nil {
		return ident, nil, nil, err
	}

	return ident, src, data, nil
}

// RequireNodeData returns the node's captured data descriptor for the data path, or an error
// when the node carries no status.data. Callers that legitimately have no data (aggregators,
// manifest-only nodes) must NOT call this: it enforces the data-bearing invariant only where a
// volume payload is expected. It additionally enforces full completeness (source.uid, artifact
// identity), which is guaranteed only on a Ready node.
func RequireNodeData(node *Node) (*NodeData, error) {
	if node == nil || node.Data == nil {
		return nil, fmt.Errorf("%s carries no status.data but a captured volume was expected", nodeIdentString(node))
	}

	if node.Data.Source.UID == "" {
		return nil, fmt.Errorf("%s status.data.source.uid is empty", nodeIdentString(node))
	}

	if node.Data.Artifact.APIVersion == "" || node.Data.Artifact.Kind == "" || node.Data.Artifact.Name == "" {
		return nil, fmt.Errorf("%s status.data.artifact is incomplete (apiVersion/kind/name required)", nodeIdentString(node))
	}

	return node.Data, nil
}

func parseStatusSourceRef(obj *unstructured.Unstructured) (*SourceRefIdentity, error) {
	m, found, err := unstructured.NestedMap(obj.Object, "status", "sourceRef")
	if err != nil {
		return nil, fmt.Errorf("%s: status.sourceRef is not an object: %w", objRefString(obj), err)
	}
	if !found {
		return nil, nil
	}

	var id SourceRefIdentity
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(m, &id); err != nil {
		return nil, fmt.Errorf("%s: decode status.sourceRef: %w", objRefString(obj), err)
	}

	if id.APIVersion == "" || id.Kind == "" || id.Name == "" || id.UID == "" {
		return nil, fmt.Errorf("%s: status.sourceRef is incomplete (apiVersion/kind/name/uid required)", objRefString(obj))
	}
	if sourceRefRequiresNamespace(id) && id.Namespace == "" {
		return nil, fmt.Errorf("%s: status.sourceRef.namespace is required for %s %s", objRefString(obj), id.APIVersion, id.Kind)
	}

	return &id, nil
}

// sourceRefRequiresNamespace reports whether a status.sourceRef of the given kind must carry a
// namespace. The root capture-Snapshot's source is the cluster-scoped Namespace (v1/Namespace),
// whose sourceRef legitimately has no namespace (per docs/2026-06-29-unified-snapshots-overview.md);
// every other source kind supported in Stage 2 is namespaced. Other cluster-scoped source kinds
// are out of scope for Stage 2.
func sourceRefRequiresNamespace(id SourceRefIdentity) bool {
	return !(id.APIVersion == "v1" && id.Kind == "Namespace")
}

func parseStatusData(obj *unstructured.Unstructured) (*NodeData, error) {
	m, found, err := unstructured.NestedMap(obj.Object, "status", "data")
	if err != nil {
		return nil, fmt.Errorf("%s: status.data is not an object: %w", objRefString(obj), err)
	}
	if !found {
		return nil, nil
	}

	var d NodeData
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(m, &d); err != nil {
		return nil, fmt.Errorf("%s: decode status.data: %w", objRefString(obj), err)
	}

	if d.Source.APIVersion == "" || d.Source.Kind == "" || d.Source.Name == "" {
		return nil, fmt.Errorf("%s: status.data.source is incomplete (apiVersion/kind/name required)", objRefString(obj))
	}
	if d.Artifact.APIVersion == "" || d.Artifact.Kind == "" || d.Artifact.Name == "" {
		return nil, fmt.Errorf("%s: status.data.artifact is incomplete (apiVersion/kind/name required)", objRefString(obj))
	}
	if d.Size != "" {
		if _, err := resource.ParseQuantity(d.Size); err != nil {
			return nil, fmt.Errorf("%s: status.data.size %q is not a valid quantity: %w", objRefString(obj), d.Size, err)
		}
	}

	return &d, nil
}

func objRefString(obj *unstructured.Unstructured) string {
	return fmt.Sprintf("%s %s/%s", obj.GetKind(), obj.GetNamespace(), obj.GetName())
}

func nodeIdentString(node *Node) string {
	if node == nil {
		return "<nil node>"
	}
	return fmt.Sprintf("%s %s/%s", node.Kind, node.Namespace, node.Name)
}
