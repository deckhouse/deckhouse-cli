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

// Package source provides the snapshot tree model and the BuildTree function
// that resolves a Snapshot hierarchy from the Kubernetes API.
package source

import (
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
)

// SpecSourceRef is the structured spec.sourceRef from a domain snapshot CR,
// preserved so the import side can recreate the CR in import mode.
// The namespace of the source object is implicit (same as the snapshot's namespace).
type SpecSourceRef struct {
	// APIVersion is the apiVersion of the source object.
	APIVersion string
	// Kind is the kind of the source object.
	Kind string
	// Name is the metadata.name of the source object.
	Name string
}

// Node is one node in the resolved snapshot tree.
//
// All nodes in a tree share the same namespace (the root Snapshot namespace).
// Cross-namespace references are structurally impossible: SnapshotChildRef carries
// no namespace field, and the tree builder always fetches children in the root namespace.
//
// There are two flavours of node:
//   - Snapshot nodes (Binding == nil): represent a snapshot CR in the tree hierarchy.
//     They carry OwnDataRefs when the node owns its volume data directly (non-aggregator
//     domain nodes such as DemoVirtualDiskSnapshot). When the node is an aggregator (has
//     VolumeSnapshot visibility-leaf children), OwnDataRefs is nil and the data is
//     represented by orphan leaf children in the Children slice.
//   - Orphan leaf volume nodes (Binding != nil): represent one captured standalone PVC.
//     Kind is always "VolumeSnapshot"; APIVersion is "snapshot.storage.k8s.io/v1".
//     OwnDataRefs and Children are always nil. These are leaves in the tree.
type Node struct {
	// APIVersion is the apiVersion of the snapshot CR for this node
	// (e.g. "storage.deckhouse.io/v1alpha1" or a domain-specific group).
	// Orphan leaf volume nodes use "snapshot.storage.k8s.io/v1".
	APIVersion string

	// Kind is the kind of the snapshot CR for this node
	// (e.g. "Snapshot", "DemoVirtualMachineSnapshot").
	// Orphan leaf volume nodes always have Kind == "VolumeSnapshot".
	Kind string

	// Name is the metadata.name of the snapshot CR.
	// For orphan leaf nodes it is the captured PVC name (dataRef.Target.Name).
	Name string

	// Namespace is the namespace of the snapshot CR.
	// For the root it is the user-supplied namespace; children inherit it.
	Namespace string

	// SourceRef is the value of the state-snapshotter.deckhouse.io/source-ref
	// annotation on the snapshot CR. It records the identity of the original
	// captured object. Empty when the annotation is absent (typically the root).
	// For orphan leaf volume nodes it is set to the binding's TargetUID (the captured PVC UID).
	// Resume identity and checksums use this raw value; directory naming uses SourceName.
	SourceRef string

	// SourceName is the .name field from the source-ref annotation, identifying
	// the original captured object by its Kubernetes metadata.name.
	// For domain snapshot nodes it is parsed from SourceRef (best-effort; empty on parse error).
	// For orphan leaf volume nodes it is set to the captured PVC name (Binding.Target.Name).
	// Empty for the root node (which carries no source-ref annotation).
	SourceName string

	// SpecSourceRef is the structured spec.sourceRef from a domain snapshot CR
	// (apiVersion/kind/name of the source object). Set for domain snapshot nodes when the
	// CR carries a spec.sourceRef; nil for core Snapshot nodes and CSI VolumeSnapshot
	// leaf nodes (which do not need spec.sourceRef for import reconstruction).
	SpecSourceRef *SpecSourceRef

	// OwnDataRefs holds the volume-to-artifact bindings for this non-aggregator snapshot node.
	// The volume data for each entry is downloaded directly into this node's directory.
	// Nil for aggregator nodes (which expose their volumes through orphan leaf children)
	// and nil for orphan leaf volume nodes (which use Binding instead).
	OwnDataRefs []snapshotapi.SnapshotDataBinding

	// Binding is non-nil for orphan leaf volume nodes only. It carries the SnapshotDataBinding
	// resolved from the child SnapshotContent (the VolumeSnapshot's own bound content, Variant A).
	// Modifications to the source content do not affect this copy.
	Binding *snapshotapi.SnapshotDataBinding

	// Parent is the parent node. Nil for the root.
	Parent *Node

	// Children are the direct child nodes: domain snapshot children first (in
	// childrenSnapshotRefs order), then orphan leaf volume children for aggregator
	// nodes (in VolumeSnapshot visibility-leaf ref order). Always nil for orphan leaf
	// volume nodes (they are leaves).
	Children []*Node
}

// Ref returns the aggregated-API node reference that addresses this node's own
// manifests-download subresource.
func (n *Node) Ref() aggapi.NodeRef {
	return aggapi.NodeRef{
		APIVersion: n.APIVersion,
		Kind:       n.Kind,
		Name:       n.Name,
		Namespace:  n.Namespace,
	}
}

// ManifestScopeRef returns the aggregated-API node reference used to fetch this node's
// own-scope manifests (manifests-download subresource).
//
// For domain snapshot nodes this is the node's own ref (its snapshot CR identity →
// own ManifestCheckpoint).
//
// For orphan leaf volume nodes (Binding != nil) this is also the node's own ref:
// APIVersion=snapshot.storage.k8s.io/v1, Kind=VolumeSnapshot, Name=VS CR name. The
// VolumeSnapshot connector (subresources.snapshot.storage.k8s.io) resolves this ref via
// VolumeSnapshot.status.boundSnapshotContentName to the leaf's own child SnapshotContent
// ManifestCheckpoint (which holds the captured PVC manifest).
func (n *Node) ManifestScopeRef() aggapi.NodeRef {
	return n.Ref()
}
