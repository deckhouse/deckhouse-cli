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
	"k8s.io/apimachinery/pkg/types"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
)

// Node is one node in the resolved snapshot tree.
//
// All nodes in a tree share the same namespace (the root Snapshot namespace).
// Cross-namespace references are structurally impossible: SnapshotChildRef carries
// no namespace field, and the tree builder always fetches children in the root namespace.
//
// Every node is built solely from the snapshot CR's own namespaced status
// (status.sourceRef / status.data via ParseNodeStatus); the tree builder never reads
// cluster-scoped SnapshotContent. There are two flavours of node:
//   - Snapshot nodes: a snapshot CR in the tree hierarchy. A non-aggregator domain node
//     (e.g. DemoVirtualDiskSnapshot) carries its own captured volume in Data; an aggregator
//     (which has VolumeSnapshot visibility-leaf children) has Data == nil and exposes data
//     through those leaf children.
//   - Orphan leaf volume nodes: one captured standalone PVC. Kind is always "VolumeSnapshot";
//     APIVersion is "snapshot.storage.k8s.io/v1"; Data holds the captured volume; Children is
//     always nil.
type Node struct {
	// APIVersion is the apiVersion of the snapshot CR for this node
	// (e.g. "state-snapshotter.deckhouse.io/v1alpha1" or a domain-specific group).
	// Orphan leaf volume nodes use "snapshot.storage.k8s.io/v1".
	APIVersion string

	// Kind is the kind of the snapshot CR for this node
	// (e.g. "Snapshot", "DemoVirtualMachineSnapshot").
	// Orphan leaf volume nodes always have Kind == "VolumeSnapshot".
	Kind string

	// Name is the metadata.name of the snapshot CR.
	// For orphan leaf nodes it is the captured VolumeSnapshot CR name.
	Name string

	// Namespace is the namespace of the snapshot CR.
	// For the root it is the user-supplied namespace; children inherit it.
	Namespace string

	// UID is the metadata.uid of the snapshot CR. Together with APIVersion/Kind/Namespace/Name
	// it forms the node's SnapshotIdentity (see identity.go), the basis for the resume key and
	// the archive collision discriminator. The readable directory base is NOT derived from it
	// (it comes from the source name; see DirBaseName).
	UID types.UID

	// SourceRef is the identity of the original captured source object, parsed from the
	// namespaced status.sourceRef (see ParseNodeStatus). It is the readable-directory base
	// (SourceRef.Name) and the domain source identity persisted for import reconstruction.
	// Nil when the CR has no status.sourceRef (e.g. some import-mode nodes).
	SourceRef *SourceRefIdentity

	// Data is the node's captured volume payload parsed from the namespaced status.data
	// (Variant A: at most one per node), or nil for aggregators and manifest-only nodes.
	Data *NodeData

	// Parent is the parent node. Nil for the root.
	Parent *Node

	// Children are the direct child nodes: domain snapshot children first (in
	// childrenSnapshotRefs order), then orphan leaf volume children for aggregator
	// nodes (in VolumeSnapshot visibility-leaf ref order). Always nil for orphan leaf
	// volume nodes (they are leaves).
	Children []*Node
}

// Identity returns the node's structural SnapshotIdentity (apiVersion/kind/namespace/name/uid),
// the basis for the resume key and the archive collision discriminator.
func (n *Node) Identity() SnapshotIdentity {
	return SnapshotIdentity{
		APIVersion: n.APIVersion,
		Kind:       n.Kind,
		Namespace:  n.Namespace,
		Name:       n.Name,
		UID:        n.UID,
	}
}

// IsVolumeLeaf reports whether this node is a CSI VolumeSnapshot visibility-leaf
// (a captured standalone PVC exposed as a leaf child of an aggregator).
func (n *Node) IsVolumeLeaf() bool {
	return n.APIVersion == volumeSnapshotAPIVersion && n.Kind == "VolumeSnapshot"
}

// DirBaseName returns the human-readable base for this node's archive directory: the captured
// source object name (status.sourceRef.name) when present, else the snapshot CR name. Uniqueness
// and resume identity are NOT tied to this value — they use the node's SnapshotIdentity (incl UID)
// via the collision discriminator (see archive.NodeDirName / resume identity).
func (n *Node) DirBaseName() string {
	if n.SourceRef != nil && n.SourceRef.Name != "" {
		return n.SourceRef.Name
	}

	return n.Name
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
// For orphan leaf volume nodes this is also the node's own ref:
// APIVersion=snapshot.storage.k8s.io/v1, Kind=VolumeSnapshot, Name=VS CR name. The
// VolumeSnapshot connector (subresources.snapshot.storage.k8s.io) resolves this ref via
// VolumeSnapshot.status.boundSnapshotContentName to the leaf's own child SnapshotContent
// ManifestCheckpoint (which holds the captured PVC manifest).
func (n *Node) ManifestScopeRef() aggapi.NodeRef {
	return n.Ref()
}
