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
	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
)

// Node is one node in the resolved snapshot tree.
//
// All nodes in a tree share the same namespace (the root Snapshot namespace).
// Cross-namespace references are structurally impossible: SnapshotChildRef carries
// no namespace field, and the tree builder always fetches children in the root namespace.
//
// There are two flavours of node:
//   - Snapshot nodes (Binding == nil): represent a snapshot CR in the tree hierarchy.
//     Their DataRefs are the raw volume bindings from the bound SnapshotContent; each
//     binding materialises as a separate VolumeSnapshot child node under snapshots/.
//   - Volume nodes (Binding != nil): represent one captured volume. Kind is always
//     "VolumeSnapshot"; Name is naming.ShadowName(binding.Artifact.Name); DataRefs and
//     Children are always nil. These are leaves in the tree.
type Node struct {
	// APIVersion is the apiVersion of the snapshot CR for this node
	// (e.g. "storage.deckhouse.io/v1alpha1" or a domain-specific group).
	// Volume nodes use "snapshot.storage.k8s.io/v1".
	APIVersion string

	// Kind is the kind of the snapshot CR for this node
	// (e.g. "Snapshot", "DemoVirtualMachineSnapshot").
	// Volume nodes always have Kind == "VolumeSnapshot".
	Kind string

	// Name is the metadata.name of the snapshot CR.
	Name string

	// Namespace is the namespace of the snapshot CR.
	// For the root it is the user-supplied namespace; children inherit it.
	Namespace string

	// SourceRef is the value of the state-snapshotter.deckhouse.io/source-ref
	// annotation on the snapshot CR. It records the identity of the original
	// captured object. Empty when the annotation is absent (typically the root).
	// For volume nodes it is set to the binding's TargetUID (the captured PVC UID).
	SourceRef string

	// ManifestCheckpointName is the cluster-scoped ManifestCheckpoint name for
	// this node's own-scope manifests. Empty if no manifest capture ran.
	// For volume nodes it is the PARENT snapshot node's checkpoint name (the
	// captured PVC manifest lives in the parent's checkpoint).
	ManifestCheckpointName string

	// DataRefs holds the volume-to-artifact bindings from the bound SnapshotContent.
	// Each binding is materialised as a separate VolumeSnapshot child node; the
	// DataRefs slice is retained on the snapshot node for manifest routing.
	// Nil for volume nodes.
	DataRefs []snapshotapi.SnapshotDataBinding

	// Binding is non-nil for volume nodes only. It carries the SnapshotDataBinding
	// that gave rise to this volume node (copied from the parent's DataRefs slice so
	// that modifications to the source slice do not affect the tree).
	Binding *snapshotapi.SnapshotDataBinding

	// Parent is the parent node. Nil for the root.
	Parent *Node

	// Children are the direct child nodes: snapshot children first (in
	// childrenSnapshotRefs order), then volume children (in DataRefs order).
	// Always nil for volume nodes (they are leaves).
	Children []*Node
}
