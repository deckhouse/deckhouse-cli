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
type Node struct {
	// APIVersion is the apiVersion of the snapshot CR for this node
	// (e.g. "storage.deckhouse.io/v1alpha1" or a domain-specific group).
	APIVersion string

	// Kind is the kind of the snapshot CR for this node
	// (e.g. "Snapshot", "DemoVirtualMachineSnapshot").
	Kind string

	// Name is the metadata.name of the snapshot CR.
	Name string

	// Namespace is the namespace of the snapshot CR.
	// For the root it is the user-supplied namespace; children inherit it.
	Namespace string

	// ManifestCheckpointName is the cluster-scoped ManifestCheckpoint name for
	// this node's own-scope manifests. Empty if no manifest capture ran.
	ManifestCheckpointName string

	// DataRefs holds the volume-to-artifact bindings from the bound SnapshotContent.
	// The tree builder enforces that len(DataRefs) <= 1 (ErrMultipleVolumes otherwise).
	DataRefs []snapshotapi.SnapshotDataBinding

	// Parent is the parent node. Nil for the root.
	Parent *Node

	// Children are the direct child nodes in the order they appear in childrenSnapshotRefs.
	Children []*Node
}
