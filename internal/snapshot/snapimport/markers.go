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

package snapimport

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
)

const (
	// snapshotKind is the core structural snapshot CR.
	snapshotKind = "Snapshot"
	// volumeSnapshotKind is the CSI VolumeSnapshot data leaf (extended connector fork).
	volumeSnapshotKind = aggapi.VolumeSnapshotKind
)

// isStructural reports whether the node is a core Snapshot (no own volume data; carries
// child refs).
func (n PlannedNode) isStructural() bool {
	return n.Kind == snapshotKind
}

// isVolumeSnapshotLeaf reports whether the node is a CSI VolumeSnapshot data leaf.
func (n PlannedNode) isVolumeSnapshotLeaf() bool {
	return n.Ref("").IsVolumeSnapshotLeaf()
}

// hasChildren reports whether the node carries any direct child snapshot refs.
func (n PlannedNode) hasChildren() bool {
	return len(n.Children) > 0
}

// canBeImportRoot reports whether the node can be the independent root of an import: a
// core Snapshot tree, a CSI VolumeSnapshot data leaf, or a domain data leaf. These kinds
// are either materialised by the namespace Snapshot import orchestrator (core Snapshot) or
// carry their own volume data (leaves), so the CLI can drive them with no parent present.
// Domain aggregators and manifest-only domain nodes are intentionally excluded: imported
// standalone they have no parent SnapshotContent to attach to (the server's genericbinder
// skips root generic imports), so they can only be reconstructed as non-root nodes within
// their parent tree (a full-archive import, or a --node selection of an ancestor Snapshot).
func (n PlannedNode) canBeImportRoot() bool {
	return n.isStructural() || n.isVolumeSnapshotLeaf() || n.isDomainDataLeaf()
}

// isDomainAggregator reports whether the node is a domain aggregator: a domain-kind
// snapshot that carries no own volume data but DOES reference child snapshots (e.g. an
// intermediate DemoVirtualMachineSnapshot fanning out to child DemoVirtualDiskSnapshot
// nodes). Aggregators are reconstructed server-side: the CLI creates the unified import
// marker and uploads the node's manifests + child refs, then the genericbinder aggregates
// the children's SnapshotContents into the aggregator's content. This is fully client-
// drivable as a NON-ROOT node, so an aggregator is importable as part of its parent tree;
// it just cannot be selected as a standalone --node root (see canBeImportRoot). A domain
// node with neither data nor children is a manifest-only node, not an aggregator.
func (n PlannedNode) isDomainAggregator() bool {
	return !n.isStructural() && !n.isVolumeSnapshotLeaf() && !n.isDomainDataLeaf() && n.hasChildren()
}

// importMarkerCR builds the minimal import-mode CR the server requires to exist before
// it will accept a manifests-and-children-refs-upload for this node.
//
// Every node kind uses the same unified marker: spec.mode: Import. The server keys import
// mode off this field. Core Snapshot trees and domain aggregators are materialised
// server-side from the uploaded manifests + child refs (the genericbinder aggregates the
// children's SnapshotContents); data leaves additionally stream their volume bytes, matched
// to their DataImport by a reverse-lookup on the DataImport's targetRef (group/kind/name);
// manifest-only domain nodes materialise from manifests alone.
func importMarkerCR(node PlannedNode, namespace string) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(node.APIVersion)
	obj.SetKind(node.Kind)
	obj.SetNamespace(namespace)
	obj.SetName(node.Name)

	if err := unstructured.SetNestedField(obj.Object, string(snapshotapi.SnapshotModeImport), "spec", "mode"); err != nil {
		return nil, fmt.Errorf("set import marker: %w", err)
	}

	return obj, nil
}
