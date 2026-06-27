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

// supported reports whether the CLI can client-drive the import of this node kind.
// Core Snapshot trees, CSI VolumeSnapshot data leaves, and domain data leaves (e.g.
// DemoVirtualDiskSnapshot with volume data) are supported. Domain aggregator nodes
// (e.g. DemoVirtualMachineSnapshot with no own volume data) are not: they require a
// server-side import path and cannot be imported client-side.
func (n PlannedNode) supported() bool {
	return n.isStructural() || n.isVolumeSnapshotLeaf() || n.isDomainDataLeaf()
}

// isDomainAggregator reports whether the node is a domain aggregator: a domain-kind
// snapshot that carries no own volume data (only child snapshot refs). Domain aggregators
// cannot be imported client-side — they require the server's genericbinder import path
// to materialise a parent SnapshotContent, which is a server-side-only operation.
func (n PlannedNode) isDomainAggregator() bool {
	return !n.isStructural() && !n.isVolumeSnapshotLeaf() && !n.isDomainDataLeaf()
}

// importMarkerCR builds the minimal import-mode CR the server requires to exist before
// it will accept a manifests-and-children-refs-upload for this node.
//
// All supported node kinds — core Snapshot trees, CSI VolumeSnapshot leaves, and domain
// data leaves — use the same unified marker: spec.source.import: {}. The server keys import
// mode off this marker; for data leaves the volume content is matched to its DataImport by a
// reverse-lookup on the DataImport's targetRef (group/kind/name), so the leaf no longer needs
// to name its DataImport or mirror the captured source.
func importMarkerCR(node PlannedNode, namespace string) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(node.APIVersion)
	obj.SetKind(node.Kind)
	obj.SetNamespace(namespace)
	obj.SetName(node.Name)

	if !node.supported() {
		return nil, unsupportedNodeError(node)
	}

	if err := unstructured.SetNestedMap(obj.Object, map[string]interface{}{}, "spec", "source", "import"); err != nil {
		return nil, fmt.Errorf("set import marker: %w", err)
	}

	return obj, nil
}

// unsupportedNodeError returns a clear, actionable error for domain aggregator nodes the
// CLI cannot client-drive during import. Called from importMarkerCR default case.
func unsupportedNodeError(node PlannedNode) error {
	return fmt.Errorf("import of %s/%s is not supported by this CLI: "+
		"domain aggregator nodes (e.g. intermediate DemoVirtualMachineSnapshot) "+
		"require a server-side import path and cannot be imported client-side; "+
		"use --node <Kind>/<name> to import a supported subtree", node.Kind, node.Name)
}
