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
//   - core Snapshot:  spec.source.import: {}
//   - CSI VolumeSnapshot leaf: spec.source.dataImportName: <dataImportName>
//
// dataImportName is only consulted for VolumeSnapshot leaves.
func importMarkerCR(node PlannedNode, namespace, dataImportName string) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(node.APIVersion)
	obj.SetKind(node.Kind)
	obj.SetNamespace(namespace)
	obj.SetName(node.Name)

	switch {
	case node.isStructural():
		if err := unstructured.SetNestedMap(obj.Object, map[string]interface{}{}, "spec", "source", "import"); err != nil {
			return nil, fmt.Errorf("set import marker: %w", err)
		}

	case node.isVolumeSnapshotLeaf():
		if dataImportName == "" {
			return nil, fmt.Errorf("leaf VolumeSnapshot %q requires a DataImport name", node.Name)
		}

		if err := unstructured.SetNestedField(obj.Object, dataImportName, "spec", "source", "dataImportName"); err != nil {
			return nil, fmt.Errorf("set dataImportName: %w", err)
		}

	case node.isDomainDataLeaf():
		if node.SourceObjectRef == nil {
			return nil, fmt.Errorf("domain data leaf %s/%s missing SourceObjectRef in snapshot.yaml; cannot build import marker", node.Kind, node.Name)
		}

		if dataImportName == "" {
			return nil, fmt.Errorf("domain data leaf %s/%s requires a DataImport name", node.Kind, node.Name)
		}

		// spec.sourceRef tells the domain controller what was captured (mirrors the
		// original capture-mode spec.sourceRef so the domain CR is self-describing).
		sourceRef := map[string]interface{}{
			"apiVersion": node.SourceObjectRef.APIVersion,
			"kind":       node.SourceObjectRef.Kind,
			"name":       node.SourceObjectRef.Name,
		}

		if err := unstructured.SetNestedMap(obj.Object, sourceRef, "spec", "sourceRef"); err != nil {
			return nil, fmt.Errorf("set spec.sourceRef: %w", err)
		}

		// spec.dataSource.name is the import-mode trigger read by the genericbinder
		// reconcileGenericImport path (snapshotImportDataImportName).
		if err := unstructured.SetNestedField(obj.Object, dataImportName, "spec", "dataSource", "name"); err != nil {
			return nil, fmt.Errorf("set spec.dataSource.name: %w", err)
		}

	default:
		return nil, unsupportedNodeError(node)
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
