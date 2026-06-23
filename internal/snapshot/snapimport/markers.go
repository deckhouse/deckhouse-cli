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
// Core Snapshot trees and CSI VolumeSnapshot data leaves are supported; domain/demo
// kinds are not (intermediate demo nodes expose no client-settable import marker, so
// their import-mode CRs must be created by the domain controller).
func (n PlannedNode) supported() bool {
	return n.isStructural() || n.isVolumeSnapshotLeaf()
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
			return nil, fmt.Errorf("VolumeSnapshot leaf %q requires a DataImport name", node.Name)
		}

		if err := unstructured.SetNestedField(obj.Object, dataImportName, "spec", "source", "dataImportName"); err != nil {
			return nil, fmt.Errorf("set dataImportName: %w", err)
		}
	default:
		return nil, unsupportedNodeError(node)
	}

	return obj, nil
}

// unsupportedNodeError returns a clear, actionable error for node kinds the CLI cannot
// client-drive during import.
func unsupportedNodeError(node PlannedNode) error {
	return fmt.Errorf("import of %s/%s is not supported by this CLI: only core Snapshot trees and "+
		"CSI VolumeSnapshot data leaves can be imported client-side; domain/demo snapshot nodes "+
		"(e.g. intermediate DemoVirtualMachineSnapshot) expose no client-settable import marker and "+
		"must be reconstructed by their domain controller", node.Kind, node.Name)
}
