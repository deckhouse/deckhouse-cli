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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SnapshotExport condition types (mirror of state-snapshotter).
const (
	SnapshotExportConditionReady     = "Ready"
	SnapshotExportConditionDataReady = "DataReady"
)

// SnapshotReference is a typed reference to a snapshot object by GroupVersionKind and name, within
// the referrer's namespace. APIVersion and Kind are optional: empty values default server-side to the
// namespaced root Snapshot (storage.deckhouse.io/v1alpha1, kind Snapshot), so a bare {name} keeps
// referencing a whole Snapshot.
type SnapshotReference struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Name       string `json:"name"`
}

// SnapshotExport orchestrates downloading (exporting) a whole Snapshot hierarchy.
type SnapshotExport struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SnapshotExportSpec   `json:"spec,omitempty"`
	Status SnapshotExportStatus `json:"status,omitempty"`
}

// SnapshotExportList is a list of SnapshotExport.
type SnapshotExportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SnapshotExport `json:"items"`
}

// SnapshotExportSpec is the desired state of a SnapshotExport.
type SnapshotExportSpec struct {
	// SnapshotRef is a typed reference to the snapshot to export (same namespace). It may be the
	// namespaced root Snapshot (the default when kind/apiVersion are empty) or any domain snapshot CR,
	// in which case the export covers that node and its subtree only.
	SnapshotRef SnapshotReference `json:"snapshotRef"`
	// TTL is the idle time-to-live for the export's data endpoints (e.g. "30m"). Required by the
	// server CRD.
	TTL string `json:"ttl,omitempty"`
	// Publish exposes the endpoints outside the cluster (Ingress/Route) when true.
	Publish bool `json:"publish,omitempty"`
}

// SnapshotExportStatus is the observed state of a SnapshotExport.
type SnapshotExportStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// IndexURL serves the opaque hierarchy index blob. The CLI downloads it verbatim and never parses
	// it: everything the client needs is mirrored per node in Snapshots.
	IndexURL string `json:"indexURL,omitempty"`
	// Snapshots is the flat, per-node export view: one entry per snapshot in the exported (sub)tree,
	// carrying that node's own manifests URL and, for data nodes, volume metadata and a download URL.
	Snapshots []SnapshotExportSnapshotEntry `json:"snapshots,omitempty"`
	// Conditions represent the latest observations (Ready, DataReady).
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// SnapshotExportSnapshotEntry is one snapshot node's export view: its own manifests URL plus, for a
// data node, the volume metadata and data download endpoint. The CLI follows these URLs without
// parsing the index blob.
type SnapshotExportSnapshotEntry struct {
	// SnapshotID is the stable archive identifier "<kind>--<namespace>--<name>".
	SnapshotID string `json:"snapshotID"`
	// ManifestsURL serves this single node's own manifests (the per-node ?node= aggregated endpoint).
	ManifestsURL string `json:"manifestsURL,omitempty"`
	// HasData is true when this node carries a data volume (DataURL is populated once ready).
	HasData bool `json:"hasData,omitempty"`
	// VolumeMode is the data volume mode (Block or Filesystem); it selects the data endpoint and the
	// on-disk layout. Empty for dataless nodes.
	VolumeMode string `json:"volumeMode,omitempty"`
	// StorageClassName is the source volume's StorageClass (informational). Empty for dataless nodes.
	StorageClassName string `json:"storageClassName,omitempty"`
	// FsType is the source volume filesystem type, when known.
	FsType string `json:"fsType,omitempty"`
	// AccessModes are the source volume access modes, when known.
	AccessModes []string `json:"accessModes,omitempty"`
	// Size is the data volume size in bytes; 0 if unknown.
	Size int64 `json:"size,omitempty"`
	// DataURL is the endpoint to download this node's volume data (data nodes only).
	DataURL string `json:"dataURL,omitempty"`
	// DataCA is the base64 PEM CA bundle to trust when downloading from the internal DataURL.
	DataCA string `json:"dataCA,omitempty"`
	// Ready indicates the data endpoint is serving (restored PVC bound + DataExport ready).
	Ready bool `json:"ready,omitempty"`
}
