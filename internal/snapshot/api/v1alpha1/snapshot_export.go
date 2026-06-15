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

// LocalSnapshotRef references a root Snapshot in the same namespace as the referrer.
type LocalSnapshotRef struct {
	Name string `json:"name"`
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
	// SnapshotRef references the root Snapshot (same namespace) to export.
	SnapshotRef LocalSnapshotRef `json:"snapshotRef"`
	// Publish exposes the endpoints outside the cluster (Ingress/Route) when true.
	Publish bool `json:"publish,omitempty"`
}

// SnapshotExportStatus is the observed state of a SnapshotExport.
type SnapshotExportStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// IndexURL serves the hierarchy index (snapshot tree + per-snapshot data metadata).
	IndexURL string `json:"indexURL,omitempty"`
	// ManifestsURL serves the whole-tree manifests archive.
	ManifestsURL string `json:"manifestsURL,omitempty"`
	// DataSnapshots lists per-data-snapshot export endpoints.
	DataSnapshots []SnapshotExportDataEntry `json:"dataSnapshots,omitempty"`
	// Conditions represent the latest observations (Ready, DataReady).
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// SnapshotExportDataEntry is one data leaf's export endpoint.
type SnapshotExportDataEntry struct {
	// SnapshotID is the stable archive identifier "<kind>--<namespace>--<name>".
	SnapshotID string `json:"snapshotID"`
	// DataURL is the endpoint to download this snapshot's volume data.
	DataURL string `json:"dataURL,omitempty"`
	// DataCA is the base64 PEM CA bundle to trust when talking to the data endpoint.
	DataCA string `json:"dataCA,omitempty"`
	// Ready indicates the data endpoint is serving.
	Ready bool `json:"ready,omitempty"`
}
