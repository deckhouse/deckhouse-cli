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

// SnapshotImport condition types (mirror of state-snapshotter).
const (
	SnapshotImportConditionIndexReceived     = "IndexReceived"
	SnapshotImportConditionManifestsReceived = "ManifestsReceived"
	SnapshotImportConditionUploadsPrepared   = "UploadsPrepared"
	SnapshotImportConditionCaptured          = "Captured"
	SnapshotImportConditionDataReceived      = "DataReceived"
	SnapshotImportConditionReady             = "Ready"
)

// SnapshotImport condition reasons (mirror of state-snapshotter) that the CLI treats as terminal
// while waiting for upload endpoints.
const (
	// SnapshotImportReasonStorageClassMappingRequired marks UploadsPrepared=False when one or more
	// source StorageClasses cannot be resolved in the target cluster.
	SnapshotImportReasonStorageClassMappingRequired = "StorageClassMappingRequired"
	// SnapshotImportReasonDataSizeUnknown marks UploadsPrepared=False when a data node's volume size
	// is unknown/zero in the uploaded index, so a PVC cannot be sized. The bundle must be regenerated.
	SnapshotImportReasonDataSizeUnknown = "DataSizeUnknown"
	// SnapshotImportReasonChildNotFound marks the import failed-closed when spec.childSnapshot does
	// not match any node in the uploaded bundle.
	SnapshotImportReasonChildNotFound = "ChildSnapshotNotFound"
	// SnapshotImportReasonNameConflict marks Ready=False when a target object name already exists in
	// the namespace and is not owned by this import.
	SnapshotImportReasonNameConflict = "NameConflict"
)

// SnapshotImport orchestrates uploading (importing) a whole Snapshot hierarchy.
type SnapshotImport struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SnapshotImportSpec   `json:"spec,omitempty"`
	Status SnapshotImportStatus `json:"status,omitempty"`
}

// SnapshotImportList is a list of SnapshotImport.
type SnapshotImportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SnapshotImport `json:"items"`
}

// SnapshotImportSpec is the desired state of a SnapshotImport.
type SnapshotImportSpec struct {
	// TargetName is the desired name of the recreated root snapshot in this namespace. When
	// ChildSnapshot is empty it names the recreated root of the uploaded bundle; when ChildSnapshot
	// selects a child, it names that re-rooted child.
	TargetName string `json:"targetName"`
	// ChildSnapshot optionally selects a single child snapshot from the uploaded bundle to import as
	// the new root (server-side re-root). Empty imports the bundle's own root. When set, apiVersion,
	// kind and name must all match a node present in the uploaded bundle exactly.
	ChildSnapshot *SnapshotReference `json:"childSnapshot,omitempty"`
	// TTL is the idle time-to-live for the import's upload endpoints (e.g. "30m"). Required by the
	// server CRD.
	TTL string `json:"ttl,omitempty"`
	// StorageClassMapping optionally remaps source StorageClass names to target names.
	StorageClassMapping map[string]string `json:"storageClassMapping,omitempty"`
	// Publish exposes upload endpoints outside the cluster (Ingress/Route) when true.
	Publish bool `json:"publish,omitempty"`
}

// SnapshotImportStatus is the observed state of a SnapshotImport.
type SnapshotImportStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// IndexUploadURL is where the client uploads the hierarchy index (opaque blob, as-is).
	IndexUploadURL string `json:"indexUploadURL,omitempty"`
	// ManifestsUploadURL is the top-level manifests upload endpoint; an empty finalize PUT here flips
	// the ManifestsReceived gate after every per-node manifest has been uploaded.
	ManifestsUploadURL string `json:"manifestsUploadURL,omitempty"`
	// Snapshots is the flat, per-node import view of the (possibly re-rooted) bundle: one entry per
	// node carrying its per-node manifests upload URL and, for data nodes, the data upload endpoint
	// and capture progress. Published after server-side re-root.
	Snapshots []SnapshotImportSnapshotEntry `json:"snapshots,omitempty"`
	// Conditions represent the latest observations.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// SnapshotImportSnapshotEntry is one snapshot node's import view: its per-node manifests upload URL
// plus, for a data node, the volume data upload endpoint and capture progress.
type SnapshotImportSnapshotEntry struct {
	// SnapshotID is the stable archive identifier "<kind>--<namespace>--<name>" from the index.
	SnapshotID string `json:"snapshotID"`
	// VolumeMode is the data volume mode (Block or Filesystem); empty for dataless nodes.
	VolumeMode string `json:"volumeMode,omitempty"`
	// ManifestsUploadURL is where the client uploads this single node's own manifests (?node=).
	ManifestsUploadURL string `json:"manifestsUploadURL,omitempty"`
	// UploadURL is the endpoint to upload this snapshot's volume data (data nodes only).
	UploadURL string `json:"uploadURL,omitempty"`
	// UploadCA is the base64 PEM CA bundle to trust when uploading to the internal UploadURL.
	UploadCA string `json:"uploadCA,omitempty"`
	// UploadReady indicates the populating PVC + importer endpoint are ready to receive data.
	UploadReady bool `json:"uploadReady,omitempty"`
	// Uploaded indicates the client signalled completion of this data upload.
	Uploaded bool `json:"uploaded,omitempty"`
	// CapturedSnapshotContentName is the durable VolumeSnapshotContent captured from the PVC.
	CapturedSnapshotContentName string `json:"capturedSnapshotContentName,omitempty"`
}
