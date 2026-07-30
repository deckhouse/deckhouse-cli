/*
Copyright 2025 Flant JSC

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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type DataImport struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DataImportSpec         `json:"spec"`
	Status DataExportImportStatus `json:"status"`
}

// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type DataImportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []DataImport `json:"items"`
}

// DataImportMode is spec.mode — the explicit discriminator that selects what a DataImport does
// with the imported bytes. It replaced the former polymorphic targetRef.kind discrimination; the
// current CRD has no spec.targetRef at all (it is pruned by the structural schema).
// See storage-foundation/api/v1alpha1/data_import.go for the SSOT.
type DataImportMode string

// DataImportModeCreatePVC is the standalone PVC-import mode: data bytes are streamed straight
// into a preserved PVC built from spec.pvcTemplate, with no snapshot capture and no
// VolumeSnapshotContent artifact. `d8 data import` only ever creates CreatePVC DataImports; the
// snapshot-leaf mode (PopulateData) is driven by `d8 snapshot upload`, which builds its own
// unstructured spec.
const DataImportModeCreatePVC DataImportMode = "CreatePVC"

// DataImportSpec mirrors the CreatePVC subset of the unified DataImport CRD spec that the CLI
// produces. The CRD CEL rules require pvcTemplate and forbid snapshotRef/storageParams when
// mode == CreatePVC, so those PopulateData-only fields are deliberately absent here.
// +k8s:deepcopy-gen=true
type DataImportSpec struct {
	TTL                  string         `json:"ttl"`
	Publish              bool           `json:"publish,omitempty"`
	WaitForFirstConsumer bool           `json:"waitForFirstConsumer,omitempty"`
	Mode                 DataImportMode `json:"mode,omitempty"`

	// PvcTemplate fully describes the destination PVC. Its metadata.name is mandatory — the
	// controller names the imported PVC after it and the server CEL rejects an empty name.
	PvcTemplate *PersistentVolumeClaimTemplateSpec `json:"pvcTemplate,omitempty"`
}

// +k8s:deepcopy-gen=true
type PersistentVolumeClaimTemplateSpec struct {
	metav1.ObjectMeta         `json:"metadata,omitempty"`
	PersistentVolumeClaimSpec `json:"spec,omitempty"`
}

// +k8s:deepcopy-gen=true
type PersistentVolumeClaimSpec struct {
	AccessModes      []PersistentVolumeAccessMode `json:"accessModes,omitempty"`
	Resources        VolumeResourceRequirements   `json:"resources,omitempty"`
	StorageClassName *string                      `json:"storageClassName,omitempty"`
	VolumeMode       *PersistentVolumeMode        `json:"volumeMode,omitempty"`
}

// VolumeResourceRequirements describes the storage resource requirements for a volume.
// +k8s:deepcopy-gen=true
type VolumeResourceRequirements struct {
	Requests ResourceList `json:"requests,omitempty"`
}

// ResourceList is a set of (resource name, quantity) pairs.
type ResourceList map[ResourceName]resource.Quantity

// +enum
type PersistentVolumeAccessMode string

const (
	// can be mounted in read/write mode to exactly 1 host
	ReadWriteOnce PersistentVolumeAccessMode = "ReadWriteOnce"
	// can be mounted in read-only mode to many hosts
	ReadOnlyMany PersistentVolumeAccessMode = "ReadOnlyMany"
	// can be mounted in read/write mode to many hosts
	ReadWriteMany PersistentVolumeAccessMode = "ReadWriteMany"
	// can be mounted in read/write mode to exactly 1 pod
	// cannot be used in combination with other access modes
	ReadWriteOncePod PersistentVolumeAccessMode = "ReadWriteOncePod"
)

// PersistentVolumeMode describes how a volume is intended to be consumed, either Block or Filesystem.
// +enum
type PersistentVolumeMode string

const (
	// PersistentVolumeBlock means the volume will not be formatted with a filesystem and will remain a raw block device.
	PersistentVolumeBlock PersistentVolumeMode = "Block"
	// PersistentVolumeFilesystem means the volume will be or is formatted with a filesystem.
	PersistentVolumeFilesystem PersistentVolumeMode = "Filesystem"
)

// +enum
type ResourceName string

const (
	// Volume size, in bytes (e.g. 5Gi = 5GiB = 5 * 1024 * 1024 * 1024)
	ResourceStorage ResourceName = "storage"
)

func (di *DataImport) GetStatus() *DataExportImportStatus {
	return &di.Status
}

type DataExportImportStatus struct {
	URL                 string             `json:"url"`
	CA                  string             `json:"ca,omitempty"`
	PublicURL           string             `json:"publicURL"`
	AccessTimestamp     metav1.Time        `json:"accessTimestamp"`
	Conditions          []metav1.Condition `json:"conditions,omitempty"`
	VolumeMode          string             `json:"volumeMode,omitempty"`
	DataImportCompleted bool               `json:"dataImportCompleted,omitempty"`
}
