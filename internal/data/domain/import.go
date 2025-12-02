/*
Copyright 2024 Flant JSC

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

package domain

// DataImport represents a data import resource
type DataImport struct {
	Name      string
	Namespace string
	TTL       string
	Publish   bool
	WFFC      bool // WaitForFirstConsumer
	PVCSpec   *PVCSpec
	Status    DataImportStatus
}

// DataImportStatus represents the status of a DataImport
type DataImportStatus struct {
	URL        string
	PublicURL  string
	CA         string
	VolumeMode VolumeMode
	Ready      bool
}

// PVCSpec represents a PersistentVolumeClaim specification
type PVCSpec struct {
	Name             string
	Namespace        string
	StorageClassName string
	AccessModes      []string
	Storage          string
}

// CreateImportParams contains parameters for creating a DataImport
type CreateImportParams struct {
	Name      string
	Namespace string
	TTL       string
	Publish   bool
	WFFC      bool
	PVCSpec   *PVCSpec
}

// UploadParams contains parameters for uploading data
type UploadParams struct {
	Name      string
	Namespace string
	FilePath  string
	DstPath   string
	Publish   bool
	Chunks    int
	Resume    bool
}

