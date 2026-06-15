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

// IndexVersion is the snapshot index format version the CLI understands.
const IndexVersion = "v1"

// Index is the machine-readable description of a snapshot hierarchy used by export/import.
// It mirrors state-snapshotter internal/usecase/restore.Index.
type Index struct {
	Version      string          `json:"version"`
	RootSnapshot IndexSnapshotID `json:"rootSnapshot"`
	// Snapshots is a flat, deterministic (pre-order) list of every snapshot node in the tree.
	Snapshots []IndexSnapshot `json:"snapshots"`
}

// IndexSnapshotID identifies the root snapshot of the hierarchy.
type IndexSnapshotID struct {
	ID         string `json:"id"`
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
}

// IndexSnapshot is one node of the hierarchy.
type IndexSnapshot struct {
	// ID is the stable archive identifier "<kind>--<namespace>--<name>".
	ID         string   `json:"id"`
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Namespace  string   `json:"namespace"`
	Name       string   `json:"name"`
	ParentID   string   `json:"parentId,omitempty"`
	Children   []string `json:"children,omitempty"`
	HasData    bool     `json:"hasData"`
	// Data holds the volume metadata for data nodes.
	Data *IndexData `json:"data,omitempty"`
}

// IndexData is the per-data-node volume metadata.
type IndexData struct {
	StorageClassName string   `json:"storageClassName,omitempty"`
	VolumeMode       string   `json:"volumeMode,omitempty"`
	FsType           string   `json:"fsType,omitempty"`
	AccessModes      []string `json:"accessModes,omitempty"`
	// Size is the volume size in bytes.
	Size int64 `json:"size,omitempty"`
	// ArtifactName is the source durable VolumeSnapshotContent name (informational).
	ArtifactName string `json:"artifactName,omitempty"`
}
