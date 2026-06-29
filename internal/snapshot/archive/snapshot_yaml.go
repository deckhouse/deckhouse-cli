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

package archive

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	sigsyaml "sigs.k8s.io/yaml"
)

// SnapshotYAML is the per-node file written at <nodeDir>/snapshot.yaml.
// It records the snapshot CR identity and the locally-computed integrity checksum.
// sigs.k8s.io/yaml uses json struct tags for marshaling and unmarshaling.
type SnapshotYAML struct {
	// APIVersion is the apiVersion of the snapshot CR (e.g. "storage.deckhouse.io/v1alpha1").
	APIVersion string `json:"apiVersion"`
	// Kind is the kind of the snapshot CR (e.g. "Snapshot", "DemoVirtualDiskSnapshot").
	Kind string `json:"kind"`
	// Name is the metadata.name of the snapshot CR.
	Name string `json:"name"`
	// Namespace is the namespace of the snapshot CR. Omitted for cluster-scoped resources.
	Namespace string `json:"namespace,omitempty"`
	// SourceRef carries the source-ref annotation from the snapshot CR, recording
	// the identity of the original captured object.
	SourceRef string `json:"sourceRef,omitempty"`
	// SourceName is the .name field from the source-ref annotation — the Kubernetes
	// metadata.name of the original captured object. Omitted when empty (root node,
	// annotation absent, or parse error). Does not affect ComputeNodeChecksum because
	// snapshot.yaml is excluded from the integrity digest.
	SourceName string `json:"sourceName,omitempty"`
	// SourceObjectRef carries the structured spec.sourceRef from a domain snapshot CR
	// ({apiVersion,kind,name} of the source object). Absent for core Snapshot nodes and
	// CSI VolumeSnapshot data leaves. Does not affect ComputeNodeChecksum because
	// snapshot.yaml is excluded from the integrity digest.
	SourceObjectRef *SourceObjectRef `json:"sourceObjectRef,omitempty"`
	// Checksum is the locally-computed node integrity digest.
	Checksum NodeChecksum `json:"checksum"`
	// Volumes lists the captured PVC volumes owned by this node.
	//
	//   - Snapshot nodes (non-aggregator) that own ≥1 OwnDataRefs carry one
	//     VolumeInfo per binding.
	//   - Orphan leaf volume nodes (Binding != nil) carry exactly one entry
	//     derived from the single Binding.
	//   - Aggregator snapshot nodes and purely-manifest nodes carry no volumes
	//     and the field is omitted (omitempty).
	//
	// snapshot.yaml is excluded from ComputeNodeChecksum/VerifyNode, so this
	// field does not affect the integrity digest.
	Volumes []VolumeInfo `json:"volumes,omitempty"`
}

// SourceObjectRef is the structured spec.sourceRef from a domain snapshot CR, persisted
// in snapshot.yaml so the import side can recreate the CR in import mode. The fields
// mirror the domain CR's spec.sourceRef (apiVersion/kind/name of the source object).
// Omitted for core Snapshot nodes and CSI VolumeSnapshot data leaves.
type SourceObjectRef struct {
	// APIVersion is the apiVersion of the source object (e.g. "demo.deckhouse.io/v1alpha1").
	APIVersion string `json:"apiVersion"`
	// Kind is the kind of the source object (e.g. "DemoVirtualDisk").
	Kind string `json:"kind"`
	// Name is the metadata.name of the source object.
	Name string `json:"name"`
}

// VolumeObjectRef is a reference to a Kubernetes object stored in the volume block
// of snapshot.yaml. It captures the identity fields needed to correlate the archive
// entry with live cluster resources.
type VolumeObjectRef struct {
	// APIVersion is the apiVersion of the referenced object.
	APIVersion string `json:"apiVersion"`
	// Kind is the kind of the referenced object.
	Kind string `json:"kind"`
	// Name is the metadata.name of the referenced object.
	Name string `json:"name"`
	// Namespace is the namespace of the referenced object. Omitted for cluster-scoped objects.
	Namespace string `json:"namespace,omitempty"`
	// UID is the metadata.uid of the referenced object. Omitted when unknown.
	UID string `json:"uid,omitempty"`
}

// VolumeInfo describes the captured volume associated with a volume node.
// It is written into the volume block of snapshot.yaml so the archive is self-describing.
type VolumeInfo struct {
	// Target is the source PVC that was captured (its apiVersion/kind/name/namespace/uid).
	Target VolumeObjectRef `json:"target"`
	// Artifact is the VolumeSnapshotContent that holds the durable data artifact.
	Artifact VolumeObjectRef `json:"artifact"`
	// VolumeMode records the source volume mode (Block or Filesystem). It feeds the
	// DataImport spec.volumeMode when re-importing this leaf (Mode A).
	VolumeMode string `json:"volumeMode,omitempty"`
	// StorageClassName records the source StorageClass of the captured volume. It feeds
	// the DataImport spec.storageClassName when re-importing this leaf (Mode A).
	StorageClassName string `json:"storageClassName,omitempty"`
	// Size records the real allocated size of the captured volume (e.g. "10Gi"), taken from
	// VolumeSnapshotContent.status.restoreSize. It feeds the DataImport spec.size on re-import.
	Size string `json:"size,omitempty"`
}

// NodeChecksum is the locally-computed integrity digest for one node directory.
// The digest covers the node's own files (manifests and volume data) but excludes
// snapshot.yaml itself and the snapshots/ child directory.
type NodeChecksum struct {
	// Algorithm is always "sha256".
	Algorithm string `json:"algorithm"`
	// Hex is the full lowercase hex-encoded SHA-256 digest.
	Hex string `json:"hex"`
	// Short is the first 8 characters of Hex, used as a collision-suffix when
	// a node directory with the same name already exists with a different checksum.
	Short string `json:"short"`
}

// WriteSnapshotYAML serialises sy to YAML and writes it atomically to
// <nodeDir>/snapshot.yaml. An existing file at that path is replaced.
func WriteSnapshotYAML(nodeDir string, sy SnapshotYAML) error {
	data, err := sigsyaml.Marshal(sy)
	if err != nil {
		return fmt.Errorf("marshal snapshot.yaml: %w", err)
	}

	path := filepath.Join(nodeDir, SnapshotYAMLName)

	return WriteFileAtomic(path, bytes.NewReader(data))
}

// ReadSnapshotYAML reads and deserialises <nodeDir>/snapshot.yaml.
// Returns an error wrapping os.ErrNotExist when the file is absent.
func ReadSnapshotYAML(nodeDir string) (SnapshotYAML, error) {
	path := filepath.Join(nodeDir, SnapshotYAMLName)

	data, err := os.ReadFile(path)
	if err != nil {
		return SnapshotYAML{}, fmt.Errorf("read snapshot.yaml: %w", err)
	}

	var sy SnapshotYAML
	if err := sigsyaml.Unmarshal(data, &sy); err != nil {
		return SnapshotYAML{}, fmt.Errorf("unmarshal snapshot.yaml: %w", err)
	}

	return sy, nil
}
