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
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/api/resource"
	sigsyaml "sigs.k8s.io/yaml"
)

// ChecksumAlgorithmSHA256 is the only checksum algorithm the archive uses; it is the value
// ComputeNodeChecksum records and ValidateSnapshotYAML requires in NodeChecksum.Algorithm.
const ChecksumAlgorithmSHA256 = "sha256"

// sha256HexLen is the length of a hex-encoded SHA-256 digest (32 bytes → 64 hex chars).
const sha256HexLen = 64

// Volume modes recorded in VolumeInfo.VolumeMode. They mirror the corev1.PersistentVolumeMode
// values written by the download side (see volume.nodeDataToVolumeInfo) and MUST agree with
// the on-disk payload kind: a block payload (data.bin[.<ext>]) is "Block", a filesystem
// payload (data.tar) is "Filesystem". ValidateSnapshotYAML enforces that agreement.
const (
	VolumeModeBlock      = "Block"
	VolumeModeFilesystem = "Filesystem"
)

// ErrInvalidSnapshotYAML is returned by ValidateSnapshotYAML/ValidateNodeMetadata when a
// node's snapshot.yaml violates a structural metadata invariant. snapshot.yaml is EXCLUDED
// from the integrity digest (ComputeNodeChecksum/VerifyNode), so these invariants are not
// covered by the checksum and must be validated separately before the archive is trusted.
var ErrInvalidSnapshotYAML = errors.New("invalid snapshot.yaml")

// SnapshotYAML is the per-node file written at <nodeDir>/snapshot.yaml.
// It records the snapshot CR identity and the locally-computed integrity checksum.
// sigs.k8s.io/yaml uses json struct tags for marshaling and unmarshaling.
type SnapshotYAML struct {
	// APIVersion is the apiVersion of the snapshot CR (e.g. "state-snapshotter.deckhouse.io/v1alpha1").
	APIVersion string `json:"apiVersion"`
	// Kind is the kind of the snapshot CR (e.g. "Snapshot", "DemoVirtualDiskSnapshot").
	Kind string `json:"kind"`
	// Name is the metadata.name of the snapshot CR.
	Name string `json:"name"`
	// Namespace is the namespace of the snapshot CR. Omitted for cluster-scoped resources.
	Namespace string `json:"namespace,omitempty"`
	// UID is the metadata.uid of the snapshot CR. It is the identity component the resume
	// scan matches (matchesIdentity), tying a node directory to the exact snapshot CR
	// (including UID) rather than to the source-object name. Does not affect
	// ComputeNodeChecksum because snapshot.yaml is excluded from the integrity digest.
	UID string `json:"uid,omitempty"`
	// SourceName is the metadata.name of the original captured source object
	// (status.sourceRef.name), recorded for readability. Omitted when the node has no
	// source (e.g. some import nodes). It is NOT an identity component (resume uses UID)
	// and does not affect ComputeNodeChecksum because snapshot.yaml is excluded from the
	// integrity digest.
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
	//   - A node that captured its own volume (namespaced status.data present) carries
	//     exactly one VolumeInfo (Variant A, cardinality ≤1) — this covers both
	//     non-aggregator domain nodes and orphan leaf volume nodes.
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
	// Artifact is the VolumeSnapshotContent that held the durable data artifact at capture
	// time. Recorded for provenance/debugging; the re-import path no longer consumes it.
	Artifact VolumeObjectRef `json:"artifact"`
	// VolumeMode records the source volume mode (Block or Filesystem). On re-import it is sent
	// as the PopulateData DataImport's spec.storageParams.volumeMode (optional).
	VolumeMode string `json:"volumeMode,omitempty"`
	// StorageClassName records the source StorageClass of the captured volume. On re-import it
	// is sent as the PopulateData DataImport's spec.storageParams.storageClassName (required).
	StorageClassName string `json:"storageClassName,omitempty"`
	// Size records the real allocated size of the captured volume (e.g. "10Gi"), taken from
	// VolumeSnapshotContent.status.restoreSize. On re-import it is sent as the PopulateData
	// DataImport's spec.storageParams.size (required).
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

// ValidateSnapshotYAML strictly validates the snapshot.yaml metadata that
// ComputeNodeChecksum/VerifyNode intentionally do NOT cover. Because snapshot.yaml is
// excluded from the integrity digest, a corrupt or mismatched metadata block would pass the
// checksum check unnoticed, so the import path validates it explicitly before any cluster
// mutation. It does NOT claim the checksum covers snapshot.yaml (it does not); it validates
// the excluded metadata as a separate, standalone check.
//
// hasBlockData and hasFilesystemData report the node's on-disk volume payload
// (data.bin[.<ext>] and data.tar respectively); ValidateNodeMetadata derives them from the
// directory. A node is a data node when it carries either payload. The rules:
//
//   - apiVersion, kind and name are required.
//   - checksum.algorithm is "sha256", checksum.hex is 64 lowercase hex chars, and
//     checksum.short is the first 8 chars of hex (ShortChecksum).
//   - sourceObjectRef is all-or-nothing: omitted, or all of apiVersion/kind/name set.
//   - at most one volume (Variant A cardinality, decision #9).
//   - a data node carries exactly one volume with a complete target and artifact identity
//     (apiVersion/kind/name each), a storageClassName, a positive parseable size, and a
//     volumeMode that agrees with the payload kind (Block for data.bin, Filesystem for
//     data.tar).
//   - a non-data node carries no volume.
//
// Authenticated/versioned snapshot.yaml evolution (signing, schema version) is a separate
// concern and out of scope here.
func ValidateSnapshotYAML(sy SnapshotYAML, hasBlockData, hasFilesystemData bool) error {
	if sy.APIVersion == "" || sy.Kind == "" || sy.Name == "" {
		return fmt.Errorf("apiVersion/kind/name are required (got apiVersion=%q kind=%q name=%q): %w",
			sy.APIVersion, sy.Kind, sy.Name, ErrInvalidSnapshotYAML)
	}

	if err := validateChecksum(sy.Checksum); err != nil {
		return err
	}

	if ref := sy.SourceObjectRef; ref != nil {
		if ref.APIVersion == "" || ref.Kind == "" || ref.Name == "" {
			return fmt.Errorf("sourceObjectRef must set all of apiVersion/kind/name or be omitted (got %+v): %w",
				*ref, ErrInvalidSnapshotYAML)
		}
	}

	if len(sy.Volumes) > 1 {
		return fmt.Errorf("a node carries at most one volume, got %d: %w", len(sy.Volumes), ErrInvalidSnapshotYAML)
	}

	if !hasBlockData && !hasFilesystemData {
		if len(sy.Volumes) != 0 {
			return fmt.Errorf("non-data node carries %d volume(s) but has no data payload: %w",
				len(sy.Volumes), ErrInvalidSnapshotYAML)
		}

		return nil
	}

	if len(sy.Volumes) != 1 {
		return fmt.Errorf("data node must carry exactly one volume, got %d: %w", len(sy.Volumes), ErrInvalidSnapshotYAML)
	}

	return validateDataVolume(sy.Volumes[0], hasBlockData)
}

// validateChecksum enforces the algorithm/hex/short consistency of a recorded NodeChecksum.
func validateChecksum(c NodeChecksum) error {
	if c.Algorithm != ChecksumAlgorithmSHA256 {
		return fmt.Errorf("checksum.algorithm must be %q, got %q: %w",
			ChecksumAlgorithmSHA256, c.Algorithm, ErrInvalidSnapshotYAML)
	}

	if len(c.Hex) != sha256HexLen || !isLowerHex(c.Hex) {
		return fmt.Errorf("checksum.hex must be %d lowercase hex characters, got %q: %w",
			sha256HexLen, c.Hex, ErrInvalidSnapshotYAML)
	}

	if want := ShortChecksum(c.Hex); c.Short != want {
		return fmt.Errorf("checksum.short %q is inconsistent with hex (want %q): %w",
			c.Short, want, ErrInvalidSnapshotYAML)
	}

	return nil
}

// validateDataVolume enforces the data-node volume invariants: complete target/artifact
// identity, a storageClassName, a positive parseable size, and a volumeMode that agrees with
// the on-disk payload kind (hasBlockData selects Block, otherwise Filesystem).
func validateDataVolume(v VolumeInfo, hasBlockData bool) error {
	if v.Target.APIVersion == "" || v.Target.Kind == "" || v.Target.Name == "" {
		return fmt.Errorf("data volume target identity is incomplete, apiVersion/kind/name required (got %+v): %w",
			v.Target, ErrInvalidSnapshotYAML)
	}

	if v.Artifact.APIVersion == "" || v.Artifact.Kind == "" || v.Artifact.Name == "" {
		return fmt.Errorf("data volume artifact identity is incomplete, apiVersion/kind/name required (got %+v): %w",
			v.Artifact, ErrInvalidSnapshotYAML)
	}

	if v.StorageClassName == "" {
		return fmt.Errorf("data volume storageClassName is required: %w", ErrInvalidSnapshotYAML)
	}

	q, err := resource.ParseQuantity(v.Size)
	if err != nil {
		return fmt.Errorf("data volume size %q is not a valid quantity: %w", v.Size, errors.Join(ErrInvalidSnapshotYAML, err))
	}

	if q.Sign() <= 0 {
		return fmt.Errorf("data volume size %q must be positive: %w", v.Size, ErrInvalidSnapshotYAML)
	}

	want := VolumeModeFilesystem
	if hasBlockData {
		want = VolumeModeBlock
	}

	if v.VolumeMode != want {
		return fmt.Errorf("data volume volumeMode %q disagrees with the on-disk payload (want %q): %w",
			v.VolumeMode, want, ErrInvalidSnapshotYAML)
	}

	return nil
}

// isLowerHex reports whether s consists solely of lowercase hexadecimal digits.
func isLowerHex(s string) bool {
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}

	return true
}
