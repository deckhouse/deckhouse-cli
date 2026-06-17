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

// Package v1alpha1 provides typed Go structs for the state-snapshotter CRDs
// consumed by the d8 snapshot download command.
// Two API groups are represented here:
//   - storage.deckhouse.io/v1alpha1 — Snapshot, SnapshotContent
//   - state-snapshotter.deckhouse.io/v1alpha1 — ManifestCheckpoint, ManifestCheckpointContentChunk
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// AnnotationSourceRef is the annotation key that carries the source identity of a snapshot node.
const AnnotationSourceRef = "state-snapshotter.deckhouse.io/source-ref"

// --- storage.deckhouse.io/v1alpha1 ---

// Snapshot requests a namespace state/configuration snapshot.
type Snapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SnapshotSpec   `json:"spec,omitempty"`
	Status SnapshotStatus `json:"status,omitempty"`
}

// DeepCopyObject implements runtime.Object.
func (s *Snapshot) DeepCopyObject() runtime.Object {
	out := *s

	return &out
}

// SnapshotList is a list of Snapshot objects.
type SnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Snapshot `json:"items"`
}

// DeepCopyObject implements runtime.Object.
func (s *SnapshotList) DeepCopyObject() runtime.Object {
	out := *s

	if s.Items != nil {
		out.Items = make([]Snapshot, len(s.Items))
		copy(out.Items, s.Items)
	}

	return &out
}

// SnapshotSpec describes the desired snapshot configuration.
type SnapshotSpec struct {
	SnapshotClassName string `json:"snapshotClassName,omitempty"`
}

// SnapshotStatus carries the latest observations for a Snapshot.
type SnapshotStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// BoundSnapshotContentName is the cluster-scoped name of the bound SnapshotContent.
	BoundSnapshotContentName string `json:"boundSnapshotContentName,omitempty"`

	// ChildrenSnapshotRefs lists child snapshot objects in the run tree.
	// Child namespace is implicit (always equals the parent Snapshot namespace).
	ChildrenSnapshotRefs []SnapshotChildRef `json:"childrenSnapshotRefs,omitempty"`

	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// SnapshotChildRef identifies one child snapshot object in the run tree.
// apiVersion and kind are required; the consumer resolves the object with a single Get.
type SnapshotChildRef struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
}

// SnapshotContent holds the result of a snapshot run.
type SnapshotContent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SnapshotContentSpec   `json:"spec,omitempty"`
	Status SnapshotContentStatus `json:"status,omitempty"`
}

// DeepCopyObject implements runtime.Object.
func (s *SnapshotContent) DeepCopyObject() runtime.Object {
	out := *s

	return &out
}

// SnapshotContentList is a list of SnapshotContent objects.
type SnapshotContentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SnapshotContent `json:"items"`
}

// DeepCopyObject implements runtime.Object.
func (s *SnapshotContentList) DeepCopyObject() runtime.Object {
	out := *s

	if s.Items != nil {
		out.Items = make([]SnapshotContent, len(s.Items))
		copy(out.Items, s.Items)
	}

	return &out
}

// SnapshotContentSpec describes the desired state of a SnapshotContent.
type SnapshotContentSpec struct {
	BackupRepositoryName string `json:"backupRepositoryName,omitempty"`

	// DeletionPolicy controls whether the controller may delete this SnapshotContent.
	DeletionPolicy string `json:"deletionPolicy,omitempty"`
}

// SnapshotContentStatus carries the latest observations for a SnapshotContent.
type SnapshotContentStatus struct {
	// ManifestCheckpointName is the cluster-scoped ManifestCheckpoint name once manifest capture has persisted.
	ManifestCheckpointName string `json:"manifestCheckpointName,omitempty"`

	// ChildrenSnapshotContentRefs lists direct child SnapshotContent objects in the snapshot tree.
	ChildrenSnapshotContentRefs []SnapshotContentChildRef `json:"childrenSnapshotContentRefs,omitempty"`

	// DataRefs lists PVC-target-to-data-artifact bindings for this snapshot node.
	DataRefs []SnapshotDataBinding `json:"dataRefs,omitempty"`

	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// SnapshotContentChildRef identifies one child SnapshotContent in the snapshot tree.
// SnapshotContent is cluster-scoped; the ref is name-only and must not carry a namespace.
type SnapshotContentChildRef struct {
	Name string `json:"name"`
}

// SnapshotSubjectRef identifies the subject (PVC or similar) captured by a data binding.
type SnapshotSubjectRef struct {
	APIVersion string    `json:"apiVersion"`
	Kind       string    `json:"kind"`
	Name       string    `json:"name"`
	Namespace  string    `json:"namespace,omitempty"`
	UID        types.UID `json:"uid,omitempty"`
}

// SnapshotDataArtifactRef points to a durable data artifact produced by the data path
// (e.g. a VolumeSnapshotContent). Must not reference transient execution requests.
type SnapshotDataArtifactRef struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
}

// SnapshotDataBinding associates a PVC target with its captured data artifact on one SnapshotContent.
type SnapshotDataBinding struct {
	// TargetUID is the map key (PersistentVolumeClaim UID).
	TargetUID string                  `json:"targetUID"`
	Target    SnapshotSubjectRef      `json:"target"`
	Artifact  SnapshotDataArtifactRef `json:"artifact"`
}

// --- state-snapshotter.deckhouse.io/v1alpha1 ---

// ManifestCheckpoint holds the persisted manifest capture result for a snapshot node.
type ManifestCheckpoint struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ManifestCheckpointSpec   `json:"spec"`
	Status ManifestCheckpointStatus `json:"status,omitempty"`
}

// DeepCopyObject implements runtime.Object.
func (m *ManifestCheckpoint) DeepCopyObject() runtime.Object {
	out := *m

	return &out
}

// ManifestCheckpointList is a list of ManifestCheckpoint objects.
type ManifestCheckpointList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ManifestCheckpoint `json:"items"`
}

// DeepCopyObject implements runtime.Object.
func (m *ManifestCheckpointList) DeepCopyObject() runtime.Object {
	out := *m

	if m.Items != nil {
		out.Items = make([]ManifestCheckpoint, len(m.Items))
		copy(out.Items, m.Items)
	}

	return &out
}

// ManifestCheckpointSpec describes the source of a ManifestCheckpoint.
type ManifestCheckpointSpec struct {
	SourceNamespace           string           `json:"sourceNamespace"`
	ManifestCaptureRequestRef *ObjectReference `json:"manifestCaptureRequestRef,omitempty"`
}

// ManifestCheckpointStatus carries the latest observations for a ManifestCheckpoint.
type ManifestCheckpointStatus struct {
	Chunks         []ChunkInfo        `json:"chunks,omitempty"`
	TotalObjects   int                `json:"totalObjects,omitempty"`
	TotalSizeBytes int64              `json:"totalSizeBytes,omitempty"`
	Conditions     []metav1.Condition `json:"conditions,omitempty"`
}

// ChunkInfo describes one manifest data chunk within a ManifestCheckpoint.
type ChunkInfo struct {
	Name         string `json:"name"`
	Index        int    `json:"index"`
	ObjectsCount int    `json:"objectsCount"`
	SizeBytes    int64  `json:"sizeBytes"`
	// Checksum is the SHA-256 hash of the compressed chunk data (base64 encoded).
	Checksum string `json:"checksum,omitempty"`
}

// ManifestCheckpointContentChunk is a cluster-scoped object holding one chunk of manifest data.
type ManifestCheckpointContentChunk struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ManifestCheckpointContentChunkSpec `json:"spec"`
}

// DeepCopyObject implements runtime.Object.
func (m *ManifestCheckpointContentChunk) DeepCopyObject() runtime.Object {
	out := *m

	return &out
}

// ManifestCheckpointContentChunkList is a list of ManifestCheckpointContentChunk objects.
type ManifestCheckpointContentChunkList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ManifestCheckpointContentChunk `json:"items"`
}

// DeepCopyObject implements runtime.Object.
func (m *ManifestCheckpointContentChunkList) DeepCopyObject() runtime.Object {
	out := *m

	if m.Items != nil {
		out.Items = make([]ManifestCheckpointContentChunk, len(m.Items))
		copy(out.Items, m.Items)
	}

	return &out
}

// ManifestCheckpointContentChunkSpec holds the payload of one manifest chunk.
// Data is base64(gzip(json[])) — the compressed JSON array of captured objects.
type ManifestCheckpointContentChunkSpec struct {
	CheckpointName string `json:"checkpointName"`
	Index          int    `json:"index"`
	// Data is the chunk payload: base64(gzip(json[])). Max 1 MiB enforced by the CRD.
	Data         string `json:"data"`
	ObjectsCount int    `json:"objectsCount"`
	// Checksum is the SHA-256 hash of the compressed chunk data (base64 encoded).
	Checksum string `json:"checksum,omitempty"`
}

// ObjectReference contains enough information to identify a referred Kubernetes object.
type ObjectReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	UID       string `json:"uid"`
}
