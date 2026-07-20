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
// consumed by the d8 snapshot commands. Only the state-snapshotter.deckhouse.io/v1alpha1
// group is represented here (Snapshot, SnapshotContent). Per-node manifests are
// read through the aggregated subresource API (see internal/snapshot/aggapi)
// rather than directly from ManifestCheckpoint CRDs.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// AnnotationSourceRef is the annotation key that carries the source identity of a snapshot node.
const AnnotationSourceRef = "state-snapshotter.deckhouse.io/source-ref"

// --- state-snapshotter.deckhouse.io/v1alpha1 ---

// Snapshot requests a namespace state/configuration snapshot.
type Snapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SnapshotSpec   `json:"spec,omitempty"`
	Status SnapshotStatus `json:"status,omitempty"`
}

// DeepCopyInto copies all fields of s into out, making a fully independent copy.
func (s *Snapshot) DeepCopyInto(out *Snapshot) {
	*out = *s
	s.ObjectMeta.DeepCopyInto(&out.ObjectMeta)

	if s.Status.ChildrenSnapshotRefs != nil {
		out.Status.ChildrenSnapshotRefs = make([]SnapshotChildRef, len(s.Status.ChildrenSnapshotRefs))
		copy(out.Status.ChildrenSnapshotRefs, s.Status.ChildrenSnapshotRefs)
	}

	if s.Status.Conditions != nil {
		out.Status.Conditions = make([]metav1.Condition, len(s.Status.Conditions))
		copy(out.Status.Conditions, s.Status.Conditions)
	}
}

// DeepCopyObject implements runtime.Object.
func (s *Snapshot) DeepCopyObject() runtime.Object {
	out := new(Snapshot)
	s.DeepCopyInto(out)

	return out
}

// SnapshotList is a list of Snapshot objects.
type SnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Snapshot `json:"items"`
}

// DeepCopyObject implements runtime.Object.
func (s *SnapshotList) DeepCopyObject() runtime.Object {
	out := new(SnapshotList)
	*out = *s
	s.ListMeta.DeepCopyInto(&out.ListMeta)

	if s.Items != nil {
		out.Items = make([]Snapshot, len(s.Items))
		for i := range s.Items {
			s.Items[i].DeepCopyInto(&out.Items[i])
		}
	}

	return out
}

// SnapshotMode selects a snapshot's lifecycle. An absent mode is treated as Capture (the
// server default), so consumers MUST distinguish "unset" from an unknown non-empty value.
type SnapshotMode string

const (
	// SnapshotModeCapture is the default: the snapshot captures live cluster state.
	SnapshotModeCapture SnapshotMode = "Capture"
	// SnapshotModeImport marks a snapshot the CLI reconstructs from a downloaded archive.
	SnapshotModeImport SnapshotMode = "Import"
)

// SnapshotSpec describes the desired snapshot configuration.
type SnapshotSpec struct {
	// Mode selects Capture (default when empty) or Import. It replaces the removed
	// snapshotClassName field, which is not part of the unified contract.
	Mode SnapshotMode `json:"mode,omitempty"`
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

// DeepCopyInto copies all fields of s into out, making a fully independent copy.
func (s *SnapshotContent) DeepCopyInto(out *SnapshotContent) {
	*out = *s
	s.ObjectMeta.DeepCopyInto(&out.ObjectMeta)

	if s.Status.ChildrenSnapshotContentRefs != nil {
		out.Status.ChildrenSnapshotContentRefs = make([]SnapshotContentChildRef, len(s.Status.ChildrenSnapshotContentRefs))
		copy(out.Status.ChildrenSnapshotContentRefs, s.Status.ChildrenSnapshotContentRefs)
	}

	if s.Status.DataRef != nil {
		dr := *s.Status.DataRef

		if s.Status.DataRef.AccessModes != nil {
			dr.AccessModes = make([]string, len(s.Status.DataRef.AccessModes))
			copy(dr.AccessModes, s.Status.DataRef.AccessModes)
		}

		out.Status.DataRef = &dr
	}

	if s.Status.Conditions != nil {
		out.Status.Conditions = make([]metav1.Condition, len(s.Status.Conditions))
		copy(out.Status.Conditions, s.Status.Conditions)
	}
}

// DeepCopyObject implements runtime.Object.
func (s *SnapshotContent) DeepCopyObject() runtime.Object {
	out := new(SnapshotContent)
	s.DeepCopyInto(out)

	return out
}

// SnapshotContentList is a list of SnapshotContent objects.
type SnapshotContentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SnapshotContent `json:"items"`
}

// DeepCopyObject implements runtime.Object.
func (s *SnapshotContentList) DeepCopyObject() runtime.Object {
	out := new(SnapshotContentList)
	*out = *s
	s.ListMeta.DeepCopyInto(&out.ListMeta)

	if s.Items != nil {
		out.Items = make([]SnapshotContent, len(s.Items))
		for i := range s.Items {
			s.Items[i].DeepCopyInto(&out.Items[i])
		}
	}

	return out
}

// SnapshotContentSpec describes the desired state of a SnapshotContent.
type SnapshotContentSpec struct {
	// DeletionPolicy controls whether the controller may delete this SnapshotContent.
	DeletionPolicy string `json:"deletionPolicy,omitempty"`
}

// SnapshotContentStatus carries the latest observations for a SnapshotContent.
type SnapshotContentStatus struct {
	// ManifestCheckpointName is the cluster-scoped ManifestCheckpoint name once manifest capture has persisted.
	ManifestCheckpointName string `json:"manifestCheckpointName,omitempty"`

	// ChildrenSnapshotContentRefs lists direct child SnapshotContent objects in the snapshot tree.
	ChildrenSnapshotContentRefs []SnapshotContentChildRef `json:"childrenSnapshotContentRefs,omitempty"`

	// DataRef is the single PVC-target-to-data-artifact binding for this logical snapshot node.
	// Variant A (cardinality ≤1): a node carries at most one data artifact; multiple volumes are
	// represented as separate child volume nodes (childrenSnapshotContentRefs), never as a list here.
	DataRef *SnapshotDataBinding `json:"dataRef,omitempty"`

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

// SnapshotDataBinding associates the single PVC target of a logical snapshot node with its captured
// data artifact. Variant A (cardinality ≤1): a SnapshotContent carries at most ONE dataRef; multiple
// volumes are modeled as child volume nodes (each its own SnapshotContent), never as a list on one node.
// Field names and types mirror state-snapshotter/api/storage/v1alpha1/snapshotcontent_types.go:97-137.
type SnapshotDataBinding struct {
	// TargetUID identifies the captured PersistentVolumeClaim (its UID) backing this node's data.
	TargetUID string `json:"targetUID"`

	// Target identifies the PVC (and related metadata) captured in MCP for this binding.
	Target SnapshotSubjectRef `json:"target"`

	// Artifact references the cluster-scoped durable data artifact (for example VolumeSnapshotContent).
	Artifact SnapshotDataArtifactRef `json:"artifact"`

	// VolumeMode records the source volume mode (Block or Filesystem).
	VolumeMode string `json:"volumeMode,omitempty"`

	// FsType records the source filesystem type (Filesystem volumes only).
	FsType string `json:"fsType,omitempty"`

	// AccessModes records the source PVC access modes (e.g. ReadWriteOnce, ReadWriteMany).
	AccessModes []string `json:"accessModes,omitempty"`

	// StorageClassName records the source StorageClass of the captured volume.
	StorageClassName string `json:"storageClassName,omitempty"`

	// Size records the real allocated size of the captured volume (e.g. "10Gi"),
	// taken from the data artifact (VolumeSnapshotContent.status.restoreSize).
	Size string `json:"size,omitempty"`
}

// DataRefList returns status.dataRef as a slice of length 0 or 1. Variant A keeps cardinality ≤1
// on a node; this bridge lets callers iterate the single binding without special-casing the nil pointer.
func (s *SnapshotContent) DataRefList() []SnapshotDataBinding {
	if s.Status.DataRef == nil {
		return nil
	}

	return []SnapshotDataBinding{*s.Status.DataRef}
}
