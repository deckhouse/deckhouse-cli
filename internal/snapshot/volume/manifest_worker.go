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

// Package volume provides workers for downloading block/filesystem volumes and
// writing node manifests into the snapshot output directory tree.
package volume

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
)

// WriteNodeManifests fetches the own-scope manifests for node from src and writes
// each object as an uncompressed YAML file into <nodeDir>/manifests/ using
// archive.WriteManifest. Collision fallback (same kind+name but different API group)
// is handled transparently by WriteManifest.
//
// PersistentVolumeClaims are excluded in two cases:
//   - Orphan leaf children (node.Children[i].Binding != nil): the captured PVC
//     manifest belongs in each leaf node's own manifests/ directory.
//   - OwnDataRefs targets (node.OwnDataRefs[i].Target): the PVC data is already
//     captured in the volume payload (data.bin[.<ext>] or data.tar); the PVC
//     identity is recorded in snapshot.yaml Volumes[].Target.
//
// Matching is by metadata.uid first; if the uid is absent in the captured manifest,
// it falls back to metadata.name.
//
// The manifests are fetched from the node's own manifests-download subresource.
//
// The operation is idempotent: rewriting an already-present object with the same
// kind, name, and API group is a no-op.
func WriteNodeManifests(ctx context.Context, src source.ManifestSource, nodeDir string, node *source.Node) error {
	objs, err := src.FetchNodeManifests(ctx, node.Ref())
	if err != nil {
		return fmt.Errorf("fetch manifests for %s/%s: %w", node.Kind, node.Name, err)
	}

	excluded := buildPVCExclusion(node.Children, node.OwnDataRefs)

	for _, obj := range objs {
		if isExcludedDataRefPVC(obj, excluded) {
			continue
		}

		if err := archive.WriteManifest(nodeDir, obj); err != nil {
			return fmt.Errorf("write manifest %s/%s: %w", obj.GetKind(), obj.GetName(), err)
		}
	}

	return nil
}

// WriteVolumeManifest fetches the manifests in the volume node's scope via src and
// writes the single PVC that corresponds to the volume node's Binding.Target into
// <volumeDir>/manifests/persistentvolumeclaim_<name>.yaml.
//
// For orphan leaf volume nodes the captured PVC manifest lives in the parent
// aggregator node's own manifests, so the scope ref is the parent ref (see
// source.Node.ManifestScopeRef).
//
// The target PVC is matched by metadata.uid first (when both the binding's TargetUID
// and the captured object's uid are non-empty); otherwise by metadata.name.
//
// Returns an error if the target PVC is not present in the fetched manifests.
func WriteVolumeManifest(ctx context.Context, src source.ManifestSource, volumeDir string, volNode *source.Node) error {
	scopeRef := volNode.ManifestScopeRef()

	objs, err := src.FetchNodeManifests(ctx, scopeRef)
	if err != nil {
		return fmt.Errorf("fetch manifests for volume node %s: %w", volNode.Name, err)
	}

	var targetUID, targetName string

	if volNode.Binding != nil {
		targetUID = volNode.Binding.TargetUID
		targetName = volNode.Binding.Target.Name
	}

	for _, obj := range objs {
		if !matchesVolumeTarget(obj, targetUID, targetName) {
			continue
		}

		return archive.WriteManifest(volumeDir, obj)
	}

	return fmt.Errorf("target PVC %q (uid=%q) not found in manifests of %s/%s",
		targetName, targetUID, scopeRef.Kind, scopeRef.Name)
}

// FinalizeNode computes the node integrity checksum over all current files in
// nodeDir (manifests/*.yaml, data.bin[.<ext>], data.tar, data/<pvc>.*) and atomically writes
// <nodeDir>/snapshot.yaml. It must be called after all manifests and volume data
// for the node are fully written.
//
// The snapshot.yaml Volumes list is populated as follows:
//   - Orphan leaf volume nodes (node.Binding != nil): one VolumeInfo from Binding.
//   - Non-aggregator snapshot nodes (node.OwnDataRefs non-empty): one VolumeInfo
//     per OwnDataRef binding.
//   - All other nodes (aggregators and manifest-only): Volumes is nil (omitted).
//
// The Volumes field does not affect ComputeNodeChecksum because snapshot.yaml is
// excluded from the integrity digest.
//
// FinalizeNode is idempotent: each call recomputes the checksum and overwrites
// snapshot.yaml with the fresh value. The pipeline calls it once per node after
// both WriteNodeManifests and any volume download have completed.
func FinalizeNode(nodeDir string, node *source.Node) error {
	checksum, err := archive.ComputeNodeChecksum(nodeDir)
	if err != nil {
		return fmt.Errorf("compute checksum for %s/%s: %w", node.Kind, node.Name, err)
	}

	sy := archive.SnapshotYAML{
		APIVersion:      node.APIVersion,
		Kind:            node.Kind,
		Name:            node.Name,
		Namespace:       node.Namespace,
		SourceRef:       node.SourceRef,
		SourceName:      node.SourceName,
		SourceObjectRef: buildSourceObjectRef(node.SpecSourceRef),
		Checksum:        checksum,
		Volumes:         buildVolumesList(node),
	}

	if err := archive.WriteSnapshotYAML(nodeDir, sy); err != nil {
		return fmt.Errorf("write snapshot.yaml for %s/%s: %w", node.Kind, node.Name, err)
	}

	return nil
}

// buildVolumesList constructs the Volumes list for snapshot.yaml from a node.
// Returns nil (omitted) when the node owns no volumes.
func buildVolumesList(node *source.Node) []archive.VolumeInfo {
	// Orphan leaf volume node: single Binding.
	if node.Binding != nil {
		return []archive.VolumeInfo{bindingToVolumeInfo(node.Binding)}
	}

	// Non-aggregator snapshot node: one entry per OwnDataRef.
	if len(node.OwnDataRefs) == 0 {
		return nil
	}

	vols := make([]archive.VolumeInfo, len(node.OwnDataRefs))
	for i := range node.OwnDataRefs {
		vols[i] = bindingToVolumeInfo(&node.OwnDataRefs[i])
	}

	return vols
}

// bindingToVolumeInfo converts a SnapshotDataBinding to a VolumeInfo. The volume metadata
// (volumeMode/storageClassName/size) is carried through so the import side can rebuild the
// DataImport spec for a Mode A re-import without re-reading the live SnapshotContent.
func bindingToVolumeInfo(b *snapshotapi.SnapshotDataBinding) archive.VolumeInfo {
	return archive.VolumeInfo{
		Target: archive.VolumeObjectRef{
			APIVersion: b.Target.APIVersion,
			Kind:       b.Target.Kind,
			Name:       b.Target.Name,
			Namespace:  b.Target.Namespace,
			UID:        string(b.Target.UID),
		},
		Artifact: archive.VolumeObjectRef{
			APIVersion: b.Artifact.APIVersion,
			Kind:       b.Artifact.Kind,
			Name:       b.Artifact.Name,
		},
		VolumeMode:       b.VolumeMode,
		StorageClassName: b.StorageClassName,
		Size:             b.Size,
	}
}

// dataRefExclusion holds PVC identifiers to skip when writing snapshot node manifests.
// It covers orphan leaf child PVCs (written into the leaf's own manifests/ dir) and
// OwnDataRef target PVCs (whose data is captured in the volume payload).
type dataRefExclusion struct {
	uids  map[string]struct{}
	names map[string]struct{}
}

// buildPVCExclusion constructs an exclusion set from both orphan leaf volume children
// (child.Binding != nil) and OwnDataRef bindings. PVCs in either category must not be
// written into the owning node's manifests/ directory.
func buildPVCExclusion(children []*source.Node, ownDataRefs []snapshotapi.SnapshotDataBinding) dataRefExclusion {
	cap := len(children) + len(ownDataRefs)
	ex := dataRefExclusion{
		uids:  make(map[string]struct{}, cap),
		names: make(map[string]struct{}, cap),
	}

	for _, child := range children {
		if child.Binding == nil {
			continue
		}

		if child.Binding.TargetUID != "" {
			ex.uids[child.Binding.TargetUID] = struct{}{}
		}

		if child.Binding.Target.Name != "" {
			ex.names[child.Binding.Target.Name] = struct{}{}
		}
	}

	for i := range ownDataRefs {
		b := &ownDataRefs[i]

		if b.TargetUID != "" {
			ex.uids[b.TargetUID] = struct{}{}
		}

		if b.Target.Name != "" {
			ex.names[b.Target.Name] = struct{}{}
		}
	}

	return ex
}

// isExcludedDataRefPVC returns true when obj is a PersistentVolumeClaim that matches
// a DataRef target. Matching is by metadata.uid first; when uid is absent in the
// captured manifest, it falls back to metadata.name.
func isExcludedDataRefPVC(obj unstructured.Unstructured, ex dataRefExclusion) bool {
	if obj.GetKind() != "PersistentVolumeClaim" {
		return false
	}

	objUID := string(obj.GetUID())
	if objUID != "" {
		_, ok := ex.uids[objUID]

		return ok
	}

	_, ok := ex.names[obj.GetName()]

	return ok
}

// matchesVolumeTarget reports whether obj is the PVC identified by the given target
// uid and name. Matching is by uid first (when both sides are non-empty), then by name.
func matchesVolumeTarget(obj unstructured.Unstructured, targetUID, targetName string) bool {
	if obj.GetKind() != "PersistentVolumeClaim" {
		return false
	}

	objUID := string(obj.GetUID())
	if targetUID != "" && objUID != "" {
		return objUID == targetUID
	}

	return targetName != "" && obj.GetName() == targetName
}

// buildSourceObjectRef maps a source.SpecSourceRef to an archive.SourceObjectRef.
// Returns nil when src is nil (core Snapshot and VolumeSnapshot leaf nodes).
func buildSourceObjectRef(src *source.SpecSourceRef) *archive.SourceObjectRef {
	if src == nil {
		return nil
	}

	return &archive.SourceObjectRef{
		APIVersion: src.APIVersion,
		Kind:       src.Kind,
		Name:       src.Name,
	}
}
