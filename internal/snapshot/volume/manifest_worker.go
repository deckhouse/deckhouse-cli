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
// PersistentVolumeClaims that correspond to volume DataRefs (node.DataRefs[].Target)
// are excluded: they belong in each volume child node's own manifests/ directory,
// not in the snapshot node's. Matching is by metadata.uid first; if the uid is
// absent in the captured manifest, it falls back to metadata.name.
//
// When node.ManifestCheckpointName is empty, no manifests exist and the function
// returns immediately (valid state).
//
// The operation is idempotent: rewriting an already-present object with the same
// kind, name, and API group is a no-op.
func WriteNodeManifests(ctx context.Context, src source.ManifestSource, nodeDir string, node *source.Node) error {
	if node.ManifestCheckpointName == "" {
		return nil
	}

	objs, err := src.FetchNodeManifests(ctx, node.ManifestCheckpointName)
	if err != nil {
		return fmt.Errorf("fetch manifests for %s/%s: %w", node.Kind, node.Name, err)
	}

	excluded := buildDataRefExclusion(node.DataRefs)

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

// WriteVolumeManifest fetches the parent checkpoint objects via src and writes the
// single PVC that corresponds to the volume node's Binding.Target into
// <volumeDir>/manifests/persistentvolumeclaim_<name>.yaml.
//
// The target PVC is matched by metadata.uid first (when both the binding's TargetUID
// and the captured object's uid are non-empty); otherwise by metadata.name.
//
// When volNode.ManifestCheckpointName is empty, no checkpoint data is available and
// the function returns nil (no-op). Callers that need the manifest for shadow-meta
// resolution will fall back to a live PVC lookup.
//
// Returns an error if the target PVC is not present in the checkpoint (when one exists).
func WriteVolumeManifest(ctx context.Context, src source.ManifestSource, volumeDir string, volNode *source.Node) error {
	if volNode.ManifestCheckpointName == "" {
		return nil
	}

	objs, err := src.FetchNodeManifests(ctx, volNode.ManifestCheckpointName)
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

	return fmt.Errorf("target PVC %q (uid=%q) not found in checkpoint %q",
		targetName, targetUID, volNode.ManifestCheckpointName)
}

// FinalizeNode computes the node integrity checksum over all current files in
// nodeDir (manifests/*.yaml, data.img.zst, data/**/*.zst) and atomically writes
// <nodeDir>/snapshot.yaml. It must be called after all manifests and volume data
// for the node are fully written.
//
// For volume nodes (node.Binding != nil) the snapshot.yaml carries a volume block
// that records the captured PVC (Target) and its data artifact (Artifact). For
// snapshot nodes the volume block is omitted (omitempty). The volume block does not
// affect ComputeNodeChecksum because snapshot.yaml is excluded from the digest.
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
		APIVersion: node.APIVersion,
		Kind:       node.Kind,
		Name:       node.Name,
		Namespace:  node.Namespace,
		SourceRef:  node.SourceRef,
		Checksum:   checksum,
	}

	if node.Binding != nil {
		sy.Volume = &archive.VolumeInfo{
			Target: archive.VolumeObjectRef{
				APIVersion: node.Binding.Target.APIVersion,
				Kind:       node.Binding.Target.Kind,
				Name:       node.Binding.Target.Name,
				Namespace:  node.Binding.Target.Namespace,
				UID:        string(node.Binding.Target.UID),
			},
			Artifact: archive.VolumeObjectRef{
				APIVersion: node.Binding.Artifact.APIVersion,
				Kind:       node.Binding.Artifact.Kind,
				Name:       node.Binding.Artifact.Name,
			},
		}
	}

	if err := archive.WriteSnapshotYAML(nodeDir, sy); err != nil {
		return fmt.Errorf("write snapshot.yaml for %s/%s: %w", node.Kind, node.Name, err)
	}

	return nil
}

// dataRefExclusion holds PVC identifiers to skip when writing snapshot node manifests.
// The captured PVC manifest for each data volume belongs in the corresponding volume
// child node's own manifests/ directory instead.
type dataRefExclusion struct {
	uids  map[string]struct{}
	names map[string]struct{}
}

// buildDataRefExclusion constructs an exclusion set from a snapshot node's DataRefs.
func buildDataRefExclusion(dataRefs []snapshotapi.SnapshotDataBinding) dataRefExclusion {
	ex := dataRefExclusion{
		uids:  make(map[string]struct{}, len(dataRefs)),
		names: make(map[string]struct{}, len(dataRefs)),
	}

	for _, ref := range dataRefs {
		if ref.TargetUID != "" {
			ex.uids[ref.TargetUID] = struct{}{}
		}

		if ref.Target.Name != "" {
			ex.names[ref.Target.Name] = struct{}{}
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
