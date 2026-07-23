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
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
)

// WriteNodeManifests fetches the own-scope manifests for node from src and writes
// each object as an uncompressed YAML file into <nodeDir>/manifests/ using
// archive.WriteManifest. Collision fallback (same kind+name but different API group)
// is handled transparently by WriteManifest.
//
// PersistentVolumeClaims are excluded in two cases:
//   - Volume-leaf children (node.Children[i].IsVolumeLeaf()): the captured PVC
//     manifest belongs in each leaf node's own manifests/ directory.
//   - The node's own captured volume (node.Data.SourceRef): the PVC data is already
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

	excluded := buildPVCExclusion(node)

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
// writes the single PVC that corresponds to the volume node's captured source into
// <volumeDir>/manifests/persistentvolumeclaim_<name>.yaml.
//
// For orphan leaf volume nodes the captured PVC manifest lives in the parent
// aggregator node's own manifests, so the scope ref is the parent ref (see
// source.Node.ManifestScopeRef).
//
// The target PVC is matched by metadata.uid first (when both the node's captured
// source uid and the captured object's uid are non-empty); otherwise by metadata.name.
//
// Returns an error if the target PVC is not present in the fetched manifests.
func WriteVolumeManifest(ctx context.Context, src source.ManifestSource, volumeDir string, volNode *source.Node) error {
	scopeRef := volNode.ManifestScopeRef()

	objs, err := src.FetchNodeManifests(ctx, scopeRef)
	if err != nil {
		return fmt.Errorf("fetch manifests for volume node %s: %w", volNode.Name, err)
	}

	var targetUID, targetName string

	if volNode.Data != nil {
		targetUID = volNode.Data.SourceRef.UID
		targetName = volNode.Data.SourceRef.Name
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

// FinalizeNode is FinalizeNodeContext with a non-cancellable context.
func FinalizeNode(nodeDir string, node *source.Node) error {
	return FinalizeNodeContext(context.Background(), nodeDir, node)
}

// FinalizeNodeContext computes the node integrity checksum over all current files in
// nodeDir (manifests/*.yaml, data.bin[.<ext>], data.tar, data/<pvc>.*) and atomically writes
// <nodeDir>/snapshot.yaml. It must be called after all manifests and volume data
// for the node are fully written.
//
// The snapshot.yaml Volumes list is populated as follows:
//   - Nodes that captured their own volume (node.Data != nil): one VolumeInfo from
//     status.data (Variant A, cardinality ≤1) — covers both non-aggregator domain nodes
//     and orphan leaf volume nodes.
//   - All other nodes (aggregators and manifest-only): Volumes is nil (omitted).
//
// The Volumes field does not affect ComputeNodeChecksum because snapshot.yaml is
// excluded from the integrity digest.
//
// FinalizeNodeContext is idempotent: each call recomputes the checksum and overwrites
// snapshot.yaml with the fresh value. The pipeline calls it once per node after
// both WriteNodeManifests and any volume download have completed.
//
// After snapshot.yaml is durably written, FinalizeNodeContext removes the resume
// identity marker (archive.NodeIdentityMarkerName). The marker exists only to
// prove a PARTIAL (snapshot.yaml-less) dir belongs to this snapshot (inv. #9);
// once snapshot.yaml — the authoritative identity record VerifyNode/ScanNode
// read — is on disk, the marker is redundant and leaving it would violate the
// documented final node layout (snapshot.yaml + manifests/ + optional
// snapshots/ + at most one volume payload). The remove happens strictly AFTER
// the snapshot.yaml write so a crash at any earlier point still leaves the
// marker in place and a partial dir always carries exactly one identity record.
// Removal is checksum-neutral (ComputeNodeChecksum/collectNodeFiles never read
// the marker), so it cannot perturb the checksum just written or any later
// VerifyNode.
func FinalizeNodeContext(ctx context.Context, nodeDir string, node *source.Node) error {
	checksum, err := archive.ComputeNodeChecksum(nodeDir)
	if err != nil {
		return fmt.Errorf("compute checksum for %s/%s: %w", node.Kind, node.Name, err)
	}

	sy := archive.SnapshotYAML{
		APIVersion:      node.APIVersion,
		Kind:            node.Kind,
		Name:            node.Name,
		Namespace:       node.Namespace,
		UID:             string(node.UID),
		SourceName:      sourceObjectName(node.SourceRef),
		SourceObjectRef: buildSourceObjectRef(node.SourceRef),
		Checksum:        checksum,
		Volumes:         buildVolumesList(node),
	}

	if err := archive.WriteSnapshotYAMLContext(ctx, nodeDir, sy); err != nil {
		return fmt.Errorf("write snapshot.yaml for %s/%s: %w", node.Kind, node.Name, err)
	}

	markerPath := filepath.Join(nodeDir, archive.NodeIdentityMarkerName)
	if err := os.Remove(markerPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove identity marker for %s/%s: %w", node.Kind, node.Name, err)
	}

	return nil
}

// buildVolumesList constructs the Volumes list for snapshot.yaml from a node.
// Returns nil (omitted) when the node captured no volume.
func buildVolumesList(node *source.Node) []archive.VolumeInfo {
	if node.Data == nil {
		return nil
	}

	return []archive.VolumeInfo{nodeDataToVolumeInfo(node.Data)}
}

// nodeDataToVolumeInfo converts a namespaced status.data descriptor to a VolumeInfo. The
// volume metadata (volumeMode/storageClassName/size) is carried through so the import side
// can rebuild the DataImport spec for a re-import without re-reading the live cluster state.
func nodeDataToVolumeInfo(d *source.NodeData) archive.VolumeInfo {
	return archive.VolumeInfo{
		Target: archive.VolumeObjectRef{
			APIVersion: d.SourceRef.APIVersion,
			Kind:       d.SourceRef.Kind,
			Name:       d.SourceRef.Name,
			Namespace:  d.SourceRef.Namespace,
			UID:        d.SourceRef.UID,
		},
		Artifact: archive.VolumeObjectRef{
			APIVersion: d.ArtifactRef.APIVersion,
			Kind:       d.ArtifactRef.Kind,
			Name:       d.ArtifactRef.Name,
		},
		VolumeMode:       d.VolumeMode,
		StorageClassName: d.StorageClassName,
		Size:             d.Size,
	}
}

// dataRefExclusion holds PVC identifiers to skip when writing snapshot node manifests.
// It covers volume-leaf child PVCs (written into the leaf's own manifests/ dir) and the
// node's own captured PVC (whose data is captured in the volume payload).
type dataRefExclusion struct {
	uids  map[string]struct{}
	names map[string]struct{}
}

// buildPVCExclusion constructs an exclusion set from the node's own captured volume
// (node.Data) and its volume-leaf children (child.IsVolumeLeaf()). PVCs in either category
// must not be written into the owning node's manifests/ directory. Domain snapshot children
// keep their own captured PVCs in their own directories and are not excluded here.
func buildPVCExclusion(node *source.Node) dataRefExclusion {
	ex := dataRefExclusion{
		uids:  make(map[string]struct{}, len(node.Children)+1),
		names: make(map[string]struct{}, len(node.Children)+1),
	}

	addDataExclusion(&ex, node.Data)

	for _, child := range node.Children {
		if child.IsVolumeLeaf() {
			addDataExclusion(&ex, child.Data)
		}
	}

	return ex
}

// addDataExclusion records the captured PVC identity (uid and name) of d into ex.
// A nil descriptor is a no-op.
func addDataExclusion(ex *dataRefExclusion, d *source.NodeData) {
	if d == nil {
		return
	}

	if d.SourceRef.UID != "" {
		ex.uids[d.SourceRef.UID] = struct{}{}
	}

	if d.SourceRef.Name != "" {
		ex.names[d.SourceRef.Name] = struct{}{}
	}
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

// buildSourceObjectRef maps the node's namespaced status.sourceRef identity to an
// archive.SourceObjectRef (apiVersion/kind/name of the captured source object), persisted so
// the import side can recreate the CR in import mode. Returns nil when the node has no
// status.sourceRef.
func buildSourceObjectRef(src *source.SourceRefIdentity) *archive.SourceObjectRef {
	if src == nil {
		return nil
	}

	return &archive.SourceObjectRef{
		APIVersion: src.APIVersion,
		Kind:       src.Kind,
		Name:       src.Name,
	}
}

// sourceObjectName returns the captured source object's name (status.sourceRef.name),
// or "" when the node has no status.sourceRef. Recorded in snapshot.yaml for readability.
func sourceObjectName(src *source.SourceRefIdentity) string {
	if src == nil {
		return ""
	}

	return src.Name
}
