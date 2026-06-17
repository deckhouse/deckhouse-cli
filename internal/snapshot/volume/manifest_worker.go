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

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
)

// WriteNodeManifests fetches the own-scope manifests for node from src and writes
// each object as an uncompressed YAML file into <nodeDir>/manifests/ using
// archive.WriteManifest. Collision fallback (same kind+name but different API group)
// is handled transparently by WriteManifest.
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

	for _, obj := range objs {
		if err := archive.WriteManifest(nodeDir, obj); err != nil {
			return fmt.Errorf("write manifest %s/%s: %w", obj.GetKind(), obj.GetName(), err)
		}
	}

	return nil
}

// FinalizeNode computes the node integrity checksum over all current files in
// nodeDir (manifests/*.yaml, data.img.zst, data/**/*.zst) and atomically writes
// <nodeDir>/snapshot.yaml. It must be called after all manifests and volume data
// for the node are fully written.
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

	if err := archive.WriteSnapshotYAML(nodeDir, sy); err != nil {
		return fmt.Errorf("write snapshot.yaml for %s/%s: %w", node.Kind, node.Name, err)
	}

	return nil
}
