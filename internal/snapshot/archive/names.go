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

// Package archive provides deterministic naming helpers for the snapshot output tree.
// All functions are pure and free of I/O.
package archive

import (
	"fmt"
	"strings"
)

// Fixed names in the per-node directory layout.
const (
	// SnapshotYAMLName is the filename for the per-node snapshot manifest plus checksum.
	SnapshotYAMLName = "snapshot.yaml"

	// ManifestsDirName is the subdirectory that holds own-scope manifests (always present).
	ManifestsDirName = "manifests"

	// SnapshotsDirName is the subdirectory that holds child node directories.
	// Present only when a node has children.
	SnapshotsDirName = "snapshots"

	// DataBlockName is the filename for a completed block-volume zstd stream.
	DataBlockName = "data.img.zst"

	// DataDirName is the directory name for a filesystem-volume (per-file .zst tree).
	DataDirName = "data"

	// BlockChunksDirName is the temporary directory that holds individual block-volume
	// zstd frames while the volume is being downloaded. It lives next to DataBlockName
	// inside the node directory and is removed after the frames are merged into
	// DataBlockName.
	BlockChunksDirName = DataBlockName + ".d"
)

// NodeDirName returns the directory name for a child snapshot node.
// The name is "<kindlower>_<name>" per the directory-tree layout rules.
// For the root node, callers use the user-supplied output directory name directly.
func NodeDirName(kind, name string) string {
	return strings.ToLower(kind) + "_" + name
}

// ManifestFileName returns the filename for a single Kubernetes manifest in manifests/.
//
//   - Normal form (no collision): "<kindlower>_<name>.yaml"
//   - Collision fallback (same kind+name, different API groups):
//     "<kindlower>.<apiGroup>_<name>.yaml"
//
// Pass an empty apiGroup for the normal (non-collision) form.
func ManifestFileName(kind, name, apiGroup string) string {
	k := strings.ToLower(kind)

	if apiGroup == "" {
		return k + "_" + name + ".yaml"
	}

	return k + "." + apiGroup + "_" + name + ".yaml"
}

// FsFileName returns the output path for a single filesystem file under data/.
// The source relative path is preserved and ".zst" is appended.
// Example: FsFileName("sub/file.txt") → "sub/file.txt.zst".
func FsFileName(relPath string) string {
	return relPath + ".zst"
}

// ChunkFileName returns the filename for block-volume chunk index i inside
// BlockChunksDirName. Indices are zero-padded to five digits:
// "chunk_00000.zst" through "chunk_99999.zst".
func ChunkFileName(i int) string {
	return fmt.Sprintf("chunk_%05d.zst", i)
}
