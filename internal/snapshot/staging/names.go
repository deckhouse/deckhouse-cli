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

// Package staging provides deterministic naming helpers for the snapshot archive
// staging tree. All functions are pure and free of I/O.
package staging

import (
	"fmt"
	"strings"
)

// NodeDirName returns the staging directory name for a child snapshot node.
// The name is "<kindlower>_<name>" per plan §3 rule 2.
// For the root node, callers should use the namespace directly.
func NodeDirName(kind, name string) string {
	return strings.ToLower(kind) + "_" + name
}

// ManifestFileName returns the archive filename for a single Kubernetes manifest.
// Per plan §3 rule 4:
//   - Normal form: "<kindlower>_<name>.yaml"
//   - Collision fallback (two manifests in the same node dir share <kind>_<name> but
//     belong to different API groups): "<kindlower>.<apiGroup>_<name>.yaml".
//
// Pass an empty apiGroup for the normal (non-collision) form.
func ManifestFileName(kind, name, apiGroup string) string {
	k := strings.ToLower(kind)

	if apiGroup == "" {
		return k + "_" + name + ".yaml"
	}

	return k + "." + apiGroup + "_" + name + ".yaml"
}

// BlockDataName returns the filename for a completed block-volume archive entry.
// Format: "<kindlower>_<name>.img.zst" per plan §3 rule 5.
func BlockDataName(kind, name string) string {
	return strings.ToLower(kind) + "_" + name + ".img.zst"
}

// FsDataName returns the filename for a completed filesystem-volume archive entry.
// Format: "<kindlower>_<name>.fs.tar.zst" per plan §3 rule 6.
func FsDataName(kind, name string) string {
	return strings.ToLower(kind) + "_" + name + ".fs.tar.zst"
}

// ChecksumsFileName is the name of the per-node integrity sidecar per plan §3 rule 9.
const ChecksumsFileName = "checksums.sha256"

// ChunkFileName returns the filename for chunk index i inside a block-volume staging dir.
// Chunk indices are zero-padded to five digits: "chunk_00000.zst" through "chunk_99999.zst".
func ChunkFileName(i int) string {
	return fmt.Sprintf("chunk_%05d.zst", i)
}

// BlockStagingDirName returns the name of the temporary staging directory that holds
// individual block-volume chunks while the volume is being downloaded.
// Format: "<kindlower>_<name>.img.zst.d" per plan §5.
func BlockStagingDirName(kind, name string) string {
	return BlockDataName(kind, name) + ".d"
}

// FsStagingDirName returns the name of the temporary staging directory that holds
// the filesystem tree while the volume is being downloaded.
// Format: "<kindlower>_<name>.fs.d" per plan §5.
func FsStagingDirName(kind, name string) string {
	return strings.ToLower(kind) + "_" + name + ".fs.d"
}
