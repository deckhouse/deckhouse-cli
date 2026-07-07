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
// All functions are pure and free of I/O, except FindBlockData which performs a
// directory glob to locate an existing block-volume file.
package archive

import (
	"fmt"
	"os"
	"path/filepath"
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

	// DataBlockBase is the base filename (without codec extension) for the completed
	// block-volume output file. The actual filename is DataBlockName(codec.Ext()).
	DataBlockBase = "data.bin"

	// FsTarName is the output filename for a single-volume filesystem volume.
	// The tar container is uncompressed; each file entry inside is individually
	// compressed with the selected codec and named <path><ext> (ext empty for none).
	FsTarName = "data.tar"

	// FsTarStagingDirName is the temporary directory that holds raw per-file downloads
	// while a filesystem volume is being assembled. It lives next to data.tar inside
	// the node directory and is removed after the tar is assembled.
	FsTarStagingDirName = "data.tar.d"

	// DataDirName is the top-level directory for multi-volume output files.
	//
	// Multi-volume block files:       data/<pvc>.bin[.<ext>]
	// Multi-volume FS tar files:      data/<pvc>.tar
	// Multi-volume block staging:     data/<pvc>.bin.d/
	// Multi-volume FS staging:        data/<pvc>.tar.d/
	DataDirName = "data"

	// BlockChunksDirName is the temporary directory that holds individual block-volume
	// frames while the volume is being downloaded. It lives next to the merged output
	// file inside the node directory and is removed after the frames are merged.
	BlockChunksDirName = DataBlockBase + ".d"
)

// DataBlockName returns the output filename for a block-volume with the given
// codec extension. ext is codec.Ext() (e.g. ".zst", ".lz4", ".gz", or "" for none).
// Examples: DataBlockName(".zst") → "data.bin.zst", DataBlockName("") → "data.bin".
func DataBlockName(ext string) string {
	return DataBlockBase + ext
}

// FindBlockData searches nodeDir for a completed block-volume file (any file
// whose name starts with DataBlockBase, excluding the staging directory
// DataBlockBase+".d"). The first non-directory match is returned as an absolute
// path. The second return value is false when no such file exists. An I/O error
// is returned in the third return value.
func FindBlockData(nodeDir string) (string, bool, error) {
	pattern := filepath.Join(nodeDir, DataBlockBase+"*")

	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", false, fmt.Errorf("glob block data in %s: %w", nodeDir, err)
	}

	for _, m := range matches {
		info, statErr := os.Stat(m)
		if statErr != nil {
			continue
		}

		if !info.IsDir() {
			return m, true, nil
		}
	}

	return "", false, nil
}

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

// ChunkFileName returns the filename for block-volume chunk index i inside
// BlockChunksDirName. Indices are zero-padded to five digits. ext is the codec
// extension (e.g. ".zst", ".lz4", ".gz", or "" for the none codec).
// Examples: ChunkFileName(0, ".zst") → "chunk_00000.zst", ChunkFileName(3, "") → "chunk_00003".
func ChunkFileName(i int, ext string) string {
	return fmt.Sprintf("chunk_%05d%s", i, ext)
}

// Multi-volume layout helpers (N > 1 own data references in a node).
//
// Layout rules:
//
//	1 own volume  → flat:  data.bin[.<ext>] (block) or data.tar (FS)
//	N > 1 volumes → namespaced under data/:
//	                  data/<pvc>.bin[.<ext>]  (block)
//	                  data/<pvc>.tar          (FS)
//
// The checksum walk covers all files under data/ recursively (staging dirs
// whose names end with ".d" are skipped), so both layouts are covered by
// ComputeNodeChecksum without extra configuration.

// MultiVolumeBlockName returns the relative path for a block-volume output file
// in the N>1 multi-volume layout: "data/<pvc>.bin[.<ext>]".
// ext is codec.Ext() (e.g. ".zst", ".lz4", ".gz", or "" for none).
// For the single-volume flat case use DataBlockName(ext) directly.
func MultiVolumeBlockName(pvc, ext string) string {
	return filepath.Join(DataDirName, pvc+".bin"+ext)
}

// MultiVolumeTarName returns the relative path for a filesystem-volume tar file
// in the N>1 multi-volume layout: "data/<pvc>.tar".
// For the single-volume flat case use FsTarName directly.
func MultiVolumeTarName(pvc string) string {
	return filepath.Join(DataDirName, pvc+".tar")
}

// MultiVolumeTarStagingDirName returns the relative path of the temporary staging
// directory for a filesystem-volume in the N>1 multi-volume layout: "data/<pvc>.tar.d".
// Raw files are staged here during download then assembled into MultiVolumeTarName(pvc).
// For the single-volume flat case use FsTarStagingDirName directly.
func MultiVolumeTarStagingDirName(pvc string) string {
	return filepath.Join(DataDirName, pvc+".tar.d")
}

// BlockChunksDirNameFor returns the relative path of the temporary chunk directory
// for a named PVC volume in the N>1 multi-volume layout: "data/<pvc>.bin.d".
// Chunks accumulate here during download and are merged into MultiVolumeBlockName(pvc, ext)
// on completion.  For the single-volume flat case use BlockChunksDirName directly.
func BlockChunksDirNameFor(pvc string) string {
	return filepath.Join(DataDirName, pvc+".bin.d")
}

// FsFileChunksDirName returns the per-file chunk directory name for one large
// filesystem-volume file: "<relPath><ext>.d". relPath is the item's forward-slash
// relative path within the volume (e.g. "disk/payload.bin") and ext is the codec
// extension (e.g. ".zst", or "" for the none codec).
//
// A caller joins this (via filepath.FromSlash) under the FS staging directory
// (FsTarStagingDirName or MultiVolumeTarStagingDirName), so the chunk directory
// nests inside the existing per-volume staging dir, e.g.
// "data.tar.d/payload.bin.zst.d/". Chunks accumulate here while a file larger
// than the effective chunk size is downloaded via Range GETs, and are merged
// into "<relPath><ext>" (the same path DownloadBlockChunks/MergeBlockChunks use
// for a single block volume) once complete. Mirrors BlockChunksDirNameFor's
// naming pattern for the per-file (rather than per-volume) case.
func FsFileChunksDirName(relPath, ext string) string {
	return relPath + ext + ".d"
}
