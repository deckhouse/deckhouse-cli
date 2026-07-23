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
// All functions are pure and free of I/O, except FindBlockData and
// ClassifyBlockPayload, which read a node directory's entries to locate an
// existing block-volume file.
package archive

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

	// NodeIdentityMarkerName is the identity sidecar written into a node directory
	// on FIRST touch (WriteNodeIdentityMarker), before any chunk/staging/volume
	// data lands. It records the node's snapshot identity so a resume scan can
	// prove a PARTIAL (not-yet-finalized) directory belongs to the planned node —
	// snapshot.yaml, the only other identity record, is written just at finalize,
	// so without this marker a partial dir carries no identity and could be
	// silently resumed into by a DIFFERENT snapshot of the same source object.
	//
	// LIFECYCLE: the marker lives ONLY for the not-yet-finalized window. Once
	// snapshot.yaml is durably written, snapshot.yaml is the authoritative
	// identity record (every Done classification reads identity from it, not the
	// marker), so volume.FinalizeNode removes the marker strictly AFTER that
	// write — a crash at any earlier point leaves it in place, so a partial dir
	// always carries exactly one identity record (inv. #9 preserved). The two
	// Done=true scan branches (classifyCompleteDir and ScanAbsolute) also remove
	// a leftover marker via healNodeIdentityMarker, self-healing the crash window
	// between the snapshot.yaml write and the finalize remove, and archives from
	// older builds. This keeps a finalized node's on-disk layout to exactly
	// snapshot.yaml + manifests/ + optional snapshots/ + at most one volume
	// payload — no stray identity.json.
	//
	// It deliberately does NOT end in ".tmp", so resume.go's stale-*.tmp sweep
	// (removeTmpFiles) never touches it, and it is not one of the fixed file/dir
	// names ComputeNodeChecksum reads (manifests/, data.bin*, data.tar, data/),
	// so its presence never perturbs a node's checksum. At codec "none" (ext == "")
	// no user/server payload is named "identity.json": block payloads are
	// data.bin, FS payloads live only inside data.tar, and per-file entries are
	// tar members, never files in the node dir — so the name cannot collide with
	// user- or server-provided content (inv. #10a).
	NodeIdentityMarkerName = "identity.json"

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

	// FSMetaDirName is the reserved metadata subdirectory inside the FS staging
	// directory (FsTarStagingDirName). It holds the
	// download machinery's OWN internal artifacts — the per-file sizes sidecar and,
	// under FSChunksDirName, every per-file chunk directory. It is dot-prefixed and
	// clearly-internal, and the FS ingestion checkpoint (volume.sanitizeRelPath)
	// rejects any server-provided path whose FIRST segment equals it, so no
	// user/server file can ever stage into this namespace — including at codec none
	// (ext == ""), where a staged user blob is a plain file in the staging root.
	// Everything under it is thus provably disjoint from the server-provided
	// staged-blob namespace (stagingDir/<relPath><ext>) at EVERY codec (inv. #10a).
	// The SSOT for this name lives here; volume.FSMetaDirName aliases it.
	FSMetaDirName = ".d8-meta"

	// FSChunksDirName is the subdirectory under FSMetaDirName holding every
	// per-file chunk directory for a chunked FS file (see FsFileChunksDirName).
	// Placing chunk dirs here — rather than beside the staged blobs — guarantees a
	// chunk-dir path can never alias a staged user blob at codec none, so
	// MergeBlockChunks' post-merge os.RemoveAll(chunkDir) can never delete a
	// user's already-staged blob (inv. #10a).
	FSChunksDirName = "chunks"

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

// fsStagingDirSuffix is the trailing suffix of the FS tar staging directory
// name (FsTarStagingDirName, "data.tar.d"). removeTmpFiles uses it to exclude
// the FS staging subtree from the resume-time *.tmp sweep, so a codec-none
// staged user blob that legitimately ends in ".tmp" is never deleted
// (inv. #10a). Block chunk dirs (".bin.d") are deliberately NOT matched: they
// hold only internal ".part"/".tmp" artifacts, never user blobs.
const fsStagingDirSuffix = ".tar.d"

// DataBlockName returns the output filename for a block-volume with the given
// codec extension. ext is codec.Ext() (e.g. ".zst", ".lz4", ".gz", or "" for none).
// Examples: DataBlockName(".zst") → "data.bin.zst", DataBlockName("") → "data.bin".
func DataBlockName(ext string) string {
	return DataBlockBase + ext
}

// FindBlockData searches nodeDir for a completed block-volume file (any file
// whose name starts with DataBlockBase, excluding the staging directory
// DataBlockBase+".d"). The first non-directory match is returned as an
// absolute path. The second return value is false when no such file exists.
// An I/O error is returned in the third return value.
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

// ErrInvalidBlockPayload is returned by ClassifyBlockPayload when a node
// directory's data.bin*-prefixed contents do not resolve to exactly one
// recognized block payload: an unrecognized or chained suffix, more than one
// block file, or a block file coexisting with the filesystem volume
// (FsTarName).
var ErrInvalidBlockPayload = errors.New("invalid block payload")

// blockPayloadExts is the fixed allow-list ClassifyBlockPayload validates
// entries against, keyed by exact filename. It is the single source of truth
// for "what counts as a block payload" — no other data.bin*-prefixed name is
// ever treated as one, however plausible it looks.
var blockPayloadExts = map[string]string{
	DataBlockBase:         "",
	DataBlockName(".zst"): ".zst",
	DataBlockName(".gz"):  ".gz",
	DataBlockName(".lz4"): ".lz4",
}

// BlockPayload identifies the single block-volume payload resolved by
// ClassifyBlockPayload for one node directory.
type BlockPayload struct {
	// Path is the absolute path to the payload file.
	Path string
	// Ext is the payload's codec extension: "" (raw/none codec), ".zst",
	// ".gz", or ".lz4" — matching compress.Codec.Ext. Callers MUST use this
	// value and never re-derive it via filepath.Ext(Path): filepath.Ext on
	// the raw name "data.bin" returns ".bin" (the base name's own suffix,
	// since "data.bin" has no separate codec suffix of its own), not "" —
	// exactly the bug this field exists to prevent downstream.
	Ext string
}

// ClassifyBlockPayload resolves nodeDir's block-volume payload strictly
// against the fixed data.bin[.<ext>] allow-list (blockPayloadExts), replacing
// the earlier first-glob-match behavior of FindBlockData. It is the single
// classifier ComputeNodeChecksum and snapimport.BuildPlan both call, so a
// node's checksum and its upload always agree on what "the block payload" is.
//
// Accepted names (exactly): "data.bin" (ext ""), "data.bin.zst", "data.bin.gz",
// "data.bin.lz4". The staging directory (BlockChunksDirName, "data.bin.d") is
// the only directory entry ignored. Every other entry whose name starts with
// DataBlockBase — an unrecognized codec suffix ("data.bin.foo"), a chained
// suffix ("data.bin.zst.bak"), or any directory other than the staging dir —
// is rejected as ErrInvalidBlockPayload rather than silently skipped:
// silently ignoring it would mean the checksum or the upload picks a
// DIFFERENT file than a human inspecting the directory would expect, or
// drops volume bytes outright. More than one recognized block file, or a
// recognized block file coexisting with the filesystem volume (FsTarName,
// "data.tar"), is rejected for the same reason — a node owns AT MOST ONE
// volume payload.
//
// Returns (BlockPayload{}, false, nil) when nodeDir carries no block payload
// at all (not an error: the normal shape for a filesystem-volume or purely
// structural node, and for a nodeDir that does not exist yet).
func ClassifyBlockPayload(nodeDir string) (BlockPayload, bool, error) {
	source, err := OpenRootedSource(nodeDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return BlockPayload{}, false, nil
		}

		return BlockPayload{}, false, fmt.Errorf("read %s: %w", nodeDir, err)
	}

	defer func() { _ = source.Close() }()

	return ClassifyBlockPayloadIn(source)
}

// ClassifyBlockPayloadIn resolves a block payload relative to a pinned node source.
func ClassifyBlockPayloadIn(source *RootedSource) (BlockPayload, bool, error) {
	entries, err := source.ReadDirectory()
	if err != nil {
		return BlockPayload{}, false, fmt.Errorf("read %s: %w", source.Path(), err)
	}

	var found []BlockPayload

	hasTar := false

	for _, e := range entries {
		name := e.Name()

		if name == FsTarName {
			file, openErr := source.OpenRegularFile(name)
			if openErr != nil {
				return BlockPayload{}, false, fmt.Errorf("inspect filesystem payload %s: %w",
					filepath.Join(source.Path(), name), openErr)
			}

			_ = file.Close()
			hasTar = true

			continue
		}

		if !strings.HasPrefix(name, DataBlockBase) {
			continue
		}

		if name == BlockChunksDirName {
			staging, openErr := source.OpenDirectory(name)
			if openErr != nil {
				return BlockPayload{}, false, fmt.Errorf("%s: %q must be the staging directory: %w",
					source.Path(), name, errors.Join(ErrInvalidBlockPayload, openErr))
			}

			_ = staging.Close()

			continue
		}

		ext, recognized := blockPayloadExts[name]
		if !recognized {
			return BlockPayload{}, false, fmt.Errorf("%s: unrecognized block payload entry %q: %w",
				source.Path(), name, ErrInvalidBlockPayload)
		}

		file, openErr := source.OpenRegularFile(name)
		if openErr != nil {
			return BlockPayload{}, false, fmt.Errorf("inspect block payload %s: %w",
				filepath.Join(source.Path(), name), errors.Join(ErrInvalidBlockPayload, openErr))
		}

		_ = file.Close()

		found = append(found, BlockPayload{Path: filepath.Join(source.Path(), name), Ext: ext})
	}

	if len(found) == 0 {
		return BlockPayload{}, false, nil
	}

	if len(found) > 1 {
		names := make([]string, 0, len(found))
		for _, p := range found {
			names = append(names, filepath.Base(p.Path))
		}

		sort.Strings(names)

		return BlockPayload{}, false, fmt.Errorf("%s: multiple block payload files %v: %w",
			source.Path(), names, ErrInvalidBlockPayload)
	}

	if hasTar {
		return BlockPayload{}, false, fmt.Errorf("%s: block payload %s coexists with %s: %w",
			source.Path(), filepath.Base(found[0].Path), FsTarName, ErrInvalidBlockPayload)
	}

	return found[0], true, nil
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

// FsFileChunksDirName returns the per-file chunk directory path for one
// filesystem-volume file, RELATIVE to the FS staging directory:
// ".d8-meta/chunks/<relPath><ext>.d". relPath is the item's forward-slash
// relative path within the volume (e.g. "disk/payload.bin") and ext is the codec
// extension (e.g. ".zst", or "" for the none codec).
//
// The directory lives under the reserved metadata namespace (FSMetaDirName /
// FSChunksDirName), NOT beside the staged blob, precisely so it can never alias a
// server-provided staged blob path (stagingDir/<relPath><ext>). At codec none
// (ext == "") a user file named "<x>.d" would otherwise occupy the chunk-dir path
// of a chunked user file "<x>", and MergeBlockChunks' post-merge
// os.RemoveAll(chunkDir) (as well as ensureChunkGeometry's purge) would delete
// that already-staged user blob, forcing a needless re-download every resume.
// Because no server path can enter FSMetaDirName (volume.sanitizeRelPath rejects
// it at the single ingestion checkpoint under ANY codec) and relPath is unique
// per file, the returned path collides with neither a staged blob nor another
// chunk dir (inv. #10a). Nesting the leaf under FSChunksDirName mirrors the
// source subtree; it is the identity map, so distinct files map to distinct
// paths without any lossy separator substitution.
//
// A caller joins this (via filepath.FromSlash) under the FS staging directory
// (FsTarStagingDirName). Chunks accumulate here
// while a known-size file is downloaded via Range GETs and are merged into
// "<relPath><ext>" (the same path DownloadBlockChunks/MergeBlockChunks use for a
// single block volume) once complete. In-flight chunk dirs from trees written
// before this relocation (the old flat "stagingDir/<relPath><ext>.d") are simply
// abandoned — such a file re-downloads once.
func FsFileChunksDirName(relPath, ext string) string {
	return FSMetaDirName + "/" + FSChunksDirName + "/" + relPath + ext + ".d"
}
