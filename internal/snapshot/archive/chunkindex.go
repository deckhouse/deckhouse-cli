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

package archive

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// BlockChunkIndexSuffix is the reserved suffix appended to a completed block
// data file's own name to name its chunk-offset index sidecar (see
// BlockChunkIndexPath). Block payload file names (DataBlockName / their
// data/<pvc> multi-volume equivalents) are produced exclusively by this CLI —
// never by a user or the data-exporter server — so a name ending in this
// suffix can never collide with a payload name under any codec, including
// codec none (inv. #10a): a payload's own name never carries this suffix, and
// the suffix is only ever appended to an EXISTING payload name, never used
// standalone.
const BlockChunkIndexSuffix = ".chunkidx"

// ErrCorruptBlockChunkIndex indicates a BlockChunkIndexPath sidecar exists but
// its contents could not be parsed as JSON — e.g. a torn write from a crash
// mid-write (WriteBlockChunkIndex uses WriteFileAtomic, which makes this rare
// but not impossible). Callers MUST treat this exactly like a missing sidecar
// (fall back to the byte-zero decode-and-discard resume strategy), never as a
// fatal error: the index is a regeneratable resume-acceleration cache, not
// authoritative node content.
var ErrCorruptBlockChunkIndex = errors.New("block chunk index is corrupt")

// BlockChunkIndex records, for a merged block-volume data file, the exact
// compressed byte length of each chunk's encoded frame, in ascending
// chunk-index order. Because DownloadBlockChunks/MergeBlockChunks call
// EncodeFrame/EncodeFrameStream exactly once per chunk, chunk boundaries in
// the raw (decompressed) byte stream correspond 1:1 to frame boundaries in
// the compressed byte stream: the compressed byte offset chunk i's frame
// starts at is sum(CompressedChunkSizes[:i]). Opening a fresh compress.Reader
// over an io.NewSectionReader positioned at that offset then decodes forward
// correctly for every codec (zstd, gzip, lz4 alike) — none of them is ever
// asked to detect a frame boundary mid-stream, only to decode forward from a
// known-good frame start.
//
// ChunkSize and TotalSize mirror ChunkMeta's geometry fields so a resume path
// can validate this index against the CURRENT run's chunk geometry before
// trusting CompressedChunkSizes — an index recorded under a different
// chunkSize/totalSize describes a different chunk-to-byte-range mapping and
// must not be reused.
type BlockChunkIndex struct {
	ChunkSize            int64   `json:"chunkSize"`
	TotalSize            int64   `json:"totalSize"`
	CompressedChunkSizes []int64 `json:"compressedChunkSizes"`
}

// BlockChunkIndexPath returns the sidecar path for the completed block data
// file at dataFilePath, by appending BlockChunkIndexSuffix to it.
// Example: "data.bin.zst" -> "data.bin.zst.chunkidx".
func BlockChunkIndexPath(dataFilePath string) string {
	return dataFilePath + BlockChunkIndexSuffix
}

// WriteBlockChunkIndex atomically writes idx as the chunk-offset index
// sidecar for the block data file at dataFilePath.
func WriteBlockChunkIndex(dataFilePath string, idx BlockChunkIndex) error {
	data, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("marshal block chunk index: %w", err)
	}

	path := BlockChunkIndexPath(dataFilePath)

	if err := WriteFileAtomic(path, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("write block chunk index %s: %w", path, err)
	}

	return nil
}

// ReadBlockChunkIndex reads the chunk-offset index sidecar for the block data
// file at dataFilePath. found is false (with a nil error) when the sidecar
// does not exist — the normal case for a data file merged before this index
// existed, or one whose sidecar has not been written yet. A corrupt/unparseable
// sidecar returns an error wrapping ErrCorruptBlockChunkIndex; callers MUST
// treat that identically to found=false (degrade, never hard-fail).
func ReadBlockChunkIndex(dataFilePath string) (BlockChunkIndex, bool, error) {
	path := BlockChunkIndexPath(dataFilePath)

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return BlockChunkIndex{}, false, nil
		}

		return BlockChunkIndex{}, false, fmt.Errorf("read block chunk index %s: %w", path, err)
	}

	var idx BlockChunkIndex

	if err := json.Unmarshal(data, &idx); err != nil {
		return BlockChunkIndex{}, false, fmt.Errorf("%w: %s: %w", ErrCorruptBlockChunkIndex, path, err)
	}

	return idx, true, nil
}
