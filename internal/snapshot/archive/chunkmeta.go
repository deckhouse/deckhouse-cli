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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ChunkMetaFileName is the sidecar filename recording the exact chunk geometry
// an in-progress chunk directory was produced with. It lives inside the chunk
// dir alongside the chunk_NNNNN[.<ext>] files it describes.
//
// It is never hashed into a node checksum: collectNodeFiles never walks a
// single-volume flat chunk dir (BlockChunksDirName / FsFileChunksDirName both
// live outside the "data/" subtree it walks) and, for the multi-volume
// layout, that walk skips every directory whose name ends in ".d" — which
// every chunk dir does, by construction.
const ChunkMetaFileName = "chunks.meta"

// ChunkMetaMaxEncodedSize bounds metadata before JSON decoding.
const ChunkMetaMaxEncodedSize = 4 << 10

// ErrCorruptChunkMeta indicates ChunkMetaFileName exists but its contents
// could not be parsed as JSON — e.g. a torn write from a crash mid-write (see
// WriteChunkMeta's use of WriteFileAtomic, which makes this rare but not
// impossible) or filesystem-level corruption. Callers MUST treat this
// identically to a geometry mismatch (purge the chunk dir and re-download),
// never as a fatal error: the sidecar's only purpose is recording the byte
// ranges chunks were written under, and an unparseable sidecar means those
// ranges can no longer be trusted.
var ErrCorruptChunkMeta = errors.New("chunk metadata is corrupt")

// ChunkMeta records the chunk geometry — chunk size and total volume/file size
// — that an in-progress chunk directory was produced with. Chunk k's byte
// range is computed purely from these two values ([k*chunkSize,
// min((k+1)*chunkSize,totalSize))), and neither is encoded in ChunkFileName
// (only the index and codec extension are) — so a resumed download with a
// different chunkSize would otherwise silently reuse a chunk file that covers
// the wrong byte range. Comparing against the geometry a chunk dir was
// actually created with is the only reliable way to detect that.
type ChunkMeta struct {
	ChunkSize int64 `json:"chunkSize"`
	TotalSize int64 `json:"totalSize"`
}

// WriteChunkMeta atomically writes meta as ChunkMetaFileName inside dir.
// dir must already exist.
func WriteChunkMeta(dir string, meta ChunkMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal chunk metadata: %w", err)
	}

	path := filepath.Join(dir, ChunkMetaFileName)

	if err := WriteFileAtomic(path, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("write chunk metadata %s: %w", path, err)
	}

	return nil
}

// ReadChunkMeta reads ChunkMetaFileName from dir. found is false (with a nil
// error) when the metadata file does not exist — the valid case for a chunk
// dir that predates this guard or was never fully initialized, which callers
// must treat as an untrusted/incompatible geometry, not as "no geometry
// recorded yet, anything goes".
func ReadChunkMeta(dir string) (ChunkMeta, bool, error) {
	path := filepath.Join(dir, ChunkMetaFileName)

	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ChunkMeta{}, false, nil
		}

		return ChunkMeta{}, false, fmt.Errorf("read chunk metadata %s: %w", path, err)
	}

	defer func() { _ = f.Close() }()

	return ReadChunkMetaFrom(context.Background(), f, path)
}

// ReadChunkMetaFrom decodes bounded chunk metadata from an already-secured reader.
func ReadChunkMetaFrom(ctx context.Context, reader io.Reader, source string) (ChunkMeta, bool, error) {
	data, err := io.ReadAll(io.LimitReader(&chunkMetaContextReader{ctx: ctx, reader: reader}, ChunkMetaMaxEncodedSize+1))
	if err != nil {
		return ChunkMeta{}, false, fmt.Errorf("read chunk metadata %s: %w", source, err)
	}

	if len(data) > ChunkMetaMaxEncodedSize {
		return ChunkMeta{}, false, fmt.Errorf(
			"%w: %s exceeds %d-byte encoded limit",
			ErrCorruptChunkMeta,
			source,
			ChunkMetaMaxEncodedSize,
		)
	}

	var meta ChunkMeta

	if err := json.Unmarshal(data, &meta); err != nil {
		return ChunkMeta{}, false, fmt.Errorf("%w: %s: %w", ErrCorruptChunkMeta, source, err)
	}

	return meta, true, nil
}

type chunkMetaContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *chunkMetaContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}

	return r.reader.Read(p)
}
