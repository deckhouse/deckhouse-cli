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

// Package volume provides workers for downloading block and filesystem volumes
// from a data-exporter HTTP endpoint into the snapshot output directory tree.
package volume

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
)

// DefaultChunkSize is the default raw-byte size of each block-volume chunk.
// A 256 MiB chunk produces a single zstd frame that fits comfortably in memory
// and keeps the chunk-file count manageable for large volumes.
const DefaultChunkSize = 256 * 1024 * 1024 // 256 MiB

// DownloadBlockChunks downloads all chunks of a block volume into chunkDir.
// Chunk k covers raw bytes [k*chunkSize, min((k+1)*chunkSize, totalSize)).
// Each chunk is fetched via a Range GET, encoded as an independent frame using
// codec, and atomically written as chunk_NNNNN[.<ext>] where ext is codec.Ext().
//
// chunkDir is the absolute path to the chunk directory (the caller constructs it
// using archive.BlockChunksDirName or archive.BlockChunksDirNameFor for
// multi-volume layouts).
//
// Already-complete chunks (final file exists) are skipped. Stale *.tmp files
// are cleaned before a chunk is fetched. workers bounds parallelism; the first
// error cancels all in-flight work.
//
// Memory note: each in-flight worker buffers a full raw chunk (io.ReadAll) plus
// the encoded frame simultaneously.  Worst-case RSS for this call alone is
// workers × (chunkSize + compressed frame size).  The outer pipeline multiplies
// this by the number of concurrent nodes (pipeline.Config.Workers); total peak
// ≈ pipeline.Config.Workers × workers × (chunkSize + frame).
func DownloadBlockChunks(
	ctx context.Context,
	log *slog.Logger,
	chunkDir string,
	blockURL string,
	totalSize int64,
	chunkSize int64,
	workers int,
	fetcher *exporter.Fetcher,
	codec compress.Codec,
	onProgress func(n int),
) error {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	if workers <= 0 {
		workers = 1
	}

	if err := ensureChunkGeometry(log, chunkDir, chunkSize, totalSize); err != nil {
		return fmt.Errorf("verify chunk geometry for %s: %w", chunkDir, err)
	}

	numChunks := int((totalSize + chunkSize - 1) / chunkSize)

	log.Info("downloading block chunks",
		slog.String("dir", chunkDir),
		slog.Int64("total_bytes", totalSize),
		slog.Int("chunks", numChunks),
		slog.Int("workers", workers))

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for i := range numChunks {
		chunkIdx := i

		g.Go(func() error {
			return downloadChunk(gctx, log, chunkDir, blockURL, chunkIdx, chunkSize, totalSize, fetcher, codec, onProgress)
		})
	}

	return g.Wait()
}

// ensureChunkGeometry guards against resuming a chunk directory that was
// produced with a DIFFERENT chunkSize or totalSize than the current call.
// Chunk k's byte range is derived purely from chunkSize/totalSize (see
// downloadChunk), and neither value is encoded in the chunk's filename — so
// silently reusing chunks written under a different geometry would duplicate
// or truncate byte ranges in the merged output without either
// DownloadBlockChunks or MergeBlockChunks ever noticing (chunks are opaque
// compressed frames; merged length cannot be checked against totalSize).
//
// If chunkDir does not exist yet, it is created and the current geometry is
// recorded: a fresh download. If it exists and archive.ReadChunkMeta reports a
// geometry that exactly matches chunkSize/totalSize, it is left untouched: a
// same-geometry resume, safe to reuse existing chunks from. Otherwise
// (missing metadata or a mismatch) the ENTIRE directory is purged and
// recreated with the current geometry recorded — every chunk in it must be
// re-fetched, since none of them can be trusted to cover the byte range the
// current geometry expects.
func ensureChunkGeometry(log *slog.Logger, chunkDir string, chunkSize, totalSize int64) error {
	_, statErr := os.Stat(chunkDir)

	switch {
	case statErr == nil:
		// Fall through to the metadata check below.
	case os.IsNotExist(statErr):
		return createChunkDir(chunkDir, chunkSize, totalSize)
	default:
		return fmt.Errorf("stat chunk dir %s: %w", chunkDir, statErr)
	}

	meta, found, err := archive.ReadChunkMeta(chunkDir)
	if err != nil {
		return fmt.Errorf("read chunk metadata: %w", err)
	}

	if found && meta.ChunkSize == chunkSize && meta.TotalSize == totalSize {
		return nil
	}

	log.Info("chunk geometry changed since last run, discarding stale chunks and re-downloading",
		slog.String("dir", chunkDir),
		slog.Int64("current_chunk_size", chunkSize),
		slog.Int64("current_total_size", totalSize),
		slog.Bool("previous_metadata_found", found),
		slog.Int64("previous_chunk_size", meta.ChunkSize),
		slog.Int64("previous_total_size", meta.TotalSize))

	if err := os.RemoveAll(chunkDir); err != nil {
		return fmt.Errorf("remove stale chunk dir %s: %w", chunkDir, err)
	}

	return createChunkDir(chunkDir, chunkSize, totalSize)
}

// createChunkDir creates chunkDir and records the geometry it is being
// created for via chunks.meta, so a later run can detect a changed
// chunkSize/totalSize before trusting any chunk file already inside it.
func createChunkDir(chunkDir string, chunkSize, totalSize int64) error {
	if err := archive.EnsureDir(chunkDir); err != nil {
		return fmt.Errorf("create chunk dir %s: %w", chunkDir, err)
	}

	if err := archive.WriteChunkMeta(chunkDir, archive.ChunkMeta{ChunkSize: chunkSize, TotalSize: totalSize}); err != nil {
		return fmt.Errorf("write chunk metadata for %s: %w", chunkDir, err)
	}

	return nil
}

// downloadChunk fetches one chunk, encodes it with codec, and writes it
// atomically. It is safe to call concurrently from multiple goroutines.
func downloadChunk(
	ctx context.Context,
	log *slog.Logger,
	chunkDir string,
	blockURL string,
	chunkIdx int,
	chunkSize int64,
	totalSize int64,
	fetcher *exporter.Fetcher,
	codec compress.Codec,
	onProgress func(n int),
) error {
	finalPath := filepath.Join(chunkDir, archive.ChunkFileName(chunkIdx, codec.Ext()))

	// startByte/endByte (and the raw length they cover) are computed up front so
	// the skip-existing-chunk branch below can credit onProgress before any
	// early return.
	startByte := int64(chunkIdx) * chunkSize
	endByte := min(startByte+chunkSize, totalSize) - 1 // Range header is inclusive
	rawLen := endByte - startByte + 1

	// Skip chunks that are already complete (resume). The skip still credits
	// this chunk's RAW (uncompressed) length to onProgress — the same units
	// SetTotal/HeadVolume's totalSize use — so the numerator can reach the
	// denominator on a resumed download. Crediting the on-disk (possibly
	// codec-compressed) file size instead would under-count and could leave
	// the bar short of 100% even though every chunk is present; mirrors
	// stageCompressedFile's identical skip-credit on the filesystem path.
	if _, err := os.Stat(finalPath); err == nil {
		log.Debug("chunk already present, skipping", slog.Int("chunk", chunkIdx))

		if onProgress != nil && rawLen > 0 {
			onProgress(int(rawLen))
		}

		return nil
	}

	// Remove any stale temporary file from a previous aborted attempt.
	tmpPath := finalPath + ".tmp"

	if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale tmp %s: %w", tmpPath, err)
	}

	log.Debug("fetching chunk",
		slog.Int("chunk", chunkIdx),
		slog.Int64("start", startByte),
		slog.Int64("end", endByte))

	body, err := fetcher.RangeGet(ctx, blockURL, startByte, endByte)
	if err != nil {
		return fmt.Errorf("range get chunk %d: %w", chunkIdx, err)
	}

	// Wrap body in countingReader (fs.go) so bytes are credited to onProgress
	// as the transport delivers them via io.ReadAll's internal Read loop,
	// instead of once in a single terminal call after the whole chunk has
	// arrived — the same incremental-reporting contract stageWholeFile uses
	// for the filesystem path.
	raw, err := io.ReadAll(&countingReader{r: body, onProgress: onProgress})

	_ = body.Close()

	if err != nil {
		return fmt.Errorf("read chunk %d body: %w", chunkIdx, err)
	}

	frame, err := codec.EncodeFrame(raw)
	if err != nil {
		return fmt.Errorf("encode chunk %d: %w", chunkIdx, err)
	}

	if err := archive.WriteFileAtomic(finalPath, bytes.NewReader(frame)); err != nil {
		return fmt.Errorf("write chunk %d: %w", chunkIdx, err)
	}

	log.Debug("chunk written", slog.Int("chunk", chunkIdx), slog.Int("frame_bytes", len(frame)))

	return nil
}
