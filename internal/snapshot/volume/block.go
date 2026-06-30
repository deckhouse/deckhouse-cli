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

	if err := archive.EnsureDir(chunkDir); err != nil {
		return fmt.Errorf("create chunk dir %s: %w", chunkDir, err)
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

	// Skip chunks that are already complete.
	if _, err := os.Stat(finalPath); err == nil {
		log.Debug("chunk already present, skipping", slog.Int("chunk", chunkIdx))

		return nil
	}

	// Remove any stale temporary file from a previous aborted attempt.
	tmpPath := finalPath + ".tmp"

	if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale tmp %s: %w", tmpPath, err)
	}

	startByte := int64(chunkIdx) * chunkSize
	endByte := min(startByte+chunkSize, totalSize) - 1 // Range header is inclusive

	log.Debug("fetching chunk",
		slog.Int("chunk", chunkIdx),
		slog.Int64("start", startByte),
		slog.Int64("end", endByte))

	body, err := fetcher.RangeGet(ctx, blockURL, startByte, endByte)
	if err != nil {
		return fmt.Errorf("range get chunk %d: %w", chunkIdx, err)
	}

	raw, err := io.ReadAll(body)

	_ = body.Close()

	if err != nil {
		return fmt.Errorf("read chunk %d body: %w", chunkIdx, err)
	}

	if onProgress != nil {
		onProgress(len(raw))
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
