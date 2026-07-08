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
	"errors"
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

// partSuffix names the durable, resumable raw-byte partial file for an
// in-flight chunk: "<chunk_NNNNN[.<ext>]>.part". It is DELIBERATELY not
// ".tmp": archive.resume.go removeTmpFiles deletes every *.tmp file under a
// node directory on every resume scan, which would silently discard durable
// mid-chunk progress on the very first resume attempt. A ".part" file holds
// RAW (uncompressed) bytes, never a partial codec frame — a compressed frame
// cannot be truncated and resumed by appending, but raw bytes can.
const partSuffix = ".part"

// partSyncInterval bounds how much of a chunk's durable partial download can
// be lost to an unsynced OS page cache if the process is killed mid-chunk:
// once this many bytes have been written to the .part file since the last
// fsync, it is synced again. Small enough to bound loss to a few MiB on a
// hard kill; large enough that fsync overhead does not dominate a healthy,
// uninterrupted download.
const partSyncInterval = 4 * 1024 * 1024 // 4 MiB

// ErrShortChunkRead is returned when a chunk's Range GET body delivers fewer
// bytes than the requested range promised, leaving the durable ".part" file
// short of rawLen. It is never finalized into a codec frame.
var ErrShortChunkRead = errors.New("chunk range body ended before the requested range was fully delivered")

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
// Durable sub-chunk resume: each chunk's raw bytes are streamed directly to a
// durable "<chunk>.part" file as they arrive (see partSuffix) instead of being
// buffered in memory for the whole chunk. An interrupted chunk resumes with a
// Range GET starting at the ".part" file's persisted length, so a kill mid-chunk
// loses at most the tail written since the last fsync (partSyncInterval), never
// the whole chunk. The final codec frame is produced, and the ".part" file
// consumed, only once the raw bytes are fully durable on disk.
//
// Memory note: EncodeFrame's signature requires the full raw chunk as a []byte,
// so once a chunk's ".part" file is complete it is read back into memory for
// exactly one EncodeFrame call (this replaces the old network-buffer io.ReadAll,
// it does not add a second concurrent buffer). Worst-case RSS for this call
// alone is still workers × (chunkSize + compressed frame size), now backed by a
// durable file instead of a discarded network buffer. The outer pipeline
// multiplies this by the number of concurrent nodes (pipeline.Config.Workers);
// total peak ≈ pipeline.Config.Workers × workers × (chunkSize + frame).
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
// (missing metadata, a mismatch, or an unparseable sidecar) the ENTIRE
// directory is purged and recreated with the current geometry recorded —
// every chunk in it must be re-fetched, since none of them can be trusted to
// cover the byte range the current geometry expects.
//
// A sidecar that exists but fails to parse (archive.ErrCorruptChunkMeta, e.g.
// a torn write from a crash) is deliberately routed to the SAME purge path as
// a mismatch, not treated as a fatal error: the geometry it would have
// described is unknowable either way, and degrading to a clean re-fetch is
// the only response that keeps resume idempotent. Any OTHER ReadChunkMeta
// error (a genuine I/O failure, e.g. EACCES) still hard-aborts, since that is
// not a geometry problem this function can safely paper over.
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

	switch {
	case err != nil && errors.Is(err, archive.ErrCorruptChunkMeta):
		log.Warn("chunk geometry sidecar unreadable, discarding stale chunks and re-downloading",
			slog.String("dir", chunkDir))
	case err != nil:
		return fmt.Errorf("read chunk metadata: %w", err)
	case found && meta.ChunkSize == chunkSize && meta.TotalSize == totalSize:
		return nil
	default:
		log.Info("chunk geometry changed since last run, discarding stale chunks and re-downloading",
			slog.String("dir", chunkDir),
			slog.Int64("current_chunk_size", chunkSize),
			slog.Int64("current_total_size", totalSize),
			slog.Bool("previous_metadata_found", found),
			slog.Int64("previous_chunk_size", meta.ChunkSize),
			slog.Int64("previous_total_size", meta.TotalSize))
	}

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

// downloadChunk fetches one chunk into a durable raw ".part" file (resuming
// from the part's persisted length if one already exists), encodes the
// complete raw bytes with codec, and writes the result atomically as the
// final chunk file. It is safe to call concurrently from multiple goroutines.
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
	partPath := finalPath + partSuffix

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

	// Remove any stale AtomicWriter temporary file from a previous aborted
	// finalize attempt. This is distinct from partPath: the ".part" file below
	// holds durable resumable progress and must survive across runs.
	tmpPath := finalPath + ".tmp"

	if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale tmp %s: %w", tmpPath, err)
	}

	if err := fetchChunkRaw(ctx, log, fetcher, blockURL, partPath, chunkIdx, startByte, endByte, rawLen, onProgress); err != nil {
		return err
	}

	raw, err := os.ReadFile(partPath)
	if err != nil {
		return fmt.Errorf("read persisted chunk %d: %w", chunkIdx, err)
	}

	frame, err := codec.EncodeFrame(raw)
	if err != nil {
		return fmt.Errorf("encode chunk %d: %w", chunkIdx, err)
	}

	if err := archive.WriteFileAtomic(finalPath, bytes.NewReader(frame)); err != nil {
		return fmt.Errorf("write chunk %d: %w", chunkIdx, err)
	}

	// A crash between the atomic rename above and this removal is harmless: the
	// final chunk file now exists, so the next run's skip check at the top of
	// this function fires before the stale ".part" is ever looked at again, and
	// MergeBlockChunks removes the whole chunk directory (including any stale
	// ".part") once every final chunk is present.
	if err := os.Remove(partPath); err != nil && !os.IsNotExist(err) {
		log.Warn("failed to remove durable partial chunk file after finalize",
			slog.String("path", partPath),
			slog.String("error", err.Error()))
	}

	log.Debug("chunk written", slog.Int("chunk", chunkIdx), slog.Int("frame_bytes", len(frame)))

	return nil
}

// fetchChunkRaw ensures partPath durably holds exactly rawLen raw bytes
// covering [startByte, endByte], resuming from partPath's current size
// (truncating away anything larger than rawLen, which cannot be trusted) and
// fetching only the missing suffix via a Range GET. A resume credit for
// already-persisted bytes is reported to onProgress before any network call,
// and newly-arrived bytes are credited incrementally as they stream in —
// together the two credits always sum to exactly rawLen.
//
// The response body is capped with io.LimitReader at the exact number of
// missing bytes, so a server (or a proxy/MITM) that over-sends for the
// requested range can never grow partPath past rawLen — bounding both the
// merged-stream corruption risk and a disk-fill DoS. After the copy, the
// resulting partPath size is asserted to equal rawLen; a server that
// under-sends (a short read) is reported as ErrShortChunkRead rather than
// silently finalizing a truncated chunk.
func fetchChunkRaw(
	ctx context.Context,
	log *slog.Logger,
	fetcher *exporter.Fetcher,
	blockURL string,
	partPath string,
	chunkIdx int,
	startByte, endByte, rawLen int64,
	onProgress func(n int),
) error {
	have, err := partialChunkSize(partPath, rawLen)
	if err != nil {
		return fmt.Errorf("stat partial chunk %d: %w", chunkIdx, err)
	}

	if onProgress != nil && have > 0 {
		onProgress(int(have))
	}

	if have >= rawLen {
		// The durable partial already covers the whole chunk (e.g. a crash
		// between finishing the raw download and finalizing the frame on a
		// previous run): nothing left to fetch.
		return nil
	}

	log.Debug("fetching chunk",
		slog.Int("chunk", chunkIdx),
		slog.Int64("start", startByte+have),
		slog.Int64("end", endByte),
		slog.Int64("resumed_from", have))

	body, err := fetcher.RangeGet(ctx, blockURL, startByte+have, endByte)
	if err != nil {
		return fmt.Errorf("range get chunk %d: %w", chunkIdx, err)
	}

	defer func() { _ = body.Close() }()

	f, err := os.OpenFile(partPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open partial chunk %d: %w", chunkIdx, err)
	}

	sw := &syncingWriter{f: f, syncInterval: partSyncInterval}
	remaining := rawLen - have
	cr := &countingReader{r: io.LimitReader(body, remaining), onProgress: onProgress}

	_, copyErr := io.Copy(sw, cr)
	syncErr := f.Sync()
	closeErr := f.Close()

	if copyErr != nil {
		return fmt.Errorf("stream chunk %d body: %w", chunkIdx, copyErr)
	}

	if syncErr != nil {
		return fmt.Errorf("sync partial chunk %d: %w", chunkIdx, syncErr)
	}

	if closeErr != nil {
		return fmt.Errorf("close partial chunk %d: %w", chunkIdx, closeErr)
	}

	info, statErr := os.Stat(partPath)
	if statErr != nil {
		return fmt.Errorf("stat finalized partial chunk %d: %w", chunkIdx, statErr)
	}

	if info.Size() != rawLen {
		return fmt.Errorf("chunk %d: %w: partial file holds %d bytes, want %d", chunkIdx, ErrShortChunkRead, info.Size(), rawLen)
	}

	return nil
}

// ScanBlockChunkProgress computes durably-committed raw bytes and the raw
// total byte size recorded for chunkDir's on-disk geometry, purely from local
// state — no network call. It mirrors downloadChunk/fetchChunkRaw's own
// chunk-boundary formula exactly: each already-final chunk contributes its
// full raw length, and a still-open chunk contributes its durable ".part"
// prefix (capped at that chunk's raw length).
//
// It returns (0, 0, nil) when chunkDir carries no trustworthy geometry yet
// (chunks.meta missing or corrupt) — the same case ensureChunkGeometry treats
// as "nothing to resume from", so there is nothing safe to seed either.
//
// The pipeline uses this to seed a volume's progress stream with its
// already-downloaded bytes as soon as the stream is created — well before the
// DataExport becomes ready or a fresh HEAD confirms totalSize — and then
// cancels the seed back out (progress.Stream.SetCurrent(0)) immediately
// before calling DownloadBlockChunks, so downloadChunk/fetchChunkRaw's own
// resume-skip crediting re-derives and re-credits the identical bytes without
// double counting. Because nothing mutates chunkDir between the two calls (no
// worker has started yet), the two computations always agree exactly.
func ScanBlockChunkProgress(chunkDir, ext string) (int64, int64, error) {
	meta, found, err := archive.ReadChunkMeta(chunkDir)
	if err != nil && !errors.Is(err, archive.ErrCorruptChunkMeta) {
		return 0, 0, fmt.Errorf("read chunk metadata in %s: %w", chunkDir, err)
	}

	if err != nil || !found || meta.ChunkSize <= 0 || meta.TotalSize <= 0 {
		return 0, 0, nil
	}

	numChunks := int((meta.TotalSize + meta.ChunkSize - 1) / meta.ChunkSize)

	var committed int64

	for idx := range numChunks {
		startByte := int64(idx) * meta.ChunkSize
		endByte := min(startByte+meta.ChunkSize, meta.TotalSize) - 1
		rawLen := endByte - startByte + 1

		finalPath := filepath.Join(chunkDir, archive.ChunkFileName(idx, ext))
		if _, statErr := os.Stat(finalPath); statErr == nil {
			committed += rawLen
			continue
		}

		partial, partErr := partialChunkSize(finalPath+partSuffix, rawLen)
		if partErr != nil {
			return 0, 0, fmt.Errorf("stat partial chunk %d in %s: %w", idx, chunkDir, partErr)
		}

		committed += partial
	}

	return committed, meta.TotalSize, nil
}

// partialChunkSize returns the current size of the durable partial file at
// partPath, or 0 if it does not exist. A partial larger than rawLen cannot
// belong to the current chunk geometry (ensureChunkGeometry purges the whole
// chunk directory on any geometry change, so this should not happen in
// practice) and is removed so the chunk restarts cleanly rather than trusting
// bytes that may cover the wrong range.
func partialChunkSize(partPath string, rawLen int64) (int64, error) {
	info, err := os.Stat(partPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}

		return 0, fmt.Errorf("stat %s: %w", partPath, err)
	}

	if info.Size() > rawLen {
		if err := os.Remove(partPath); err != nil && !os.IsNotExist(err) {
			return 0, fmt.Errorf("remove oversized partial %s: %w", partPath, err)
		}

		return 0, nil
	}

	return info.Size(), nil
}

// syncingWriter wraps an *os.File and fsyncs it every syncInterval bytes
// written, bounding how much of a durable partial download can be lost to an
// unsynced OS page cache if the process is killed mid-chunk.
type syncingWriter struct {
	f             *os.File
	syncInterval  int64
	sinceLastSync int64
}

// Write implements io.Writer.
func (w *syncingWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	if err != nil {
		return n, fmt.Errorf("write partial chunk: %w", err)
	}

	w.sinceLastSync += int64(n)
	if w.sinceLastSync < w.syncInterval {
		return n, nil
	}

	w.sinceLastSync = 0

	if syncErr := w.f.Sync(); syncErr != nil {
		return n, fmt.Errorf("sync partial chunk: %w", syncErr)
	}

	return n, nil
}
