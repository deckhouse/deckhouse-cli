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
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
//
// This bound is only TRUE on resume because of partOffsetSuffix below: a
// hard kill (SIGKILL/OOM/power loss) can leave the ".part" file's on-disk
// SIZE ahead of its durably-flushed DATA (e.g. ext4 data=writeback), so
// os.Stat's reported length cannot be trusted as "bytes safe to resume
// from" on its own. partialChunkSize instead trusts only the offset last
// recorded in the sidecar and truncates away anything beyond it before a
// resumed download is allowed to append, which is what actually makes "at
// most partSyncInterval bytes lost" hold across a hard kill, not just a
// graceful interruption.
const partSyncInterval = 4 * 1024 * 1024 // 4 MiB

// partOffsetSuffix names the sidecar file that records the last byte offset
// (measured from the start of the chunk's raw bytes) PROVEN durable by a
// successful fsync of the corresponding ".part" file:
// "<chunk_NNNNN[.<ext>]>.part.offset". It is written only by
// syncingWriter.sync, immediately after the fsync it follows succeeds (see
// writeDurablePartOffset), so it can never claim an offset the data has not
// already been flushed to stable storage for.
//
// Like partSuffix, this is deliberately not ".tmp": archive.removeTmpFiles
// deletes every *.tmp file under a node directory on every resume scan,
// which would discard this durability record out from under an otherwise
// perfectly resumable ".part" file.
const partOffsetSuffix = ".offset"

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
// using archive.BlockChunksDirName).
//
// Already-complete chunks (final file exists) are skipped. Stale *.tmp files
// are cleaned before a chunk is fetched. workers bounds parallelism; the first
// error cancels all in-flight work.
//
// Durable sub-chunk resume: each chunk's raw bytes are streamed directly to a
// durable "<chunk>.part" file as they arrive (see partSuffix) instead of being
// buffered in memory for the whole chunk. An interrupted chunk resumes with a
// Range GET starting at the ".part" file's TRUSTED prefix — the offset last
// proven durable by a successful fsync (see partOffsetSuffix), never the raw
// file size alone — truncating away any tail the file may physically hold
// beyond that offset first. This bounds a kill mid-chunk to losing at most
// the bytes written since the last fsync (partSyncInterval), even when the
// process is killed hard enough that the file's on-disk SIZE outruns its
// DATA. The final codec frame is produced, and the ".part" file consumed,
// only once the raw bytes are fully durable on disk.
//
// Memory note: once a chunk's ".part" file is complete, finalizeChunkFrame
// streams it through codec.EncodeFrameStream directly into the final chunk's
// AtomicWriter — the whole raw chunk is never read into memory as a []byte
// here. For codecs that can genuinely stream (none/gzip/lz4), finalize's
// peak memory is bounded by the codec's own small internal buffer,
// independent of chunkSize. zstd's public streaming API cannot reproduce
// EncodeFrame's output byte-for-byte for chunk-sized input (see
// compress.zstdCodec.EncodeFrameStream), so its implementation still reads
// the chunk fully — for that codec (the default), worst-case RSS per
// in-flight chunk remains chunkSize + compressed frame size. Every
// production block download/merge hardcodes chunkSize to DefaultChunkSize
// (256 MiB; see block-chunk-size-hardcode-only) — there is no per-run knob —
// so this is a fixed, known bound, not a user-configurable maximum. The
// outer pipeline multiplies this by the number of concurrent nodes
// (pipeline.Config.Workers); total peak ≈ pipeline.Config.Workers × workers
// × (DefaultChunkSize + frame) for zstd, and pipeline.Config.Workers ×
// workers × (small internal buffer) for the genuinely-streaming codecs.
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
// from the part's persisted length if one already exists), then streams the
// complete raw bytes through codec (see finalizeChunkFrame) and writes the
// result atomically as the final chunk file. It is safe to call concurrently
// from multiple goroutines.
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

	frameBytes, err := finalizeChunkFrame(finalPath, partPath, rawLen, codec)
	if err != nil {
		return fmt.Errorf("encode chunk %d: %w", chunkIdx, err)
	}

	// A crash between the atomic rename above and this removal is harmless: the
	// final chunk file now exists, so the next run's skip check at the top of
	// this function fires before the stale ".part" (or its offset sidecar) is
	// ever looked at again, and MergeBlockChunks removes the whole chunk
	// directory (including any stale ".part"/".part.offset") once every final
	// chunk is present.
	if err := os.Remove(partPath); err != nil && !os.IsNotExist(err) {
		log.Warn("failed to remove durable partial chunk file after finalize",
			slog.String("path", partPath),
			slog.String("error", err.Error()))
	}

	if err := os.Remove(partOffsetPath(partPath)); err != nil && !os.IsNotExist(err) {
		log.Warn("failed to remove durable partial offset sidecar after finalize",
			slog.String("path", partOffsetPath(partPath)),
			slog.String("error", err.Error()))
	}

	log.Debug("chunk written", slog.Int("chunk", chunkIdx), slog.Int64("frame_bytes", frameBytes))

	return nil
}

// finalizeChunkFrame produces finalPath's independent codec frame by
// streaming partPath's durable raw bytes through codec.EncodeFrameStream
// directly into an AtomicWriter — no whole-chunk buffer is read into
// memory here, unlike the os.ReadFile+EncodeFrame this replaces. It returns
// the resulting frame's byte size (for logging only).
func finalizeChunkFrame(finalPath, partPath string, rawLen int64, codec compress.Codec) (int64, error) {
	partFile, err := os.Open(partPath)
	if err != nil {
		return 0, fmt.Errorf("open persisted chunk: %w", err)
	}

	aw, err := archive.NewAtomicWriter(finalPath)
	if err != nil {
		_ = partFile.Close()
		return 0, fmt.Errorf("create chunk writer: %w", err)
	}

	encodeErr := codec.EncodeFrameStream(aw, partFile, rawLen)
	closeErr := partFile.Close()

	if encodeErr != nil {
		aw.Abort()
		return 0, fmt.Errorf("stream-encode: %w", encodeErr)
	}

	if closeErr != nil {
		aw.Abort()
		return 0, fmt.Errorf("close persisted chunk: %w", closeErr)
	}

	if err := aw.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}

	info, err := os.Stat(finalPath)
	if err != nil {
		return 0, fmt.Errorf("stat finalized chunk: %w", err)
	}

	return info.Size(), nil
}

// fetchChunkRaw ensures partPath durably holds exactly rawLen raw bytes
// covering [startByte, endByte], resuming from partPath's TRUSTED prefix
// (see partialChunkSize — never the raw file size alone, and never anything
// larger than rawLen) and fetching only the missing suffix via a Range GET.
// A resume credit for already-persisted bytes is reported to onProgress
// before any network call, and newly-arrived bytes are credited
// incrementally as they stream in — together the two credits always sum to
// exactly rawLen.
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

	sw := &syncingWriter{f: f, partPath: partPath, syncInterval: partSyncInterval, durableBase: have}
	remaining := rawLen - have
	cr := &countingReader{r: io.LimitReader(body, remaining), onProgress: onProgress}

	// finish (an explicit final fsync + durable-offset record) runs
	// regardless of copyErr, exactly like the plain f.Sync() it replaces
	// always did: a GRACEFUL interrupt (a network error or ctx cancellation
	// the process survives long enough to reach this line) still gets
	// whatever it wrote durably recorded as the trusted resume point, while
	// a HARD KILL — which never runs any Go code past the point of death —
	// never reaches this call at all, so only Write's own interval
	// checkpoints (see syncingWriter) remain trusted on the next resume.
	_, copyErr := io.Copy(sw, cr)
	finishErr := sw.finish()
	closeErr := f.Close()

	if copyErr != nil {
		return fmt.Errorf("stream chunk %d body: %w", chunkIdx, copyErr)
	}

	if finishErr != nil {
		return fmt.Errorf("finalize partial chunk %d: %w", chunkIdx, finishErr)
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
// state — no network call, and NO filesystem mutation: it is a pure
// observation used to seed a progress display before any transfer starts. It
// mirrors downloadChunk/fetchChunkRaw's own chunk-boundary formula exactly:
// each already-final chunk contributes its full raw length, and a still-open
// chunk contributes its TRUSTED ".part" prefix (see partialChunkSizeReadOnly
// — the offset proven durable by an fsync, capped at that chunk's raw
// length; never the raw file size alone). Unlike the download path's
// partialChunkSize, an oversized ".part" is left untouched on disk here —
// the file gets treated as contributing only its trusted (safe) prefix, not
// removed or truncated; that cleanup remains the download path's job (see
// partialChunkSize, used by fetchChunkRaw), which is the only place actually
// about to act on the chunk's geometry.
//
// It returns (0, 0, nil) when chunkDir carries no trustworthy geometry yet
// (chunks.meta missing or corrupt) — the same case ensureChunkGeometry treats
// as "nothing to resume from", so there is nothing safe to seed either.
//
// The pipeline uses this to seed a volume's progress stream with its
// already-downloaded bytes as soon as the stream is created — well before the
// DataExport becomes ready or a fresh HEAD confirms totalSize — and keeps the
// seeded value in place (no reset) once DownloadBlockChunks starts:
// downloadChunk/fetchChunkRaw's own resume-skip crediting re-derives and
// re-credits the identical already-committed bytes, and the pipeline wraps
// that crediting with skipSeededBytes(seeded, ...) so the re-derived bytes
// are discarded instead of double-counted, rather than dropping the stream
// to 0 first (see pipeline.seedStreamFromDisk / pipeline.skipSeededBytes).
// Because this scan never mutates chunkDir between the two calls (no worker
// has started yet), the two
// computations always agree exactly for the normal (already-trusted) case;
// the only divergence is deliberate: because this scan is strictly read-only,
// an oversized ".part" never disappears out from under a display-only scan.
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

		partial, partErr := partialChunkSizeReadOnly(finalPath+partSuffix, rawLen)
		if partErr != nil {
			return 0, 0, fmt.Errorf("stat partial chunk %d in %s: %w", idx, chunkDir, partErr)
		}

		committed += partial
	}

	return committed, meta.TotalSize, nil
}

// partialChunkSize returns the number of bytes at the START of the durable
// partial file at partPath that are safe to resume from — 0 if partPath does
// not exist — truncating partPath in place to that trusted prefix whenever
// the file on disk physically holds more. It is the download path's variant
// (used by fetchChunkRaw, which is about to act on the chunk's geometry by
// appending to it): see partialChunkSizeReadOnly for the side-effect-free
// variant used by display-only progress scans.
//
// The raw file SIZE alone is not sufficient to trust. syncingWriter fsyncs
// partPath only every partSyncInterval bytes (plus once more, unconditionally,
// when the caller finishes writing — see fetchChunkRaw/syncingWriter.finish),
// and on some filesystem/mount-option combinations (e.g. ext4
// data=writeback) a file's SIZE metadata can become durable strictly AHEAD
// of its DATA. After a hard kill (SIGKILL/OOM/power loss) — which runs no Go
// code past the point of death, so a mid-interval fsync that had not yet
// happened simply never will — os.Stat can therefore report a length whose
// tail is actually zero or garbage. Trusting that stale size and appending
// after it would bake corrupt bytes into the finalized chunk frame
// undetected: block has no source digest to catch wrong BYTES within a
// correctly-SIZED chunk (the decoded-length check only catches a
// wrong TOTAL length).
//
// The trusted size is therefore capped at the offset recorded in the
// "<partPath>.offset" sidecar (see readDurablePartOffset), which is only
// ever written immediately after an fsync of partPath that itself already
// succeeded — so it can never name an offset the data has not already been
// proven durable for. A missing/corrupt sidecar trusts nothing (offset 0).
// Any physical bytes beyond the trusted offset — or beyond rawLen, which
// cannot belong to the current chunk geometry (see ensureChunkGeometry) —
// are truncated away before the caller is allowed to resume by appending.
// This bounds the worst-case re-fetch to at most partSyncInterval bytes: the
// same "at most the unsynced tail is lost" guarantee partSyncInterval
// already documents for a graceful interruption, now also holding across a
// hard kill.
func partialChunkSize(partPath string, rawLen int64) (int64, error) {
	trusted, info, err := trustedPartPrefix(partPath, rawLen)
	if err != nil || info == nil {
		return trusted, err
	}

	if info.Size() == trusted {
		return trusted, nil
	}

	if err := os.Truncate(partPath, trusted); err != nil {
		return 0, fmt.Errorf("truncate partial %s to trusted offset %d: %w", partPath, trusted, err)
	}

	return trusted, nil
}

// partialChunkSizeReadOnly returns the same TRUSTED prefix length as
// partialChunkSize (see its doc for the full durability rationale) but never
// mutates partPath: a ".part" file whose on-disk size exceeds the trusted
// offset is left exactly as it is on disk, and only the trusted (safe)
// portion is reported as committed for display purposes.
//
// Callers that only OBSERVE resume progress before any transfer starts
// (ScanBlockChunkProgress, and transitively ScanFSStagingProgress) MUST use
// this variant, never partialChunkSize: a scan used purely to seed a
// progress display must be side-effect-free, and discarding a stale/oversized
// partial is a decision that belongs to the actual download path
// (ensureChunkGeometry / fetchChunkRaw via partialChunkSize), which is about
// to act on the chunk's geometry anyway.
func partialChunkSizeReadOnly(partPath string, rawLen int64) (int64, error) {
	trusted, _, err := trustedPartPrefix(partPath, rawLen)
	return trusted, err
}

// trustedPartPrefix is the shared, side-effect-free computation behind both
// partialChunkSize and partialChunkSizeReadOnly: it stats partPath and its
// durable-offset sidecar and returns the resulting TRUSTED prefix length
// (min(durable offset, on-disk size, rawLen)), together with the os.FileInfo
// it stat'd so a mutating caller can compare it against the trusted value
// without a second stat. It returns (0, nil, nil) when partPath does not
// exist yet. It never itself truncates or removes anything — see
// partialChunkSize for the caller that does.
func trustedPartPrefix(partPath string, rawLen int64) (int64, os.FileInfo, error) {
	info, err := os.Stat(partPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil, nil
		}

		return 0, nil, fmt.Errorf("stat %s: %w", partPath, err)
	}

	durable, err := readDurablePartOffset(partPath)
	if err != nil {
		return 0, nil, fmt.Errorf("read durable offset for %s: %w", partPath, err)
	}

	return min(durable, info.Size(), rawLen), info, nil
}

// partOffsetPath returns the durable-offset sidecar path for partPath (see
// partOffsetSuffix).
func partOffsetPath(partPath string) string {
	return partPath + partOffsetSuffix
}

// readDurablePartOffset reads the durable-offset sidecar for partPath. A
// missing or unparseable sidecar returns 0 (not an error): both mean no
// offset has ever been proven durable for this ".part" file, the safe
// default that forces a re-fetch from the start of the chunk rather than
// trusting an unproven value. An unparseable sidecar can only result from a
// torn write mid-crash (writeDurablePartOffset uses archive.WriteFileAtomic,
// which makes this rare but not impossible), mirroring how
// archive.ErrCorruptChunkMeta is handled for chunks.meta elsewhere in this
// file: degrade to the safe default, never a hard error.
func readDurablePartOffset(partPath string) (int64, error) {
	path := partOffsetPath(partPath)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}

		return 0, fmt.Errorf("read durable offset %s: %w", path, err)
	}

	offset, parseErr := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if parseErr != nil || offset < 0 {
		return 0, nil
	}

	return offset, nil
}

// writeDurablePartOffset atomically and durably records offset as the
// trusted resume point for partPath. Callers MUST only invoke this
// immediately after an fsync of partPath's data has already succeeded (see
// syncingWriter.sync), so offset never claims more than what is already
// proven durable.
func writeDurablePartOffset(partPath string, offset int64) error {
	path := partOffsetPath(partPath)

	if err := archive.WriteFileAtomic(path, strings.NewReader(strconv.FormatInt(offset, 10))); err != nil {
		return fmt.Errorf("write durable offset %s: %w", path, err)
	}

	return nil
}

// syncingWriter wraps an *os.File and fsyncs it every syncInterval bytes
// written, bounding how much of a durable partial download can be lost to an
// unsynced OS page cache if the process is killed mid-chunk. Every fsync it
// performs — whether triggered by crossing syncInterval mid-write (Write) or
// by the caller's final call (finish) — is immediately followed by durably
// recording the resulting file length in partPath's offset sidecar (see
// writeDurablePartOffset), which is the only offset partialChunkSize will
// ever trust on a later resume.
type syncingWriter struct {
	f            *os.File
	partPath     string
	syncInterval int64
	// durableBase is the offset (bytes already durable before this writer's
	// lifetime began, i.e. the "have" partialChunkSize returned) that every
	// offset persisted below is measured from — so the sidecar always
	// records an absolute position from the start of the chunk's raw bytes,
	// not just bytes written during this writer's lifetime.
	durableBase   int64
	written       int64
	sinceLastSync int64
}

// Write implements io.Writer.
func (w *syncingWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.written += int64(n)

	if err != nil {
		return n, fmt.Errorf("write partial chunk: %w", err)
	}

	w.sinceLastSync += int64(n)
	if w.sinceLastSync < w.syncInterval {
		return n, nil
	}

	w.sinceLastSync = 0

	if err := w.sync(); err != nil {
		return n, err
	}

	return n, nil
}

// finish fsyncs the file exactly once more — covering any bytes written
// since the last interval checkpoint — and durably records the result as the
// trusted resume offset, regardless of whether the caller's copy loop that
// fed this writer succeeded or was interrupted by an error. This is what
// lets a GRACEFUL interrupt (a network error or context cancellation the
// process survives long enough to run this code) resume from the exact byte
// it reached, while a HARD KILL — which never reaches this call at all —
// still limits a resumed download to trusting only the interval checkpoints
// Write itself already persisted.
func (w *syncingWriter) finish() error {
	return w.sync()
}

// sync fsyncs the underlying file and, only if that succeeds, durably
// records durableBase+written as the trusted resume offset — never the
// other way around, so the sidecar can never claim an offset the data has
// not already been proven durable for.
func (w *syncingWriter) sync() error {
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("sync partial chunk: %w", err)
	}

	if err := writeDurablePartOffset(w.partPath, w.durableBase+w.written); err != nil {
		return err
	}

	return nil
}
