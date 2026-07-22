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

package volume

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

// ErrMissingChunk is returned by MergeBlockChunks when one or more expected
// chunk files are absent, preventing a gap-free merge.
var ErrMissingChunk = errors.New("block chunk missing")

// ErrDecodedLengthMismatch is returned by MergeBlockChunks when the merged
// volume, once decoded, does not yield exactly the declared totalSize. The
// block exporter emits no content digest — only Content-Length (see
// storage-volume-data-manager images/data-exporter/internal/export_block/handler.go
// prepareHead) — so decoded length is the strongest source-independent
// invariant available to detect a truncated, over-sent, or otherwise
// corrupted merged data.bin[.<ext>].
var ErrDecodedLengthMismatch = errors.New("merged block volume decoded length mismatch")

// MergeBlockChunks assembles all chunk_%05d[.<ext>] files from chunkDir into
// a single outPath, in strict ascending-index order. ext is the codec
// extension (e.g. ".zst"); use "" for the none codec.
//
// chunkDir is the absolute path to the directory containing the chunk files.
// outPath is the absolute destination path for the merged block file.
//
// Pre-conditions (enforced):
//   - All chunks 0 .. ceil(totalSize/chunkSize)-1 must be present.
//   - If any chunk is missing, ErrMissingChunk is returned and no output is written.
//
// MERGE STRATEGY is the same for every codec (gzip, lz4, none, and zstd):
// mergeByConcatenation copies each chunk's already-finalized, independently-
// encoded frame byte-for-byte into outPath, in ascending order. Concatenation
// of independent frames is itself a valid multi-frame stream decodable by
// stock tools.
//
// HISTORY: a native-zstd-seekable-format merge path (decoding each zstd chunk
// back to raw bytes and re-encoding it through a seekable.Writer to embed a
// seek table) was implemented and then reverted; see .agent/tasks.json's
// notes_on_plan_switch for the full history and the chunk-boundary-skipping
// redesign now being planned in its place.
//
// Post-conditions on success:
//   - outPath is a fully durable (fsynced) stream that decodes to exactly
//     totalSize raw bytes (verified; see ErrDecodedLengthMismatch).
//   - The chunk directory and all its contents are removed.
//   - No sidecar file is ever written alongside outPath, for any codec: the
//     chunk-offset index sidecar this function used to write is superseded by
//     the zstd path's own embedded seek table (the sidecar machinery itself
//     has since been deleted — nothing produces or consumes it anymore).
//
// chunkSize ≤ 0 falls back to DefaultChunkSize.
//
// A totalSize of 0 is a first-class case: under the frame-concatenation format
// zero raw bytes are zero frames, i.e. an EMPTY file. It is committed
// atomically, the (empty or absent) chunkDir is removed, and decoded-length
// verification is SKIPPED. An empty stream trivially decodes to the 0 wanted
// raw bytes, and for gzip the empty concatenation of zero frames IS the correct
// on-disk representation even though kgzip.NewReader rejects an empty stream
// with EOF (a gzip member requires a header). Without this short-circuit a
// zero-size gzip volume loops forever: merge → verify-fail → remove → retry.
// The restore/import decode counterparts rely on this "zero frames == zero
// bytes" contract, so it must not change without updating them.
//
// ctx is checked once per chunk during the copy/re-encode loop; if it is
// cancelled mid-merge, the in-progress AtomicWriter is aborted (so no partial
// file is ever visible at outPath) and a wrapped ctx.Err() is returned
// (checkable via errors.Is). A hard kill or graceful cancellation here never
// loses data: the chunk directory is only removed after a full successful
// Commit, so the merge simply resumes from the same chunks on the next run.
func MergeBlockChunks(ctx context.Context, chunkDir, outPath string, totalSize, chunkSize int64, ext string) error {
	if totalSize == 0 {
		return commitEmptyBlock(chunkDir, outPath)
	}

	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	numChunks := int((totalSize + chunkSize - 1) / chunkSize)

	// Verify all chunks are present before writing anything.
	for i := range numChunks {
		p := filepath.Join(chunkDir, archive.ChunkFileName(i, ext))

		_, err := os.Stat(p)
		if os.IsNotExist(err) {
			return fmt.Errorf("chunk %d (%s): %w", i, p, ErrMissingChunk)
		}

		if err != nil {
			return fmt.Errorf("stat chunk %d: %w", i, err)
		}
	}

	if err := mergeByConcatenation(ctx, chunkDir, outPath, numChunks, ext); err != nil {
		return err
	}

	// Remove the temporary chunk directory after successful commit.
	if err := os.RemoveAll(chunkDir); err != nil {
		return fmt.Errorf("remove chunk dir %s: %w", chunkDir, err)
	}

	// Verify the merged stream decodes to exactly totalSize AFTER chunkDir is
	// already gone (matching the filesystem MD5-verification precedent in
	// stageChunkedFile): a mismatch removes the corrupt outPath and returns,
	// so the NEXT run finds neither a chunk dir nor a data.bin and re-fetches
	// every chunk from the exporter fresh rather than repeatedly re-merging
	// the same untrustworthy chunk files forever.
	if err := verifyDecodedLength(outPath, ext, totalSize); err != nil {
		if removeErr := os.Remove(outPath); removeErr != nil && !os.IsNotExist(removeErr) {
			return errors.Join(
				fmt.Errorf("verify merged %s: %w", outPath, err),
				fmt.Errorf("remove corrupt merged file: %w", removeErr),
			)
		}

		return fmt.Errorf("verify merged %s: %w", outPath, err)
	}

	return nil
}

// mergeByConcatenation implements MergeBlockChunks' merge path for every
// codec (gzip, lz4, none, and zstd): each chunk's already-finalized,
// independently-encoded frame is copied byte-for-byte into outPath, in
// ascending order, via AtomicWriter.
func mergeByConcatenation(ctx context.Context, chunkDir, outPath string, numChunks int, ext string) error {
	aw, err := archive.NewAtomicWriter(outPath)
	if err != nil {
		return fmt.Errorf("open atomic writer for %s: %w", outPath, err)
	}

	for i := range numChunks {
		if err := ctx.Err(); err != nil {
			aw.Abort()
			return fmt.Errorf("merge cancelled before chunk %d: %w", i, err)
		}

		p := filepath.Join(chunkDir, archive.ChunkFileName(i, ext))

		if err := copyFile(aw, p); err != nil {
			aw.Abort()
			return fmt.Errorf("copy chunk %d into merged file: %w", i, err)
		}
	}

	if err := aw.Commit(); err != nil {
		return fmt.Errorf("commit %s: %w", outPath, err)
	}

	return nil
}

// commitEmptyBlock durably writes an empty outPath and drops the (empty or
// absent) chunkDir. It is the zero-size branch of MergeBlockChunks: see that
// function's doc comment for the "zero frames == zero bytes" contract and why
// verification is skipped here. Idempotent on re-run — os.RemoveAll tolerates a
// missing chunkDir and the AtomicWriter rename replaces any prior empty file.
func commitEmptyBlock(chunkDir, outPath string) error {
	aw, err := archive.NewAtomicWriter(outPath)
	if err != nil {
		return fmt.Errorf("open atomic writer for %s: %w", outPath, err)
	}

	if err := aw.Commit(); err != nil {
		return fmt.Errorf("commit empty %s: %w", outPath, err)
	}

	if err := os.RemoveAll(chunkDir); err != nil {
		return fmt.Errorf("remove chunk dir %s: %w", chunkDir, err)
	}

	return nil
}

// verifyDecodedLength decodes outPath — a codec-compressed stream identified
// by ext (as codec.Ext() returns it: "", ".zst", ".gz", or ".lz4") that may
// be a concatenation of multiple independent frames — and asserts it yields
// exactly wantSize raw bytes. Decoding streams through the codec's reader
// into a discarding counter (decodeVolumeStream, shared with the filesystem
// per-file verification path in fs.go), so no whole-volume buffer is
// introduced regardless of volume size.
//
// EXTENSION POINT: if a future exporter version starts sending a block-level
// content digest, verify it here alongside the length check — do not invent
// one now (the current exporter sends none, see ErrDecodedLengthMismatch).
func verifyDecodedLength(outPath, ext string, wantSize int64) error {
	f, err := os.Open(outPath)
	if err != nil {
		return fmt.Errorf("open merged file %s: %w", outPath, err)
	}

	defer func() { _ = f.Close() }()

	// Defensive symmetry with the MergeBlockChunks zero-size short-circuit: an
	// empty file trivially decodes to zero raw bytes under every codec, but
	// gzip's reader rejects an empty stream with EOF (a gzip member needs a
	// header). Treat an empty input as a valid zero-length decode so ad-hoc
	// callers stay safe without a per-codec special-case.
	if wantSize == 0 {
		info, statErr := f.Stat()
		if statErr != nil {
			return fmt.Errorf("stat merged file %s: %w", outPath, statErr)
		}

		if info.Size() == 0 {
			return nil
		}
	}

	var counter byteCounter

	if err := decodeVolumeStream(&counter, f, ext); err != nil {
		return fmt.Errorf("decode merged file %s: %w", outPath, err)
	}

	if counter.n != wantSize {
		return fmt.Errorf("decoded %d bytes, want %d: %w", counter.n, wantSize, ErrDecodedLengthMismatch)
	}

	return nil
}

// byteCounter is an io.Writer that discards its input and sums the total
// bytes written, used to measure a decoded stream's length without buffering it.
type byteCounter struct {
	n int64
}

// Write implements io.Writer.
func (c *byteCounter) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}

// copyFile copies the contents of src into dst.
func copyFile(dst io.Writer, src string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(dst, f); err != nil {
		return err
	}

	return nil
}
