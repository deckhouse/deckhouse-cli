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
// encoded frame byte-for-byte into an unpublished AtomicWriter temporary file,
// in ascending order. Concatenation of independent frames is itself a valid
// multi-frame stream decodable by stock tools.
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
// ctx is checked once per chunk during the copy loop and on every read during
// decoded-length verification. Cancellation aborts the in-progress
// AtomicWriter (so no partial file is ever visible at outPath) and returns a
// wrapped ctx.Err() (checkable via errors.Is). A hard kill or graceful
// cancellation never loses data: the chunk directory is only removed after
// verification and a full successful Commit, so the merge resumes from the
// same chunks on the next run.
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

	aw, err := mergeByConcatenation(ctx, chunkDir, outPath, numChunks, ext)
	if err != nil {
		return err
	}

	if err := verifyPendingBlock(ctx, aw, outPath, ext, totalSize); err != nil {
		aw.Abort()

		return fmt.Errorf("verify merged %s: %w", outPath, err)
	}

	if err := aw.Commit(); err != nil {
		aw.Abort()

		return fmt.Errorf("commit verified %s: %w", outPath, err)
	}

	// Chunks remain the recovery source until the verified final artifact is
	// durably published. A removal error therefore leaves both the verified
	// final and the redundant chunks for the pipeline's already-merged cleanup.
	if err := os.RemoveAll(chunkDir); err != nil {
		return fmt.Errorf("remove chunk dir %s: %w", chunkDir, err)
	}

	return nil
}

// mergeByConcatenation implements MergeBlockChunks' merge path for every
// codec (gzip, lz4, none, and zstd): each chunk's already-finalized,
// independently-encoded frame is copied byte-for-byte into an unpublished
// AtomicWriter temporary file, in ascending order. The caller verifies and
// commits the returned writer.
func mergeByConcatenation(
	ctx context.Context,
	chunkDir string,
	outPath string,
	numChunks int,
	ext string,
) (*archive.AtomicWriter, error) {
	aw, err := archive.NewAtomicWriter(outPath)
	if err != nil {
		return nil, fmt.Errorf("open atomic writer for %s: %w", outPath, err)
	}

	for i := range numChunks {
		if err := ctx.Err(); err != nil {
			aw.Abort()
			return nil, fmt.Errorf("merge cancelled before chunk %d: %w", i, err)
		}

		p := filepath.Join(chunkDir, archive.ChunkFileName(i, ext))

		if err := copyFile(aw, p); err != nil {
			aw.Abort()
			return nil, fmt.Errorf("copy chunk %d into merged file: %w", i, err)
		}
	}

	return aw, nil
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
		aw.Abort()

		return fmt.Errorf("commit empty %s: %w", outPath, err)
	}

	if err := os.RemoveAll(chunkDir); err != nil {
		return fmt.Errorf("remove chunk dir %s: %w", chunkDir, err)
	}

	return nil
}

// verifyPendingBlock opens the AtomicWriter's unpublished temporary file and
// verifies it before Commit can make outPath visible.
func verifyPendingBlock(
	ctx context.Context,
	aw *archive.AtomicWriter,
	outPath string,
	ext string,
	wantSize int64,
) error {
	reader, err := aw.OpenTempReader()
	if err != nil {
		return err
	}

	verifyErr := verifyDecodedReader(ctx, reader, outPath, ext, wantSize)
	closeErr := reader.Close()

	if verifyErr != nil {
		if closeErr != nil {
			return errors.Join(verifyErr, fmt.Errorf("close merged file %s after verification: %w", outPath, closeErr))
		}

		return verifyErr
	}

	if closeErr != nil {
		return fmt.Errorf("close merged file %s after verification: %w", outPath, closeErr)
	}

	return nil
}

// verifyDecodedLength verifies an already-published file. MergeBlockChunks uses
// verifyPendingBlock instead so the final path remains invisible until this
// check has succeeded.
func verifyDecodedLength(outPath, ext string, wantSize int64) error {
	f, err := os.Open(outPath)
	if err != nil {
		return fmt.Errorf("open merged file %s: %w", outPath, err)
	}

	// An empty file is the canonical zero-frame representation of a zero-size
	// volume, including gzip whose reader otherwise rejects it with EOF.
	if wantSize == 0 {
		info, statErr := f.Stat()
		if statErr != nil {
			_ = f.Close()

			return fmt.Errorf("stat merged file %s: %w", outPath, statErr)
		}

		if info.Size() == 0 {
			if closeErr := f.Close(); closeErr != nil {
				return fmt.Errorf("close merged file %s after verification: %w", outPath, closeErr)
			}

			return nil
		}
	}

	verifyErr := verifyDecodedReader(context.Background(), f, outPath, ext, wantSize)
	closeErr := f.Close()

	if verifyErr != nil {
		if closeErr != nil {
			return errors.Join(verifyErr, fmt.Errorf("close merged file %s after verification: %w", outPath, closeErr))
		}

		return verifyErr
	}

	if closeErr != nil {
		return fmt.Errorf("close merged file %s after verification: %w", outPath, closeErr)
	}

	return nil
}

// verifyDecodedReader decodes src — a codec-compressed stream identified
// by ext (as codec.Ext() returns it: "", ".zst", ".gz", or ".lz4") that may
// be a concatenation of multiple independent frames — and asserts it yields
// exactly wantSize raw bytes. Decoding streams through the codec's reader
// into a discarding counter (decodeVolumeStream, shared with the filesystem
// per-file verification path in fs.go), so no whole-volume buffer is
// introduced regardless of volume size. contextReader checks ctx before every
// compressed-input read, making cancellation errors errors.Is-compatible.
//
// EXTENSION POINT: if a future exporter version starts sending a block-level
// content digest, verify it here alongside the length check — do not invent
// one now (the current exporter sends none, see ErrDecodedLengthMismatch).
func verifyDecodedReader(ctx context.Context, src io.Reader, outPath, ext string, wantSize int64) error {
	verifier := decodedLengthVerifier{ctx: ctx, want: wantSize}

	if err := decodeVolumeStream(&verifier, &contextReader{ctx: ctx, src: src}, ext); err != nil {
		return fmt.Errorf("decode merged file %s: %w", outPath, err)
	}

	if verifier.n != wantSize {
		return fmt.Errorf("decoded %d bytes, want %d: %w", verifier.n, wantSize, ErrDecodedLengthMismatch)
	}

	return nil
}

type contextReader struct {
	ctx context.Context
	src io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, fmt.Errorf("block verification cancelled: %w", err)
	}

	return r.src.Read(p)
}

// decodedLengthVerifier checks cancellation on decoded output as well as
// compressed input, and stops as soon as the declared length is exceeded.
// This keeps verification responsive even when a decoder buffers compressed
// input internally and bounds decompression work for an over-sent stream.
type decodedLengthVerifier struct {
	ctx  context.Context
	want int64
	n    int64
}

func (v *decodedLengthVerifier) Write(p []byte) (int, error) {
	if err := v.ctx.Err(); err != nil {
		return 0, fmt.Errorf("block verification cancelled: %w", err)
	}

	n := int64(len(p))
	if n > v.want-v.n {
		return 0, fmt.Errorf("decoded more than %d bytes: %w", v.want, ErrDecodedLengthMismatch)
	}

	v.n += n

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
