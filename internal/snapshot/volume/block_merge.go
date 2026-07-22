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

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/klauspost/compress/zstd"

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
// MERGE STRATEGY is codec-dependent (see tasks.json's notes_on_plan_switch,
// NATIVE-ZSTD-SEEKABLE-RESUME PIVOT entry, for the full design history):
//   - gzip, lz4, none (mergeByConcatenation): each chunk's already-finalized,
//     independently-encoded frame is copied byte-for-byte into outPath.
//     Concatenation of independent frames is itself a valid multi-frame
//     stream decodable by stock tools. No comparably mature/maintained
//     seekable-format Go library exists for lz4 or gzip, so both keep the
//     plain byte-zero discard-and-fast-forward resume behavior on the read
//     side; this merge path is byte-for-byte unchanged from before the
//     native-zstd-seek pivot.
//   - zstd (mergeZstdSeekable): each chunk's already-finalized frame is
//     decoded back to its exact raw bytes and re-fed through a
//     seekable.Writer, which re-encodes it as a fresh frame and appends one
//     seek-table entry. After the last chunk, the seek table is embedded as
//     the file's own trailing skippable frame — still transparently
//     decodable by a stock zstd reader (see
//     compress/seekable_spike_test.go), but now also directly seekable to
//     any chunk's raw offset without a byte-zero scan.
//     COST TRADEOFF: this pays a ONE-TIME decode-then-re-encode pass over the
//     zstd volume's data at merge time (replacing the previous plain
//     byte-concatenation merge zstd used too), in exchange for eliminating
//     resume-time decode-and-discard entirely on an interrupted upload —
//     however large the already-uploaded prefix is, at most one chunk's
//     worth of extra work is ever repeated (see
//     putblockcompressed-native-zstd-seek-resume). gzip/lz4/none pay neither
//     this merge-time cost nor gain that resume-time benefit.
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

	if ext == ".zst" {
		if err := mergeZstdSeekable(ctx, chunkDir, outPath, chunkSize, numChunks); err != nil {
			return err
		}
	} else {
		if err := mergeByConcatenation(ctx, chunkDir, outPath, numChunks, ext); err != nil {
			return err
		}
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
// codec except zstd (gzip, lz4, none): each chunk's already-finalized,
// independently-encoded frame is copied byte-for-byte into outPath, in
// ascending order, via AtomicWriter. This is byte-for-byte identical to
// MergeBlockChunks' merge behavior before the native-zstd-seek pivot (see
// that function's doc comment) — untouched by this task for these codecs.
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

// mergeZstdSeekable implements MergeBlockChunks' zstd-specific merge path:
// see that function's doc comment for the full design and cost-tradeoff
// rationale. It decodes each chunk's already-finalized, independent zstd
// frame back to its exact raw bytes and re-feeds them through a
// seekable.Writer wrapping aw, so the final outPath carries an embedded seek
// table as its own trailing skippable frame.
//
// MEMORY BOUNDING: raw is allocated ONCE, sized to chunkSize (the largest
// possible per-chunk length), and reused for every chunk — decodeChunkFrame
// only ever fills raw[:n] for the CURRENT chunk before sw.Write consumes it
// (Writer.Write's EncodeAll call is synchronous, so the bytes are safe to
// overwrite on the next iteration). Peak additional memory for this whole
// function therefore stays bounded by chunkSize, never by totalSize — the
// same chunkSize bound DownloadBlockChunks' doc comment already documents for
// zstd's encode-side EncodeFrameStream exception, not a new multiplicative
// concern (see also this function's WithEncoderConcurrency(1)/
// WithDecoderConcurrency(1) note below, the OTHER load-bearing half of this
// bound).
func mergeZstdSeekable(ctx context.Context, chunkDir, outPath string, chunkSize int64, numChunks int) error {
	aw, err := archive.NewAtomicWriter(outPath)
	if err != nil {
		return fmt.Errorf("open atomic writer for %s: %w", outPath, err)
	}

	// A fresh encoder/decoder pair for this merge call only: re-encoding at
	// merge time is independent of whatever level the chunks were originally
	// encoded at (that level is not recorded anywhere MergeBlockChunks can
	// see), so LevelDefault is used deliberately here rather than threading a
	// level through this function's signature.
	//
	// WithEncoderConcurrency(1)/WithDecoderConcurrency(1) are load-bearing for
	// the memory-bounding invariant, not a performance tweak: klauspost/zstd's
	// DEFAULT concurrency is GOMAXPROCS, backed by a pool of that many
	// independent encoder/decoder workers, and this function drives many
	// one-shot EncodeAll/decode calls through the SAME long-lived enc/dec
	// instance in a tight loop (one call per chunk) -- once every pooled
	// worker has been used at least once, EACH one retains its own
	// windowSize-sized internal state for the rest of the merge. Empirically
	// confirmed (see this task's progress note): on a 12-core machine, the
	// default pool made a 30-chunk/8MiB-chunk merge retain ~220 MiB live
	// (~GOMAXPROCS × one window each) instead of the intended single-chunk
	// bound -- forcing both pools down to exactly one worker keeps retained
	// state to one window's worth, independent of GOMAXPROCS and totalSize.
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderCRC(true), zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		aw.Abort()
		return fmt.Errorf("create zstd encoder for seekable writer: %w", err)
	}

	defer func() { _ = enc.Close() }()

	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		aw.Abort()
		return fmt.Errorf("create zstd decoder for chunk re-encode: %w", err)
	}
	defer dec.Close()

	sw, err := seekable.NewWriter(aw, enc)
	if err != nil {
		aw.Abort()
		return fmt.Errorf("create seekable zstd writer for %s: %w", outPath, err)
	}

	// raw's capacity is chunkSize -- the largest possible per-chunk raw
	// length under the CURRENT geometry. decodeChunkFrame never reads more
	// than that (an actual overrun is reported as an error, see its doc
	// comment); a chunk that decodes to FEWER bytes (e.g. the volume's last,
	// shorter chunk, or a genuinely truncated/corrupt frame) is not an error
	// here -- the post-merge verifyDecodedLength aggregate check (unchanged)
	// catches a real shortfall across the whole volume exactly as it did for
	// the byte-concatenation merge path.
	raw := make([]byte, chunkSize)

	for i := range numChunks {
		if err := ctx.Err(); err != nil {
			aw.Abort()
			return fmt.Errorf("merge cancelled before chunk %d: %w", i, err)
		}

		p := filepath.Join(chunkDir, archive.ChunkFileName(i, ".zst"))

		n, err := decodeChunkFrame(dec, p, raw)
		if err != nil {
			aw.Abort()
			return fmt.Errorf("decode chunk %d for seekable re-encode: %w", i, err)
		}

		if _, err := sw.Write(raw[:n]); err != nil {
			aw.Abort()
			return fmt.Errorf("write chunk %d into seekable stream: %w", i, err)
		}
	}

	if err := sw.Close(); err != nil {
		aw.Abort()
		return fmt.Errorf("close seek table trailer for %s: %w", outPath, err)
	}

	if err := aw.Commit(); err != nil {
		return fmt.Errorf("commit %s: %w", outPath, err)
	}

	return nil
}

// decodeChunkFrame decodes the single independent zstd frame at path into
// buf, reusing dec (via Reset) so no new decoder — and its internal buffers —
// is allocated per chunk. It returns n, the frame's ACTUAL decoded length,
// which may be less than len(buf): a chunk covering less than a full
// chunkSize (typically the volume's last chunk) legitimately decodes short,
// and that is not an error here (see MergeBlockChunks' post-merge
// verifyDecodedLength, which is the actual aggregate-length authority).
//
// What decodeChunkFrame DOES guard is an OVERRUN: if the frame still has
// bytes remaining after filling buf to capacity, that chunk decodes to MORE
// than the current geometry's chunkSize allows for — silently truncating it
// at len(buf) would mask real corruption (or a stale/mismatched-geometry
// chunk file) as a shorter-than-actual merge instead of surfacing it, so this
// case returns an error wrapping ErrDecodedLengthMismatch instead.
func decodeChunkFrame(dec *zstd.Decoder, path string, buf []byte) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open chunk file %s: %w", path, err)
	}

	defer func() { _ = f.Close() }()

	if err := dec.Reset(f); err != nil {
		return 0, fmt.Errorf("reset zstd decoder for %s: %w", path, err)
	}

	n, err := io.ReadFull(dec, buf)

	switch {
	case err == nil:
		// buf was filled to capacity; check whether the frame has MORE bytes
		// beyond that -- an overrun relative to the current chunkSize.
		var extra [1]byte

		_, peekErr := dec.Read(extra[:])

		switch {
		case peekErr == nil:
			return 0, fmt.Errorf("chunk %s decodes to more than the expected %d bytes: %w", path, len(buf), ErrDecodedLengthMismatch)
		case errors.Is(peekErr, io.EOF):
			// Frame ends exactly at len(buf): the common case for every
			// non-last chunk under an unchanged geometry.
		default:
			return 0, fmt.Errorf("read decoded chunk %s: %w", path, peekErr)
		}
	case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
		// Short frame: fewer bytes than len(buf). Not an error here -- see
		// this function's doc comment.
	default:
		return 0, fmt.Errorf("read decoded chunk %s: %w", path, err)
	}

	return n, nil
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
