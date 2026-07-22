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

// This file is the empirical spike for the native-zstd-seekable-resume design
// (see notes_on_plan_switch's NATIVE-ZSTD-SEEKABLE-RESUME PIVOT entry, 2026-07-22).
// It pins down, with permanent regression coverage, the exact behavior of
// github.com/SaveTheRbtz/zstd-seekable-format-go/pkg that later tasks
// (block-merge-embed-native-zstd-seek-table, putblockcompressed-native-zstd-seek-resume)
// build on: genuine seeking (not scanning) to a chunk boundary, stock
// decodability of the produced file by this repo's own compress.NewReader, and
// the non-panicking failure shape when no seek table is embedded.
package compress_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/klauspost/compress/zstd"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
)

// recordingReadSeeker wraps an in-memory buffer as an io.ReadSeeker that also
// implements io.ReaderAt, recording the byte offset of every ReadAt call.
// seekable.Reader's default read environment (readSeekerEnvImpl) prefers
// io.ReaderAt when the wrapped source implements it (matching a real
// *os.File-backed archive), routing every per-frame data fetch through ReadAt
// while the one-time footer/seek-table parse at construction uses Seek+Read
// directly — recording only ReadAt calls therefore isolates genuine per-frame
// data fetches from that one-time bookkeeping, which is exactly what the
// "does not scan/decode earlier chunks" assertion needs.
type recordingReadSeeker struct {
	data []byte
	pos  int64

	frameReadOffsets []int64
}

func newRecordingReadSeeker(data []byte) *recordingReadSeeker {
	return &recordingReadSeeker{data: data}
}

// Read implements io.Reader, used only by the library's one-time footer/seek-table parse.
func (r *recordingReadSeeker) Read(p []byte) (int, error) {
	if r.pos >= int64(len(r.data)) {
		return 0, io.EOF
	}

	n := copy(p, r.data[r.pos:])
	r.pos += int64(n)

	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}

// Seek implements io.Seeker, used only by the library's one-time footer/seek-table parse.
func (r *recordingReadSeeker) Seek(offset int64, whence int) (int64, error) {
	var newPos int64

	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = r.pos + offset
	case io.SeekEnd:
		newPos = int64(len(r.data)) + offset
	default:
		return 0, fmt.Errorf("recordingReadSeeker: unknown whence %d", whence)
	}

	if newPos < 0 {
		return 0, fmt.Errorf("recordingReadSeeker: negative position %d", newPos)
	}

	r.pos = newPos

	return r.pos, nil
}

// ReadAt implements io.ReaderAt — the path seekable.Reader uses to fetch one
// compressed frame's bytes by its known compressed offset. Every call is
// recorded so the test can assert which byte ranges were actually touched.
func (r *recordingReadSeeker) ReadAt(p []byte, off int64) (int, error) {
	r.frameReadOffsets = append(r.frameReadOffsets, off)

	if off < 0 || off >= int64(len(r.data)) {
		return 0, io.EOF
	}

	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}

// seekableChunks returns independent-content chunks of varying sizes, large
// enough to span multiple zstd blocks each, used to build the fixture shared
// by the round-trip and stock-decodability sub-tests.
func seekableChunks() [][]byte {
	return [][]byte{
		bytes.Repeat([]byte("chunk0-alpha-"), 4000),
		bytes.Repeat([]byte("chunk1-beta--"), 2500),
		bytes.Repeat([]byte("chunk2-gamma-"), 6000),
		bytes.Repeat([]byte("chunk3-delta-"), 1800),
		bytes.Repeat([]byte("chunk4-epsln-"), 7000),
	}
}

// buildSeekableFixture writes chunks through seekable.Writer, one Write call
// per chunk (the 1:1 chunk-to-frame-to-seek-table-entry mapping this repo's
// block-chunk model relies on), and returns the finished file bytes alongside
// the plain concatenation of the original chunks for comparison.
func buildSeekableFixture(t *testing.T, chunks [][]byte) (file []byte, plain []byte) {
	t.Helper()

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderCRC(true), zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		t.Fatalf("zstd.NewWriter (encoder): %v", err)
	}
	defer func() {
		if closeErr := enc.Close(); closeErr != nil {
			t.Errorf("closing zstd encoder: %v", closeErr)
		}
	}()

	var buf bytes.Buffer

	w, err := seekable.NewWriter(&buf, enc)
	if err != nil {
		t.Fatalf("seekable.NewWriter: %v", err)
	}

	for i, chunk := range chunks {
		if _, err := w.Write(chunk); err != nil {
			t.Fatalf("Write chunk %d: %v", i, err)
		}

		plain = append(plain, chunk...)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}

	return buf.Bytes(), plain
}

// TestSeekableFormat_RoundTripSeeksLateChunkWithoutScanning proves sub-test (a):
// Seek lands correctly on a late/mid chunk boundary and the library genuinely
// seeks — it never re-fetches bytes belonging to an earlier chunk.
func TestSeekableFormat_RoundTripSeeksLateChunkWithoutScanning(t *testing.T) {
	t.Helper()

	chunks := seekableChunks()
	file, plain := buildSeekableFixture(t, chunks)

	// cumulativeOffset returns the decompressed byte offset at which chunk n starts.
	cumulativeOffset := func(n int) int64 {
		var off int64
		for i := 0; i < n; i++ {
			off += int64(len(chunks[i]))
		}

		return off
	}

	cases := []struct {
		name   string
		offset int64
	}{
		// Exact chunk boundary: the first byte of the 4th (late) chunk.
		{name: "chunk_boundary_late_chunk", offset: cumulativeOffset(3)},
		// Mid-chunk: partway into the final chunk, not aligned to any frame start.
		{name: "mid_chunk_last_chunk", offset: cumulativeOffset(4) + int64(len(chunks[4])/2)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()

			src := newRecordingReadSeeker(file)

			dec, err := zstd.NewReader(nil)
			if err != nil {
				t.Fatalf("zstd.NewReader (decoder): %v", err)
			}
			defer dec.Close()

			r, err := seekable.NewReader(src, dec)
			if err != nil {
				t.Fatalf("seekable.NewReader: %v", err)
			}
			defer func() {
				if closeErr := r.Close(); closeErr != nil {
					t.Errorf("Reader.Close: %v", closeErr)
				}
			}()

			table, err := r.SeekTable()
			if err != nil {
				t.Fatalf("SeekTable: %v", err)
			}

			targetEntry, ok := table.EntryByDecompressedOffset(uint64(tc.offset))
			if !ok {
				t.Fatalf("EntryByDecompressedOffset(%d): no entry found", tc.offset)
			}

			if _, err := r.Seek(tc.offset, io.SeekStart); err != nil {
				t.Fatalf("Seek(%d): %v", tc.offset, err)
			}

			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("ReadAll after Seek: %v", err)
			}

			want := plain[tc.offset:]
			if !bytes.Equal(got, want) {
				t.Fatalf("suffix mismatch after Seek(%d): got len=%d want len=%d", tc.offset, len(got), len(want))
			}

			// The discriminating assertion: every actual frame fetch (ReadAt call)
			// must land at or after the target frame's own compressed offset. A
			// library that scanned from byte zero instead of genuinely seeking
			// would issue at least one ReadAt at an earlier compressed offset.
			if len(src.frameReadOffsets) == 0 {
				t.Fatal("expected at least one frame ReadAt call; got none")
			}

			for _, off := range src.frameReadOffsets {
				if off < int64(targetEntry.CompressedOffset) {
					t.Errorf("frame ReadAt at compressed offset %d precedes the target frame's own"+
						" compressed offset %d — the reader scanned earlier chunks instead of seeking",
						off, targetEntry.CompressedOffset)
				}
			}
		})
	}
}

// TestSeekableFormat_StockCompressReaderDecodesIt proves sub-test (b): this
// repo's own, UNMODIFIED compress.NewReader(".zst", ...) fully decodes a
// seekable-format file end-to-end — the embedded seek-table trailer is a
// skippable frame that a stock decoder (klauspost/compress/zstd, which
// compress.NewReader wraps unchanged) ignores, exactly as the community
// Zstandard Seekable Format spec promises.
func TestSeekableFormat_StockCompressReaderDecodesIt(t *testing.T) {
	t.Helper()

	chunks := seekableChunks()
	file, plain := buildSeekableFixture(t, chunks)

	r, err := compress.NewReader(".zst", bytes.NewReader(file))
	if err != nil {
		t.Fatalf("compress.NewReader(\".zst\"): %v", err)
	}

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll via compress.NewReader: %v", err)
	}

	if err := r.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	if !bytes.Equal(got, plain) {
		t.Fatalf("stock-decode mismatch: got len=%d want len=%d", len(got), len(plain))
	}
}

// TestSeekableFormat_NoSeekTableFailsGracefully proves sub-test (c): opening a
// PLAIN (ordinary concatenated-EncodeFrame, non-seekable) .zst file through
// seekable.NewReader fails cleanly — a non-nil, non-panicking error and a nil
// Reader — rather than panicking or silently returning a Reader that then
// misbehaves.
//
// Recorded finding for putblockcompressed-native-zstd-seek-resume (the next
// task that must detect this condition): the library exposes NO exported
// sentinel or typed error for "no seek table found". The failure surfaces from
// seek_table_parser.go's readSeekTable, which blindly parses the LAST 9 BYTES
// of the source as a seekTableFooter — for a plain concatenated-frame file
// those bytes are real (final-frame) zstd payload, not a footer, so the
// specific message that comes back (magic mismatch vs. reserved-bits-nonzero
// vs., for a very short input, a Seek/read failure) depends on the incidental
// content of the file's last 9 bytes and is NOT a stable string or wrapped
// sentinel to assert on. The next task's dispatch MUST therefore treat ANY
// non-nil error from seekable.NewReader as "no usable seek table, fall back to
// the byte-zero discard path" — it cannot branch on errors.Is/errors.As against
// a specific error value, because the library provides none.
func TestSeekableFormat_NoSeekTableFailsGracefully(t *testing.T) {
	t.Helper()

	codec, err := compress.New("zstd", 0)
	if err != nil {
		t.Fatalf("compress.New(zstd): %v", err)
	}

	var plainFile []byte

	for _, chunk := range seekableChunks() {
		frame, err := codec.EncodeFrame(chunk)
		if err != nil {
			t.Fatalf("EncodeFrame: %v", err)
		}

		plainFile = append(plainFile, frame...)
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("zstd.NewReader (decoder): %v", err)
	}
	defer dec.Close()

	r, err := seekable.NewReader(bytes.NewReader(plainFile), dec)
	if err == nil {
		t.Fatal("expected a non-nil error opening a plain (non-seekable) zstd file; got nil")
	}

	if r != nil {
		t.Errorf("expected a nil Reader on error; got %v", r)
	}

	t.Logf("observed no-seek-table error shape (informational, not asserted verbatim): %v", err)
}
