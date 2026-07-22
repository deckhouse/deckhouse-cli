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

package volume_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/klauspost/compress/zstd"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
)

// makeChunkFrames writes N zstd frames (one per chunk) into chunkDir and
// returns the raw byte slices so callers can reconstruct the expected output.
func makeChunkFrames(t *testing.T, chunkDir string, payloads [][]byte) {
	t.Helper()

	makeChunkFramesWithCodec(t, chunkDir, payloads, "zstd")
}

// makeChunkFramesWithCodec writes N independent frames (one per chunk),
// encoded with the named codec, into chunkDir and returns the codec's
// extension so callers can pass it to MergeBlockChunks.
func makeChunkFramesWithCodec(t *testing.T, chunkDir string, payloads [][]byte, codecName string) string {
	t.Helper()

	codec, err := compress.New(codecName, 0)
	if err != nil {
		t.Fatalf("compress.New(%s): %v", codecName, err)
	}

	for i, payload := range payloads {
		frame, err := codec.EncodeFrame(payload)
		if err != nil {
			t.Fatalf("EncodeFrame chunk %d: %v", i, err)
		}

		p := filepath.Join(chunkDir, archive.ChunkFileName(i, codec.Ext()))
		if err := os.WriteFile(p, frame, 0o600); err != nil {
			t.Fatalf("write chunk %d: %v", i, err)
		}
	}

	return codec.Ext()
}

// decodeZstdStream decodes a multi-frame zstd stream and returns the raw bytes.
func decodeZstdStream(t *testing.T, data []byte) []byte {
	t.Helper()

	dec, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("zstd.NewReader: %v", err)
	}
	defer dec.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(dec); err != nil {
		t.Fatalf("decode zstd stream: %v", err)
	}

	return buf.Bytes()
}

func TestMergeBlockChunks_MergesInOrder(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	outPath := filepath.Join(nodeDir, archive.DataBlockName(".zst"))

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	payloads := [][]byte{
		[]byte("chunk-zero-data"),
		[]byte("chunk-one--data"),
		[]byte("chunk-two--data"),
	}

	chunkSize := int64(15) // matches payload length
	totalSize := int64(len(payloads)) * chunkSize

	makeChunkFrames(t, chunkDir, payloads)

	if err := volume.MergeBlockChunks(context.Background(), chunkDir, outPath, totalSize, chunkSize, ".zst"); err != nil {
		t.Fatalf("MergeBlockChunks: %v", err)
	}
	finalPath := outPath
	raw, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("read merged block file: %v", err)
	}

	// Decode the multi-frame stream; result must equal payload concat.
	decoded := decodeZstdStream(t, raw)
	want := bytes.Join(payloads, nil)

	if !bytes.Equal(decoded, want) {
		t.Errorf("decoded content mismatch: got %q want %q", decoded, want)
	}
}

// TestMergeBlockChunks_NoChunkIndexSidecar_AnyCodec proves that MergeBlockChunks
// never writes a '.chunkidx' sidecar next to its merged output, for ANY codec
// (zstd, gzip, lz4, none): the sidecar mechanism is superseded for zstd by its
// own embedded seek table (see TestMergeBlockChunks_Zstd_EmbedsSeekTable) and
// was never anything but overhead for the others (see
// notes_on_plan_switch's NATIVE-ZSTD-SEEKABLE-RESUME PIVOT entry in
// tasks.json).
func TestMergeBlockChunks_NoChunkIndexSidecar_AnyCodec(t *testing.T) {
	payloads := [][]byte{
		[]byte("chunk0"),
		[]byte("chunk1"),
		[]byte("chunk2"),
	}
	chunkSize := int64(6) // matches each payload's length
	totalSize := int64(len(payloads)) * chunkSize

	for _, codecName := range []string{"zstd", "gzip", "lz4", "none"} {
		t.Run(codecName, func(t *testing.T) {
			nodeDir := t.TempDir()
			chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

			if err := os.MkdirAll(chunkDir, 0o755); err != nil {
				t.Fatal(err)
			}

			ext := makeChunkFramesWithCodec(t, chunkDir, payloads, codecName)
			outPath := filepath.Join(nodeDir, archive.DataBlockName(ext))

			if err := volume.MergeBlockChunks(context.Background(), chunkDir, outPath, totalSize, chunkSize, ext); err != nil {
				t.Fatalf("MergeBlockChunks(%s): %v", codecName, err)
			}

			if _, statErr := os.Stat(archive.BlockChunkIndexPath(outPath)); !os.IsNotExist(statErr) {
				t.Errorf("expected no chunk index sidecar for codec %s, Stat returned: %v", codecName, statErr)
			}
		})
	}
}

// TestMergeBlockChunks_Zstd_EmbedsSeekTable proves the core contract of the
// native-zstd-seekable-resume merge path: the merged data.bin.zst, opened via
// seekable.NewReader, reports a valid SeekTable whose NumFrames() equals the
// number of chunks merged and whose Size() equals totalSize -- i.e. the seek
// table genuinely describes the whole merged volume, not just the last write.
func TestMergeBlockChunks_Zstd_EmbedsSeekTable(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	outPath := filepath.Join(nodeDir, archive.DataBlockName(".zst"))

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	payloads := [][]byte{
		[]byte("chunk0"),
		[]byte("chunk1"),
		[]byte("chunk2"),
		[]byte("chunk3"),
	}
	chunkSize := int64(6) // matches each payload's length
	totalSize := int64(len(payloads)) * chunkSize

	makeChunkFrames(t, chunkDir, payloads)

	if err := volume.MergeBlockChunks(context.Background(), chunkDir, outPath, totalSize, chunkSize, ".zst"); err != nil {
		t.Fatalf("MergeBlockChunks: %v", err)
	}

	f, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open merged output: %v", err)
	}
	defer f.Close()

	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("zstd.NewReader (decoder): %v", err)
	}
	defer dec.Close()

	sr, err := seekable.NewReader(f, dec)
	if err != nil {
		t.Fatalf("seekable.NewReader: %v", err)
	}
	defer sr.Close()

	table, err := sr.SeekTable()
	if err != nil {
		t.Fatalf("SeekTable: %v", err)
	}

	if got := table.NumFrames(); got != int64(len(payloads)) {
		t.Errorf("NumFrames() = %d, want %d (one frame per merged chunk)", got, len(payloads))
	}

	if got := table.Size(); got != uint64(totalSize) {
		t.Errorf("Size() = %d, want %d", got, totalSize)
	}

	// The embedded seek table must not just report the right totals: it must
	// still let a plain stock compress.NewReader(".zst", ...) decode the
	// whole file, unaffected by the trailing skippable seek-table frame.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek merged output back to start: %v", err)
	}

	stockReader, err := compress.NewReader(".zst", f)
	if err != nil {
		t.Fatalf("compress.NewReader: %v", err)
	}

	decoded, err := io.ReadAll(stockReader)
	if err != nil {
		t.Fatalf("stock decode of merged output: %v", err)
	}

	if err := stockReader.Close(); err != nil {
		t.Errorf("close stock reader: %v", err)
	}

	want := bytes.Join(payloads, nil)
	if !bytes.Equal(decoded, want) {
		t.Errorf("stock-decoded content mismatch: got %q want %q", decoded, want)
	}
}

// measurePeakHeapAlloc runs fn while a background goroutine polls
// runtime.MemStats.HeapAlloc on a tight ticker, returning the highest value
// observed. This is the "sample continuously while the code under test runs"
// variant of the heap-delta technique used elsewhere in this repo (see
// snapimport/volume_test.go's requestBodyReadTracker): MergeBlockChunks has
// no single natural callback hook (like an HTTP body's first Read) to sample
// at, since its zstd merge path processes many chunks in one call, so peak
// live heap is tracked continuously for the call's whole duration instead.
func measurePeakHeapAlloc(t *testing.T, fn func()) uint64 {
	t.Helper()

	stop := make(chan struct{})
	peakCh := make(chan uint64, 1)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		var peak uint64

		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				peakCh <- peak
				return
			case <-ticker.C:
				// Force a GC pass before sampling: HeapAlloc alone also
				// counts not-yet-swept garbage, which -- over a whole
				// multi-chunk merge -- can accumulate to a large number even
				// for a genuinely chunk-bounded implementation, simply
				// because Go's own GC pacing never happened to trigger
				// during this short a run. Forcing a collection on every
				// sample makes HeapAlloc reflect truly LIVE (still
				// referenced) bytes only, which is the property this test
				// actually needs: a full-buffering regression's big buffer
				// stays referenced (and so stays large after a forced GC)
				// for as long as the merge holds it, while a bounded
				// implementation's small per-chunk buffers get freed by it.
				runtime.GC()

				var ms runtime.MemStats

				runtime.ReadMemStats(&ms)

				if ms.HeapAlloc > peak {
					peak = ms.HeapAlloc
				}
			}
		}
	}()

	fn()
	close(stop)
	wg.Wait()

	return <-peakCh
}

// writeMemoryBoundedZstdFixture writes numChunks independent zstd frames of
// chunkSize raw bytes each into chunkDir, varying content slightly per chunk
// so it is not trivially a single repeated frame. It is a separate function
// (not inlined into its caller) so the large synthetic payloads it builds are
// unreachable -- their whole stack frame is gone -- as soon as it returns,
// rather than merely unreferenced-but-still-in-scope in the calling test.
func writeMemoryBoundedZstdFixture(t *testing.T, chunkDir string, chunkSize, numChunks int) {
	t.Helper()

	pattern := []byte("memory-bound-merge-regression-test-data. ")

	payloads := make([][]byte, numChunks)
	for i := range payloads {
		p := bytes.Repeat(pattern, chunkSize/len(pattern)+2)
		p = p[:chunkSize]
		p[0] = byte('A' + i%26)

		payloads[i] = p
	}

	makeChunkFrames(t, chunkDir, payloads)
}

// TestMergeBlockChunks_Zstd_ReencodeIsMemoryBounded is the regression test
// for the zstd decode-then-reencode merge path's memory-bounding invariant
// (cross-cutting invariant #2/#11 in .agent/implementer-prompt.md): merging a
// large, multi-chunk volume must never hold more than roughly one chunk's
// worth of decoded bytes live at once, regardless of totalSize.
//
// Per invariant #11, tracking only the merged output's on-disk size (or any
// transport-facing metric) would NOT discriminate a genuinely chunk-bounded
// merge from a regression that decodes every chunk into one big buffer
// before writing it out — both produce byte-identical output. This test
// instead samples the process's live heap continuously while the merge runs
// (measurePeakHeapAlloc) and was EMPIRICALLY confirmed, by temporarily
// reintroducing exactly that full-buffering variant into mergeZstdSeekable
// (decoding every chunk into one concatenated buffer up front, then writing
// each chunk's slice out of it), to fail against the reintroduced regression
// before being trusted here — see this task's progress note.
func TestMergeBlockChunks_Zstd_ReencodeIsMemoryBounded(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	outPath := filepath.Join(nodeDir, archive.DataBlockName(".zst"))

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	const chunkSize = 8 * 1024 * 1024 // 8 MiB
	const numChunks = 30              // 240 MiB total raw

	totalSize := int64(chunkSize) * int64(numChunks)

	// Fixture creation is isolated in its own function so its 240 MiB of
	// synthetic payloads (and the codec's own encode-side buffers) are
	// provably out of scope -- not merely unreferenced-but-still-on-this-
	// frame -- by the time the baseline below is captured.
	writeMemoryBoundedZstdFixture(t, chunkDir, chunkSize, numChunks)

	runtime.GC()

	var before runtime.MemStats

	runtime.ReadMemStats(&before)

	var mergeErr error

	peak := measurePeakHeapAlloc(t, func() {
		mergeErr = volume.MergeBlockChunks(context.Background(), chunkDir, outPath, totalSize, chunkSize, ".zst")
	})

	if mergeErr != nil {
		t.Fatalf("MergeBlockChunks: %v", mergeErr)
	}

	// A regression that decodes every chunk into one buffer before writing
	// would hold on the order of totalSize (240 MiB) live at once; a
	// genuinely chunk-bounded merge stays within a small multiple of
	// chunkSize plus the zstd encoder/decoder's own fixed per-instance
	// window state (empirically ~35 MiB combined for one
	// WithEncoderConcurrency(1)/WithDecoderConcurrency(1) pair on an 8 MiB
	// chunk size -- see mergeZstdSeekable's doc comment). The ceiling sits
	// well below totalSize but comfortably above that fixed cost to absorb
	// allocator/runtime overhead and sampling jitter.
	const ceiling = 8 * chunkSize

	delta := int64(peak) - int64(before.HeapAlloc)
	if delta > ceiling {
		t.Errorf("peak live heap grew by %d bytes (%.1f MiB) merging a %d-byte (%.0f MiB) zstd volume, "+
			"want <= %d bytes (%d MiB): suspect a full-buffering regression in mergeZstdSeekable",
			delta, float64(delta)/(1024*1024), totalSize, float64(totalSize)/(1024*1024),
			int64(ceiling), ceiling/(1024*1024))
	}
}

func TestMergeBlockChunks_ChunkDirRemovedAfterSuccess(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	outPath := filepath.Join(nodeDir, archive.DataBlockName(".zst"))

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	payloads := [][]byte{[]byte("hello"), []byte("world")}
	chunkSize := int64(5)
	totalSize := int64(10)

	makeChunkFrames(t, chunkDir, payloads)

	if err := volume.MergeBlockChunks(context.Background(), chunkDir, outPath, totalSize, chunkSize, ".zst"); err != nil {
		t.Fatalf("MergeBlockChunks: %v", err)
	}

	if _, err := os.Stat(chunkDir); !os.IsNotExist(err) {
		t.Errorf("chunk dir should be removed after merge, but Stat returned: %v", err)
	}
}

func TestMergeBlockChunks_MissingChunkErrors(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	outPath := filepath.Join(nodeDir, archive.DataBlockName(".zst"))

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write chunks 0 and 2 only; chunk 1 is missing (gap).
	codec, err := compress.New("zstd", 0)
	if err != nil {
		t.Fatal(err)
	}

	for _, idx := range []int{0, 2} {
		frame, err := codec.EncodeFrame([]byte("data"))
		if err != nil {
			t.Fatal(err)
		}

		p := filepath.Join(chunkDir, archive.ChunkFileName(idx, codec.Ext()))
		if err := os.WriteFile(p, frame, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	chunkSize := int64(4)
	totalSize := int64(12)

	err = volume.MergeBlockChunks(context.Background(), chunkDir, outPath, totalSize, chunkSize, ".zst")
	if err == nil {
		t.Fatal("expected error for missing chunk, got nil")
	}

	if !errors.Is(err, volume.ErrMissingChunk) {
		t.Errorf("expected ErrMissingChunk, got: %v", err)
	}

	// outPath must NOT have been written.
	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Error("output file should not exist when merge fails due to missing chunk")
	}
}

// TestMergeBlockChunks_ZeroSize_AllCodecs proves the zero-size short-circuit:
// for totalSize=0 the merge succeeds for every codec (including gzip, whose
// reader rejects an empty stream with EOF — the exact asymmetry that used to
// loop merge -> verify-fail -> remove -> retry forever), producing a durable
// empty output and removing the chunk dir. It must also be idempotent: a
// second (resume) invocation on the same dir succeeds identically, never
// looping.
func TestMergeBlockChunks_ZeroSize_AllCodecs(t *testing.T) {
	for _, codecName := range []string{"none", "zstd", "gzip", "lz4"} {
		t.Run(codecName, func(t *testing.T) {
			codec, err := compress.New(codecName, 0)
			if err != nil {
				t.Fatalf("compress.New(%s): %v", codecName, err)
			}

			ext := codec.Ext()
			nodeDir := t.TempDir()
			chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
			outPath := filepath.Join(nodeDir, archive.DataBlockName(ext))

			// A zero-size volume downloads zero chunks; the chunk dir may exist
			// (created empty) or not at all. Exercise the "exists but empty" case.
			if err := os.MkdirAll(chunkDir, 0o755); err != nil {
				t.Fatal(err)
			}

			assertZeroMerge := func(stage string) {
				t.Helper()

				if err := volume.MergeBlockChunks(context.Background(), chunkDir, outPath, 0, 0, ext); err != nil {
					t.Fatalf("%s: MergeBlockChunks(totalSize=0, %s): %v", stage, codecName, err)
				}

				info, statErr := os.Stat(outPath)
				if statErr != nil {
					t.Fatalf("%s: merged output missing for codec %s: %v", stage, codecName, statErr)
				}

				if info.Size() != 0 {
					t.Errorf("%s: merged output should be empty, got %d bytes", stage, info.Size())
				}

				if _, statErr := os.Stat(chunkDir); !os.IsNotExist(statErr) {
					t.Errorf("%s: chunk dir should be removed, Stat returned: %v", stage, statErr)
				}

				if _, statErr := os.Stat(archive.BlockChunkIndexPath(outPath)); !os.IsNotExist(statErr) {
					t.Errorf("%s: zero-size merge should never write a chunk index sidecar, Stat returned: %v", stage, statErr)
				}
			}

			assertZeroMerge("first run")
			// Re-run with the chunk dir already gone and the empty output already
			// present: the resume path must still succeed, not loop.
			assertZeroMerge("resume run")
		})
	}
}

func TestMergeBlockChunks_DefaultChunkSize(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	outPath := filepath.Join(nodeDir, archive.DataBlockName(".zst"))

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// One chunk smaller than DefaultChunkSize.
	payload := bytes.Repeat([]byte("x"), 1024)
	totalSize := int64(len(payload))

	makeChunkFrames(t, chunkDir, [][]byte{payload})

	// Pass chunkSize=0 to trigger DefaultChunkSize fallback.
	if err := volume.MergeBlockChunks(context.Background(), chunkDir, outPath, totalSize, 0, ".zst"); err != nil {
		t.Fatalf("MergeBlockChunks with default chunk size: %v", err)
	}

	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	decoded := decodeZstdStream(t, raw)
	if !bytes.Equal(decoded, payload) {
		t.Errorf("decoded content mismatch (length %d vs %d)", len(decoded), len(payload))
	}
}

// TestMergeBlockChunks_CancelledContext proves that MergeBlockChunks honors an
// already-cancelled context: it must return promptly (bounded by this test's
// own execution, not a real timing race) with an error wrapping ctx.Err(), and
// must not leave a partial file at the final (non-.tmp) output path — the
// in-progress AtomicWriter is aborted rather than committed.
func TestMergeBlockChunks_CancelledContext(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	outPath := filepath.Join(nodeDir, archive.DataBlockName(".zst"))

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	payloads := [][]byte{[]byte("hello"), []byte("world")}
	chunkSize := int64(5)
	totalSize := int64(10)

	makeChunkFrames(t, chunkDir, payloads)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := volume.MergeBlockChunks(ctx, chunkDir, outPath, totalSize, chunkSize, ".zst")
	if err == nil {
		t.Fatal("expected an error from MergeBlockChunks with an already-cancelled context, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected error to wrap context.Canceled, got: %v", err)
	}

	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Error("output file should not exist when the merge is cancelled before completion")
	}

	// The chunk directory must survive intact so a subsequent run can resume
	// the merge from the same chunks (cancellation is not a data-loss event).
	if _, statErr := os.Stat(chunkDir); statErr != nil {
		t.Errorf("chunk dir should survive a cancelled merge, but Stat returned: %v", statErr)
	}
}

// TestMergeBlockChunks_VerifiesDecodedLength_AllCodecs proves the new
// post-merge decoded-length check succeeds (does not falsely reject) a
// correctly merged volume for every registered codec, including gzip and
// lz4, whose concatenated-frame decoding is exercised end-to-end here for
// the first time via a multi-chunk merge (gzip relies on its reader's
// built-in multistream support; lz4 loops one reader per frame internally).
func TestMergeBlockChunks_VerifiesDecodedLength_AllCodecs(t *testing.T) {
	payloads := [][]byte{
		[]byte("chunk0"),
		[]byte("chunk1"),
		[]byte("chunk2"),
	}
	chunkSize := int64(6) // matches each payload's length
	totalSize := int64(len(payloads)) * chunkSize

	for _, codecName := range []string{"none", "zstd", "gzip", "lz4"} {
		t.Run(codecName, func(t *testing.T) {
			nodeDir := t.TempDir()
			chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

			if err := os.MkdirAll(chunkDir, 0o755); err != nil {
				t.Fatal(err)
			}

			ext := makeChunkFramesWithCodec(t, chunkDir, payloads, codecName)
			outPath := filepath.Join(nodeDir, archive.DataBlockName(ext))

			if err := volume.MergeBlockChunks(context.Background(), chunkDir, outPath, totalSize, chunkSize, ext); err != nil {
				t.Fatalf("MergeBlockChunks(%s): %v", codecName, err)
			}

			if _, err := os.Stat(outPath); err != nil {
				t.Fatalf("merged output missing for codec %s: %v", codecName, err)
			}
		})
	}
}

// TestMergeBlockChunks_DecodedLengthMismatch_FailsAndRemovesOutput simulates
// a truncated/short merged stream: two 5-byte chunks decode to 10 raw bytes,
// but the declared totalSize (20) claims twice that. MergeBlockChunks must
// fail with ErrDecodedLengthMismatch and must not leave the corrupt merged
// file at outPath -- otherwise a future resume's data.bin* glob check would
// treat this node's volume as already complete forever.
func TestMergeBlockChunks_DecodedLengthMismatch_FailsAndRemovesOutput(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	outPath := filepath.Join(nodeDir, archive.DataBlockName(".zst"))

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	payloads := [][]byte{[]byte("hello"), []byte("world")}
	chunkSize := int64(10)
	declaredTotalSize := int64(20)

	makeChunkFrames(t, chunkDir, payloads)

	err := volume.MergeBlockChunks(context.Background(), chunkDir, outPath, declaredTotalSize, chunkSize, ".zst")
	if err == nil {
		t.Fatal("expected an error for decoded length mismatch, got nil")
	}

	if !errors.Is(err, volume.ErrDecodedLengthMismatch) {
		t.Errorf("expected ErrDecodedLengthMismatch, got: %v", err)
	}

	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Error("merged output should be removed after a decoded-length mismatch")
	}

	// The chunk directory is removed unconditionally right after a
	// successful commit, before verification runs -- see the resume-on-
	// corruption doc comment on MergeBlockChunks: this forces the next run
	// to re-fetch every chunk from the exporter rather than re-merging the
	// same untrustworthy chunk files forever.
	if _, statErr := os.Stat(chunkDir); !os.IsNotExist(statErr) {
		t.Error("chunk dir should be removed after commit even when verification later fails")
	}
}

// TestMergeBlockChunks_DecodedLengthOverrun_FailsAndRemovesOutput mirrors the
// truncation case above for an over-sent merged stream: chunk 1's frame
// decodes to MORE raw bytes (10) than its slot's share of totalSize implies,
// so the two present chunks (satisfying the numChunks=2 presence check)
// decode to 15 total raw bytes against a declared totalSize of 10.
func TestMergeBlockChunks_DecodedLengthOverrun_FailsAndRemovesOutput(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	outPath := filepath.Join(nodeDir, archive.DataBlockName(".zst"))

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	payloads := [][]byte{[]byte("aaaaa"), []byte("bbbbbbbbbb")}
	chunkSize := int64(5)
	declaredTotalSize := int64(10)

	makeChunkFrames(t, chunkDir, payloads)

	err := volume.MergeBlockChunks(context.Background(), chunkDir, outPath, declaredTotalSize, chunkSize, ".zst")
	if err == nil {
		t.Fatal("expected an error for decoded length mismatch, got nil")
	}

	if !errors.Is(err, volume.ErrDecodedLengthMismatch) {
		t.Errorf("expected ErrDecodedLengthMismatch, got: %v", err)
	}

	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Error("merged output should be removed after a decoded-length mismatch")
	}
}

// TestMergeBlockChunks_LargeSyntheticTotal_DecodesToExactLength exercises the
// verification path against a multi-megabyte, multi-chunk volume (well
// beyond the few-byte payloads used elsewhere in this file) to prove the
// mechanism scales correctly. Both the merge and the verification decode
// stream through the codec's reader via io.Copy (verifyDecodedLength,
// decodeVolumeStream) rather than buffering the whole volume, so peak memory
// stays bounded regardless of totalSize.
func TestMergeBlockChunks_LargeSyntheticTotal_DecodesToExactLength(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	outPath := filepath.Join(nodeDir, archive.DataBlockName(".zst"))

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	const chunkSize = 1 * 1024 * 1024 // 1 MiB
	const numChunks = 5

	payloads := make([][]byte, numChunks)
	for i := range payloads {
		payloads[i] = bytes.Repeat([]byte{byte('A' + i)}, chunkSize)
	}

	totalSize := int64(chunkSize) * int64(numChunks)

	makeChunkFrames(t, chunkDir, payloads)

	if err := volume.MergeBlockChunks(context.Background(), chunkDir, outPath, totalSize, chunkSize, ".zst"); err != nil {
		t.Fatalf("MergeBlockChunks: %v", err)
	}

	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read merged block file: %v", err)
	}

	decoded := decodeZstdStream(t, raw)
	want := bytes.Join(payloads, nil)

	if !bytes.Equal(decoded, want) {
		t.Errorf("decoded content mismatch: got %d bytes, want %d bytes", len(decoded), len(want))
	}
}
