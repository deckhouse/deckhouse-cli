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
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

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

type cancelDuringVerificationContext struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *cancelDuringVerificationContext) Err() error {
	c.checks++
	if c.checks >= 2 {
		c.cancel()
	}

	return c.Context.Err()
}

func TestMergeBlockChunks_CancelledDuringVerificationPreservesChunks(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	outPath := filepath.Join(nodeDir, archive.DataBlockName(""))

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	payload := []byte("verification must be cancellable")
	chunkSize := int64(len(payload))
	makeChunkFramesWithCodec(t, chunkDir, [][]byte{payload}, "none")

	if err := archive.WriteChunkMeta(chunkDir, archive.ChunkMeta{ChunkSize: chunkSize, TotalSize: chunkSize}); err != nil {
		t.Fatalf("WriteChunkMeta: %v", err)
	}

	baseCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ctx := &cancelDuringVerificationContext{Context: baseCtx, cancel: cancel}
	err := volume.MergeBlockChunks(ctx, chunkDir, outPath, chunkSize, chunkSize, "")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("MergeBlockChunks error = %v; want context.Canceled", err)
	}

	if ctx.checks < 2 {
		t.Fatalf("context checked %d times; want cancellation during verification", ctx.checks)
	}

	if _, statErr := os.Stat(outPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("final output must remain unpublished after verification cancellation, Stat error: %v", statErr)
	}

	if _, statErr := os.Stat(outPath + ".tmp"); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("temporary output must be aborted after verification cancellation, Stat error: %v", statErr)
	}

	if _, statErr := os.Stat(filepath.Join(chunkDir, archive.ChunkFileName(0, ""))); statErr != nil {
		t.Fatalf("chunk must survive verification cancellation: %v", statErr)
	}

	if _, found, metaErr := archive.ReadChunkMeta(chunkDir); metaErr != nil || !found {
		t.Fatalf("chunks.meta must survive verification cancellation: found=%v err=%v", found, metaErr)
	}
}

// TestMergeBlockChunks_VerifiesDecodedLength_AllCodecs proves the new
// pre-publish decoded-length check succeeds (does not falsely reject) a
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

// TestMergeBlockChunks_DecodedLengthMismatch_FailsBeforePublish simulates
// a truncated/short merged stream: two 5-byte chunks decode to 10 raw bytes,
// but the declared totalSize (20) claims twice that. MergeBlockChunks must
// fail with ErrDecodedLengthMismatch before the final path is visible, abort
// the temporary file, and preserve the chunks and geometry for resume.
func TestMergeBlockChunks_DecodedLengthMismatch_FailsBeforePublish(t *testing.T) {
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

	if err := archive.WriteChunkMeta(chunkDir, archive.ChunkMeta{ChunkSize: chunkSize, TotalSize: declaredTotalSize}); err != nil {
		t.Fatalf("WriteChunkMeta: %v", err)
	}

	err := volume.MergeBlockChunks(context.Background(), chunkDir, outPath, declaredTotalSize, chunkSize, ".zst")
	if err == nil {
		t.Fatal("expected an error for decoded length mismatch, got nil")
	}

	if !errors.Is(err, volume.ErrDecodedLengthMismatch) {
		t.Errorf("expected ErrDecodedLengthMismatch, got: %v", err)
	}

	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Error("merged output must never be published after a decoded-length mismatch")
	}

	if _, statErr := os.Stat(outPath + ".tmp"); !os.IsNotExist(statErr) {
		t.Error("temporary merged output must be aborted after a decoded-length mismatch")
	}

	for i := range payloads {
		if _, statErr := os.Stat(filepath.Join(chunkDir, archive.ChunkFileName(i, ".zst"))); statErr != nil {
			t.Errorf("chunk %d must survive a decoded-length mismatch: %v", i, statErr)
		}
	}

	meta, found, metaErr := archive.ReadChunkMeta(chunkDir)
	if metaErr != nil || !found {
		t.Fatalf("chunks.meta must survive a decoded-length mismatch: found=%v err=%v", found, metaErr)
	}

	if meta != (archive.ChunkMeta{ChunkSize: chunkSize, TotalSize: declaredTotalSize}) {
		t.Errorf("chunks.meta changed: got %+v", meta)
	}
}

// TestMergeBlockChunks_DecodedLengthOverrun_FailsBeforePublish mirrors the
// truncation case above for an over-sent merged stream: chunk 1's frame
// decodes to MORE raw bytes (10) than its slot's share of totalSize implies,
// so the two present chunks (satisfying the numChunks=2 presence check)
// decode to 15 total raw bytes against a declared totalSize of 10.
func TestMergeBlockChunks_DecodedLengthOverrun_FailsBeforePublish(t *testing.T) {
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

func TestMergeBlockChunks_DecodeErrorFailsBeforePublish(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	outPath := filepath.Join(nodeDir, archive.DataBlockName(".zst"))

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	chunkPath := filepath.Join(chunkDir, archive.ChunkFileName(0, ".zst"))
	if err := os.WriteFile(chunkPath, []byte("not a zstd frame"), 0o600); err != nil {
		t.Fatalf("write corrupt chunk: %v", err)
	}

	if err := archive.WriteChunkMeta(chunkDir, archive.ChunkMeta{ChunkSize: 16, TotalSize: 16}); err != nil {
		t.Fatalf("WriteChunkMeta: %v", err)
	}

	err := volume.MergeBlockChunks(context.Background(), chunkDir, outPath, 16, 16, ".zst")
	if err == nil {
		t.Fatal("expected a decode error, got nil")
	}

	if _, statErr := os.Stat(outPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("final output must remain unpublished after a decode error, Stat error: %v", statErr)
	}

	if _, statErr := os.Stat(outPath + ".tmp"); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("temporary output must be aborted after a decode error, Stat error: %v", statErr)
	}

	if _, statErr := os.Stat(chunkPath); statErr != nil {
		t.Fatalf("corrupt chunk must remain available for resume handling: %v", statErr)
	}

	if _, found, metaErr := archive.ReadChunkMeta(chunkDir); metaErr != nil || !found {
		t.Fatalf("chunks.meta must survive a decode error: found=%v err=%v", found, metaErr)
	}
}

func TestMergeBlockChunks_CommitErrorPreservesChunksAndAbortsTemp(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	outPath := filepath.Join(nodeDir, archive.DataBlockName(""))

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	payload := []byte("verified bytes")
	chunkSize := int64(len(payload))
	makeChunkFramesWithCodec(t, chunkDir, [][]byte{payload}, "none")

	// A directory at the final path deterministically makes the atomic rename
	// fail after verification, without weakening production encapsulation with
	// a test-only commit hook.
	if err := os.Mkdir(outPath, 0o755); err != nil {
		t.Fatalf("Mkdir final-path blocker: %v", err)
	}

	err := volume.MergeBlockChunks(context.Background(), chunkDir, outPath, chunkSize, chunkSize, "")
	if err == nil {
		t.Fatal("expected a commit error, got nil")
	}

	info, statErr := os.Stat(outPath)
	if statErr != nil {
		t.Fatalf("stat final-path blocker: %v", statErr)
	}

	if !info.IsDir() {
		t.Fatal("failed commit must not replace the final-path blocker")
	}

	if _, statErr := os.Stat(outPath + ".tmp"); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("temporary output must be aborted after a commit error, Stat error: %v", statErr)
	}

	if _, statErr := os.Stat(filepath.Join(chunkDir, archive.ChunkFileName(0, ""))); statErr != nil {
		t.Fatalf("chunk must survive a commit error: %v", statErr)
	}
}

func TestMergeBlockChunks_RecoversPublishedDurabilityBeforeChunkCleanup(t *testing.T) {
	tests := []struct {
		name      string
		totalSize int64
		seed      func(*testing.T, string)
		want      []byte
	}{
		{
			name:      "ChunkedMerge",
			totalSize: int64(len("verified merged bytes")),
			seed: func(t *testing.T, chunkDir string) {
				t.Helper()

				makeChunkFramesWithCodec(t, chunkDir, [][]byte{[]byte("verified merged bytes")}, "none")
			},
			want: []byte("verified merged bytes"),
		},
		{
			name:      "ZeroSize",
			totalSize: 0,
			seed: func(*testing.T, string) {
			},
			want: []byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodeDir := t.TempDir()
			chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
			outPath := filepath.Join(nodeDir, archive.DataBlockName(""))
			syncErr := errors.New("block parent sync sentinel")

			require.NoError(t, os.MkdirAll(chunkDir, 0o755))
			tt.seed(t, chunkDir)

			baseCtx, cancel := context.WithCancel(context.Background())
			ctx := archive.WithDirectorySyncHook(baseCtx, func(path string, _ func() error) error {
				require.Equal(t, nodeDir, path)
				require.DirExists(t, chunkDir, "chunks must remain until directory durability is confirmed")

				cancel()

				return syncErr
			})

			err := volume.MergeBlockChunks(ctx, chunkDir, outPath, tt.totalSize, tt.totalSize, "")
			require.ErrorIs(t, err, syncErr)
			require.NotErrorIs(t, err, context.Canceled)
			require.Equal(t, archive.PublicationPublished, archive.CommitPublicationState(err))
			require.FileExists(t, outPath, "rename must leave the verified block published")
			require.NoFileExists(t, outPath+".tmp")
			require.DirExists(t, chunkDir, "post-rename sync failure must preserve recovery chunks")

			published, err := os.ReadFile(outPath)
			require.NoError(t, err)
			require.Equal(t, tt.want, published)

			require.NoError(t, archive.ConfirmFileDurability(context.Background(), outPath))
			require.DirExists(t, chunkDir, "durability confirmation alone must not consume recovery chunks")
			require.NoError(t, os.RemoveAll(chunkDir))
			require.NoDirExists(t, chunkDir)

			recovered, err := os.ReadFile(outPath)
			require.NoError(t, err)
			require.Equal(t, published, recovered, "durability retry must not rewrite published bytes")
		})
	}
}

func TestMergeBlockChunks_ZeroSizeCancellationPreservesOldFinal(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	outPath := filepath.Join(nodeDir, archive.DataBlockName(""))
	oldFinal := []byte("old final")

	require.NoError(t, os.MkdirAll(chunkDir, 0o755))
	require.NoError(t, os.WriteFile(outPath, oldFinal, 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := volume.MergeBlockChunks(ctx, chunkDir, outPath, 0, 0, "")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, archive.PublicationUnpublished, archive.CommitPublicationState(err))
	require.DirExists(t, chunkDir)
	require.NoFileExists(t, outPath+".tmp")

	got, err := os.ReadFile(outPath)
	require.NoError(t, err)
	require.Equal(t, oldFinal, got)
}

func TestMergeBlockChunks_RemoveErrorOccursAfterVerifiedCommit(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("permission bits are not enforced for root; cannot force os.RemoveAll to fail")
	}

	chunkParent := t.TempDir()
	chunkDir := filepath.Join(chunkParent, archive.BlockChunksDirName)
	outPath := filepath.Join(t.TempDir(), archive.DataBlockName(""))

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	payload := []byte("durable before cleanup")
	chunkSize := int64(len(payload))
	makeChunkFramesWithCodec(t, chunkDir, [][]byte{payload}, "none")

	if err := os.Chmod(chunkParent, 0o555); err != nil {
		t.Fatalf("make chunk parent read-only: %v", err)
	}

	t.Cleanup(func() { _ = os.Chmod(chunkParent, 0o755) })

	err := volume.MergeBlockChunks(context.Background(), chunkDir, outPath, chunkSize, chunkSize, "")
	if err == nil {
		t.Fatal("expected a chunk removal error, got nil")
	}

	got, readErr := os.ReadFile(outPath)
	if readErr != nil {
		t.Fatalf("verified final must be committed before chunk removal: %v", readErr)
	}

	if !bytes.Equal(got, payload) {
		t.Fatalf("committed output = %q; want %q", got, payload)
	}

	if _, statErr := os.Stat(outPath + ".tmp"); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("temporary output must be absent after verified commit, Stat error: %v", statErr)
	}

	if _, statErr := os.Stat(chunkDir); statErr != nil {
		t.Fatalf("failed removal must leave the chunk directory for pipeline cleanup: %v", statErr)
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
