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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
)

// makeChunkFrames writes N zstd frames (one per chunk) into chunkDir and
// returns the raw byte slices so callers can reconstruct the expected output.
func makeChunkFrames(t *testing.T, chunkDir string, payloads [][]byte) {
	t.Helper()

	enc, err := compress.NewEncoder(compress.LevelDefault)
	if err != nil {
		t.Fatalf("compress.NewEncoder: %v", err)
	}

	for i, payload := range payloads {
		frame, err := enc.EncodeFrame(payload)
		if err != nil {
			t.Fatalf("EncodeFrame chunk %d: %v", i, err)
		}

		p := filepath.Join(chunkDir, archive.ChunkFileName(i))
		if err := os.WriteFile(p, frame, 0o600); err != nil {
			t.Fatalf("write chunk %d: %v", i, err)
		}
	}
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

	if err := volume.MergeBlockChunks(nodeDir, totalSize, chunkSize); err != nil {
		t.Fatalf("MergeBlockChunks: %v", err)
	}

	// data.img.zst must exist.
	finalPath := filepath.Join(nodeDir, archive.DataBlockName)
	raw, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("read data.img.zst: %v", err)
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

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	payloads := [][]byte{[]byte("hello"), []byte("world")}
	chunkSize := int64(5)
	totalSize := int64(10)

	makeChunkFrames(t, chunkDir, payloads)

	if err := volume.MergeBlockChunks(nodeDir, totalSize, chunkSize); err != nil {
		t.Fatalf("MergeBlockChunks: %v", err)
	}

	if _, err := os.Stat(chunkDir); !os.IsNotExist(err) {
		t.Errorf("chunk dir should be removed after merge, but Stat returned: %v", err)
	}
}

func TestMergeBlockChunks_MissingChunkErrors(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write chunks 0 and 2 only; chunk 1 is missing (gap).
	enc, err := compress.NewEncoder(compress.LevelDefault)
	if err != nil {
		t.Fatal(err)
	}

	for _, idx := range []int{0, 2} {
		frame, err := enc.EncodeFrame([]byte("data"))
		if err != nil {
			t.Fatal(err)
		}

		p := filepath.Join(chunkDir, archive.ChunkFileName(idx))
		if err := os.WriteFile(p, frame, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	chunkSize := int64(4)
	totalSize := int64(12)

	err = volume.MergeBlockChunks(nodeDir, totalSize, chunkSize)
	if err == nil {
		t.Fatal("expected error for missing chunk, got nil")
	}

	if !errors.Is(err, volume.ErrMissingChunk) {
		t.Errorf("expected ErrMissingChunk, got: %v", err)
	}

	// data.img.zst must NOT have been written.
	finalPath := filepath.Join(nodeDir, archive.DataBlockName)
	if _, statErr := os.Stat(finalPath); !os.IsNotExist(statErr) {
		t.Error("data.img.zst should not exist when merge fails due to missing chunk")
	}
}

func TestMergeBlockChunks_DefaultChunkSize(t *testing.T) {
	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// One chunk smaller than DefaultChunkSize.
	payload := bytes.Repeat([]byte("x"), 1024)
	totalSize := int64(len(payload))

	makeChunkFrames(t, chunkDir, [][]byte{payload})

	// Pass chunkSize=0 to trigger DefaultChunkSize fallback.
	if err := volume.MergeBlockChunks(nodeDir, totalSize, 0); err != nil {
		t.Fatalf("MergeBlockChunks with default chunk size: %v", err)
	}

	finalPath := filepath.Join(nodeDir, archive.DataBlockName)
	raw, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("read data.img.zst: %v", err)
	}

	decoded := decodeZstdStream(t, raw)
	if !bytes.Equal(decoded, payload) {
		t.Errorf("decoded content mismatch (length %d vs %d)", len(decoded), len(payload))
	}
}
