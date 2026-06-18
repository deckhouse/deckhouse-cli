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
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
)

// blockPayload is 25 bytes — deliberately not a multiple of the test chunk size
// so the last chunk is smaller than the others.
var blockPayload = []byte("ABCDE FGHIJ KLMNO PQRST UVWXY")

func newBlockServer(t *testing.T, data []byte) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/block", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, "data.img", time.Time{}, bytes.NewReader(data))
	})

	return httptest.NewServer(mux)
}

// decodeAll decodes a zstd-compressed file and returns the plaintext bytes.
func decodeAll(t *testing.T, path string) []byte {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err, "open %s", path)

	defer func() { _ = f.Close() }()

	dec, err := zstd.NewReader(f)
	require.NoError(t, err, "new zstd reader")

	defer dec.Close()

	out, err := io.ReadAll(dec)
	require.NoError(t, err, "decode %s", path)

	return out
}

// listChunkFiles returns the sorted file names in the chunk directory.
func listChunkFiles(t *testing.T, chunkDir string) []string {
	t.Helper()

	entries, err := os.ReadDir(chunkDir)
	require.NoError(t, err, "read chunk dir")

	names := make([]string, 0, len(entries))

	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}

	sort.Strings(names)

	return names
}

func TestDownloadBlockChunks_Basic(t *testing.T) {
	t.Parallel()

	srv := newBlockServer(t, blockPayload)
	defer srv.Close()

	blockURL := srv.URL + "/api/v1/block"
	fetcher := exporter.NewFetcher(srv.Client())

	enc, err := compress.NewEncoder(compress.LevelFastest)
	require.NoError(t, err)

	nodeDir := t.TempDir()
	totalSize := int64(len(blockPayload))

	// Use a small chunk size so we get multiple chunks.
	// 10-byte chunks: chunks 0-1 are 10 bytes each, chunk 2 is 9 bytes → 3 chunks.
	const chunkSize = 10

	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

	err = volume.DownloadBlockChunks(
		context.Background(),
		slog.Default(),
		chunkDir,
		blockURL,
		totalSize,
		chunkSize,
		2,
		fetcher,
		enc,
	)
	require.NoError(t, err)

	names := listChunkFiles(t, chunkDir)

	wantChunks := int((totalSize + chunkSize - 1) / chunkSize) // = 3
	assert.Len(t, names, wantChunks, "expected %d chunk files", wantChunks)

	for i, name := range names {
		assert.Equal(t, archive.ChunkFileName(i), name, "chunk %d filename", i)
	}
}

func TestDownloadBlockChunks_ConcatDecodesCorrectly(t *testing.T) {
	t.Parallel()

	srv := newBlockServer(t, blockPayload)
	defer srv.Close()

	blockURL := srv.URL + "/api/v1/block"
	fetcher := exporter.NewFetcher(srv.Client())

	enc, err := compress.NewEncoder(compress.LevelFastest)
	require.NoError(t, err)

	nodeDir := t.TempDir()
	totalSize := int64(len(blockPayload))

	const chunkSize = 10

	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

	err = volume.DownloadBlockChunks(
		context.Background(),
		slog.Default(),
		chunkDir,
		blockURL,
		totalSize,
		chunkSize,
		1,
		fetcher,
		enc,
	)
	require.NoError(t, err)

	names := listChunkFiles(t, chunkDir)

	// Decode each chunk and concatenate; result must equal original payload.
	var reconstructed []byte

	for _, name := range names {
		decoded := decodeAll(t, filepath.Join(chunkDir, name))
		reconstructed = append(reconstructed, decoded...)
	}

	assert.Equal(t, blockPayload, reconstructed, "reconstructed payload mismatch")
}

func TestDownloadBlockChunks_SkipsExistingChunks(t *testing.T) {
	t.Parallel()

	srv := newBlockServer(t, blockPayload)
	defer srv.Close()

	blockURL := srv.URL + "/api/v1/block"
	fetcher := exporter.NewFetcher(srv.Client())

	enc, err := compress.NewEncoder(compress.LevelFastest)
	require.NoError(t, err)

	nodeDir := t.TempDir()
	totalSize := int64(len(blockPayload))

	const chunkSize = 10

	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

	// First download.
	err = volume.DownloadBlockChunks(
		context.Background(),
		slog.Default(),
		chunkDir,
		blockURL,
		totalSize,
		chunkSize,
		1,
		fetcher,
		enc,
	)
	require.NoError(t, err)

	// Record modification times of all chunk files.
	names := listChunkFiles(t, chunkDir)
	mtimes := make(map[string]time.Time, len(names))

	for _, name := range names {
		info, err := os.Stat(filepath.Join(chunkDir, name))
		require.NoError(t, err)

		mtimes[name] = info.ModTime()
	}

	// Second download — all chunks already exist.
	err = volume.DownloadBlockChunks(
		context.Background(),
		slog.Default(),
		chunkDir,
		blockURL,
		totalSize,
		chunkSize,
		1,
		fetcher,
		enc,
	)
	require.NoError(t, err)

	// Files must not have been modified (skipped, not re-fetched).
	for _, name := range names {
		info, err := os.Stat(filepath.Join(chunkDir, name))
		require.NoError(t, err)

		assert.Equal(t, mtimes[name], info.ModTime(), "chunk %s was re-written on second run", name)
	}
}

func TestDownloadBlockChunks_CleansStaleTemp(t *testing.T) {
	t.Parallel()

	srv := newBlockServer(t, blockPayload)
	defer srv.Close()

	blockURL := srv.URL + "/api/v1/block"
	fetcher := exporter.NewFetcher(srv.Client())

	enc, err := compress.NewEncoder(compress.LevelFastest)
	require.NoError(t, err)

	nodeDir := t.TempDir()
	totalSize := int64(len(blockPayload))

	const chunkSize = 10

	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	require.NoError(t, os.MkdirAll(chunkDir, 0o755))

	// Place a stale .tmp for chunk 0 to simulate a previous aborted attempt.
	staleFile := filepath.Join(chunkDir, archive.ChunkFileName(0)+".tmp")
	require.NoError(t, os.WriteFile(staleFile, []byte("stale"), 0o644))

	// Download should remove the stale tmp and succeed.
	err = volume.DownloadBlockChunks(
		context.Background(),
		slog.Default(),
		chunkDir,
		blockURL,
		totalSize,
		chunkSize,
		1,
		fetcher,
		enc,
	)
	require.NoError(t, err)

	// Stale .tmp must be gone.
	_, statErr := os.Stat(staleFile)
	assert.True(t, os.IsNotExist(statErr), "stale tmp should be removed")

	// Final chunk file must exist.
	finalFile := filepath.Join(chunkDir, archive.ChunkFileName(0))
	_, err = os.Stat(finalFile)
	assert.NoError(t, err, "final chunk file must exist")
}

func TestDownloadBlockChunks_ChunkBoundaries(t *testing.T) {
	t.Parallel()

	// Use a payload that is exactly 3× chunkSize to verify exact boundaries.
	const chunkSize = 5
	payload := []byte("ABCDEFGHIJKLMNO") // 15 bytes → 3 chunks of 5

	srv := newBlockServer(t, payload)
	defer srv.Close()

	blockURL := srv.URL + "/api/v1/block"
	fetcher := exporter.NewFetcher(srv.Client())

	enc, err := compress.NewEncoder(compress.LevelFastest)
	require.NoError(t, err)

	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

	err = volume.DownloadBlockChunks(
		context.Background(),
		slog.Default(),
		chunkDir,
		blockURL,
		int64(len(payload)),
		chunkSize,
		1,
		fetcher,
		enc,
	)
	require.NoError(t, err)

	names := listChunkFiles(t, chunkDir)

	assert.Len(t, names, 3, "expected 3 chunks")

	// Verify each chunk decodes to the correct slice.
	wantSlices := [][]byte{
		payload[0:5],
		payload[5:10],
		payload[10:15],
	}

	for i, name := range names {
		got := decodeAll(t, filepath.Join(chunkDir, name))
		assert.Equal(t, wantSlices[i], got, "chunk %d content mismatch", i)
	}
}
