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
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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

// listChunkFiles returns the sorted chunk_NNNNN[.<ext>] file names in the
// chunk directory, excluding the archive.ChunkMetaFileName geometry sidecar
// (chunks.meta is not a chunk and is never valid codec-frame content, so
// every existing caller that decodes each returned name as a compressed
// chunk would otherwise fail against it).
func listChunkFiles(t *testing.T, chunkDir string) []string {
	t.Helper()

	entries, err := os.ReadDir(chunkDir)
	require.NoError(t, err, "read chunk dir")

	names := make([]string, 0, len(entries))

	for _, e := range entries {
		if !e.IsDir() && e.Name() != archive.ChunkMetaFileName {
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

	codec, err := compress.New("zstd", int(compress.LevelFastest))
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
		codec,
		nil,
	)
	require.NoError(t, err)

	names := listChunkFiles(t, chunkDir)

	wantChunks := int((totalSize + chunkSize - 1) / chunkSize) // = 3
	assert.Len(t, names, wantChunks, "expected %d chunk files", wantChunks)

	for i, name := range names {
		assert.Equal(t, archive.ChunkFileName(i, codec.Ext()), name, "chunk %d filename", i)
	}
}

func TestDownloadBlockChunks_ConcatDecodesCorrectly(t *testing.T) {
	t.Parallel()

	srv := newBlockServer(t, blockPayload)
	defer srv.Close()

	blockURL := srv.URL + "/api/v1/block"
	fetcher := exporter.NewFetcher(srv.Client())

	codec, err := compress.New("zstd", int(compress.LevelFastest))
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
		codec,
		nil,
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

	codec, err := compress.New("zstd", int(compress.LevelFastest))
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
		codec,
		nil,
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

	// Second download — all chunks already exist. A recording onProgress proves
	// the resume-skip path still credits each skipped chunk's raw length: the
	// bar must be able to reach totalSize even when every chunk is already on
	// disk, matching the already-fixed filesystem-path behavior
	// (stageCompressedFile). Before the fix, the skip branch never called
	// onProgress at all, so this sum would be 0.
	var (
		mu       sync.Mutex
		credited int64
	)

	recordProgress := func(n int) {
		mu.Lock()
		credited += int64(n)
		mu.Unlock()
	}

	err = volume.DownloadBlockChunks(
		context.Background(),
		slog.Default(),
		chunkDir,
		blockURL,
		totalSize,
		chunkSize,
		1,
		fetcher,
		codec,
		recordProgress,
	)
	require.NoError(t, err)

	assert.Equal(t, totalSize, credited,
		"resume skip must credit onProgress with each chunk's raw length so the sum reaches totalSize")

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

	codec, err := compress.New("zstd", int(compress.LevelFastest))
	require.NoError(t, err)

	nodeDir := t.TempDir()
	totalSize := int64(len(blockPayload))

	const chunkSize = 10

	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	require.NoError(t, os.MkdirAll(chunkDir, 0o755))

	// Place a stale .tmp for chunk 0 to simulate a previous aborted attempt.
	staleFile := filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())+".tmp")
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
		codec,
		nil,
	)
	require.NoError(t, err)

	// Stale .tmp must be gone.
	_, statErr := os.Stat(staleFile)
	assert.True(t, os.IsNotExist(statErr), "stale tmp should be removed")

	// Final chunk file must exist.
	finalFile := filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext()))
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

	codec, err := compress.New("zstd", int(compress.LevelFastest))
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
		codec,
		nil,
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

// TestDownloadBlockChunks_ChunkSizeChanged_PurgesStaleChunks is the regression
// test for the silent-corruption bug fixed by the chunks.meta geometry guard:
// resuming a block-volume download with a DIFFERENT --chunk-size than the
// interrupted run must discard every chunk from the OLD geometry and
// re-fetch the whole volume at the NEW stride, rather than silently reusing a
// same-indexed chunk file that in fact covers the wrong byte range (chunk
// k's range is derived purely from chunkSize/totalSize — see downloadChunk —
// and is not recorded anywhere in the chunk's filename). Before the fix, both
// directions below produced a corrupt merged file (duplicated bytes for a
// smaller new chunk size, truncated bytes for a larger one) that was still
// accepted as complete.
func TestDownloadBlockChunks_ChunkSizeChanged_PurgesStaleChunks(t *testing.T) {
	t.Parallel()

	// 40 bytes, evenly divisible by both 10 and 20, so any leftover remainder
	// in the assertions below is attributable only to the chunk-size change,
	// not to boundary rounding.
	payload := bytes.Repeat([]byte("0123456789"), 4)

	testCases := []struct {
		name        string
		firstChunk  int64
		secondChunk int64
	}{
		{name: "smaller chunk size on resume", firstChunk: 20, secondChunk: 10},
		{name: "larger chunk size on resume", firstChunk: 10, secondChunk: 20},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newBlockServer(t, payload)
			defer srv.Close()

			blockURL := srv.URL + "/api/v1/block"
			fetcher := exporter.NewFetcher(srv.Client())

			codec, err := compress.New("zstd", int(compress.LevelFastest))
			require.NoError(t, err)

			nodeDir := t.TempDir()
			totalSize := int64(len(payload))
			chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

			// First (interrupted) run at the old geometry: populates chunkDir
			// with chunks that a naive index-only resume would otherwise trust.
			require.NoError(t, volume.DownloadBlockChunks(
				context.Background(), slog.Default(), chunkDir, blockURL, totalSize,
				tc.firstChunk, 1, fetcher, codec, nil))

			// Resume at a DIFFERENT chunk size. The geometry guard must purge
			// the stale directory and re-fetch every chunk at the new stride.
			require.NoError(t, volume.DownloadBlockChunks(
				context.Background(), slog.Default(), chunkDir, blockURL, totalSize,
				tc.secondChunk, 1, fetcher, codec, nil))

			secondNames := listChunkFiles(t, chunkDir)

			wantChunks := int((totalSize + tc.secondChunk - 1) / tc.secondChunk)
			require.Len(t, secondNames, wantChunks, "chunk file count must match the NEW geometry")

			// Decode every chunk and assert it covers exactly the byte range the
			// NEW geometry expects. A stale chunk kept from the old geometry
			// would decode to the WRONG slice even at a shared index (e.g.
			// chunk_00000 exists under both a 10-byte and a 20-byte geometry,
			// but covers a different range in each) — a presence-only check
			// would miss that, which is exactly how the original bug slipped
			// through MergeBlockChunks' presence-only verification.
			for _, name := range secondNames {
				idx, parseErr := chunkIndexFromName(name, codec.Ext())
				require.NoError(t, parseErr, "parse chunk index from %s", name)

				decoded := decodeAll(t, filepath.Join(chunkDir, name))
				start := int64(idx) * tc.secondChunk
				end := min(start+tc.secondChunk, totalSize)
				assert.Equal(t, payload[start:end], decoded, "chunk %d content must match the NEW geometry", idx)
			}

			outPath := filepath.Join(nodeDir, archive.DataBlockName(codec.Ext()))
			require.NoError(t, volume.MergeBlockChunks(context.Background(), chunkDir, outPath, totalSize, tc.secondChunk, codec.Ext()))

			merged := decodeAll(t, outPath)
			assert.Equal(t, payload, merged, "merged block file must be byte-identical to the original after a chunk-size change")
		})
	}

	// Contrast case: resuming with the SAME chunk size must NOT discard or
	// re-fetch any chunk (mirrors TestDownloadBlockChunks_SkipsExistingChunks;
	// this subtest only proves the geometry guard itself does not regress
	// that path now that it runs on every call).
	t.Run("same chunk size on resume does not discard chunks", func(t *testing.T) {
		t.Parallel()

		var requestCount int

		var mu sync.Mutex

		mux := http.NewServeMux()
		mux.HandleFunc("/api/v1/block", func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			requestCount++
			mu.Unlock()

			w.Header().Set("Content-Type", "application/octet-stream")
			http.ServeContent(w, r, "data.img", time.Time{}, bytes.NewReader(payload))
		})

		srv := httptest.NewServer(mux)
		defer srv.Close()

		blockURL := srv.URL + "/api/v1/block"
		fetcher := exporter.NewFetcher(srv.Client())

		codec, err := compress.New("zstd", int(compress.LevelFastest))
		require.NoError(t, err)

		nodeDir := t.TempDir()
		totalSize := int64(len(payload))
		const chunkSize = 10

		chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

		require.NoError(t, volume.DownloadBlockChunks(
			context.Background(), slog.Default(), chunkDir, blockURL, totalSize,
			chunkSize, 1, fetcher, codec, nil))

		mu.Lock()
		firstRunRequests := requestCount
		mu.Unlock()
		require.Positive(t, firstRunRequests, "first run must fetch every chunk")

		require.NoError(t, volume.DownloadBlockChunks(
			context.Background(), slog.Default(), chunkDir, blockURL, totalSize,
			chunkSize, 1, fetcher, codec, nil))

		mu.Lock()
		secondRunRequests := requestCount
		mu.Unlock()
		assert.Equal(t, firstRunRequests, secondRunRequests,
			"a same-chunk-size resume must not re-fetch any chunk from the server")
	})
}

// chunkIndexFromName parses the zero-padded index back out of a
// archive.ChunkFileName(idx, ext) result, so tests can assert per-chunk byte
// ranges without hand-tracking index-to-file ordering.
func chunkIndexFromName(name, ext string) (int, error) {
	trimmed := strings.TrimSuffix(name, ext)
	trimmed = strings.TrimPrefix(trimmed, "chunk_")

	var idx int

	_, err := fmt.Sscanf(trimmed, "%05d", &idx)

	return idx, err
}
