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
// chunk would otherwise fail against it) and any durable ".part" partial
// files (raw, uncompressed bytes — also not valid codec-frame content).
func listChunkFiles(t *testing.T, chunkDir string) []string {
	t.Helper()

	entries, err := os.ReadDir(chunkDir)
	require.NoError(t, err, "read chunk dir")

	names := make([]string, 0, len(entries))

	for _, e := range entries {
		if e.IsDir() || e.Name() == archive.ChunkMetaFileName || strings.HasSuffix(e.Name(), ".part") {
			continue
		}

		names = append(names, e.Name())
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

// TestDownloadBlockChunks_ProgressIsIncremental proves onProgress is credited
// as the Range GET body is actually read, not once after the whole chunk has
// been buffered. A payload well beyond any single net/http transport read
// (the client's default bufio.Reader is 4KiB) forces io.ReadAll to issue many
// Read calls; a test asserting only the summed total (like
// TestDownloadBlockChunks_SkipsExistingChunks) would pass against the
// pre-fix batched-single-call code just as well, so this test also asserts
// the call COUNT — the property that actually regressed.
func TestDownloadBlockChunks_ProgressIsIncremental(t *testing.T) {
	t.Parallel()

	const payloadSize = 1 << 20 // 1 MiB, far larger than any single transport Read.

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	srv := newBlockServer(t, payload)
	defer srv.Close()

	blockURL := srv.URL + "/api/v1/block"
	fetcher := exporter.NewFetcher(srv.Client())

	codec, err := compress.New("zstd", int(compress.LevelFastest))
	require.NoError(t, err)

	nodeDir := t.TempDir()
	totalSize := int64(len(payload))

	// chunkSize >= totalSize: exactly one chunk, so every recorded increment
	// belongs to the same downloadChunk call.
	const chunkSize = payloadSize

	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

	var (
		mu        sync.Mutex
		credited  int64
		callCount int
	)

	recordProgress := func(n int) {
		mu.Lock()
		credited += int64(n)
		callCount++
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

	assert.Equal(t, totalSize, credited, "summed increments must equal the chunk's raw length")
	assert.Greater(t, callCount, 1,
		"onProgress must be called more than once per chunk to prove incremental, not batched, reporting")
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

// TestDownloadBlockChunks_CorruptChunkMeta_PurgesAndRedownloads is the
// regression test for treating an unparseable chunks.meta as untrusted
// geometry: ensureChunkGeometry must purge the chunk dir and re-download,
// exactly like a geometry mismatch, rather than hard-aborting the whole
// volume when the sidecar exists but fails to parse (e.g. a torn write from
// a crash mid-WriteFileAtomic).
func TestDownloadBlockChunks_CorruptChunkMeta_PurgesAndRedownloads(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("0123456789"), 4) // 40 bytes

	var (
		mu           sync.Mutex
		requestCount int
	)

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

	// First (successful) run establishes a valid chunk dir at this geometry.
	require.NoError(t, volume.DownloadBlockChunks(
		context.Background(), slog.Default(), chunkDir, blockURL, totalSize,
		chunkSize, 1, fetcher, codec, nil))

	mu.Lock()
	firstRunRequests := requestCount
	mu.Unlock()
	require.Positive(t, firstRunRequests, "first run must fetch every chunk")

	// Simulate a torn write: overwrite the sidecar with garbage while leaving
	// the SAME-geometry chunk files in place. The guard must purge
	// unconditionally on an unparseable sidecar without inspecting the chunk
	// files at all — trusting them here would mask exactly the class of bug
	// this task fixes.
	wantChunks := int((totalSize + chunkSize - 1) / chunkSize)
	require.NoError(t, os.WriteFile(filepath.Join(chunkDir, archive.ChunkMetaFileName), []byte("{not valid json"), 0o644))

	// Resume with a corrupt sidecar must NOT abort the volume.
	err = volume.DownloadBlockChunks(
		context.Background(), slog.Default(), chunkDir, blockURL, totalSize,
		chunkSize, 1, fetcher, codec, nil)
	require.NoError(t, err, "a corrupt chunks.meta must be treated as untrusted geometry, not a hard error")

	mu.Lock()
	secondRunRequests := requestCount
	mu.Unlock()
	assert.Equal(t, wantChunks, secondRunRequests-firstRunRequests,
		"a corrupt sidecar must force every chunk to be re-fetched from byte zero")

	names := listChunkFiles(t, chunkDir)
	require.Len(t, names, wantChunks)

	// The purge-and-recreate path must leave a fresh, valid sidecar behind —
	// not just working chunk files — so a subsequent resume (before any
	// merge) can trust it. Checked BEFORE MergeBlockChunks: merging removes
	// the whole staging dir, chunks.meta included, on success.
	meta, found, metaErr := archive.ReadChunkMeta(chunkDir)
	require.NoError(t, metaErr)
	assert.True(t, found)
	assert.Equal(t, archive.ChunkMeta{ChunkSize: chunkSize, TotalSize: totalSize}, meta)

	outPath := filepath.Join(nodeDir, archive.DataBlockName(codec.Ext()))
	require.NoError(t, volume.MergeBlockChunks(context.Background(), chunkDir, outPath, totalSize, chunkSize, codec.Ext()))

	merged := decodeAll(t, outPath)
	assert.Equal(t, payload, merged, "merged output must be correct after recovering from a corrupt sidecar")
}

// errSimulatedInterrupt is returned by truncatingBody once its byte budget is
// exhausted, standing in for a real Ctrl-C/connection-drop mid-transfer
// without any real sleeps, timeouts, or network flakiness.
var errSimulatedInterrupt = errors.New("simulated interrupt: connection dropped mid-chunk")

// truncatingBody wraps an http response body and returns errSimulatedInterrupt
// after delivering exactly budget bytes, deterministically simulating an
// interrupt partway through a chunk's Range GET body.
type truncatingBody struct {
	r      io.ReadCloser
	budget int64
}

func (b *truncatingBody) Read(p []byte) (int, error) {
	if b.budget <= 0 {
		return 0, errSimulatedInterrupt
	}

	if int64(len(p)) > b.budget {
		p = p[:b.budget]
	}

	n, err := b.r.Read(p)
	b.budget -= int64(n)

	if err == nil && b.budget <= 0 {
		err = errSimulatedInterrupt
	}

	return n, err
}

func (b *truncatingBody) Close() error {
	return b.r.Close()
}

// recordingDoer wraps a real exporter.Doer, recording every request's Range
// header in call order and optionally truncating the response body of one
// designated call (cutOnCall, 1-based; 0 disables truncation) after
// cutBytes bytes to simulate a mid-transfer interrupt.
type recordingDoer struct {
	inner     exporter.Doer
	cutOnCall int
	cutBytes  int64

	mu     sync.Mutex
	calls  int
	ranges []string
}

func (d *recordingDoer) Do(req *http.Request) (*http.Response, error) {
	d.mu.Lock()
	d.calls++
	callIdx := d.calls
	d.ranges = append(d.ranges, req.Header.Get("Range"))
	d.mu.Unlock()

	resp, err := d.inner.Do(req)
	if err != nil {
		return resp, err
	}

	if callIdx == d.cutOnCall {
		resp.Body = &truncatingBody{r: resp.Body, budget: d.cutBytes}
	}

	return resp, nil
}

func (d *recordingDoer) recordedRanges() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	out := make([]string, len(d.ranges))
	copy(out, d.ranges)

	return out
}

// failIfCalledDoer fails the test immediately if Do is ever invoked, proving
// a code path made zero network requests.
type failIfCalledDoer struct {
	t *testing.T
}

func (d *failIfCalledDoer) Do(*http.Request) (*http.Response, error) {
	d.t.Helper()
	d.t.Fatal("unexpected HTTP request: expected zero network calls")

	return nil, nil
}

// TestDownloadBlockChunks_ResumesPartialChunkFromOffset is the regression test
// for the root-cause fix: an interrupted chunk must persist its raw bytes
// durably and resume from the exact persisted offset on the next run, instead
// of re-fetching the whole chunk from byte zero. Before the fix, downloadChunk
// buffered the entire raw chunk in memory via io.ReadAll and never touched
// disk until the whole chunk had arrived, so an interrupt anywhere inside a
// chunk discarded ALL of that chunk's progress.
func TestDownloadBlockChunks_ResumesPartialChunkFromOffset(t *testing.T) {
	t.Parallel()

	// A single chunk covering the whole payload, so there is exactly one
	// downloadChunk call per run and the interrupt/resume behavior is
	// unambiguous.
	payload := blockPayload // 25 bytes
	const cutBytes = 17     // interrupt partway through the payload (len(blockPayload) == 29)

	srv := newBlockServer(t, payload)
	defer srv.Close()

	blockURL := srv.URL + "/api/v1/block"

	doer := &recordingDoer{inner: srv.Client(), cutOnCall: 1, cutBytes: cutBytes}
	fetcher := exporter.NewFetcher(doer)

	codec, err := compress.New("zstd", int(compress.LevelFastest))
	require.NoError(t, err)

	nodeDir := t.TempDir()
	totalSize := int64(len(payload))
	chunkSize := totalSize // one chunk

	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	partPath := filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())+".part")

	// Run 1: interrupted mid-chunk.
	err = volume.DownloadBlockChunks(
		context.Background(), slog.Default(), chunkDir, blockURL, totalSize, chunkSize, 1, fetcher, codec, nil)
	require.Error(t, err, "interrupted run must return an error, not silently succeed")
	assert.ErrorIs(t, err, errSimulatedInterrupt)

	// The durable partial must hold exactly the bytes delivered before the
	// interrupt — proving progress survived on disk, not just in a discarded
	// in-memory buffer.
	partInfo, statErr := os.Stat(partPath)
	require.NoError(t, statErr, "durable partial file must exist after an interrupted chunk")
	assert.Equal(t, int64(cutBytes), partInfo.Size(), "durable partial must hold exactly the bytes delivered before the interrupt")

	// The chunk must NOT have finalized.
	finalPath := filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext()))
	_, statErr = os.Stat(finalPath)
	assert.True(t, os.IsNotExist(statErr), "chunk must not finalize when interrupted mid-download")

	// Run 2: resume, with truncation disabled (cutOnCall left pointing at a
	// call index run 2 will never reach).
	var (
		mu            sync.Mutex
		firstCredit   int
		creditsCalled int
	)

	onProgress := func(n int) {
		mu.Lock()
		defer mu.Unlock()

		creditsCalled++
		if creditsCalled == 1 {
			firstCredit = n
		}
	}

	err = volume.DownloadBlockChunks(
		context.Background(), slog.Default(), chunkDir, blockURL, totalSize, chunkSize, 1, fetcher, codec, onProgress)
	require.NoError(t, err, "resumed run must succeed")

	// The resumed run's request must start exactly at the persisted partial
	// length, never at byte 0 of an already-partially-downloaded chunk.
	ranges := doer.recordedRanges()
	require.Len(t, ranges, 2, "expected exactly one Range request per run")
	assert.Equal(t, fmt.Sprintf("bytes=0-%d", totalSize-1), ranges[0], "run 1 must request the whole chunk")
	assert.Equal(t, fmt.Sprintf("bytes=%d-%d", cutBytes, totalSize-1), ranges[1], "run 2 must resume from the persisted partial length, not byte 0")

	// onProgress's first credit on the resumed run must equal the persisted
	// partial length (the durable resume credit issued before any network call).
	assert.Equal(t, cutBytes, firstCredit, "first post-resume progress credit must equal the persisted partial length")

	// The durable partial must be cleaned up once the chunk finalizes.
	_, statErr = os.Stat(partPath)
	assert.True(t, os.IsNotExist(statErr), "durable partial must be removed once the chunk finalizes")

	// Final chunk must decode to the original payload, and the merged output
	// must be byte-identical to the source.
	decoded := decodeAll(t, finalPath)
	assert.Equal(t, payload, decoded, "finalized chunk must decode to the original payload")

	outPath := filepath.Join(nodeDir, archive.DataBlockName(codec.Ext()))
	require.NoError(t, volume.MergeBlockChunks(context.Background(), chunkDir, outPath, totalSize, chunkSize, codec.Ext()))

	merged := decodeAll(t, outPath)
	assert.Equal(t, payload, merged, "merged output must be byte-identical to the source after a resumed download")
}

// TestDownloadBlockChunks_FullPartFinalizesWithoutNetwork proves that a
// durable partial file whose size already equals the chunk's raw length is
// finalized (encoded + atomically written) with NO network request at all —
// e.g. a crash between finishing the raw download and finalizing the codec
// frame on a prior run must not re-fetch anything on resume.
func TestDownloadBlockChunks_FullPartFinalizesWithoutNetwork(t *testing.T) {
	t.Parallel()

	payload := blockPayload[:10] // exactly one 10-byte chunk

	codec, err := compress.New("zstd", int(compress.LevelFastest))
	require.NoError(t, err)

	nodeDir := t.TempDir()
	totalSize := int64(len(payload))
	chunkSize := totalSize // one chunk

	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	require.NoError(t, archive.EnsureDir(chunkDir))
	require.NoError(t, archive.WriteChunkMeta(chunkDir, archive.ChunkMeta{ChunkSize: chunkSize, TotalSize: totalSize}))

	partPath := filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())+".part")
	require.NoError(t, os.WriteFile(partPath, payload, 0o644))

	fetcher := exporter.NewFetcher(&failIfCalledDoer{t: t})

	var (
		mu       sync.Mutex
		credited int64
	)

	onProgress := func(n int) {
		mu.Lock()
		credited += int64(n)
		mu.Unlock()
	}

	err = volume.DownloadBlockChunks(
		context.Background(), slog.Default(), chunkDir, "http://unused.invalid/api/v1/block",
		totalSize, chunkSize, 1, fetcher, codec, onProgress)
	require.NoError(t, err, "a fully-downloaded durable partial must finalize without any network request")

	assert.Equal(t, totalSize, credited, "the full partial length must be credited to onProgress")

	finalPath := filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext()))
	decoded := decodeAll(t, finalPath)
	assert.Equal(t, payload, decoded, "finalized chunk must decode to the original payload")

	_, statErr := os.Stat(partPath)
	assert.True(t, os.IsNotExist(statErr), "durable partial must be removed once the chunk finalizes")
}

// TestDownloadBlockChunks_PartialSurvivesResumeScan proves the durable ".part"
// suffix (as opposed to ".tmp") is what protects an in-flight chunk's
// progress from archive.ScanNode's stale-tmp cleanup pass — see
// TestScanNode_BlockPartialAllPartFiles in the archive package for the
// resume-classification half of this guarantee.
func TestDownloadBlockChunks_PartialSurvivesResumeScan(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	id := archive.NodeIdentity{Kind: "VirtualDiskSnapshot", Name: "disk-inflight"}
	nodeDir := filepath.Join(parent, archive.NodeDirName(id.Kind, id.Name))
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	require.NoError(t, os.MkdirAll(chunkDir, 0o755))

	partPath := filepath.Join(chunkDir, archive.ChunkFileName(0, ".zst")+".part")
	require.NoError(t, os.WriteFile(partPath, []byte("in-flight raw bytes"), 0o644))

	assert.False(t, strings.HasSuffix(partPath, ".tmp"), "durable partial must not use the .tmp suffix removeTmpFiles sweeps")

	plan, err := archive.ScanNode(parent, id)
	require.NoError(t, err)

	assert.Equal(t, archive.NodeStateBlockPartial, plan.State)

	_, statErr := os.Stat(partPath)
	assert.NoError(t, statErr, "durable partial must survive ScanNode's stale-tmp cleanup")
}

// overservingBody wraps a genuine, correctly-ranged response body and, once
// the wrapped reader reaches a clean EOF, keeps yielding extra bytes instead
// of stopping — simulating a misbehaving server or proxy that over-sends
// beyond the promised range body, with no real network flakiness involved.
type overservingBody struct {
	r     io.ReadCloser
	extra []byte
}

func (b *overservingBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if err == io.EOF && len(b.extra) > 0 {
		m := copy(p[n:], b.extra)
		b.extra = b.extra[m:]

		return n + m, nil
	}

	return n, err
}

func (b *overservingBody) Close() error {
	return b.r.Close()
}

// overservingDoer wraps a Doer and appends extra bytes to every response
// body via overservingBody, standing in for a server/proxy that over-sends
// past a requested Range.
type overservingDoer struct {
	inner exporter.Doer
	extra []byte
}

func (d *overservingDoer) Do(req *http.Request) (*http.Response, error) {
	resp, err := d.inner.Do(req)
	if err != nil {
		return resp, err
	}

	resp.Body = &overservingBody{r: resp.Body, extra: d.extra}

	return resp, nil
}

// TestDownloadBlockChunks_ServerOverSends_BoundedAtRawLen is the regression
// test for the io.LimitReader bound in fetchChunkRaw: a server/proxy that
// keeps sending bytes past the requested range must never grow the durable
// ".part" file — and, transitively, the finalized chunk and merged output —
// past rawLen. Before the fix, io.Copy(sw, cr) had no upper bound and kept
// reading until the (over-sending) body itself returned EOF, so the extra
// bytes would have been written into the chunk and survived into the merged
// data.bin, decoding longer than totalSize.
func TestDownloadBlockChunks_ServerOverSends_BoundedAtRawLen(t *testing.T) {
	t.Parallel()

	payload := blockPayload

	srv := newBlockServer(t, payload)
	defer srv.Close()

	blockURL := srv.URL + "/api/v1/block"

	doer := &overservingDoer{inner: srv.Client(), extra: bytes.Repeat([]byte("Z"), 4096)}
	fetcher := exporter.NewFetcher(doer)

	codec, err := compress.New("zstd", int(compress.LevelFastest))
	require.NoError(t, err)

	nodeDir := t.TempDir()
	totalSize := int64(len(payload))
	chunkSize := totalSize // one chunk covering the whole payload

	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

	err = volume.DownloadBlockChunks(
		context.Background(), slog.Default(), chunkDir, blockURL, totalSize, chunkSize, 1, fetcher, codec, nil)
	require.NoError(t, err, "an over-sending server must not fail the download once the requested range is satisfied")

	partPath := filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())+".part")
	_, statErr := os.Stat(partPath)
	assert.True(t, os.IsNotExist(statErr), "durable partial must be removed once the (bounded) chunk finalizes")

	finalPath := filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext()))
	decoded := decodeAll(t, finalPath)
	assert.Equal(t, payload, decoded, "finalized chunk must not include any bytes the server over-sent past the range")

	outPath := filepath.Join(nodeDir, archive.DataBlockName(codec.Ext()))
	require.NoError(t, volume.MergeBlockChunks(context.Background(), chunkDir, outPath, totalSize, chunkSize, codec.Ext()))

	merged := decodeAll(t, outPath)
	assert.Equal(t, payload, merged, "merged output must decode to exactly totalSize bytes despite the server over-sending")
}

// cleanEOFBody wraps a response body and returns a clean io.EOF (nil error)
// after delivering exactly budget bytes, standing in for a server that
// legitimately ends the body early (e.g. a short Content-Length) rather than
// an abrupt connection drop. Unlike truncatingBody's errSimulatedInterrupt,
// io.Copy treats this as ordinary successful completion — so only the
// post-copy exact-size assertion in fetchChunkRaw, not the copy error, can
// catch this kind of short read.
type cleanEOFBody struct {
	r      io.ReadCloser
	budget int64
}

func (b *cleanEOFBody) Read(p []byte) (int, error) {
	if b.budget <= 0 {
		return 0, io.EOF
	}

	if int64(len(p)) > b.budget {
		p = p[:b.budget]
	}

	n, err := b.r.Read(p)
	b.budget -= int64(n)

	return n, err
}

func (b *cleanEOFBody) Close() error {
	return b.r.Close()
}

// shortSendDoer wraps a Doer and truncates the response body of one
// designated call (cutOnCall, 1-based) to cutBytes via cleanEOFBody, always
// ending that body with a clean EOF rather than an error.
type shortSendDoer struct {
	inner     exporter.Doer
	cutOnCall int
	cutBytes  int64

	mu    sync.Mutex
	calls int
}

func (d *shortSendDoer) Do(req *http.Request) (*http.Response, error) {
	d.mu.Lock()
	d.calls++
	callIdx := d.calls
	d.mu.Unlock()

	resp, err := d.inner.Do(req)
	if err != nil {
		return resp, err
	}

	if callIdx == d.cutOnCall {
		resp.Body = &cleanEOFBody{r: resp.Body, budget: d.cutBytes}
	}

	return resp, nil
}

// TestDownloadBlockChunks_ServerShortSends_ReturnsErrShortChunkRead is the
// regression test for the post-copy exact-size assertion: a server that ends
// a range response early with a CLEAN EOF (no read error at all — io.Copy
// alone reports this as success) must still fail the chunk with
// volume.ErrShortChunkRead rather than finalizing a truncated ".part" into a
// codec frame.
func TestDownloadBlockChunks_ServerShortSends_ReturnsErrShortChunkRead(t *testing.T) {
	t.Parallel()

	payload := blockPayload
	const cutBytes = 10 // strictly less than len(blockPayload)

	srv := newBlockServer(t, payload)
	defer srv.Close()

	blockURL := srv.URL + "/api/v1/block"

	doer := &shortSendDoer{inner: srv.Client(), cutOnCall: 1, cutBytes: cutBytes}
	fetcher := exporter.NewFetcher(doer)

	codec, err := compress.New("zstd", int(compress.LevelFastest))
	require.NoError(t, err)

	nodeDir := t.TempDir()
	totalSize := int64(len(payload))
	chunkSize := totalSize // one chunk, so the short-send is unambiguous

	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

	err = volume.DownloadBlockChunks(
		context.Background(), slog.Default(), chunkDir, blockURL, totalSize, chunkSize, 1, fetcher, codec, nil)
	require.Error(t, err, "a short-sending server must fail the download, not finalize a truncated chunk")
	assert.ErrorIs(t, err, volume.ErrShortChunkRead)

	finalPath := filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext()))
	_, statErr := os.Stat(finalPath)
	assert.True(t, os.IsNotExist(statErr), "chunk must not finalize on a short read")

	partPath := finalPath + ".part"
	partInfo, statErr := os.Stat(partPath)
	require.NoError(t, statErr, "the short durable partial must remain on disk for a future resume attempt")
	assert.Equal(t, int64(cutBytes), partInfo.Size(), "durable partial must hold exactly the bytes actually delivered")
}
