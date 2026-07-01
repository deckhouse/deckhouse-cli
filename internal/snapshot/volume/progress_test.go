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
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
)

// progressCounter is a concurrency-safe accumulator for onProgress callbacks.
type progressCounter struct {
	mu    sync.Mutex
	total int
}

func (c *progressCounter) inc(n int) {
	c.mu.Lock()
	c.total += n
	c.mu.Unlock()
}

func (c *progressCounter) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.total
}

// progressCallCounter is a concurrency-safe accumulator that also tracks how
// many times onProgress was invoked, so a test can distinguish incremental
// per-Read reporting from a single one-shot call with the total.
type progressCallCounter struct {
	mu    sync.Mutex
	total int
	calls int
}

func (c *progressCallCounter) inc(n int) {
	c.mu.Lock()
	c.total += n
	c.calls++
	c.mu.Unlock()
}

func (c *progressCallCounter) get() (total, calls int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.total, c.calls
}

// TestDownloadBlockChunks_OnProgressTotalsBytes verifies that the sum of all onProgress
// increments equals the total raw bytes of the block volume for every supported
// combination of chunk size and worker count.
func TestDownloadBlockChunks_OnProgressTotalsBytes(t *testing.T) {
	t.Parallel()

	// t.Cleanup ensures the server stays alive until all parallel subtests complete.
	srv := newBlockServer(t, blockPayload)
	t.Cleanup(srv.Close)

	blockURL := srv.URL + "/api/v1/block"
	fetcher := exporter.NewFetcher(srv.Client())

	codec, err := compress.New("zstd", int(compress.LevelFastest))
	require.NoError(t, err)

	totalSize := int64(len(blockPayload))

	cases := []struct {
		name      string
		chunkSize int64
		workers   int
	}{
		{"single chunk single worker", totalSize, 1},
		{"multiple chunks single worker", 10, 1},
		{"multiple chunks parallel workers", 10, 2},
		{"chunk larger than payload", totalSize * 2, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nodeDir := t.TempDir()
			chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)

			var counter progressCounter

			err := volume.DownloadBlockChunks(
				context.Background(),
				slog.Default(),
				chunkDir,
				blockURL,
				totalSize,
				tc.chunkSize,
				tc.workers,
				fetcher,
				codec,
				counter.inc,
			)
			require.NoError(t, err)

			require.Equal(t, int(totalSize), counter.get(),
				"sum of onProgress increments must equal totalSize for chunk=%d workers=%d",
				tc.chunkSize, tc.workers)
		})
	}
}

// TestDownloadBlockChunks_NilOnProgress verifies that nil onProgress is a safe no-op:
// the download completes normally and no panic occurs.
func TestDownloadBlockChunks_NilOnProgress(t *testing.T) {
	t.Parallel()

	srv := newBlockServer(t, blockPayload)
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
		int64(len(blockPayload)),
		10,
		1,
		fetcher,
		codec,
		nil,
	)
	require.NoError(t, err, "nil onProgress must not cause an error or panic")
}

// TestDownloadFilesystemVolume_OnProgressTotalsBytes verifies that the sum of all
// onProgress increments equals the total raw content bytes served by the FS volume.
// Each call to onProgress carries the byte count of one downloaded file body.
func TestDownloadFilesystemVolume_OnProgressTotalsBytes(t *testing.T) {
	t.Parallel()

	srv, files := fsTestServer(t)

	filesURL := srv.URL + "/files/"
	fetcher := exporter.NewFetcher(srv.Client())

	codec, err := compress.New("none", 0)
	require.NoError(t, err)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	var wantTotal int

	for _, f := range files {
		wantTotal += len(f.content)
	}

	var counter progressCounter

	err = volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		tarPath,
		stagingDir,
		filesURL,
		2,
		fetcher,
		codec,
		nil,
		counter.inc,
	)
	require.NoError(t, err)

	require.Equal(t, wantTotal, counter.get(),
		"sum of onProgress increments must equal total file content bytes")
}

// TestDownloadFilesystemVolume_NilOnProgress verifies that nil onProgress is a safe
// no-op: the FS volume download completes normally and no panic occurs.
func TestDownloadFilesystemVolume_NilOnProgress(t *testing.T) {
	t.Parallel()

	srv, _ := fsTestServer(t)

	filesURL := srv.URL + "/files/"
	fetcher := exporter.NewFetcher(srv.Client())

	codec, err := compress.New("none", 0)
	require.NoError(t, err)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	err = volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		tarPath,
		stagingDir,
		filesURL,
		1,
		fetcher,
		codec,
		nil,
		nil,
	)
	require.NoError(t, err, "nil onProgress must not cause an error or panic")
}

// largeFSFileServer builds an httptest.Server serving a single filesystem
// volume file of size content bytes, deterministic and large enough that
// io.Copy inside codec.EncodeStream performs multiple underlying Read calls.
func largeFSFileServer(t *testing.T, content []byte) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()

	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w,
				`{"apiVersion":"v1","items":[`+
					`{"name":"big.bin","type":"file","uri":"big.bin","attributes":{}}`+
					`]}`)

		case "/files/big.bin":
			_, _ = w.Write(content)

		default:
			http.NotFound(w, r)
		}
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv
}

// TestDownloadFilesystemVolume_OnProgressIsIncremental verifies that a large
// single-file FS volume reports progress incrementally as bytes stream through
// countingReader.Read, not as one batched call after the whole file is staged.
// Without the per-Read reporting fix, onProgress is invoked exactly once with
// the full file size, which leaves a TTY progress bar at 0% for the entire
// transfer before jumping to 100%.
func TestDownloadFilesystemVolume_OnProgressIsIncremental(t *testing.T) {
	t.Parallel()

	const size = 512 * 1024 // 512 KiB: large enough to force multiple Read calls.

	content := bytes.Repeat([]byte("0123456789abcdef"), size/16)
	require.Len(t, content, size)

	srv := largeFSFileServer(t, content)

	filesURL := srv.URL + "/files/"
	fetcher := exporter.NewFetcher(srv.Client())

	// codec "none" avoids compression framing complexity when reasoning about
	// exact byte counts (mirrors TestDownloadFilesystemVolume_OnProgressTotalsBytes).
	codec, err := compress.New("none", 0)
	require.NoError(t, err)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	var counter progressCallCounter

	err = volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		tarPath,
		stagingDir,
		filesURL,
		1,
		fetcher,
		codec,
		nil,
		counter.inc,
	)
	require.NoError(t, err)

	total, calls := counter.get()

	require.Greater(t, calls, 1,
		"onProgress must be called more than once to prove incremental reporting")
	require.Equal(t, len(content), total,
		"sum of onProgress increments must equal the exact served file size")
}
