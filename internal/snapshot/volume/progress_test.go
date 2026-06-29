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
	"context"
	"log/slog"
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
	)
	require.NoError(t, err, "nil onProgress must not cause an error or panic")
}
