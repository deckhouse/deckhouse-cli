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
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

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
		0,
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
		0,
		fetcher,
		codec,
		nil,
		nil,
	)
	require.NoError(t, err, "nil onProgress must not cause an error or panic")
}

// fsTestServerWithSizes builds an httptest.Server serving the same two-file
// tree as fsTestServer (root.txt, subdir/nested.txt) but with a declared
// "size" attribute on each file item, matching the real data-exporter
// contract field consumed by parseItemSize/sumFileSizes. fsTestServer itself
// serves empty attributes, so it cannot exercise the declared-size resume
// credit this test needs.
func fsTestServerWithSizes(t *testing.T) (*httptest.Server, []fsTestFile) {
	t.Helper()

	files := []fsTestFile{
		{relPath: "root.txt", content: []byte("root-content")},
		{relPath: "subdir/nested.txt", content: []byte("nested-content")},
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w,
				fmt.Sprintf(`{"apiVersion":"v1","items":[`+
					`{"name":"root.txt","type":"file","uri":"root.txt","attributes":{"size":%d}},`+
					`{"name":"subdir","type":"dir","uri":"subdir/","attributes":{}}`+
					`]}`, len(files[0].content)))

		case "/files/subdir/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w,
				fmt.Sprintf(`{"apiVersion":"v1","items":[`+
					`{"name":"nested.txt","type":"file","uri":"subdir/nested.txt","attributes":{"size":%d}}`+
					`]}`, len(files[1].content)))

		case "/files/root.txt":
			// Both items declare a "size", so they download via the durable
			// chunked path (stageChunkedFile/DownloadBlockChunks), which issues
			// Range GETs — http.ServeContent (mirroring the real data-exporter's
			// sendFile idiom) is required to honor them.
			http.ServeContent(w, r, "root.txt", time.Time{}, bytes.NewReader(files[0].content))

		case "/files/subdir/nested.txt":
			http.ServeContent(w, r, "nested.txt", time.Time{}, bytes.NewReader(files[1].content))

		default:
			http.NotFound(w, r)
		}
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv, files
}

// TestDownloadFilesystemVolume_ResumeSkipReachesFullTotal verifies the
// resume-skip corollary of the incremental-progress invariant: when data.tar
// is absent but one file was already staged in a prior partial run, the skip
// branch in stageCompressedFile must still credit that file's declared size
// to onProgress. Without that credit, the denominator set once up front from
// sumFileSizes(items) (which includes the skipped file) could never be
// reached by a numerator that only counts freshly-downloaded bytes, so the
// byte progress bar would never reach 100% on a resumed run even though tar
// assembly completes successfully.
func TestDownloadFilesystemVolume_ResumeSkipReachesFullTotal(t *testing.T) {
	t.Parallel()

	srv, files := fsTestServerWithSizes(t)

	filesURL := srv.URL + "/files/"
	fetcher := exporter.NewFetcher(srv.Client())

	codec := mustCodec(t, "zstd")

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	require.NoError(t, os.MkdirAll(stagingDir, 0o755))

	// Simulate a prior partial run: root.txt was already staged (compressed
	// blob written under stagingDir) but data.tar was never assembled.
	// The staged bytes need not be a valid zstd stream for this assertion —
	// stageCompressedFile's skip branch never decodes them, it only checks
	// for the destination file's existence (see
	// TestDownloadFilesystemVolume_SkipsExistingCompressedStaged in
	// fs_test.go, which relies on the same property).
	sentinel := []byte("sentinel-not-server-content")
	preStaged := filepath.Join(stagingDir, "root.txt"+codec.Ext())
	require.NoError(t, os.WriteFile(preStaged, sentinel, 0o644))

	var wantTotal int

	for _, f := range files {
		wantTotal += len(f.content)
	}

	var counter progressCounter

	err := volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		tarPath,
		stagingDir,
		filesURL,
		2,
		0,
		fetcher,
		codec,
		nil,
		counter.inc,
	)
	require.NoError(t, err)

	require.Equal(t, wantTotal, counter.get(),
		"sum of onProgress increments must equal the total declared size across "+
			"ALL items (skipped-and-staged plus freshly-downloaded) so a partial "+
			"resume still reaches 100%%")

	// The skip must still have avoided re-download: the tar entry for the
	// pre-staged file carries the sentinel bytes, not freshly downloaded
	// content.
	f, err := os.Open(tarPath)
	require.NoError(t, err)

	defer func() { _ = f.Close() }()

	tr := tar.NewReader(f)

	var foundSentinel bool

	for {
		hdr, nextErr := tr.Next()
		if nextErr == io.EOF {
			break
		}

		require.NoError(t, nextErr)

		if hdr.Name != "root.txt"+codec.Ext() {
			continue
		}

		got, readErr := io.ReadAll(tr)
		require.NoError(t, readErr)
		require.Equal(t, sentinel, got, "pre-staged file must not be re-downloaded")

		foundSentinel = true
	}

	require.True(t, foundSentinel, "tar entry for pre-staged file not found")
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
		0,
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

// ── ScanBlockChunkProgress / ScanFSStagingProgress (resume-progress seeding) ──

// TestScanBlockChunkProgress_FinalizedAndPartialChunks verifies the core
// download-progress-seed-committed-bytes accounting: given a chunk dir with
// one finalized chunk and one durable ".part" prefix, the scan returns the
// finalized chunk's full raw length plus the partial's min(size, rawLen),
// without touching the network or the chunk files themselves.
func TestScanBlockChunkProgress_FinalizedAndPartialChunks(t *testing.T) {
	t.Parallel()

	const (
		chunkSize int64 = 100
		totalSize int64 = 300 // 3 chunks: 100, 100, 100
	)

	codec := mustCodec(t, "zstd")

	chunkDir := t.TempDir()
	require.NoError(t, archive.WriteChunkMeta(chunkDir, archive.ChunkMeta{ChunkSize: chunkSize, TotalSize: totalSize}))

	// Chunk 0: finalized (full raw length must be credited).
	frame, err := codec.EncodeFrame(bytes.Repeat([]byte("A"), int(chunkSize)))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())), frame, 0o644))

	// Chunk 1: a durable partial covering 37 of its 100 raw bytes. The
	// matching ".part.offset" sidecar records that prefix as fsync-proven —
	// exactly what fetchChunkRaw's own syncingWriter would have left behind
	// after a real partial download — since ScanBlockChunkProgress must
	// trust the identical (sidecar-backed) prefix fetchChunkRaw itself will
	// resume from (see the "two computations always agree exactly" doc on
	// ScanBlockChunkProgress).
	const partialBytes = 37
	partPath := filepath.Join(chunkDir, archive.ChunkFileName(1, codec.Ext())+".part")
	require.NoError(t, os.WriteFile(partPath, bytes.Repeat([]byte("B"), partialBytes), 0o644))
	require.NoError(t, os.WriteFile(partPath+".offset", []byte(fmt.Sprintf("%d", partialBytes)), 0o644))

	// Chunk 2: nothing on disk yet.

	committed, total, err := volume.ScanBlockChunkProgress(chunkDir, codec.Ext())
	require.NoError(t, err)
	require.Equal(t, totalSize, total, "total must come from chunks.meta")
	require.Equal(t, chunkSize+partialBytes, committed,
		"committed must equal chunk 0's full raw length plus chunk 1's partial length")
}

// TestScanBlockChunkProgress_NoGeometryYet verifies that a chunk dir with no
// (or a corrupt) chunks.meta yields (0, 0, nil) — there is nothing
// trustworthy to seed, mirroring ensureChunkGeometry's own "purge and
// re-download" treatment of the same condition.
func TestScanBlockChunkProgress_NoGeometryYet(t *testing.T) {
	t.Parallel()

	t.Run("missing chunk dir", func(t *testing.T) {
		t.Parallel()

		committed, total, err := volume.ScanBlockChunkProgress(filepath.Join(t.TempDir(), "absent"), ".zst")
		require.NoError(t, err)
		require.Zero(t, committed)
		require.Zero(t, total)
	})

	t.Run("chunk dir exists but chunks.meta is absent", func(t *testing.T) {
		t.Parallel()

		chunkDir := t.TempDir()

		committed, total, err := volume.ScanBlockChunkProgress(chunkDir, ".zst")
		require.NoError(t, err)
		require.Zero(t, committed)
		require.Zero(t, total)
	})

	t.Run("chunks.meta is corrupt", func(t *testing.T) {
		t.Parallel()

		chunkDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(chunkDir, archive.ChunkMetaFileName), []byte("{not json"), 0o644))

		committed, total, err := volume.ScanBlockChunkProgress(chunkDir, ".zst")
		require.NoError(t, err, "corrupt metadata must degrade to (0, 0, nil), not an error")
		require.Zero(t, committed)
		require.Zero(t, total)
	})
}

// TestScanFSStagingProgress_InProgressPerFileChunkDir verifies the
// filesystem-side accounting: a still-open per-file chunk directory (a large
// file interrupted mid-transfer, so its chunk dir has not been merged yet)
// contributes its committed bytes via the identical ScanBlockChunkProgress
// formula.
func TestScanFSStagingProgress_InProgressPerFileChunkDir(t *testing.T) {
	t.Parallel()

	const (
		chunkSize int64 = 100
		totalSize int64 = 250 // 3 chunks: 100, 100, 50
	)

	codec := mustCodec(t, "zstd")

	stagingDir := t.TempDir()
	fileChunkDir := filepath.Join(stagingDir, archive.FsFileChunksDirName("big.bin", codec.Ext()))
	require.NoError(t, os.MkdirAll(fileChunkDir, 0o755))
	require.NoError(t, archive.WriteChunkMeta(fileChunkDir, archive.ChunkMeta{ChunkSize: chunkSize, TotalSize: totalSize}))

	// Chunk 0: finalized.
	frame, err := codec.EncodeFrame(bytes.Repeat([]byte("A"), int(chunkSize)))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(fileChunkDir, archive.ChunkFileName(0, codec.Ext())), frame, 0o644))

	// Chunk 1: a durable partial covering 42 of its 100 raw bytes, with its
	// matching ".part.offset" sidecar recording that prefix as fsync-proven
	// (see the analogous note in TestScanBlockChunkProgress_FinalizedAndPartialChunks).
	const partialBytes = 42
	partPath := filepath.Join(fileChunkDir, archive.ChunkFileName(1, codec.Ext())+".part")
	require.NoError(t, os.WriteFile(partPath, bytes.Repeat([]byte("B"), partialBytes), 0o644))
	require.NoError(t, os.WriteFile(partPath+".offset", []byte(fmt.Sprintf("%d", partialBytes)), 0o644))

	committed, err := volume.ScanFSStagingProgress(stagingDir, codec.Ext())
	require.NoError(t, err)
	require.Equal(t, chunkSize+partialBytes, committed)
}

// TestScanFSStagingProgress_CompletedFlatFileNotCounted pins the documented
// trade-off: a file that has ALREADY been fully staged (its chunk dir was
// already merged away into a flat <relPath><ext> blob by MergeBlockChunks)
// contributes NOTHING to the scan, because its original raw declared size is
// not recoverable from disk once chunks.meta — the only place that size was
// ever recorded — is gone. This must stay zero so the pipeline never
// double-counts against stageCompressedFile's own resume-skip credit for the
// same file (see ScanFSStagingProgress's doc comment).
func TestScanFSStagingProgress_CompletedFlatFileNotCounted(t *testing.T) {
	t.Parallel()

	codec := mustCodec(t, "zstd")

	stagingDir := t.TempDir()

	frame, err := codec.EncodeFrame([]byte("already fully staged content"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(stagingDir, "small.txt"+codec.Ext()), frame, 0o644))

	committed, err := volume.ScanFSStagingProgress(stagingDir, codec.Ext())
	require.NoError(t, err)
	require.Zero(t, committed, "a fully-staged flat file must not be counted (its raw size is unrecoverable)")
}

// TestScanFSStagingProgress_MissingStagingDir verifies the fresh-download case:
// a staging dir that does not exist yet yields (0, nil).
func TestScanFSStagingProgress_MissingStagingDir(t *testing.T) {
	t.Parallel()

	committed, err := volume.ScanFSStagingProgress(filepath.Join(t.TempDir(), "absent"), ".zst")
	require.NoError(t, err)
	require.Zero(t, committed)
}
