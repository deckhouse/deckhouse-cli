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
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
)

// fsTestFile holds test fixture data for one file served by the fs test server.
type fsTestFile struct {
	relPath string
	content []byte
}

// fsTestServer builds an httptest.Server that serves a two-level filesystem tree:
//
//	<root>/
//	  root.txt          → "root-content"
//	  subdir/
//	    nested.txt      → "nested-content"
//	  symlink.txt       → type="link" (no body served)
//
// The server handles the listing and file-download endpoints.
func fsTestServer(t *testing.T) (*httptest.Server, []fsTestFile) {
	t.Helper()

	files := []fsTestFile{
		{relPath: "root.txt", content: []byte("root-content")},
		{relPath: "subdir/nested.txt", content: []byte("nested-content")},
	}

	mux := http.NewServeMux()

	// Root listing: URIs are root-relative paths (real data-exporter contract).
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w,
				`{"apiVersion":"v1","items":[`+
					`{"name":"root.txt","type":"file","uri":"root.txt","attributes":{}},`+
					`{"name":"subdir","type":"dir","uri":"subdir/","attributes":{}},`+
					`{"name":"symlink.txt","type":"link","uri":"","targetPath":"/other","attributes":{}}`+
					`]}`)

		case "/files/subdir/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w,
				`{"apiVersion":"v1","items":[`+
					`{"name":"nested.txt","type":"file","uri":"subdir/nested.txt","attributes":{}}`+
					`]}`)

		case "/files/root.txt":
			_, _ = w.Write(files[0].content)

		case "/files/subdir/nested.txt":
			_, _ = w.Write(files[1].content)

		default:
			http.NotFound(w, r)
		}
	})

	srv := httptest.NewServer(mux)

	t.Cleanup(srv.Close)

	return srv, files
}

func newFSFetcher(srv *httptest.Server) *exporter.Fetcher {
	return exporter.NewFetcher(srv.Client())
}

// mustCodec creates a compress.Codec by name or fails the test.
func mustCodec(t *testing.T, name string) compress.Codec {
	t.Helper()

	c, err := compress.New(name, 0)
	if err != nil {
		t.Fatalf("compress.New(%q): %v", name, err)
	}

	return c
}

// readTarContents reads all regular-file entries from a tar file and returns a
// map of relPath → raw bytes.
func readTarContents(t *testing.T, tarPath string) map[string][]byte {
	t.Helper()

	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatalf("open tar %s: %v", tarPath, err)
	}

	defer func() { _ = f.Close() }()

	result := map[string][]byte{}
	tr := tar.NewReader(f)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatalf("read tar header: %v", err)
		}

		if hdr.Typeflag != tar.TypeReg {
			continue
		}

		data, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("read tar entry %s: %v", hdr.Name, err)
		}

		result[hdr.Name] = data
	}

	return result
}

func TestDownloadFilesystemVolume_DownloadsTree(t *testing.T) {
	srv, files := fsTestServer(t)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)
	rootURL := srv.URL + "/files/"

	err := volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		tarPath,
		stagingDir,
		rootURL,
		2,
		0,
		newFSFetcher(srv),
		mustCodec(t, "none"),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	// Tar must exist.
	if _, err := os.Stat(tarPath); err != nil {
		t.Fatalf("data.tar not created: %v", err)
	}

	// Staging dir must be cleaned up.
	if _, err := os.Stat(stagingDir); !os.IsNotExist(err) {
		t.Error("staging dir should have been removed after tar assembly")
	}

	// Verify each expected file is in the tar with correct content.
	entries := readTarContents(t, tarPath)

	for _, f := range files {
		data, ok := entries[f.relPath]
		if !ok {
			t.Errorf("tar missing entry %q", f.relPath)

			continue
		}

		if !bytes.Equal(data, f.content) {
			t.Errorf("entry %q: got %q, want %q", f.relPath, data, f.content)
		}
	}
}

func TestDownloadFilesystemVolume_SkipsIfTarExists(t *testing.T) {
	srv, _ := fsTestServer(t)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	// Pre-create a tar to simulate a completed prior download.
	if err := os.WriteFile(tarPath, []byte("existing"), 0o644); err != nil {
		t.Fatal(err)
	}

	fi1, _ := os.Stat(tarPath)

	rootURL := srv.URL + "/files/"

	if err := volume.DownloadFilesystemVolume(context.Background(), slog.Default(), tarPath, stagingDir, rootURL, 1, 0, newFSFetcher(srv), mustCodec(t, "none"), nil, nil); err != nil {
		t.Fatalf("second run: %v", err)
	}

	// Tar must not have been replaced.
	fi2, err := os.Stat(tarPath)
	if err != nil {
		t.Fatalf("stat after second run: %v", err)
	}

	if !fi2.ModTime().Equal(fi1.ModTime()) {
		t.Error("data.tar was overwritten when it already existed (should be skipped)")
	}
}

func TestDownloadFilesystemVolume_CleansStaleTmp(t *testing.T) {
	srv, _ := fsTestServer(t)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Plant a stale .tmp inside the staging dir.
	staleTmp := filepath.Join(stagingDir, "root.txt.tmp")
	if err := os.WriteFile(staleTmp, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}

	rootURL := srv.URL + "/files/"

	if err := volume.DownloadFilesystemVolume(context.Background(), slog.Default(), tarPath, stagingDir, rootURL, 1, 0, newFSFetcher(srv), mustCodec(t, "none"), nil, nil); err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	// Stale .tmp must be gone (staging dir removed entirely on success).
	if _, err := os.Stat(staleTmp); !os.IsNotExist(err) {
		t.Error("stale .tmp should have been removed")
	}

	// Final tar must exist.
	if _, err := os.Stat(tarPath); err != nil {
		t.Errorf("data.tar should exist after download: %v", err)
	}
}

func TestDownloadFilesystemVolume_LinkNotInTar(t *testing.T) {
	srv, _ := fsTestServer(t)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)
	rootURL := srv.URL + "/files/"

	if err := volume.DownloadFilesystemVolume(context.Background(), slog.Default(), tarPath, stagingDir, rootURL, 1, 0, newFSFetcher(srv), mustCodec(t, "none"), nil, nil); err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = f.Close() }()

	tr := tar.NewReader(f)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatalf("read tar: %v", err)
		}

		// Symlinks in the test server have no body and targetPath="/other";
		// they should be written as TypeSymlink entries, not TypeReg entries.
		if hdr.Name == "symlink.txt" && hdr.Typeflag == tar.TypeReg {
			t.Error("symlink was written as a regular file entry in the tar")
		}
	}
}

func TestDownloadFilesystemVolume_StagesCompressed(t *testing.T) {
	// With a non-none codec, each file is staged as <relPath><ext> containing
	// the compressed bytes; the tar entries carry those compressed names too.
	srv, files := fsTestServer(t)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)
	rootURL := srv.URL + "/files/"

	codec := mustCodec(t, "zstd")

	err := volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		tarPath,
		stagingDir,
		rootURL,
		2,
		0,
		newFSFetcher(srv),
		codec,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	// The tar should have compressed entries named <relPath>.zst.
	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatalf("open tar: %v", err)
	}

	defer func() { _ = f.Close() }()

	dec, decErr := zstd.NewReader(nil)
	if decErr != nil {
		t.Fatalf("zstd.NewReader: %v", decErr)
	}

	defer dec.Close()

	tr := tar.NewReader(f)
	found := map[string][]byte{}

	for {
		hdr, nextErr := tr.Next()
		if nextErr == io.EOF {
			break
		}

		if nextErr != nil {
			t.Fatalf("read tar: %v", nextErr)
		}

		if hdr.Typeflag != tar.TypeReg {
			continue
		}

		compressed, readErr := io.ReadAll(tr)
		if readErr != nil {
			t.Fatalf("read tar entry %s: %v", hdr.Name, readErr)
		}

		// Each regular entry must be a valid zstd stream.
		plain, decErr := dec.DecodeAll(compressed, nil)
		if decErr != nil {
			t.Fatalf("entry %q: zstd decode failed: %v", hdr.Name, decErr)
		}

		found[hdr.Name] = plain
	}

	for _, fixture := range files {
		wantName := fixture.relPath + ".zst"

		plain, ok := found[wantName]
		if !ok {
			t.Errorf("tar missing compressed entry %q", wantName)

			continue
		}

		if !bytes.Equal(plain, fixture.content) {
			t.Errorf("entry %q: decoded %q, want %q", wantName, plain, fixture.content)
		}
	}
}

// readTarHeaders reads all entries from a tar file and returns a map of
// entry name → *tar.Header so callers can assert header fields.
func readTarHeaders(t *testing.T, tarPath string) map[string]*tar.Header {
	t.Helper()

	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatalf("open tar %s: %v", tarPath, err)
	}

	defer func() { _ = f.Close() }()

	result := map[string]*tar.Header{}
	tr := tar.NewReader(f)

	for {
		hdr, hdrErr := tr.Next()
		if hdrErr == io.EOF {
			break
		}

		if hdrErr != nil {
			t.Fatalf("read tar header: %v", hdrErr)
		}

		result[hdr.Name] = hdr
	}

	return result
}

func TestDownloadFilesystemVolume_SkipsExistingCompressedStaged(t *testing.T) {
	// An already-staged compressed file must not be re-downloaded.
	// Proof: pre-stage root.txt.zst with a sentinel payload; verify that the
	// tar entry root.txt.zst still carries that sentinel (not bytes from server).
	srv, _ := fsTestServer(t)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)
	rootURL := srv.URL + "/files/"

	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Pre-stage root.txt with a sentinel that is NOT the server's content.
	// stageCompressedFile must skip it when it sees the file already present.
	sentinel := []byte("sentinel-not-server-content")
	preStaged := filepath.Join(stagingDir, "root.txt.zst")

	if err := os.WriteFile(preStaged, sentinel, 0o644); err != nil {
		t.Fatal(err)
	}

	codec := mustCodec(t, "zstd")

	if err := volume.DownloadFilesystemVolume(context.Background(), slog.Default(), tarPath, stagingDir, rootURL, 1, 0, newFSFetcher(srv), codec, nil, nil); err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	// The staging dir is removed; the tar must have the sentinel as-is.
	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatalf("open tar: %v", err)
	}

	defer func() { _ = f.Close() }()

	tr := tar.NewReader(f)

	for {
		hdr, nextErr := tr.Next()
		if nextErr == io.EOF {
			break
		}

		if nextErr != nil {
			t.Fatalf("read tar: %v", nextErr)
		}

		if hdr.Name != "root.txt.zst" {
			continue
		}

		got, readErr := io.ReadAll(tr)
		if readErr != nil {
			t.Fatalf("read entry: %v", readErr)
		}

		if !bytes.Equal(got, sentinel) {
			t.Errorf("root.txt.zst content = %q; want sentinel %q (file was re-downloaded)", got, sentinel)
		}

		return
	}

	t.Error("tar entry root.txt.zst not found")
}

func TestDownloadFilesystemVolume_RealisticAttributes(t *testing.T) {
	// The real data-exporter emits "permissions" (octal string) and "modtime"
	// (RFC3339 string), not "mode"/"mtime". Verify that mode, mtime, uid, and
	// gid are preserved in the assembled data.tar headers.
	// A symlink with "permissions":"0750" must not get the default 0777 applied.
	const timeStr = "2024-01-15T10:30:00Z"

	wantMtime, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		t.Fatalf("parse mtime: %v", err)
	}

	fileContent := []byte("hello")

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w,
				`{"apiVersion":"v1","items":[`+
					`{"name":"file.txt","type":"file","uri":"file.txt","attributes":{"permissions":"0640","modtime":"`+timeStr+`","uid":1000,"gid":2000,"size":5}},`+
					`{"name":"link.lnk","type":"link","uri":"","targetPath":"/target","attributes":{"permissions":"0750","modtime":"`+timeStr+`","uid":0,"gid":0}}`+
					`]}`)

		case "/files/file.txt":
			_, _ = w.Write(fileContent)

		default:
			http.NotFound(w, r)
		}
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	var gotTotal int64

	if err := volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		tarPath,
		stagingDir,
		srv.URL+"/files/",
		1,
		0,
		newFSFetcher(srv),
		mustCodec(t, "none"),
		func(total int64) { gotTotal = total },
		nil,
	); err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	// setTotal must receive the summed "size" of file items (file.txt is 5 bytes;
	// the symlink has no size and contributes nothing).
	if gotTotal != int64(len(fileContent)) {
		t.Errorf("setTotal = %d; want %d", gotTotal, len(fileContent))
	}

	headers := readTarHeaders(t, tarPath)

	// File entry: mode, mtime, uid, gid must match served attributes (not defaults).
	fileHdr, ok := headers["file.txt"]
	if !ok {
		t.Fatal("tar missing entry file.txt")
	}

	if fileHdr.Mode != 0o640 {
		t.Errorf("file.txt Mode = %04o; want 0640", fileHdr.Mode)
	}

	if !fileHdr.ModTime.Equal(wantMtime) {
		t.Errorf("file.txt ModTime = %v; want %v", fileHdr.ModTime, wantMtime)
	}

	if fileHdr.Uid != 1000 {
		t.Errorf("file.txt Uid = %d; want 1000", fileHdr.Uid)
	}

	if fileHdr.Gid != 2000 {
		t.Errorf("file.txt Gid = %d; want 2000", fileHdr.Gid)
	}

	// Symlink entry: "permissions":"0750" must be preserved, not replaced by the 0777 default.
	linkHdr, ok := headers["link.lnk"]
	if !ok {
		t.Fatal("tar missing entry link.lnk")
	}

	if linkHdr.Mode != 0o750 {
		t.Errorf("link.lnk Mode = %04o; want 0750", linkHdr.Mode)
	}
}

// ── chunked large-file staging (fs-large-file-chunked-range-resume) ──────────

// rangeTrackingServer records, for each request received by the tracked file
// handler, whether the request carried a Range header. Safe for concurrent use.
type rangeTrackingServer struct {
	mu     sync.Mutex
	ranges []bool
}

func (s *rangeTrackingServer) record(hasRange bool) {
	s.mu.Lock()
	s.ranges = append(s.ranges, hasRange)
	s.mu.Unlock()
}

func (s *rangeTrackingServer) snapshot() []bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]bool(nil), s.ranges...)
}

// newRangeTrackingFSServer builds an httptest.Server exposing a single-file FS
// volume ("/files/" listing + "/files/<fileName>" content) and records, on
// every GET to the file, whether the request carried a Range header — the
// signal that distinguishes the chunked path (RangeGet, always sends Range)
// from the single-shot path (GetFile, never sends Range). Serving via
// http.ServeContent mirrors the real data-exporter's sendFile idiom, so Range
// GETs are honored exactly as they are in production.
func newRangeTrackingFSServer(t *testing.T, fileName string, content []byte) (*httptest.Server, *rangeTrackingServer) {
	t.Helper()

	tracker := &rangeTrackingServer{}
	filePath := "/files/" + fileName

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":"`+fileName+`","type":"file","uri":"`+fileName+`","attributes":{"size":`+strconv.Itoa(len(content))+`}}`+
				`]}`)

		case filePath:
			tracker.record(r.Header.Get("Range") != "")
			http.ServeContent(w, r, fileName, time.Time{}, bytes.NewReader(content))

		default:
			http.NotFound(w, r)
		}
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv, tracker
}

// decodeZstdTarEntry opens tarPath and returns the zstd-decoded content of the
// entry named entryName, failing the test if the entry is missing.
func decodeZstdTarEntry(t *testing.T, tarPath, entryName string) []byte {
	t.Helper()

	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatalf("open tar %s: %v", tarPath, err)
	}

	defer func() { _ = f.Close() }()

	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("zstd.NewReader: %v", err)
	}

	defer dec.Close()

	tr := tar.NewReader(f)

	for {
		hdr, nextErr := tr.Next()
		if nextErr == io.EOF {
			break
		}

		if nextErr != nil {
			t.Fatalf("read tar header: %v", nextErr)
		}

		if hdr.Name != entryName {
			continue
		}

		compressed, readErr := io.ReadAll(tr)
		if readErr != nil {
			t.Fatalf("read tar entry %s: %v", entryName, readErr)
		}

		plain, decErr := dec.DecodeAll(compressed, nil)
		if decErr != nil {
			t.Fatalf("zstd decode entry %s: %v", entryName, decErr)
		}

		return plain
	}

	t.Fatalf("tar missing entry %q", entryName)

	return nil
}

// TestDownloadFilesystemVolume_LargeFile_ChunkedPathByteIdentical verifies that
// a file whose declared size exceeds chunkSize is staged via Range-based
// chunks (proven by every request to it carrying a Range header) and produces
// a byte-identical result to the same file staged via the non-chunked
// single-shot path (chunkSize larger than the file).
func TestDownloadFilesystemVolume_LargeFile_ChunkedPathByteIdentical(t *testing.T) {
	t.Parallel()

	content := bytes.Repeat([]byte("chunked-fs-large-file-payload-"), 20) // 620 bytes
	codec := mustCodec(t, "zstd")

	const testChunkSize = 64

	// Chunked run: chunkSize smaller than the file forces the Range-based path.
	chunkedSrv, chunkedTracker := newRangeTrackingFSServer(t, "big.bin", content)

	chunkedNodeDir := t.TempDir()
	chunkedTarPath := filepath.Join(chunkedNodeDir, archive.FsTarName)
	chunkedStagingDir := filepath.Join(chunkedNodeDir, archive.FsTarStagingDirName)

	if err := volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		chunkedTarPath,
		chunkedStagingDir,
		chunkedSrv.URL+"/files/",
		2,
		testChunkSize,
		newFSFetcher(chunkedSrv),
		codec,
		nil,
		nil,
	); err != nil {
		t.Fatalf("chunked DownloadFilesystemVolume: %v", err)
	}

	chunkedRanges := chunkedTracker.snapshot()
	if len(chunkedRanges) < 2 {
		t.Fatalf("expected multiple chunk requests, got %d", len(chunkedRanges))
	}

	for i, sawRange := range chunkedRanges {
		if !sawRange {
			t.Errorf("request %d to big.bin had no Range header; expected the chunked path", i)
		}
	}

	// Non-chunked run: chunkSize larger than the file keeps the single-shot path.
	plainSrv, plainTracker := newRangeTrackingFSServer(t, "big.bin", content)

	plainNodeDir := t.TempDir()
	plainTarPath := filepath.Join(plainNodeDir, archive.FsTarName)
	plainStagingDir := filepath.Join(plainNodeDir, archive.FsTarStagingDirName)

	if err := volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		plainTarPath,
		plainStagingDir,
		plainSrv.URL+"/files/",
		2,
		int64(len(content)*2),
		newFSFetcher(plainSrv),
		codec,
		nil,
		nil,
	); err != nil {
		t.Fatalf("non-chunked DownloadFilesystemVolume: %v", err)
	}

	plainRanges := plainTracker.snapshot()
	if len(plainRanges) != 1 || plainRanges[0] {
		t.Fatalf("expected exactly one non-Range request, got %v", plainRanges)
	}

	chunkedContent := decodeZstdTarEntry(t, chunkedTarPath, "big.bin"+codec.Ext())
	plainContent := decodeZstdTarEntry(t, plainTarPath, "big.bin"+codec.Ext())

	if !bytes.Equal(chunkedContent, content) {
		t.Error("chunked path content does not match original")
	}

	if !bytes.Equal(plainContent, content) {
		t.Error("non-chunked path content does not match original")
	}

	if !bytes.Equal(chunkedContent, plainContent) {
		t.Error("chunked and non-chunked paths produced different decoded content")
	}
}

// TestDownloadFilesystemVolume_SmallFile_NoChunkDirCreated verifies that a file
// at or below chunkSize keeps the unchanged single-shot path: exactly one
// non-Range GET is issued and no per-file chunk directory is ever created.
//
// WriteTar is deliberately made to fail (by pre-occupying its AtomicWriter's
// "<tarPath>.tmp" target with a directory) so DownloadFilesystemVolume returns
// an error right after per-file staging completes but before the staging
// directory is removed — otherwise a successful run always removes stagingDir
// (and, on the chunked path, MergeBlockChunks always removes the now-empty
// chunk directory too), leaving nothing on disk to assert against either way.
func TestDownloadFilesystemVolume_SmallFile_NoChunkDirCreated(t *testing.T) {
	t.Parallel()

	content := []byte("small-file-content-well-below-the-chunk-size-threshold")
	codec := mustCodec(t, "zstd")

	srv, tracker := newRangeTrackingFSServer(t, "small.bin", content)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	if err := os.MkdirAll(tarPath+".tmp", 0o755); err != nil {
		t.Fatal(err)
	}

	testChunkSize := int64(len(content)) * 2

	err := volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		tarPath,
		stagingDir,
		srv.URL+"/files/",
		1,
		testChunkSize,
		newFSFetcher(srv),
		codec,
		nil,
		nil,
	)
	if err == nil {
		t.Fatal("expected DownloadFilesystemVolume to fail (poisoned tar.tmp path)")
	}

	destPath := filepath.Join(stagingDir, "small.bin"+codec.Ext())
	if _, statErr := os.Stat(destPath); statErr != nil {
		t.Fatalf("expected staged file at %s: %v", destPath, statErr)
	}

	chunkDir := filepath.Join(stagingDir, filepath.FromSlash(archive.FsFileChunksDirName("small.bin", codec.Ext())))
	if _, statErr := os.Stat(chunkDir); !os.IsNotExist(statErr) {
		t.Errorf("chunk directory %s must not exist for a file at/below chunkSize", chunkDir)
	}

	if snap := tracker.snapshot(); len(snap) != 1 || snap[0] {
		t.Errorf("expected exactly one non-Range GET, got %v", snap)
	}
}

// TestDownloadFilesystemVolume_PartialChunkResume_OnlyMissingChunkFetched
// verifies the sub-file resume path this task exists for: a large file with
// one chunk already staged from a prior interrupted run must have only its
// missing chunks re-fetched, the pre-existing chunk's raw length must still be
// credited to onProgress (inherited from downloadChunk's resume-skip branch),
// and the final merged content must be byte-identical to the source.
func TestDownloadFilesystemVolume_PartialChunkResume_OnlyMissingChunkFetched(t *testing.T) {
	t.Parallel()

	const testChunkSize int64 = 100

	content := bytes.Repeat([]byte("R"), 250) // 3 chunks: 100, 100, 50

	var (
		mu            sync.Mutex
		rangesFetched []string
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":"big.bin","type":"file","uri":"big.bin","attributes":{"size":`+strconv.Itoa(len(content))+`}}`+
				`]}`)

		case "/files/big.bin":
			mu.Lock()
			rangesFetched = append(rangesFetched, r.Header.Get("Range"))
			mu.Unlock()

			http.ServeContent(w, r, "big.bin", time.Time{}, bytes.NewReader(content))

		default:
			http.NotFound(w, r)
		}
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	codec := mustCodec(t, "zstd")

	// Pre-seed chunk 0 as a real zstd frame, simulating a crash after the first
	// chunk was downloaded but before the remaining chunks were fetched.
	chunkDir := filepath.Join(stagingDir, filepath.FromSlash(archive.FsFileChunksDirName("big.bin", codec.Ext())))
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	chunk0Frame, err := codec.EncodeFrame(content[:testChunkSize])
	if err != nil {
		t.Fatalf("encode chunk 0: %v", err)
	}

	if err := os.WriteFile(filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())), chunk0Frame, 0o644); err != nil {
		t.Fatal(err)
	}

	var (
		progMu   sync.Mutex
		credited int64
	)

	onProgress := func(n int) {
		progMu.Lock()
		credited += int64(n)
		progMu.Unlock()
	}

	err = volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		tarPath,
		stagingDir,
		srv.URL+"/files/",
		1,
		testChunkSize,
		newFSFetcher(srv),
		codec,
		nil,
		onProgress,
	)
	if err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	mu.Lock()
	gotRanges := append([]string(nil), rangesFetched...)
	mu.Unlock()

	for _, hdr := range gotRanges {
		if hdr == "bytes=0-99" {
			t.Error("chunk 0 was pre-seeded and must not be re-fetched")
		}
	}

	var foundChunk1, foundChunk2 bool

	for _, hdr := range gotRanges {
		switch hdr {
		case "bytes=100-199":
			foundChunk1 = true
		case "bytes=200-249":
			foundChunk2 = true
		}
	}

	if !foundChunk1 || !foundChunk2 {
		t.Errorf("expected chunks 1 and 2 to be fetched, got ranges %v", gotRanges)
	}

	if credited != int64(len(content)) {
		t.Errorf("onProgress credited = %d; want %d (sum of all chunk raw lengths, including the pre-seeded one)",
			credited, len(content))
	}

	got := decodeZstdTarEntry(t, tarPath, "big.bin"+codec.Ext())
	if !bytes.Equal(got, content) {
		t.Error("merged big.bin content does not match original")
	}
}
