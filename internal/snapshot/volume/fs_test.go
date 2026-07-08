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
	"crypto/md5" //nolint:gosec // test fixture digest, matches the exporter's own hash.md5 attribute
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
					`{"name":"symlink.txt","type":"link","uri":"","targetPath":"root.txt","attributes":{}}`+
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

		// Symlinks in the test server have no body and targetPath="root.txt";
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
					`{"name":"link.lnk","type":"link","uri":"","targetPath":"file.txt","attributes":{"permissions":"0750","modtime":"`+timeStr+`","uid":0,"gid":0}}`+
					`]}`)

		case "/files/file.txt":
			// The listing declares a "size" for file.txt, so it now downloads via
			// the durable chunked path (stageChunkedFile/DownloadBlockChunks),
			// which issues Range GETs — http.ServeContent (mirroring the real
			// data-exporter's sendFile idiom) is required to honor them.
			http.ServeContent(w, r, "file.txt", time.Time{}, bytes.NewReader(fileContent))

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
// a file whose declared size exceeds chunkSize is staged via MULTIPLE
// Range-based chunks (proven by every request to it carrying a Range header)
// and produces a byte-identical result to the same file staged with a
// chunkSize larger than the file, which still uses the durable chunked path
// (every known-size file does, since fs-whole-file-durable-partial-resume) but
// collapses to a SINGLE chunk.
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

	// Single-chunk run: chunkSize larger than the file still uses the chunked
	// path (every known-size file does), but collapses to exactly one chunk.
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
		t.Fatalf("single-chunk DownloadFilesystemVolume: %v", err)
	}

	plainRanges := plainTracker.snapshot()
	if len(plainRanges) != 1 || !plainRanges[0] {
		t.Fatalf("expected exactly one Range request (single-chunk chunked path), got %v", plainRanges)
	}

	chunkedContent := decodeZstdTarEntry(t, chunkedTarPath, "big.bin"+codec.Ext())
	plainContent := decodeZstdTarEntry(t, plainTarPath, "big.bin"+codec.Ext())

	if !bytes.Equal(chunkedContent, content) {
		t.Error("multi-chunk path content does not match original")
	}

	if !bytes.Equal(plainContent, content) {
		t.Error("single-chunk path content does not match original")
	}

	if !bytes.Equal(chunkedContent, plainContent) {
		t.Error("multi-chunk and single-chunk paths produced different decoded content")
	}
}

// TestDownloadFilesystemVolume_KnownSizeSmallFile_UsesDurableChunkedPath
// verifies the fs-whole-file-durable-partial-resume fix: a file with a KNOWN
// declared size, even one well below chunkSize, is staged via the durable
// Range-based chunked path (stageChunkedFile) rather than the old whole-file
// single-shot GET — proven by its GET carrying a Range header — so it
// inherits sub-file durable resume regardless of its size relative to
// chunkSize. Before this fix, such a file used stageWholeFile and any
// interrupt mid-transfer restarted it from byte zero.
func TestDownloadFilesystemVolume_KnownSizeSmallFile_UsesDurableChunkedPath(t *testing.T) {
	t.Parallel()

	content := []byte("small-file-content-well-below-the-chunk-size-threshold")
	codec := mustCodec(t, "zstd")

	srv, tracker := newRangeTrackingFSServer(t, "small.bin", content)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	testChunkSize := int64(len(content)) * 2 // threshold well above the file's size

	if err := volume.DownloadFilesystemVolume(
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
	); err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	if snap := tracker.snapshot(); len(snap) != 1 || !snap[0] {
		t.Errorf("expected exactly one Range GET (durable chunked path), got %v", snap)
	}

	got := decodeZstdTarEntry(t, tarPath, "small.bin"+codec.Ext())
	if !bytes.Equal(got, content) {
		t.Error("staged content does not match original")
	}
}

// TestDownloadFilesystemVolume_UnknownSizeFile_UsesWholeFileFallback verifies
// that a file whose listing omits "size" (indistinguishable from a genuinely
// empty file, see parseItemSize) keeps the original single-shot GET +
// codec.EncodeStream path: DownloadBlockChunks/MergeBlockChunks need a
// trustworthy total size up front to compute chunk geometry, which an
// unknown declared size cannot provide.
func TestDownloadFilesystemVolume_UnknownSizeFile_UsesWholeFileFallback(t *testing.T) {
	t.Parallel()

	content := []byte("unknown-size-file-content")
	codec := mustCodec(t, "zstd")

	tracker := &rangeTrackingServer{}

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":"unknown.bin","type":"file","uri":"unknown.bin","attributes":{}}`+
				`]}`)

		case "/files/unknown.bin":
			tracker.record(r.Header.Get("Range") != "")
			_, _ = w.Write(content)

		default:
			http.NotFound(w, r)
		}
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	if err := volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		tarPath,
		stagingDir,
		srv.URL+"/files/",
		1,
		0,
		newFSFetcher(srv),
		codec,
		nil,
		nil,
	); err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	if snap := tracker.snapshot(); len(snap) != 1 || snap[0] {
		t.Errorf("expected exactly one non-Range GET (whole-file fallback), got %v", snap)
	}

	got := decodeZstdTarEntry(t, tarPath, "unknown.bin"+codec.Ext())
	if !bytes.Equal(got, content) {
		t.Error("staged content does not match original")
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

	// A real interrupted run always has a chunks.meta recording the geometry
	// (createChunkDir writes it before the first chunk is even fetched — see
	// the chunk-size-mismatch-resume-corruption-guard fix), so seed one here
	// matching this run's geometry. Without it the geometry guard cannot tell
	// this partial chunk dir apart from one produced under a different
	// chunkSize and would (correctly) purge and re-fetch chunk 0 too.
	if err := archive.WriteChunkMeta(chunkDir, archive.ChunkMeta{ChunkSize: testChunkSize, TotalSize: int64(len(content))}); err != nil {
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

// TestDownloadFilesystemVolume_LargeFile_ChunkSizeChanged_ResumeNotCorrupted
// is the filesystem-path counterpart of
// TestDownloadBlockChunks_ChunkSizeChanged_PurgesStaleChunks: it shares the
// exact same underlying machinery (stageChunkedFile calls
// DownloadBlockChunks/MergeBlockChunks unchanged), so the same silent-
// corruption bug applied here too. A large file chunked in an interrupted run
// at one chunk size must have its stale chunks discarded and be re-fetched in
// full at a DIFFERENT chunk size on resume, rather than assembling a data.tar
// entry from chunks whose byte ranges no longer match the new geometry.
func TestDownloadFilesystemVolume_LargeFile_ChunkSizeChanged_ResumeNotCorrupted(t *testing.T) {
	t.Parallel()

	const (
		firstChunkSize  int64 = 100
		secondChunkSize int64 = 60
	)

	// 240 bytes: does not divide evenly by either chunk size, exercising a
	// short final chunk under both geometries.
	content := bytes.Repeat([]byte("Q"), 240)

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

	// Simulate an interrupted first run at firstChunkSize: pre-seed the FULL
	// set of chunks that geometry would produce (as if the run got all the
	// way through chunking but crashed before merge) plus the chunks.meta a
	// real run would have written for that geometry.
	chunkDir := filepath.Join(stagingDir, filepath.FromSlash(archive.FsFileChunksDirName("big.bin", codec.Ext())))
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	firstNumChunks := int((int64(len(content)) + firstChunkSize - 1) / firstChunkSize)

	for i := range firstNumChunks {
		start := int64(i) * firstChunkSize
		end := min(start+firstChunkSize, int64(len(content)))

		frame, err := codec.EncodeFrame(content[start:end])
		if err != nil {
			t.Fatalf("encode chunk %d: %v", i, err)
		}

		if err := os.WriteFile(filepath.Join(chunkDir, archive.ChunkFileName(i, codec.Ext())), frame, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	if err := archive.WriteChunkMeta(chunkDir, archive.ChunkMeta{ChunkSize: firstChunkSize, TotalSize: int64(len(content))}); err != nil {
		t.Fatal(err)
	}

	// Resume at a DIFFERENT chunk size.
	err := volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		tarPath,
		stagingDir,
		srv.URL+"/files/",
		1,
		secondChunkSize,
		newFSFetcher(srv),
		codec,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	mu.Lock()
	gotRanges := append([]string(nil), rangesFetched...)
	mu.Unlock()

	secondNumChunks := int((int64(len(content)) + secondChunkSize - 1) / secondChunkSize)
	if len(gotRanges) != secondNumChunks {
		t.Errorf("expected the stale chunks to be discarded and all %d chunks re-fetched at the new geometry, got %d requests: %v",
			secondNumChunks, len(gotRanges), gotRanges)
	}

	got := decodeZstdTarEntry(t, tarPath, "big.bin"+codec.Ext())
	if !bytes.Equal(got, content) {
		t.Error("merged big.bin content does not match original after a chunk-size change on resume")
	}
}

// NOTE: TestDownloadFilesystemVolume_ThresholdCrossing_OrphanChunkDirRemoved
// (the "a file crossing the chunk-size threshold falls back to stageWholeFile
// and must clean up its now-orphaned chunk dir" test) was REMOVED by
// fs-whole-file-durable-partial-resume: the code path it exercised no longer
// exists. stageCompressedFile's branch is now item.size > 0 (chunked) vs.
// item.size <= 0 (whole-file), not item.size vs. chunkSize, so a chunkSize
// change alone can never move a known-size file between the two strategies
// any more — it only ever changes how many chunks that file's chunked
// download uses, which TestDownloadFilesystemVolume_LargeFile_ChunkSizeChanged_ResumeNotCorrupted
// already covers (ensureChunkGeometry purges and re-fetches under the new
// geometry).

// TestDownloadFilesystemVolume_SmallFile_InterruptedResumesFromPersistedOffset
// is the primary regression test for fs-whole-file-durable-partial-resume,
// exercised through a file BELOW the chunk-size threshold — exactly the class
// stageWholeFile used to own. Before the fix, an interrupt anywhere in that
// single GET discarded 100% of the file's progress (removeTmpFiles would also
// have wiped the AtomicWriter's ".tmp" on the next resume scan); now the file
// goes through the same durable ".part"-based mechanism as a large chunked
// file (block.go's downloadChunk/fetchChunkRaw, unchanged here), with exactly
// one chunk covering the whole file, and resumes from its persisted offset.
func TestDownloadFilesystemVolume_SmallFile_InterruptedResumesFromPersistedOffset(t *testing.T) {
	t.Parallel()

	content := bytes.Repeat([]byte("R"), 40) // well below the chunk-size threshold
	const cutBytes = 13                      // interrupt partway through the file

	srv, _ := newRangeTrackingFSServer(t, "small.bin", content)

	// recordingDoer (defined in block_test.go, shared package volume_test)
	// counts EVERY request through it, including the directory listing GET
	// that precedes each file download: call 1 is the listing, call 2 is the
	// file's Range GET (there is exactly one chunk, since the file is well
	// below chunkSize).
	doer := &recordingDoer{inner: srv.Client(), cutOnCall: 2, cutBytes: cutBytes}
	fetcher := exporter.NewFetcher(doer)

	codec := mustCodec(t, "zstd")

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	testChunkSize := int64(len(content)) * 10 // threshold well above the file's size

	chunkDir := filepath.Join(stagingDir, filepath.FromSlash(archive.FsFileChunksDirName("small.bin", codec.Ext())))
	partPath := filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())+".part")

	// Run 1: interrupted mid-file.
	err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, testChunkSize, fetcher, codec, nil, nil,
	)
	if err == nil {
		t.Fatal("expected the interrupted run to return an error")
	}

	if !errors.Is(err, errSimulatedInterrupt) {
		t.Fatalf("expected errSimulatedInterrupt, got: %v", err)
	}

	partInfo, statErr := os.Stat(partPath)
	if statErr != nil {
		t.Fatalf("durable partial must exist after an interrupted below-threshold file: %v", statErr)
	}

	if partInfo.Size() != cutBytes {
		t.Errorf("durable partial size = %d, want %d", partInfo.Size(), int64(cutBytes))
	}

	if _, statErr := os.Stat(tarPath); !os.IsNotExist(statErr) {
		t.Error("data.tar must not exist after an interrupted run")
	}

	// A real interrupted run stamps the identity marker on first touch
	// (ensureNodeSubdirs); seed it so ScanAbsolute proves the partial dir's
	// identity and resumes rather than rejecting an unverifiable (marker-less)
	// dir with ErrIdentityMismatch — see partial-node-dir-identity-marker.
	if err := archive.WriteNodeIdentityMarker(nodeDir, archive.NodeIdentity{}); err != nil {
		t.Fatalf("WriteNodeIdentityMarker: %v", err)
	}

	// The resumable partial must survive a resume scan's stale-*.tmp sweep: it
	// uses ".part", not ".tmp" (archive.resume.go's removeTmpFiles only
	// targets "*.tmp").
	if _, scanErr := archive.ScanAbsolute(nodeDir, archive.NodeIdentity{}); scanErr != nil {
		t.Fatalf("ScanAbsolute: %v", scanErr)
	}

	if _, statErr := os.Stat(partPath); statErr != nil {
		t.Fatalf("durable partial must survive a resume scan (removeTmpFiles): %v", statErr)
	}

	// Run 2: resume. cutOnCall (2) will never match again since the doer's
	// call counter keeps incrementing across both runs.
	err = volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, testChunkSize, fetcher, codec, nil, nil,
	)
	if err != nil {
		t.Fatalf("resumed run must succeed: %v", err)
	}

	ranges := doer.recordedRanges()
	lastRange := ranges[len(ranges)-1]
	wantRange := fmt.Sprintf("bytes=%d-%d", cutBytes, len(content)-1)

	if lastRange != wantRange {
		t.Errorf("resumed run's Range header = %q, want %q (resume from persisted offset, not byte 0)", lastRange, wantRange)
	}

	got := decodeZstdTarEntry(t, tarPath, "small.bin"+codec.Ext())
	if !bytes.Equal(got, content) {
		t.Error("resumed download must produce byte-identical content")
	}

	if _, statErr := os.Stat(partPath); !os.IsNotExist(statErr) {
		t.Error("durable partial must be removed after the chunk finalizes")
	}
}

// ── source MD5 verification (download-fs-verify-md5-against-source) ────────

// warnCapture is a slog.Handler that collects Warn-or-above log messages for assertions.
type warnCapture struct {
	mu   sync.Mutex
	msgs []string
}

func (h *warnCapture) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *warnCapture) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		h.mu.Lock()
		h.msgs = append(h.msgs, r.Message)
		h.mu.Unlock()
	}

	return nil
}

func (h *warnCapture) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *warnCapture) WithGroup(_ string) slog.Handler { return h }

func (h *warnCapture) warnMessages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]string, len(h.msgs))
	copy(out, h.msgs)

	return out
}

// hexMD5 returns the lowercase hex MD5 digest of content.
func hexMD5(content []byte) string {
	sum := md5.Sum(content) //nolint:gosec // test fixture digest, matches the exporter's own hash.md5 attribute

	return hex.EncodeToString(sum[:])
}

// newMD5FSServer builds an httptest.Server exposing a single-file FS volume whose
// listing item.Attributes carries "hash.md5": advertisedMD5, so it exercises the same
// contract exporter.ListDir requests (attribute=hash.md5) without depending on that
// package. withSize controls whether the listing also declares "size" — selecting
// stageChunkedFile (known size) vs. stageWholeFile (unknown size) in the CLI.
func newMD5FSServer(t *testing.T, fileName string, served []byte, advertisedMD5 string, withSize bool) *httptest.Server {
	t.Helper()

	filePath := "/files/" + fileName

	attrs := `"hash.md5":"` + advertisedMD5 + `"`
	if advertisedMD5 == "" {
		attrs = ""
	}

	if withSize {
		sizeAttr := `"size":` + strconv.Itoa(len(served))
		if attrs == "" {
			attrs = sizeAttr
		} else {
			attrs = sizeAttr + "," + attrs
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":"`+fileName+`","type":"file","uri":"`+fileName+`","attributes":{`+attrs+`}}`+
				`]}`)

		case filePath:
			// http.ServeContent honors Range so the known-size (chunked) case works too.
			http.ServeContent(w, r, fileName, time.Time{}, bytes.NewReader(served))

		default:
			http.NotFound(w, r)
		}
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv
}

// TestDownloadFilesystemVolume_MD5Verify_WholeFile_Success verifies that a file staged
// via the whole-file path (unknown declared size) whose plaintext matches the exporter-
// advertised hash.md5 stages successfully and decodes back to the original content.
func TestDownloadFilesystemVolume_MD5Verify_WholeFile_Success(t *testing.T) {
	t.Parallel()

	content := []byte("whole-file content verified against the source MD5")
	srv := newMD5FSServer(t, "file.bin", content, hexMD5(content), false)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)
	codec := mustCodec(t, "zstd")

	if err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, 0, newFSFetcher(srv), codec, nil, nil,
	); err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	got := decodeZstdTarEntry(t, tarPath, "file.bin"+codec.Ext())
	if !bytes.Equal(got, content) {
		t.Error("staged content does not match original")
	}
}

// TestDownloadFilesystemVolume_MD5Verify_WholeFile_Mismatch verifies that a whole-file
// item whose served bytes do NOT match the advertised hash.md5 fails with the sentinel
// ErrSourceHashMismatch and never finalizes data.tar.
func TestDownloadFilesystemVolume_MD5Verify_WholeFile_Mismatch(t *testing.T) {
	t.Parallel()

	content := []byte("whole-file content that will not match the advertised digest")
	wrongMD5 := hexMD5([]byte("a completely different payload"))
	srv := newMD5FSServer(t, "file.bin", content, wrongMD5, false)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, 0, newFSFetcher(srv), mustCodec(t, "zstd"), nil, nil,
	)
	if err == nil {
		t.Fatal("expected an MD5 mismatch error, got nil")
	}

	if !errors.Is(err, volume.ErrSourceHashMismatch) {
		t.Errorf("expected errors.Is(err, ErrSourceHashMismatch), got: %v", err)
	}

	if _, statErr := os.Stat(tarPath); !os.IsNotExist(statErr) {
		t.Error("data.tar must not be finalized when a file fails MD5 verification")
	}
}

// TestDownloadFilesystemVolume_MD5Verify_ChunkedFile_Success verifies that a
// known-size file staged via the durable chunked path (stageChunkedFile ->
// DownloadBlockChunks/MergeBlockChunks) whose reassembled plaintext matches the
// exporter-advertised hash.md5 stages successfully.
func TestDownloadFilesystemVolume_MD5Verify_ChunkedFile_Success(t *testing.T) {
	t.Parallel()

	content := bytes.Repeat([]byte("chunked-md5-verified-payload-"), 10)
	srv := newMD5FSServer(t, "big.bin", content, hexMD5(content), true)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)
	codec := mustCodec(t, "zstd")

	if err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, 0, newFSFetcher(srv), codec, nil, nil,
	); err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	got := decodeZstdTarEntry(t, tarPath, "big.bin"+codec.Ext())
	if !bytes.Equal(got, content) {
		t.Error("staged content does not match original")
	}
}

// TestDownloadFilesystemVolume_MD5Verify_ChunkedFile_Mismatch verifies that a
// known-size file whose reassembled plaintext does NOT match the advertised
// hash.md5 fails with ErrSourceHashMismatch, removes the corrupt staged artifact
// (so a resume re-fetches instead of trusting it), and never finalizes data.tar.
func TestDownloadFilesystemVolume_MD5Verify_ChunkedFile_Mismatch(t *testing.T) {
	t.Parallel()

	content := bytes.Repeat([]byte("chunked-content-that-will-not-match-"), 10)
	wrongMD5 := hexMD5([]byte("a completely different payload"))
	srv := newMD5FSServer(t, "big.bin", content, wrongMD5, true)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)
	codec := mustCodec(t, "zstd")

	err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, 0, newFSFetcher(srv), codec, nil, nil,
	)
	if err == nil {
		t.Fatal("expected an MD5 mismatch error, got nil")
	}

	if !errors.Is(err, volume.ErrSourceHashMismatch) {
		t.Errorf("expected errors.Is(err, ErrSourceHashMismatch), got: %v", err)
	}

	if _, statErr := os.Stat(tarPath); !os.IsNotExist(statErr) {
		t.Error("data.tar must not be finalized when a file fails MD5 verification")
	}

	destPath := filepath.Join(stagingDir, "big.bin"+codec.Ext())
	if _, statErr := os.Stat(destPath); !os.IsNotExist(statErr) {
		t.Error("corrupt staged file must be removed after an MD5 mismatch, not left for a resume to trust")
	}
}

// TestDownloadFilesystemVolume_MD5Verify_MissingDigest_WarnOnly verifies that a listing
// item with no hash.md5 attribute (an older exporter, or a request the server did not
// honor) degrades to a single WARN log and still succeeds — never a hard failure.
// Covers both the whole-file and chunked paths in one run.
func TestDownloadFilesystemVolume_MD5Verify_MissingDigest_WarnOnly(t *testing.T) {
	t.Parallel()

	wholeContent := []byte("whole-file content with no advertised digest")
	chunkedContent := bytes.Repeat([]byte("chunked-content-with-no-advertised-digest-"), 5)

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":"whole.bin","type":"file","uri":"whole.bin","attributes":{}},`+
				`{"name":"chunked.bin","type":"file","uri":"chunked.bin","attributes":{"size":`+strconv.Itoa(len(chunkedContent))+`}}`+
				`]}`)

		case "/files/whole.bin":
			http.ServeContent(w, r, "whole.bin", time.Time{}, bytes.NewReader(wholeContent))

		case "/files/chunked.bin":
			http.ServeContent(w, r, "chunked.bin", time.Time{}, bytes.NewReader(chunkedContent))

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

	lh := &warnCapture{}
	log := slog.New(lh)

	if err := volume.DownloadFilesystemVolume(
		context.Background(), log, tarPath, stagingDir, srv.URL+"/files/",
		2, 0, newFSFetcher(srv), codec, nil, nil,
	); err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	warnCount := 0

	for _, msg := range lh.warnMessages() {
		if msg == "no source MD5 available for file, skipping integrity verification" {
			warnCount++
		}
	}

	if warnCount != 2 {
		t.Errorf("expected exactly 2 missing-digest WARNs (one per file), got %d: %v", warnCount, lh.warnMessages())
	}

	gotWhole := decodeZstdTarEntry(t, tarPath, "whole.bin"+codec.Ext())
	if !bytes.Equal(gotWhole, wholeContent) {
		t.Error("whole-file staged content does not match original")
	}

	gotChunked := decodeZstdTarEntry(t, tarPath, "chunked.bin"+codec.Ext())
	if !bytes.Equal(gotChunked, chunkedContent) {
		t.Error("chunked-file staged content does not match original")
	}
}

// ── resume progress sizes sidecar (fs-resume-progress-sizes-sidecar) ───────

// TestDownloadFilesystemVolume_SizesSidecar_SeedsResumeWithoutNetwork is the
// primary regression test for fs-resume-progress-sizes-sidecar: it proves the
// sidecar is durably written as soon as the listing is known (even though the
// run is interrupted right after), that it records every known-size file's
// exact declared size and their sum, and that ScanFSStagingSizes can credit
// an already-fully-staged file (its chunk dir already merged away) purely
// from local state — no network call — while correctly NOT crediting a
// sibling file that is still mid-transfer. It also proves the resumed run
// still produces byte-identical content for both files.
func TestDownloadFilesystemVolume_SizesSidecar_SeedsResumeWithoutNetwork(t *testing.T) {
	t.Parallel()

	firstContent := []byte("first-file-fully-staged-before-the-interrupt")
	secondContent := bytes.Repeat([]byte("S"), 80)

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":"a.bin","type":"file","uri":"a.bin","attributes":{"size":`+strconv.Itoa(len(firstContent))+`}},`+
				`{"name":"b.bin","type":"file","uri":"b.bin","attributes":{"size":`+strconv.Itoa(len(secondContent))+`}}`+
				`]}`)

		case "/files/a.bin":
			http.ServeContent(w, r, "a.bin", time.Time{}, bytes.NewReader(firstContent))

		case "/files/b.bin":
			http.ServeContent(w, r, "b.bin", time.Time{}, bytes.NewReader(secondContent))

		default:
			http.NotFound(w, r)
		}
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)
	codec := mustCodec(t, "none")

	// workers=1 forces strictly serialized per-file staging in listing order
	// (a.bin then b.bin), so the doer's call sequence is deterministic:
	// call 1 = the listing GET, call 2 = a.bin's (single-chunk) Range GET,
	// call 3 = b.bin's Range GET, truncated mid-transfer.
	const cutBytes = 20

	doer := &recordingDoer{inner: srv.Client(), cutOnCall: 3, cutBytes: cutBytes}
	fetcher := exporter.NewFetcher(doer)

	err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, 0, fetcher, codec, nil, nil,
	)
	if err == nil {
		t.Fatal("expected the interrupted run to return an error")
	}

	if !errors.Is(err, errSimulatedInterrupt) {
		t.Fatalf("expected errSimulatedInterrupt, got: %v", err)
	}

	// The sidecar must exist even though the run never finished: it is
	// written right after the listing succeeds, before any file is staged.
	sizes, found, err := volume.ReadFSSizesSidecar(stagingDir)
	if err != nil {
		t.Fatalf("ReadFSSizesSidecar: %v", err)
	}

	if !found {
		t.Fatal("sizes sidecar must exist after the listing was fetched, even though the run was interrupted")
	}

	wantTotal := int64(len(firstContent) + len(secondContent))
	if sizes.Total != wantTotal {
		t.Errorf("sidecar Total = %d; want %d", sizes.Total, wantTotal)
	}

	if sizes.Files["a.bin"] != int64(len(firstContent)) {
		t.Errorf("sidecar Files[a.bin] = %d; want %d", sizes.Files["a.bin"], len(firstContent))
	}

	if sizes.Files["b.bin"] != int64(len(secondContent)) {
		t.Errorf("sidecar Files[b.bin] = %d; want %d", sizes.Files["b.bin"], len(secondContent))
	}

	// a.bin must already be a flat, fully-staged blob (its chunk dir merged
	// away); b.bin must still be an in-progress chunk dir, not a flat blob.
	if _, statErr := os.Stat(filepath.Join(stagingDir, "a.bin"+codec.Ext())); statErr != nil {
		t.Fatalf("a.bin must be fully staged as a flat blob: %v", statErr)
	}

	if _, statErr := os.Stat(filepath.Join(stagingDir, "b.bin"+codec.Ext())); !os.IsNotExist(statErr) {
		t.Fatalf("b.bin must NOT be a flat blob yet (still interrupted): stat err = %v", statErr)
	}

	// ScanFSStagingSizes must credit ONLY a.bin's persisted declared size —
	// the fully-staged flat-blob case — while reporting the full sidecar
	// total, all from local state.
	gotTotal, gotStaged, gotFound, err := volume.ScanFSStagingSizes(stagingDir, codec.Ext())
	if err != nil {
		t.Fatalf("ScanFSStagingSizes: %v", err)
	}

	if !gotFound {
		t.Fatal("ScanFSStagingSizes must report found=true when the sidecar exists")
	}

	if gotTotal != wantTotal {
		t.Errorf("ScanFSStagingSizes total = %d; want %d", gotTotal, wantTotal)
	}

	if gotStaged != int64(len(firstContent)) {
		t.Errorf("ScanFSStagingSizes staged = %d; want %d (only a.bin, the fully-staged flat blob)", gotStaged, len(firstContent))
	}

	// A real interrupted run stamps the identity marker on first touch
	// (ensureNodeSubdirs); seed it so ScanAbsolute proves the partial dir's
	// identity and resumes rather than rejecting an unverifiable (marker-less)
	// dir with ErrIdentityMismatch — see partial-node-dir-identity-marker.
	if err := archive.WriteNodeIdentityMarker(nodeDir, archive.NodeIdentity{}); err != nil {
		t.Fatalf("WriteNodeIdentityMarker: %v", err)
	}

	// The sidecar must survive a resume scan: it does not end in ".tmp", so
	// archive.resume.go's removeTmpFiles (invoked by ScanAbsolute) must not
	// remove it.
	if _, scanErr := archive.ScanAbsolute(nodeDir, archive.NodeIdentity{}); scanErr != nil {
		t.Fatalf("ScanAbsolute: %v", scanErr)
	}

	if _, found, err := volume.ReadFSSizesSidecar(stagingDir); err != nil || !found {
		t.Fatalf("sizes sidecar must survive a resume scan: found=%v err=%v", found, err)
	}

	// Resume: interruption never re-fires (the doer's call counter keeps
	// incrementing past cutOnCall==3 across both runs). The run must succeed
	// and both files must be byte-identical to their source content.
	err = volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, 0, fetcher, codec, nil, nil,
	)
	if err != nil {
		t.Fatalf("resumed run must succeed: %v", err)
	}

	entries := readTarContents(t, tarPath)

	if !bytes.Equal(entries["a.bin"], firstContent) {
		t.Error("a.bin content mismatch after resume")
	}

	if !bytes.Equal(entries["b.bin"], secondContent) {
		t.Error("b.bin content mismatch after resume")
	}
}

// TestScanFSStagingSizes_NoSidecar_ReportsNotFound verifies that
// ScanFSStagingSizes distinguishes "no sidecar yet" (a from-scratch run, or a
// staging dir predating this feature) from a legitimate zero total: found
// must be false, not merely a zero total, so callers do not mistake "not
// seeded yet" for "seeded at zero".
func TestScanFSStagingSizes_NoSidecar_ReportsNotFound(t *testing.T) {
	t.Parallel()

	t.Run("MissingStagingDir", func(t *testing.T) {
		t.Parallel()

		stagingDir := filepath.Join(t.TempDir(), "does-not-exist")

		total, staged, found, err := volume.ScanFSStagingSizes(stagingDir, ".zst")
		if err != nil {
			t.Fatalf("ScanFSStagingSizes: %v", err)
		}

		if found {
			t.Error("found = true; want false when the staging dir does not exist")
		}

		if total != 0 || staged != 0 {
			t.Errorf("total=%d staged=%d; want 0, 0", total, staged)
		}
	})

	t.Run("StagingDirWithoutSidecar", func(t *testing.T) {
		t.Parallel()

		stagingDir := t.TempDir()

		total, staged, found, err := volume.ScanFSStagingSizes(stagingDir, ".zst")
		if err != nil {
			t.Fatalf("ScanFSStagingSizes: %v", err)
		}

		if found {
			t.Error("found = true; want false when the staging dir exists but has no sidecar yet")
		}

		if total != 0 || staged != 0 {
			t.Errorf("total=%d staged=%d; want 0, 0", total, staged)
		}
	})
}

// TestDownloadFilesystemVolume_SizesSidecar_FromScratchUnchanged verifies that
// a completed, from-scratch download (no interruption, no prior sidecar) is
// unaffected by the sidecar feature: the sidecar is written and then removed
// along with the rest of the staging dir on successful tar assembly, exactly
// like every other staging file.
func TestDownloadFilesystemVolume_SizesSidecar_FromScratchUnchanged(t *testing.T) {
	t.Parallel()

	srv, _ := fsTestServer(t)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)
	rootURL := srv.URL + "/files/"

	if err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, rootURL,
		2, 0, newFSFetcher(srv), mustCodec(t, "none"), nil, nil,
	); err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	if _, err := os.Stat(tarPath); err != nil {
		t.Fatalf("data.tar not created: %v", err)
	}

	if _, err := os.Stat(stagingDir); !os.IsNotExist(err) {
		t.Error("staging dir (and its sidecar) should have been removed after a successful from-scratch run")
	}
}

// ── path sanitization (sanitize-server-provided-paths) ─────────────────────

// singleItemFSServer builds an httptest.Server exposing a one-item filesystem listing
// at "/files/" whose sole item carries itemName verbatim, JSON-encoded via
// encoding/json so quotes/backslashes/control bytes survive intact — simulating a
// malicious or corrupted data-exporter response that a hand-written JSON literal could
// not safely express.
func singleItemFSServer(t *testing.T, itemName string) *httptest.Server {
	t.Helper()

	body, err := json.Marshal(struct {
		APIVersion string          `json:"apiVersion"`
		Items      []exporter.Item `json:"items"`
	}{
		APIVersion: "v1",
		Items: []exporter.Item{
			{Name: itemName, Type: "file", URI: "file.bin", Attributes: map[string]any{}},
		},
	})
	if err != nil {
		t.Fatalf("marshal listing: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/files/" {
			http.NotFound(w, r)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv
}

// TestDownloadFilesystemVolume_RejectsUnsafeItemNames is the primary regression test
// for sanitize-server-provided-paths: a listing item whose name is an absolute path,
// carries a ".." element, is empty/dot-only, or contains a control byte must be
// rejected with a wrapped ErrUnsafePath BEFORE any filepath.Join, so nothing is ever
// staged outside stagingDir. A legitimate nested path ("a/b/c"-shaped, arising from
// directory recursion) is exercised elsewhere by
// TestDownloadFilesystemVolume_DownloadsTree (subdir/nested.txt) and must keep working
// unchanged — this test only covers the rejection side.
func TestDownloadFilesystemVolume_RejectsUnsafeItemNames(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		itemName string
	}{
		{name: "ParentDirEscape", itemName: "../evil"},
		{name: "AbsolutePath", itemName: "/etc/passwd"},
		{name: "EmbeddedParentDirEscape", itemName: "a/../../b"},
		{name: "Empty", itemName: ""},
		{name: "DotOnly", itemName: "."},
		{name: "DotDotOnly", itemName: ".."},
		{name: "ControlByte", itemName: "abc\x00def"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := singleItemFSServer(t, tc.itemName)

			nodeDir := t.TempDir()
			tarPath := filepath.Join(nodeDir, archive.FsTarName)
			stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

			err := volume.DownloadFilesystemVolume(
				context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
				1, 0, newFSFetcher(srv), mustCodec(t, "none"), nil, nil,
			)
			if err == nil {
				t.Fatal("expected an error for an unsafe item name, got nil")
			}

			if !errors.Is(err, volume.ErrUnsafePath) {
				t.Errorf("expected errors.Is(err, ErrUnsafePath), got: %v", err)
			}

			if _, statErr := os.Stat(tarPath); !os.IsNotExist(statErr) {
				t.Error("data.tar must not be created when the listing carries an unsafe name")
			}

			// Nothing must ever be staged outside nodeDir: walk the temp root's
			// parent-free own tree and confirm no file escaped stagingDir/nodeDir.
			// (An unsafe name is rejected in collectAllFSItems, before any file is
			// staged, so nodeDir itself should contain at most the empty staging dir.)
			entries, readErr := os.ReadDir(nodeDir)
			if readErr != nil {
				t.Fatalf("read node dir: %v", readErr)
			}

			for _, e := range entries {
				if e.Name() != filepath.Base(stagingDir) {
					t.Errorf("unexpected entry %q in node dir after rejected listing", e.Name())
				}
			}
		})
	}
}
