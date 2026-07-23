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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

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

func readRegularTarHeaders(t *testing.T, tarPath string) map[string]*tar.Header {
	t.Helper()

	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatalf("open tar %s: %v", tarPath, err)
	}

	defer func() { _ = f.Close() }()

	result := map[string]*tar.Header{}
	tr := tar.NewReader(f)

	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return result
		}

		if err != nil {
			t.Fatalf("read tar header: %v", err)
		}

		if hdr.Typeflag == tar.TypeReg || hdr.Typeflag == 0 {
			result[hdr.Name] = hdr
		}
	}
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

	headers := readRegularTarHeaders(t, tarPath)
	for _, file := range files {
		metadata, parseErr := archive.ParseFSMetadata(headers[file.relPath])
		if parseErr != nil {
			t.Fatalf("parse metadata for %s: %v", file.relPath, parseErr)
		}

		if metadata.Codec != "none" || metadata.OriginalPath != file.relPath || metadata.RawSize != int64(len(file.content)) {
			t.Errorf("metadata for %s = %#v", file.relPath, metadata)
		}
	}
}

func TestDownloadFilesystemVolume_RejectsUnsupportedListingItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		itemName   string
		itemType   string
		attributes string
	}{
		{
			name:       "Other",
			itemName:   "execq",
			itemType:   "other",
			attributes: `{"permissions":"0660","modtime":"2026-07-23T12:00:00Z","uid":0,"gid":999}`,
		},
		{
			name:       "LinkError",
			itemName:   "broken-link",
			itemType:   "linkErr",
			attributes: `{"permissions":"0777","modtime":"2026-07-23T12:00:00Z","uid":0,"gid":0}`,
		},
		{
			name:       "UnknownFutureType",
			itemName:   "future-entry",
			itemType:   "deviceV2",
			attributes: `{}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var fileGets atomic.Int64

			mux := http.NewServeMux()
			mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/files/":
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprintf(w,
						`{"apiVersion":"v1","items":[`+
							`{"name":"regular.txt","type":"file","uri":"regular.txt","attributes":{"size":4}},`+
							`{"name":%q,"type":%q,"uri":%q,"attributes":%s}`+
							`]}`,
						tt.itemName, tt.itemType, tt.itemName, tt.attributes)
				case "/files/regular.txt":
					fileGets.Add(1)
					_, _ = io.WriteString(w, "data")
				default:
					http.NotFound(w, r)
				}
			})

			srv := httptest.NewServer(mux)
			t.Cleanup(srv.Close)

			nodeDir := t.TempDir()
			tarPath := filepath.Join(nodeDir, archive.FsTarName)
			stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

			err := volume.DownloadFilesystemVolume(
				context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
				1, 0, newFSFetcher(srv), mustCodec(t, "none"), nil, nil,
			)
			if err == nil {
				t.Fatal("expected unsupported listing item to fail the download")
			}

			if !strings.Contains(err.Error(), tt.itemName) {
				t.Errorf("error %q does not contain item path %q", err, tt.itemName)
			}

			if !strings.Contains(err.Error(), tt.itemType) {
				t.Errorf("error %q does not contain wire type %q", err, tt.itemType)
			}

			if _, statErr := os.Stat(tarPath); !os.IsNotExist(statErr) {
				t.Errorf("data.tar must not be published after unsupported listing item: %v", statErr)
			}

			if got := fileGets.Load(); got != 0 {
				t.Errorf("regular file was fetched %d times before listing validation completed", got)
			}
		})
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

	metaDir := filepath.Join(stagingDir, volume.FSMetaDirName)
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(metaDir, "inventory.jsonl"), []byte("stale"), 0o600); err != nil {
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

	if _, err := os.Stat(stagingDir); !os.IsNotExist(err) {
		t.Errorf("completed-tar skip must remove stale inventory staging: %v", err)
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

	// Pre-stage root.txt with a valid encoded sentinel that is NOT the server's
	// content. The resume path decodes it once to recover exact rawSize because
	// this legacy fixture listing omits size and MD5, but must not re-download it.
	sentinel := []byte("sentinel-not-server-content")
	preStaged := filepath.Join(stagingDir, "root.txt.zst")
	codec := mustCodec(t, "zstd")

	var encoded bytes.Buffer
	if err := codec.EncodeStream(&encoded, bytes.NewReader(sentinel)); err != nil {
		t.Fatalf("encode sentinel: %v", err)
	}

	if err := os.WriteFile(preStaged, encoded.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

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

		if !bytes.Equal(got, encoded.Bytes()) {
			t.Errorf("root.txt.zst content changed (file was re-downloaded)")
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
			if r.Method == http.MethodGet {
				tracker.record(r.Header.Get("Range") != "")
			}

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
			if r.Method == http.MethodGet {
				tracker.record(r.Header.Get("Range") != "")
				_, _ = w.Write(content)
			}

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
			if r.Method == http.MethodGet {
				mu.Lock()
				rangesFetched = append(rangesFetched, r.Header.Get("Range"))
				mu.Unlock()
			}

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
			if r.Method == http.MethodGet {
				mu.Lock()
				rangesFetched = append(rangesFetched, r.Header.Get("Range"))
				mu.Unlock()
			}

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
	// counts EVERY request through it: call 1 is the listing, call 2 is the
	// source-hash HEAD, and call 3 is the file's Range GET (there is exactly
	// one chunk, since the file is well below chunkSize).
	doer := &recordingDoer{inner: srv.Client(), cutOnCall: 3, cutBytes: cutBytes}
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
// stat-only listing declares optional size metadata and whose dedicated HEAD
// attribute request returns advertisedMD5. withSize selects stageChunkedFile
// (known size) vs. stageWholeFile (unknown size) in the CLI.
func newMD5FSServer(t *testing.T, fileName string, served []byte, advertisedMD5 string, withSize bool) *httptest.Server {
	t.Helper()

	filePath := "/files/" + fileName

	attrs := ""
	if withSize {
		attrs = `"size":` + strconv.Itoa(len(served))
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
			if r.Method == http.MethodHead && r.URL.Query().Get("attribute") == "hash.md5" && advertisedMD5 != "" {
				w.Header().Set("X-Attribute-Hash-Md5", advertisedMD5)
			}

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

func TestDownloadFilesystemVolume_SourceHashOutlivesOrdinaryHeaderTimeout(t *testing.T) {
	t.Parallel()

	const (
		fileName        = "large.bin"
		ordinaryTimeout = 30 * time.Millisecond
		hashDelay       = 90 * time.Millisecond
	)

	content := bytes.Repeat([]byte("producer-shaped-source-hash-content-"), 8)
	wantMD5 := hexMD5(content)

	var (
		mu           sync.Mutex
		listAttrs    []string
		hashRequests int
		fileGets     int
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			mu.Lock()
			listAttrs = append([]string(nil), r.URL.Query()["attribute"]...)
			mu.Unlock()

			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":"`+fileName+`","type":"file","uri":"`+fileName+`","attributes":{"size":`+
				strconv.Itoa(len(content))+`}}]}`)

		case "/files/" + fileName:
			if r.Method == http.MethodHead {
				if got := r.URL.Query()["attribute"]; len(got) != 1 || got[0] != "hash.md5" {
					t.Errorf("hash attribute query = %v, want [hash.md5]", got)
				}

				mu.Lock()
				hashRequests++
				mu.Unlock()

				time.Sleep(hashDelay)
				w.Header().Set("Content-Length", strconv.Itoa(len(content)))
				w.Header().Set("X-Attribute-Hash-Md5", wantMD5)
				w.WriteHeader(http.StatusOK)

				return
			}

			mu.Lock()
			fileGets++
			mu.Unlock()

			http.ServeContent(w, r, fileName, time.Time{}, bytes.NewReader(content))

		default:
			http.NotFound(w, r)
		}
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	ordinaryTransport := srv.Client().Transport.(*http.Transport).Clone()
	ordinaryTransport.ResponseHeaderTimeout = ordinaryTimeout

	hashTransport := srv.Client().Transport.(*http.Transport).Clone()
	hashTransport.ResponseHeaderTimeout = time.Second

	fetcher := exporter.NewFetcher(
		&http.Client{Transport: ordinaryTransport},
		exporter.WithSourceHashDoer(&http.Client{Transport: hashTransport}),
	)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)
	codec := mustCodec(t, "zstd")

	if err := volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		tarPath,
		stagingDir,
		srv.URL+"/files/",
		1,
		0,
		fetcher,
		codec,
		nil,
		nil,
	); err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	mu.Lock()
	gotListAttrs := append([]string(nil), listAttrs...)
	gotHashRequests := hashRequests
	gotFileGets := fileGets
	mu.Unlock()

	if len(gotListAttrs) != 1 || gotListAttrs[0] != "stat" {
		t.Errorf("listing attributes = %v, want [stat]", gotListAttrs)
	}

	if gotHashRequests != 1 {
		t.Errorf("source hash requests = %d, want 1", gotHashRequests)
	}

	if gotFileGets == 0 {
		t.Error("file bytes were not fetched")
	}

	if got := decodeZstdTarEntry(t, tarPath, fileName+codec.Ext()); !bytes.Equal(got, content) {
		t.Errorf("decoded content = %q, want %q", got, content)
	}
}

// TestDownloadFilesystemVolume_MD5Verify_WholeFile_Success verifies that a file staged
// via the whole-file path (unknown declared size) whose plaintext matches the exporter-
// reported hash.md5 stages successfully and decodes back to the original content.
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
	// listing, a.bin hash HEAD + Range GET, then b.bin hash HEAD + Range GET.
	// The fifth call is truncated mid-transfer.
	const cutBytes = 20

	doer := &recordingDoer{inner: srv.Client(), cutOnCall: 5, cutBytes: cutBytes}
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
	gotTotal, gotStaged, gotFound, err := volume.ScanFSStagingSizes(context.Background(), stagingDir, codec.Ext())
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

		total, staged, found, err := volume.ScanFSStagingSizes(context.Background(), stagingDir, ".zst")
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

		total, staged, found, err := volume.ScanFSStagingSizes(context.Background(), stagingDir, ".zst")
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

func TestScanFSStagingSizes_RejectsOversizedTokensBeforeMaterialization(t *testing.T) {
	const (
		oversizedBytes = 20 << 20
		maxAllocated   = 4 << 20
	)

	tests := []struct {
		name   string
		prefix string
		fill   byte
		suffix string
	}{
		{
			name:   "FilePath",
			prefix: `{"files":{"`,
			fill:   'a',
			suffix: `":1},"total":1}`,
		},
		{
			name:   "FileSize",
			prefix: `{"files":{"safe":`,
			fill:   '1',
			suffix: `},"total":1}`,
		},
		{
			name:   "TopLevelKey",
			prefix: `{"`,
			fill:   'x',
			suffix: `":0,"files":{},"total":0}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stagingDir := t.TempDir()
			metaDir := filepath.Join(stagingDir, volume.FSMetaDirName)
			if err := os.MkdirAll(metaDir, 0o755); err != nil {
				t.Fatal(err)
			}

			sidecarPath := filepath.Join(metaDir, volume.FSSizesSidecarName)
			sidecar, err := os.Create(sidecarPath)
			if err != nil {
				t.Fatal(err)
			}

			if _, err := io.WriteString(sidecar, tt.prefix); err != nil {
				t.Fatal(err)
			}

			if _, err := io.CopyN(sidecar, repeatedByteReader(tt.fill), oversizedBytes); err != nil {
				t.Fatal(err)
			}

			if _, err := io.WriteString(sidecar, tt.suffix); err != nil {
				t.Fatal(err)
			}

			if err := sidecar.Close(); err != nil {
				t.Fatal(err)
			}

			runtime.GC()

			var baseline runtime.MemStats
			runtime.ReadMemStats(&baseline)

			_, _, _, err = volume.ScanFSStagingSizes(context.Background(), stagingDir, "")
			if err == nil || !strings.Contains(err.Error(), "encoded limit") {
				t.Fatalf("ScanFSStagingSizes error = %v, want encoded-limit rejection", err)
			}

			var current runtime.MemStats
			runtime.ReadMemStats(&current)

			if allocated := current.TotalAlloc - baseline.TotalAlloc; allocated > maxAllocated {
				t.Fatalf("sidecar scan allocated %d bytes, want <= %d before rejecting oversized token", allocated, maxAllocated)
			}
		})
	}
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

// ── resume skip-branch MD5 re-check (fs-skip-branch-md5-recheck) ───────────

// newFileGetCountingFSServer builds an httptest.Server exposing a single
// known-size file whose dedicated HEAD attribute request returns advertisedMD5.
// It counts every GET issued to the file body (Range or not), so a test can
// prove that a resume skip did NOT re-download an already-staged file.
// http.ServeContent honors Range so the known-size chunked re-stage path works
// too.
func newFileGetCountingFSServer(t *testing.T, fileName string, served []byte, advertisedMD5 string) (*httptest.Server, func() int) {
	t.Helper()

	filePath := "/files/" + fileName

	attrs := `"size":` + strconv.Itoa(len(served))

	var (
		mu     sync.Mutex
		getCnt int
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":"`+fileName+`","type":"file","uri":"`+fileName+`","attributes":{`+attrs+`}}`+
				`]}`)

		case filePath:
			if r.Method == http.MethodHead {
				if r.URL.Query().Get("attribute") == "hash.md5" && advertisedMD5 != "" {
					w.Header().Set("X-Attribute-Hash-Md5", advertisedMD5)
				}
			} else {
				mu.Lock()
				getCnt++
				mu.Unlock()
			}

			http.ServeContent(w, r, fileName, time.Time{}, bytes.NewReader(served))

		default:
			http.NotFound(w, r)
		}
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	count := func() int {
		mu.Lock()
		defer mu.Unlock()

		return getCnt
	}

	return srv, count
}

// decodeTarEntryByCodec returns the decoded plaintext of the entry named
// relPath+codec.Ext() from tarPath. Only the none/zstd codecs used by the
// resume-skip test matrix are handled.
func decodeTarEntryByCodec(t *testing.T, tarPath, relPath string, codec compress.Codec) []byte {
	t.Helper()

	entryName := relPath + codec.Ext()

	if codec.Ext() == ".zst" {
		return decodeZstdTarEntry(t, tarPath, entryName)
	}

	entries := readTarContents(t, tarPath)

	data, ok := entries[entryName]
	if !ok {
		t.Fatalf("tar missing entry %q", entryName)
	}

	return data
}

func TestDownloadFilesystemVolume_CodecSuffixedSourceNameMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		codec      string
		storedPath string
	}{
		{name: "none does not reinterpret source suffix", codec: "none", storedPath: "report.zst"},
		{name: "zstd appends a distinct storage suffix", codec: "zstd", storedPath: "report.zst.zst"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			content := []byte("codec-suffixed source filename")
			codec := mustCodec(t, tc.codec)
			srv, _ := newFileGetCountingFSServer(t, "report.zst", content, hexMD5(content))
			nodeDir := t.TempDir()
			tarPath := filepath.Join(nodeDir, archive.FsTarName)

			err := volume.DownloadFilesystemVolume(
				context.Background(),
				slog.Default(),
				tarPath,
				filepath.Join(nodeDir, archive.FsTarStagingDirName),
				srv.URL+"/files/",
				1,
				0,
				newFSFetcher(srv),
				codec,
				nil,
				nil,
			)
			if err != nil {
				t.Fatalf("DownloadFilesystemVolume: %v", err)
			}

			headers := readRegularTarHeaders(t, tarPath)
			hdr, ok := headers[tc.storedPath]
			if !ok {
				t.Fatalf("tar missing stored entry %q", tc.storedPath)
			}

			metadata, err := archive.ParseFSMetadata(hdr)
			if err != nil {
				t.Fatalf("ParseFSMetadata: %v", err)
			}

			if metadata.Codec != tc.codec || metadata.OriginalPath != "report.zst" ||
				metadata.RawSize != int64(len(content)) {
				t.Fatalf("metadata = %#v", metadata)
			}

			if got := decodeTarEntryByCodec(t, tarPath, "report.zst", codec); !bytes.Equal(got, content) {
				t.Fatalf("decoded content = %q, want %q", got, content)
			}
		})
	}
}

// TestDownloadFilesystemVolume_ResumeSkip_VerifiedBlobSkipped verifies that an
// already-staged blob whose decoded MD5 matches the exporter-advertised digest
// is skipped exactly as before: no file GET is issued, its declared size is
// credited to onProgress exactly once, and the assembled tar carries it.
func TestDownloadFilesystemVolume_ResumeSkip_VerifiedBlobSkipped(t *testing.T) {
	t.Parallel()

	for _, codecName := range []string{"none", "zstd"} {
		codecName := codecName

		t.Run(codecName, func(t *testing.T) {
			t.Parallel()

			content := []byte("resume-skip verified staged blob content")
			codec := mustCodec(t, codecName)
			srv, getCount := newFileGetCountingFSServer(t, "file.bin", content, hexMD5(content))

			nodeDir := t.TempDir()
			tarPath := filepath.Join(nodeDir, archive.FsTarName)
			stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

			if err := os.MkdirAll(stagingDir, 0o755); err != nil {
				t.Fatal(err)
			}

			// Pre-stage the correctly-encoded blob a prior run would have left:
			// its decoded MD5 matches the advertised digest.
			frame, err := codec.EncodeFrame(content)
			if err != nil {
				t.Fatalf("encode frame: %v", err)
			}

			if err := os.WriteFile(filepath.Join(stagingDir, "file.bin"+codec.Ext()), frame, 0o644); err != nil {
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

			if err := volume.DownloadFilesystemVolume(
				context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
				1, 0, newFSFetcher(srv), codec, nil, onProgress,
			); err != nil {
				t.Fatalf("DownloadFilesystemVolume: %v", err)
			}

			if getCount() != 0 {
				t.Errorf("verified staged file was re-downloaded: %d file GET(s), want 0", getCount())
			}

			if credited != int64(len(content)) {
				t.Errorf("onProgress credited = %d; want %d (declared size once)", credited, len(content))
			}

			got := decodeTarEntryByCodec(t, tarPath, "file.bin", codec)
			if !bytes.Equal(got, content) {
				t.Errorf("tar entry content = %q; want %q", got, content)
			}

			hdr := readRegularTarHeaders(t, tarPath)["file.bin"+codec.Ext()]
			metadata, err := archive.ParseFSMetadata(hdr)
			if err != nil {
				t.Fatalf("ParseFSMetadata: %v", err)
			}

			if metadata.RawSize != int64(len(content)) {
				t.Errorf("resumed entry rawSize = %d, want %d", metadata.RawSize, len(content))
			}
		})
	}
}

// TestDownloadFilesystemVolume_ResumeSkip_MismatchedBlobRestaged verifies that
// an already-staged blob whose decoded MD5 does NOT match the advertised digest
// is removed and re-staged within the same run (proven by a file GET being
// issued), and that the re-staged result is verified again by the fresh-path
// logic (proven by the tar entry decoding to the true source content). No error
// is returned for this self-healing condition.
func TestDownloadFilesystemVolume_ResumeSkip_MismatchedBlobRestaged(t *testing.T) {
	t.Parallel()

	for _, codecName := range []string{"none", "zstd"} {
		codecName := codecName

		t.Run(codecName, func(t *testing.T) {
			t.Parallel()

			content := []byte("resume-skip true source content for the mismatched blob")
			codec := mustCodec(t, codecName)
			srv, getCount := newFileGetCountingFSServer(t, "file.bin", content, hexMD5(content))

			nodeDir := t.TempDir()
			tarPath := filepath.Join(nodeDir, archive.FsTarName)
			stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

			if err := os.MkdirAll(stagingDir, 0o755); err != nil {
				t.Fatal(err)
			}

			// Pre-stage a WRONG blob: a validly-encoded frame of different
			// bytes, so it decodes fine but its plaintext MD5 mismatches.
			wrong, err := codec.EncodeFrame([]byte("stale foreign staged bytes that do not match"))
			if err != nil {
				t.Fatalf("encode wrong frame: %v", err)
			}

			destPath := filepath.Join(stagingDir, "file.bin"+codec.Ext())
			if err := os.WriteFile(destPath, wrong, 0o644); err != nil {
				t.Fatal(err)
			}

			if err := volume.DownloadFilesystemVolume(
				context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
				1, 0, newFSFetcher(srv), codec, nil, nil,
			); err != nil {
				t.Fatalf("DownloadFilesystemVolume: %v", err)
			}

			if getCount() < 1 {
				t.Errorf("mismatched staged file was not re-downloaded: %d file GET(s), want >= 1", getCount())
			}

			got := decodeTarEntryByCodec(t, tarPath, "file.bin", codec)
			if !bytes.Equal(got, content) {
				t.Errorf("re-staged tar entry content = %q; want %q (true source content)", got, content)
			}
		})
	}
}

// TestDownloadFilesystemVolume_ResumeSkip_EmptyMD5SkipsWithWarn verifies that
// an already-staged blob for a listing item with no hash.md5 attribute is
// skipped WITHOUT verification (matching the fresh-path convention): the blob
// is not re-downloaded even though its bytes differ from the server's, its
// declared size is credited once, and a single WARN is logged.
func TestDownloadFilesystemVolume_ResumeSkip_EmptyMD5SkipsWithWarn(t *testing.T) {
	t.Parallel()

	content := []byte("server content that must never be fetched on an empty-md5 skip")
	codec := mustCodec(t, "none")
	srv, getCount := newFileGetCountingFSServer(t, "file.bin", content, "")

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Sentinel differs from the server content: with no advertised MD5 the skip
	// branch must NOT verify it and must NOT re-download it.
	sentinel := []byte("sentinel-not-server-content")
	if err := os.WriteFile(filepath.Join(stagingDir, "file.bin"), sentinel, 0o644); err != nil {
		t.Fatal(err)
	}

	lh := &warnCapture{}
	log := slog.New(lh)

	var (
		progMu   sync.Mutex
		credited int64
	)

	onProgress := func(n int) {
		progMu.Lock()
		credited += int64(n)
		progMu.Unlock()
	}

	if err := volume.DownloadFilesystemVolume(
		context.Background(), log, tarPath, stagingDir, srv.URL+"/files/",
		1, 0, newFSFetcher(srv), codec, nil, onProgress,
	); err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	if getCount() != 0 {
		t.Errorf("empty-md5 staged file was re-downloaded: %d file GET(s), want 0", getCount())
	}

	warnCount := 0

	for _, msg := range lh.warnMessages() {
		if msg == "no source MD5 available for file, skipping integrity verification" {
			warnCount++
		}
	}

	if warnCount != 1 {
		t.Errorf("expected exactly 1 missing-digest WARN, got %d: %v", warnCount, lh.warnMessages())
	}

	entries := readTarContents(t, tarPath)
	if !bytes.Equal(entries["file.bin"], sentinel) {
		t.Errorf("file.bin content = %q; want sentinel %q (skipped without verification)", entries["file.bin"], sentinel)
	}

	if credited != int64(len(content)) {
		t.Errorf("onProgress credited = %d; want %d (declared size once)", credited, len(content))
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
		// The reserved metadata namespace: a root-level ".d8-meta" (or anything
		// under it as a first segment) must be rejected so no server-provided
		// path can shadow the internal sidecar dir (inv. #10a).
		{name: "ReservedMetadataDir", itemName: ".d8-meta"},
		{name: "ReservedMetadataDirChild", itemName: ".d8-meta/x"},
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
			// (An unsafe name is rejected while building the inventory, before any
			// file is staged, so nodeDir should contain at most staging metadata.)
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

// ── reserved metadata namespace (fs-sizes-sidecar-reserved-namespace) ──────

// TestDownloadFilesystemVolume_UserFileNamedSizesJson_NotShadowed is the
// primary regression test for fs-sizes-sidecar-reserved-namespace: at codec
// none (codec.Ext() == "") a USER file literally named "sizes.json" at the
// volume root once staged to the exact path the internal sizes sidecar
// occupied (stagingDir/sizes.json), so the sidecar shadowed it and the tar
// packed internal JSON under the user's filename — silent data replacement.
// With the sidecar relocated under the reserved FSMetaDirName the two never
// collide: the user's bytes are staged, MD5-verified, and packed into
// data.tar, while the sidecar lives at stagingDir/.d8-meta/sizes.json. The run
// is interrupted mid-second-file so the (otherwise removed-on-success) staging
// state can be inspected before a clean resume completes it.
func TestDownloadFilesystemVolume_UserFileNamedSizesJson_NotShadowed(t *testing.T) {
	t.Parallel()

	userContent := []byte(`user file that merely happens to be named sizes.json`)
	secondContent := bytes.Repeat([]byte("S"), 80)
	userMD5 := hexMD5(userContent)
	secondMD5 := hexMD5(secondContent)

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":"sizes.json","type":"file","uri":"sizes.json","attributes":{"size":`+strconv.Itoa(len(userContent))+`}},`+
				`{"name":"zz.bin","type":"file","uri":"zz.bin","attributes":{"size":`+strconv.Itoa(len(secondContent))+`}}`+
				`]}`)

		case "/files/sizes.json":
			if r.Method == http.MethodHead {
				w.Header().Set("X-Attribute-Hash-Md5", userMD5)
			}

			http.ServeContent(w, r, "sizes.json", time.Time{}, bytes.NewReader(userContent))

		case "/files/zz.bin":
			if r.Method == http.MethodHead {
				w.Header().Set("X-Attribute-Hash-Md5", secondMD5)
			}

			http.ServeContent(w, r, "zz.bin", time.Time{}, bytes.NewReader(secondContent))

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

	// workers=1 serializes staging in sorted inventory order (sizes.json, then
	// zz.bin):
	// call 1 = listing, calls 2-3 = sizes.json's hash HEAD and Range GET,
	// calls 4-5 = zz.bin's hash HEAD and Range GET. The fifth call is truncated
	// mid-transfer so sizes.json is fully staged and zz.bin is not.
	doer := &recordingDoer{inner: srv.Client(), cutOnCall: 5, cutBytes: 20}
	fetcher := exporter.NewFetcher(doer)

	err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, 0, fetcher, codec, nil, nil,
	)
	if !errors.Is(err, errSimulatedInterrupt) {
		t.Fatalf("expected errSimulatedInterrupt, got: %v", err)
	}

	// The internal sidecar must live under the reserved metadata dir, never at
	// the staging root where it would collide with the user's file.
	sidecarPath := filepath.Join(stagingDir, volume.FSMetaDirName, volume.FSSizesSidecarName)

	sidecarBytes, statErr := os.ReadFile(sidecarPath)
	if statErr != nil {
		t.Fatalf("internal sidecar must exist under %s/%s: %v", volume.FSMetaDirName, volume.FSSizesSidecarName, statErr)
	}

	var sidecar volume.FSSizesSidecar
	if jsonErr := json.Unmarshal(sidecarBytes, &sidecar); jsonErr != nil {
		t.Fatalf("internal sidecar must be valid FSSizesSidecar JSON: %v", jsonErr)
	}

	if sidecar.Files["sizes.json"] != int64(len(userContent)) {
		t.Errorf("sidecar Files[sizes.json] = %d; want %d (the user file's declared size)", sidecar.Files["sizes.json"], len(userContent))
	}

	// The user file must be staged as a flat blob at the staging ROOT, holding
	// the USER's bytes — not the internal JSON. At codec none the flat blob is
	// byte-identical to the source content.
	stagedUser, readErr := os.ReadFile(filepath.Join(stagingDir, "sizes.json"))
	if readErr != nil {
		t.Fatalf("user file sizes.json must be staged at the staging root: %v", readErr)
	}

	if !bytes.Equal(stagedUser, userContent) {
		t.Errorf("staged sizes.json holds %q; want the user content %q (internal JSON must never shadow it)", stagedUser, userContent)
	}

	// A real interrupted run stamps the identity marker on first touch; seed it
	// so the resume scan proves identity instead of rejecting a marker-less dir.
	if err := archive.WriteNodeIdentityMarker(nodeDir, archive.NodeIdentity{}); err != nil {
		t.Fatalf("WriteNodeIdentityMarker: %v", err)
	}

	// Resume to completion (truncation never re-fires: the call counter is past
	// cutOnCall). Both files must land in data.tar with their true source bytes.
	if err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, 0, fetcher, codec, nil, nil,
	); err != nil {
		t.Fatalf("resumed run must succeed: %v", err)
	}

	entries := readTarContents(t, tarPath)

	if !bytes.Equal(entries["sizes.json"], userContent) {
		t.Errorf("tar entry sizes.json = %q; want the user content %q", entries["sizes.json"], userContent)
	}

	if !bytes.Equal(entries["zz.bin"], secondContent) {
		t.Error("tar entry zz.bin content mismatch after resume")
	}
}

// TestDownloadFilesystemVolume_NestedReservedName_Accepted verifies the
// first-segment scoping of the reserved-namespace guard: a file literally
// named ".d8-meta" nested under a user directory ("a/.d8-meta") stages under a
// user subtree, cannot collide with stagingDir/.d8-meta, and MUST download
// normally. Only a root-level ".d8-meta" is rejected.
func TestDownloadFilesystemVolume_NestedReservedName_Accepted(t *testing.T) {
	t.Parallel()

	nestedContent := []byte("legitimate user file that happens to be named .d8-meta")
	nestedMD5 := hexMD5(nestedContent)

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":"a","type":"dir","uri":"a/","attributes":{}}`+
				`]}`)

		case "/files/a/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":".d8-meta","type":"file","uri":"a/.d8-meta","attributes":{"size":`+strconv.Itoa(len(nestedContent))+`}}`+
				`]}`)

		case "/files/a/.d8-meta":
			if r.Method == http.MethodHead {
				w.Header().Set("X-Attribute-Hash-Md5", nestedMD5)
			}

			http.ServeContent(w, r, ".d8-meta", time.Time{}, bytes.NewReader(nestedContent))

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

	if err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, 0, newFSFetcher(srv), codec, nil, nil,
	); err != nil {
		t.Fatalf("nested .d8-meta must download normally: %v", err)
	}

	entries := readTarContents(t, tarPath)

	if !bytes.Equal(entries["a/.d8-meta"], nestedContent) {
		t.Errorf("tar entry a/.d8-meta = %q; want %q", entries["a/.d8-meta"], nestedContent)
	}
}

// TestScanFSStagingSizes_ReadsNewMetadataPath verifies ScanFSStagingSizes
// (and, through it, ReadFSSizesSidecar) reads the sidecar from its new
// reserved-namespace location, crediting only files already present as flat
// staged blobs.
func TestScanFSStagingSizes_ReadsNewMetadataPath(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	stagedContent := bytes.Repeat([]byte("A"), 30)

	if err := os.WriteFile(filepath.Join(stagingDir, "a.bin"), stagedContent, 0o644); err != nil {
		t.Fatal(err)
	}

	metaDir := filepath.Join(stagingDir, volume.FSMetaDirName)
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}

	sidecarJSON, err := json.Marshal(volume.FSSizesSidecar{
		Files: map[string]int64{"a.bin": 30, "b.bin": 60},
		Total: 90,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(metaDir, volume.FSSizesSidecarName), sidecarJSON, 0o644); err != nil {
		t.Fatal(err)
	}

	total, staged, found, err := volume.ScanFSStagingSizes(context.Background(), stagingDir, "")
	if err != nil {
		t.Fatalf("ScanFSStagingSizes: %v", err)
	}

	if !found {
		t.Fatal("found = false; want true when the sidecar exists under the metadata dir")
	}

	if total != 90 {
		t.Errorf("total = %d; want 90", total)
	}

	if staged != 30 {
		t.Errorf("staged = %d; want 30 (only a.bin is present as a flat blob)", staged)
	}
}

// TestReadFSSizesSidecar_LegacyFallback verifies the backward-compatibility
// contract for resuming a tree written before the sidecar moved under
// FSMetaDirName: the new path wins when present; the legacy stagingDir/sizes.json
// is used only when the new path is absent; and an UNPARSEABLE legacy file
// (possibly a user file literally named sizes.json at codec none) is treated
// conservatively — reported as not-found and left untouched, never deleted or
// overwritten (the sidecar is a best-effort seed aid, so a lost seed is the
// worst acceptable outcome; wrong-bytes is not).
func TestReadFSSizesSidecar_LegacyFallback(t *testing.T) {
	t.Parallel()

	t.Run("NewPathPreferredOverLegacy", func(t *testing.T) {
		t.Parallel()

		stagingDir := t.TempDir()

		metaDir := filepath.Join(stagingDir, volume.FSMetaDirName)
		if err := os.MkdirAll(metaDir, 0o755); err != nil {
			t.Fatal(err)
		}

		newJSON, err := json.Marshal(volume.FSSizesSidecar{Files: map[string]int64{"x": 10}, Total: 10})
		if err != nil {
			t.Fatal(err)
		}

		if err := os.WriteFile(filepath.Join(metaDir, volume.FSSizesSidecarName), newJSON, 0o644); err != nil {
			t.Fatal(err)
		}

		legacyJSON, err := json.Marshal(volume.FSSizesSidecar{Files: map[string]int64{"y": 99}, Total: 99})
		if err != nil {
			t.Fatal(err)
		}

		if err := os.WriteFile(filepath.Join(stagingDir, volume.FSSizesSidecarName), legacyJSON, 0o644); err != nil {
			t.Fatal(err)
		}

		got, found, err := volume.ReadFSSizesSidecar(stagingDir)
		if err != nil || !found {
			t.Fatalf("ReadFSSizesSidecar: found=%v err=%v", found, err)
		}

		if got.Total != 10 {
			t.Errorf("Total = %d; want 10 (the new-path sidecar, not the legacy one)", got.Total)
		}
	})

	t.Run("LegacyUsedWhenNewAbsent", func(t *testing.T) {
		t.Parallel()

		stagingDir := t.TempDir()

		legacyJSON, err := json.Marshal(volume.FSSizesSidecar{Files: map[string]int64{"y": 42}, Total: 42})
		if err != nil {
			t.Fatal(err)
		}

		if err := os.WriteFile(filepath.Join(stagingDir, volume.FSSizesSidecarName), legacyJSON, 0o644); err != nil {
			t.Fatal(err)
		}

		got, found, err := volume.ReadFSSizesSidecar(stagingDir)
		if err != nil || !found {
			t.Fatalf("ReadFSSizesSidecar: found=%v err=%v", found, err)
		}

		if got.Total != 42 {
			t.Errorf("Total = %d; want 42 (legacy fallback)", got.Total)
		}
	})

	t.Run("UnparseableLegacyReturnsNotFoundAndLeavesFile", func(t *testing.T) {
		t.Parallel()

		stagingDir := t.TempDir()
		userBytes := []byte("a user file literally named sizes.json, not JSON at all\x00\xff")
		legacyPath := filepath.Join(stagingDir, volume.FSSizesSidecarName)

		if err := os.WriteFile(legacyPath, userBytes, 0o644); err != nil {
			t.Fatal(err)
		}

		got, found, err := volume.ReadFSSizesSidecar(stagingDir)
		if err != nil {
			t.Fatalf("ReadFSSizesSidecar must not error on an unparseable legacy file: %v", err)
		}

		if found {
			t.Error("found = true; want false for an unparseable legacy file (possible user data)")
		}

		if got.Total != 0 || len(got.Files) != 0 {
			t.Errorf("got non-zero sidecar %+v; want the zero value", got)
		}

		// The ambiguous file must be left exactly as-is.
		after, readErr := os.ReadFile(legacyPath)
		if readErr != nil {
			t.Fatalf("the legacy file must not be deleted: %v", readErr)
		}

		if !bytes.Equal(after, userBytes) {
			t.Error("the legacy file bytes must be left untouched")
		}
	})
}

// TestScanFSStagingProgress_CountsChunkDirsUnderReservedNamespace verifies the
// staging-progress accounting boundary after chunk dirs moved under the reserved
// metadata namespace: a real in-progress per-file chunk dir now lives at
// stagingDir/.d8-meta/chunks/<relPath><ext>.d (via FsFileChunksDirName) and MUST
// be counted, while a chunk-dir-shaped tree planted directly under .d8-meta
// (outside the chunks/ subtree — e.g. a stray artifact) must contribute nothing,
// because the scan descends ONLY into .d8-meta/chunks.
func TestScanFSStagingProgress_CountsChunkDirsUnderReservedNamespace(t *testing.T) {
	t.Parallel()

	const (
		chunkSize int64 = 100
		totalSize int64 = 100
	)

	codec := mustCodec(t, "zstd")
	ext := codec.Ext()

	stagingDir := t.TempDir()

	frame, err := codec.EncodeFrame(bytes.Repeat([]byte("A"), int(chunkSize)))
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}

	// A real in-progress per-file chunk dir, at its new reserved-namespace
	// location (.d8-meta/chunks/real.bin<ext>.d): one finalized chunk -> counted.
	realChunkDir := filepath.Join(stagingDir, filepath.FromSlash(archive.FsFileChunksDirName("real.bin", ext)))
	if err := os.MkdirAll(realChunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := archive.WriteChunkMeta(realChunkDir, archive.ChunkMeta{ChunkSize: chunkSize, TotalSize: totalSize}); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(realChunkDir, archive.ChunkFileName(0, ext)), frame, 0o644); err != nil {
		t.Fatal(err)
	}

	// A chunk-dir-shaped tree planted directly under .d8-meta but OUTSIDE the
	// chunks/ subtree. If the scan counted everything under .d8-meta it would
	// double the count; it must count only the chunks/ subtree.
	strayDir := filepath.Join(stagingDir, volume.FSMetaDirName, "stray.d")
	if err := os.MkdirAll(strayDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := archive.WriteChunkMeta(strayDir, archive.ChunkMeta{ChunkSize: chunkSize, TotalSize: totalSize}); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(strayDir, archive.ChunkFileName(0, ext)), frame, 0o644); err != nil {
		t.Fatal(err)
	}

	committed, err := volume.ScanFSStagingProgress(context.Background(), stagingDir, ext)
	if err != nil {
		t.Fatalf("ScanFSStagingProgress: %v", err)
	}

	if committed != chunkSize {
		t.Errorf("committed = %d; want %d (only the chunk dir under .d8-meta/chunks; the stray dir must be skipped)", committed, chunkSize)
	}
}

// TestDownloadFilesystemVolume_CodecNone_ChunkDirNameCannotClobberSiblingBlob
// pins the fs-reserved-suffix-collisions fix: at codec none, a chunked user file
// "payload" and a sibling user file named exactly like its OLD flat chunk-dir
// path ("payload.d") must coexist without cross-deletion, and a sibling dir named
// "other.d" must not interfere. Per-file chunk dirs now live under the reserved
// namespace (.d8-meta/chunks/), so MergeBlockChunks' post-merge
// os.RemoveAll(chunkDir) targets stagingDir/.d8-meta/chunks/payload.d — never the
// user's already-staged stagingDir/payload.d blob. The pre-seeded state
// (payload.d fully staged, payload's chunk dir complete) is the exact interleave
// in which the OLD flat layout deleted the user blob and forced a re-download.
func TestDownloadFilesystemVolume_CodecNone_ChunkDirNameCannotClobberSiblingBlob(t *testing.T) {
	t.Parallel()

	const chunkSize int64 = 100

	payload := []byte("PAYLOAD-CONTENT")         // chunked user file "payload"
	userD := []byte("USER-FILE-NAMED-PAYLOAD.D") // sibling user file "payload.d"
	payloadMD5 := hexMD5(payload)
	userDMD5 := hexMD5(userD)

	var (
		mu       sync.Mutex
		bodyGETs = map[string]int{}
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":"payload","type":"file","uri":"payload","attributes":{"size":`+strconv.Itoa(len(payload))+`}},`+
				`{"name":"payload.d","type":"file","uri":"payload.d","attributes":{"size":`+strconv.Itoa(len(userD))+`}},`+
				`{"name":"other.d","type":"dir","uri":"other.d/","attributes":{}}`+
				`]}`)

		case "/files/other.d/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[]}`)

		case "/files/payload":
			if r.Method == http.MethodHead {
				w.Header().Set("X-Attribute-Hash-Md5", payloadMD5)
			} else {
				mu.Lock()
				bodyGETs["payload"]++
				mu.Unlock()
			}

			http.ServeContent(w, r, "payload", time.Time{}, bytes.NewReader(payload))

		case "/files/payload.d":
			if r.Method == http.MethodHead {
				w.Header().Set("X-Attribute-Hash-Md5", userDMD5)
			} else {
				mu.Lock()
				bodyGETs["payload.d"]++
				mu.Unlock()
			}

			http.ServeContent(w, r, "payload.d", time.Time{}, bytes.NewReader(userD))

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

	// Pre-seed the exact crash-before-merge interleave:
	//   - payload.d fully staged as a flat blob at the staging root (user bytes);
	//   - payload's chunk dir complete (single chunk == raw content at codec none)
	//     under the reserved namespace, so its merge runs RemoveAll(chunkDir).
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(stagingDir, "payload.d"), userD, 0o644); err != nil {
		t.Fatal(err)
	}

	chunkDir := filepath.Join(stagingDir, filepath.FromSlash(archive.FsFileChunksDirName("payload", codec.Ext())))
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := archive.WriteChunkMeta(chunkDir, archive.ChunkMeta{ChunkSize: chunkSize, TotalSize: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}

	// codec none: the finalized chunk frame is the raw content verbatim.
	if err := os.WriteFile(filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())), payload, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, chunkSize, newFSFetcher(srv), codec, nil, nil,
	); err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	entries := readTarContents(t, tarPath)

	if !bytes.Equal(entries["payload"], payload) {
		t.Errorf("tar entry payload = %q; want %q", entries["payload"], payload)
	}

	if !bytes.Equal(entries["payload.d"], userD) {
		t.Errorf("tar entry payload.d = %q; want %q (user blob must survive payload's chunk-dir RemoveAll)", entries["payload.d"], userD)
	}

	mu.Lock()
	defer mu.Unlock()

	if bodyGETs["payload"] != 0 {
		t.Errorf("payload body GETs = %d; want 0 (its chunk dir was pre-seeded complete)", bodyGETs["payload"])
	}

	if bodyGETs["payload.d"] != 0 {
		t.Errorf("payload.d body GETs = %d; want 0 (pre-staged blob, MD5-verified, not re-downloaded)", bodyGETs["payload.d"])
	}
}

// TestDownloadFilesystemVolume_ChunkedResume_UsesReservedChunkDir verifies an
// interrupted chunked FS file download keeps its durable partial under the
// reserved chunk-dir location (.d8-meta/chunks/<relPath><ext>.d) and resumes
// from there on the next run.
func TestDownloadFilesystemVolume_ChunkedResume_UsesReservedChunkDir(t *testing.T) {
	t.Parallel()

	const (
		chunkSize int64 = 100
		cutBytes        = 20
	)

	content := bytes.Repeat([]byte("R"), 250) // 3 chunks: 100, 100, 50

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":"big.bin","type":"file","uri":"big.bin","attributes":{"size":`+strconv.Itoa(len(content))+`}}`+
				`]}`)

		case "/files/big.bin":
			if r.Method == http.MethodHead {
				w.Header().Set("X-Attribute-Hash-Md5", hexMD5(content))
			}

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
	codec := mustCodec(t, "none")

	// Run 1: interrupt during chunk 1's Range GET (call 1 = listing, call 2 =
	// source-hash HEAD, call 3 = chunk 0, call 4 = chunk 1).
	doer := &recordingDoer{inner: srv.Client(), cutOnCall: 4, cutBytes: cutBytes}
	fetcher := exporter.NewFetcher(doer)

	err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, chunkSize, fetcher, codec, nil, nil,
	)
	if !errors.Is(err, errSimulatedInterrupt) {
		t.Fatalf("expected errSimulatedInterrupt, got: %v", err)
	}

	// The durable partial must live under the reserved namespace, not beside the
	// staged blob.
	chunkDir := filepath.Join(stagingDir, filepath.FromSlash(archive.FsFileChunksDirName("big.bin", codec.Ext())))
	if _, statErr := os.Stat(chunkDir); statErr != nil {
		t.Fatalf("chunk dir must exist under the reserved namespace at %s: %v", chunkDir, statErr)
	}

	if _, statErr := os.Stat(filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext()))); statErr != nil {
		t.Errorf("chunk 0 must be finalized in the reserved chunk dir: %v", statErr)
	}

	// Run 2: resume to completion (the doer's call counter is now past cutOnCall,
	// so truncation never re-fires).
	if err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, chunkSize, fetcher, codec, nil, nil,
	); err != nil {
		t.Fatalf("resumed run must succeed: %v", err)
	}

	entries := readTarContents(t, tarPath)
	if !bytes.Equal(entries["big.bin"], content) {
		t.Error("resumed big.bin content mismatch")
	}
}

// ── same-origin item URI enforcement (fs-listing-enforce-same-origin-item-uri) ──

// newCountingEvilServer stands up an httptest.Server on its own port that counts
// EVERY request it receives. It plays the attacker host a hostile listing entry
// would redirect a credential-bearing GET to; a passing same-origin guard means
// this server must observe ZERO requests. It serves attacker bytes on any path so
// that a regression manifests as wrong content rather than a connection hang.
func newCountingEvilServer(t *testing.T) (*httptest.Server, func() int) {
	t.Helper()

	var (
		mu  sync.Mutex
		cnt int
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		cnt++
		mu.Unlock()

		_, _ = io.WriteString(w, "attacker-controlled-bytes")
	}))

	t.Cleanup(srv.Close)

	count := func() int {
		mu.Lock()
		defer mu.Unlock()

		return cnt
	}

	return srv, count
}

// TestDownloadFilesystemVolume_RejectsForeignOriginFileURI is the primary
// regression test for fs-listing-enforce-same-origin-item-uri: a listing file
// item whose (absolute) URI names a different host:port than the files-root base
// must be rejected with a wrapped ErrUnsafePath that names the offending URI,
// BEFORE any HTTP fetch — proven by a second httptest.Server (a different port,
// standing in for the attacker host) receiving ZERO requests. A plain errors.Is
// check alone would not prove the credential-bearing GET was never issued.
func TestDownloadFilesystemVolume_RejectsForeignOriginFileURI(t *testing.T) {
	t.Parallel()

	evilSrv, evilCount := newCountingEvilServer(t)
	leakURI := evilSrv.URL + "/leak"

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/files/" {
			http.NotFound(w, r)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
			`{"name":"leak.txt","type":"file","uri":"`+leakURI+`","attributes":{}}`+
			`]}`)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, 0, newFSFetcher(srv), mustCodec(t, "none"), nil, nil,
	)
	if err == nil {
		t.Fatal("expected a foreign-origin file URI to be rejected, got nil")
	}

	if !errors.Is(err, volume.ErrUnsafePath) {
		t.Errorf("expected errors.Is(err, ErrUnsafePath), got: %v", err)
	}

	if !strings.Contains(err.Error(), leakURI) {
		t.Errorf("error must name the offending URI %q, got: %v", leakURI, err)
	}

	if got := evilCount(); got != 0 {
		t.Errorf("evil server received %d request(s); want 0 (no fetch may reach a foreign origin)", got)
	}

	if _, statErr := os.Stat(tarPath); !os.IsNotExist(statErr) {
		t.Error("data.tar must not be created when a listing entry is rejected")
	}
}

// TestDownloadFilesystemVolume_RejectsForeignOriginDirURI proves the same guard
// covers the recursive directory walk: a "dir" item whose URI points at a foreign
// origin is rejected at the ingestion checkpoint, so the sub-listing GET (which
// would also carry the cluster credential) is never issued — the attacker server
// observes ZERO requests.
func TestDownloadFilesystemVolume_RejectsForeignOriginDirURI(t *testing.T) {
	t.Parallel()

	evilSrv, evilCount := newCountingEvilServer(t)
	dirURI := evilSrv.URL + "/subdir/"

	mux := http.NewServeMux()
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/files/" {
			http.NotFound(w, r)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
			`{"name":"subdir","type":"dir","uri":"`+dirURI+`","attributes":{}}`+
			`]}`)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, 0, newFSFetcher(srv), mustCodec(t, "none"), nil, nil,
	)
	if err == nil {
		t.Fatal("expected a foreign-origin dir URI to be rejected, got nil")
	}

	if !errors.Is(err, volume.ErrUnsafePath) {
		t.Errorf("expected errors.Is(err, ErrUnsafePath), got: %v", err)
	}

	if !strings.Contains(err.Error(), dirURI) {
		t.Errorf("error must name the offending dir URI %q, got: %v", dirURI, err)
	}

	if got := evilCount(); got != 0 {
		t.Errorf("evil server received %d request(s); want 0 (foreign dir must be rejected before recursion)", got)
	}
}

// TestDownloadFilesystemVolume_AcceptsRelativeAndSameOriginURIs verifies the
// guard rejects ONLY cross-origin URIs: a listing whose item URIs are relative
// (the real exporter's shape) and one whose item URI is ABSOLUTE but on the same
// scheme+host:port as the files root both download unchanged.
func TestDownloadFilesystemVolume_AcceptsRelativeAndSameOriginURIs(t *testing.T) {
	t.Parallel()

	t.Run("RelativeURI", func(t *testing.T) {
		t.Parallel()

		srv, files := fsTestServer(t)

		nodeDir := t.TempDir()
		tarPath := filepath.Join(nodeDir, archive.FsTarName)
		stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

		if err := volume.DownloadFilesystemVolume(
			context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
			2, 0, newFSFetcher(srv), mustCodec(t, "none"), nil, nil,
		); err != nil {
			t.Fatalf("relative-URI listing must download unchanged: %v", err)
		}

		entries := readTarContents(t, tarPath)

		for _, f := range files {
			if !bytes.Equal(entries[f.relPath], f.content) {
				t.Errorf("entry %q: got %q, want %q", f.relPath, entries[f.relPath], f.content)
			}
		}
	})

	t.Run("SameOriginAbsoluteURI", func(t *testing.T) {
		t.Parallel()

		content := []byte("same-origin-absolute-uri-content")

		mux := http.NewServeMux()
		mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/files/":
				// The item URI is ABSOLUTE but on the same origin as the files
				// root (built from the live request host) — it must be accepted.
				abs := "http://" + r.Host + "/files/root.txt"

				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
					`{"name":"root.txt","type":"file","uri":"`+abs+`","attributes":{}}`+
					`]}`)

			case "/files/root.txt":
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
			context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
			1, 0, newFSFetcher(srv), mustCodec(t, "none"), nil, nil,
		); err != nil {
			t.Fatalf("same-origin absolute URI must download unchanged: %v", err)
		}

		entries := readTarContents(t, tarPath)
		if !bytes.Equal(entries["root.txt"], content) {
			t.Errorf("entry root.txt: got %q, want %q", entries["root.txt"], content)
		}
	})
}

func TestDownloadFilesystemVolume_LargeFlatInventoryBoundedHeap(t *testing.T) {
	const (
		smallEntries = 500
		largeEntries = 30_000
		deltaLimit   = 4 << 20
	)

	smallPeak := measureLargeFlatInventoryHeap(t, smallEntries)
	largePeak := measureLargeFlatInventoryHeap(t, largeEntries)
	if largePeak > smallPeak+deltaLimit {
		t.Fatalf(
			"peak additional heap grew from %d to %d bytes for %d versus %d entries; count-dependent delta %d exceeds %d",
			smallPeak,
			largePeak,
			smallEntries,
			largeEntries,
			largePeak-smallPeak,
			deltaLimit,
		)
	}
}

func measureLargeFlatInventoryHeap(t *testing.T, entries int) uint64 {
	t.Helper()

	srv := newLargeLinkInventoryServer(t, entries, false)
	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	runtime.GC()

	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)

	stop := make(chan struct{})
	peak := make(chan uint64, 1)

	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()

		maxHeap := baseline.HeapAlloc
		for {
			select {
			case <-ticker.C:
				var current runtime.MemStats
				runtime.ReadMemStats(&current)
				if current.HeapAlloc > maxHeap {
					maxHeap = current.HeapAlloc
				}
			case <-stop:
				peak <- maxHeap

				return
			}
		}
	}()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := volume.DownloadFilesystemVolume(
		context.Background(), log, tarPath, stagingDir, srv.URL+"/files/",
		3, 0, newFSFetcher(srv), mustCodec(t, "none"), nil, nil,
	)
	close(stop)

	maxHeap := <-peak
	if err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	additional := uint64(0)
	if maxHeap > baseline.HeapAlloc {
		additional = maxHeap - baseline.HeapAlloc
	}

	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	reader := tar.NewReader(f)
	count := 0
	for {
		_, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		count++
	}

	if count != entries {
		t.Fatalf("tar entries = %d, want %d", count, entries)
	}

	return additional
}

func TestDownloadFilesystemVolume_LargeInventorySpillsBeforeFileMutation(t *testing.T) {
	const entries = 3_000

	var fileRequests atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/files/" {
			fileRequests.Add(1)
			http.Error(w, "stop before staging", http.StatusServiceUnavailable)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`)

		encoder := json.NewEncoder(w)
		for index := range entries {
			if index > 0 {
				_, _ = io.WriteString(w, ",")
			}

			if err := encoder.Encode(exporter.Item{
				Name:       fmt.Sprintf("file-%06d", index),
				Type:       "file",
				URI:        fmt.Sprintf("file-%06d", index),
				Attributes: map[string]any{"size": 1},
			}); err != nil {
				return
			}
		}

		_, _ = io.WriteString(w, `]}`)
	}))
	t.Cleanup(srv.Close)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)
	err := volume.DownloadFilesystemVolume(
		context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)),
		tarPath, stagingDir, srv.URL+"/files/",
		1, 0, newFSFetcher(srv), mustCodec(t, "none"), nil, nil,
	)
	if err == nil {
		t.Fatal("expected source-hash request to stop staging")
	}

	if fileRequests.Load() != 1 {
		t.Fatalf("file requests = %d, want exactly the first bounded worker", fileRequests.Load())
	}

	inventoryPath := filepath.Join(stagingDir, volume.FSMetaDirName, "inventory.jsonl")
	inventory, err := os.ReadFile(inventoryPath)
	if err != nil {
		t.Fatalf("read durable inventory spool: %v", err)
	}

	if lines := bytes.Count(inventory, []byte{'\n'}); lines != entries+2 {
		t.Fatalf("inventory spool lines = %d, want %d item+header+footer records", lines, entries+2)
	}

	if _, err := os.Stat(filepath.Join(stagingDir, volume.FSMetaDirName, "inventory.work")); !os.IsNotExist(err) {
		t.Fatalf("completed external merge left work runs: %v", err)
	}

	total, staged, found, err := volume.ScanFSStagingSizes(context.Background(), stagingDir, "")
	if err != nil {
		t.Fatalf("ScanFSStagingSizes: %v", err)
	}

	if !found || total != entries || staged != 0 {
		t.Fatalf("streamed sizes sidecar: found=%v total=%d staged=%d, want true,%d,0", found, total, staged, entries)
	}

	if matches, err := filepath.Glob(filepath.Join(stagingDir, "file-*")); err != nil || len(matches) != 0 {
		t.Fatalf("file staging mutated before inventory preflight: matches=%v err=%v", matches, err)
	}
}

func TestDownloadFilesystemVolume_PreservesCausalStagingWorkerError(t *testing.T) {
	const entries = 3_000

	errSourceMD5 := errors.New("source MD5 sentinel")
	errFileGET := errors.New("file GET sentinel")

	tests := []struct {
		name          string
		advertisedMD5 string
		wantErr       error
		newFetcher    func(*httptest.Server) (*exporter.Fetcher, []*observedDoer)
	}{
		{
			name:          "SourceMD5",
			advertisedMD5: md5Hex([]byte("x")),
			wantErr:       errSourceMD5,
			newFetcher: func(srv *httptest.Server) (*exporter.Fetcher, []*observedDoer) {
				sourceDoer := &observedDoer{
					do: func(*http.Request) (*http.Response, error) {
						return nil, errSourceMD5
					},
				}

				return exporter.NewFetcher(srv.Client(), exporter.WithSourceHashDoer(sourceDoer)), []*observedDoer{sourceDoer}
			},
		},
		{
			name:          "FileGET",
			advertisedMD5: md5Hex([]byte("x")),
			wantErr:       errFileGET,
			newFetcher: func(srv *httptest.Server) (*exporter.Fetcher, []*observedDoer) {
				fileDoer := &observedDoer{
					do: func(req *http.Request) (*http.Response, error) {
						if req.Method == http.MethodGet && req.URL.Path != "/files/" {
							return nil, errFileGET
						}

						return srv.Client().Do(req)
					},
				}

				return exporter.NewFetcher(fileDoer, exporter.WithSourceHashDoer(srv.Client())), []*observedDoer{fileDoer}
			},
		},
		{
			name:          "SourceDigest",
			advertisedMD5: md5Hex([]byte("different")),
			wantErr:       volume.ErrSourceHashMismatch,
			newFetcher: func(srv *httptest.Server) (*exporter.Fetcher, []*observedDoer) {
				return exporter.NewFetcher(srv.Client()), nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newLargeFileInventoryServer(t, entries, tt.advertisedMD5)
			fetcher, observed := tt.newFetcher(srv)
			nodeDir := t.TempDir()
			tarPath := filepath.Join(nodeDir, archive.FsTarName)
			stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

			err := volume.DownloadFilesystemVolume(
				context.Background(),
				slog.New(slog.NewTextHandler(io.Discard, nil)),
				tarPath,
				stagingDir,
				srv.URL+"/files/",
				1,
				0,
				fetcher,
				mustCodec(t, "none"),
				nil,
				nil,
			)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("DownloadFilesystemVolume error = %v, want errors.Is(_, %v)", err, tt.wantErr)
			}

			for _, doer := range observed {
				requireObservedDoerQuiescent(t, doer)
			}

			if _, statErr := os.Stat(tarPath); !os.IsNotExist(statErr) {
				t.Fatalf("causal worker failure committed data.tar: %v", statErr)
			}
		})
	}
}

func TestDownloadFilesystemVolume_InventoryErrorRemainsCausal(t *testing.T) {
	const entries = 50

	srv := newLargeFileInventoryServer(t, entries, md5Hex([]byte("x")))
	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)
	inventoryPath := filepath.Join(stagingDir, volume.FSMetaDirName, "inventory.jsonl")

	var truncateOnce sync.Once

	sourceDoer := &observedDoer{
		do: func(req *http.Request) (*http.Response, error) {
			var truncateErr error

			truncateOnce.Do(func() {
				info, err := os.Stat(inventoryPath)
				if err != nil {
					truncateErr = err

					return
				}

				truncateErr = os.Truncate(inventoryPath, info.Size()-64)
			})
			if truncateErr != nil {
				return nil, fmt.Errorf("truncate inventory fixture: %w", truncateErr)
			}

			return srv.Client().Do(req)
		},
	}

	err := volume.DownloadFilesystemVolume(
		context.Background(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		tarPath,
		stagingDir,
		srv.URL+"/files/",
		1,
		0,
		exporter.NewFetcher(srv.Client(), exporter.WithSourceHashDoer(sourceDoer)),
		mustCodec(t, "none"),
		nil,
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "filesystem inventory spool is corrupt") {
		t.Fatalf("DownloadFilesystemVolume error = %v, want independent inventory corruption", err)
	}

	if errors.Is(err, context.Canceled) {
		t.Fatalf("independent inventory corruption was replaced by cancellation: %v", err)
	}

	requireObservedDoerQuiescent(t, sourceDoer)
}

func TestDownloadFilesystemVolume_StagingCallerCancellationRemainsCausal(t *testing.T) {
	const entries = 3_000

	srv := newLargeFileInventoryServer(t, entries, md5Hex([]byte("x")))
	ctx, cancel := context.WithCancel(context.Background())

	sourceDoer := &observedDoer{
		do: func(req *http.Request) (*http.Response, error) {
			cancel()
			<-req.Context().Done()

			return nil, req.Context().Err()
		},
	}

	nodeDir := t.TempDir()
	err := volume.DownloadFilesystemVolume(
		ctx,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		filepath.Join(nodeDir, archive.FsTarName),
		filepath.Join(nodeDir, archive.FsTarStagingDirName),
		srv.URL+"/files/",
		1,
		0,
		exporter.NewFetcher(srv.Client(), exporter.WithSourceHashDoer(sourceDoer)),
		mustCodec(t, "none"),
		nil,
		nil,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DownloadFilesystemVolume error = %v, want parent context cancellation", err)
	}

	requireObservedDoerQuiescent(t, sourceDoer)
}

func md5Hex(data []byte) string {
	sum := md5.Sum(data)

	return hex.EncodeToString(sum[:])
}

func newLargeFileInventoryServer(t *testing.T, entries int, advertisedMD5 string) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/files/" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`)

			encoder := json.NewEncoder(w)
			for index := range entries {
				if index > 0 {
					_, _ = io.WriteString(w, ",")
				}

				if err := encoder.Encode(exporter.Item{
					Name:       fmt.Sprintf("file-%06d", index),
					Type:       "file",
					URI:        fmt.Sprintf("file-%06d", index),
					Attributes: map[string]any{"size": 1},
				}); err != nil {
					return
				}
			}

			_, _ = io.WriteString(w, `]}`)

			return
		}

		if r.Method == http.MethodHead {
			w.Header().Set("X-Attribute-Hash-Md5", advertisedMD5)
		}

		http.ServeContent(w, r, filepath.Base(r.URL.Path), time.Time{}, strings.NewReader("x"))
	}))
	t.Cleanup(srv.Close)

	return srv
}

type observedDoer struct {
	do     func(*http.Request) (*http.Response, error)
	active atomic.Int64
	calls  atomic.Int64
}

func (d *observedDoer) Do(req *http.Request) (*http.Response, error) {
	d.calls.Add(1)
	d.active.Add(1)

	defer d.active.Add(-1)

	return d.do(req)
}

func requireObservedDoerQuiescent(t *testing.T, doer *observedDoer) {
	t.Helper()

	if active := doer.active.Load(); active != 0 {
		t.Fatalf("active requests after return = %d, want 0", active)
	}

	calls := doer.calls.Load()
	for range 10 {
		runtime.Gosched()
	}

	if current := doer.calls.Load(); current != calls {
		t.Fatalf("request count changed after return: %d -> %d", calls, current)
	}
}

func TestDownloadFilesystemVolume_CancelDuringTarAssemblyPreservesInventoryForRetry(t *testing.T) {
	content := bytes.Repeat([]byte("sole-final-filesystem-entry-"), 160<<10)
	contentMD5 := md5Hex(content)

	var listingRequests atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/files/" {
			listingRequests.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(
				w,
				`{"apiVersion":"v1","items":[{"name":"large.bin","type":"file","uri":"large.bin","attributes":{"size":%d}}]}`,
				len(content),
			)

			return
		}

		if r.Method == http.MethodHead {
			w.Header().Set("X-Attribute-Hash-Md5", contentMD5)
		}

		http.ServeContent(w, r, "large.bin", time.Time{}, bytes.NewReader(content))
	}))
	t.Cleanup(srv.Close)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)
	inventoryPath := filepath.Join(stagingDir, volume.FSMetaDirName, "inventory.jsonl")
	stagedPath := filepath.Join(stagingDir, "large.bin")
	ctx := &cancelWhenTempGrowsContext{
		Context:   context.Background(),
		tempPath:  tarPath + ".tmp",
		threshold: 96 << 10,
	}

	err := volume.DownloadFilesystemVolume(
		ctx,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		tarPath,
		stagingDir,
		srv.URL+"/files/",
		1,
		1<<20,
		newFSFetcher(srv),
		mustCodec(t, "none"),
		nil,
		nil,
	)
	require.ErrorIs(t, err, context.Canceled)
	require.LessOrEqual(t, ctx.triggerSize, ctx.threshold+(128<<10))

	_, err = os.Stat(tarPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(tarPath + ".tmp")
	require.ErrorIs(t, err, os.ErrNotExist)

	inventoryBefore, err := os.ReadFile(inventoryPath)
	require.NoError(t, err, "valid inventory must survive cancelled assembly")
	require.NotEmpty(t, inventoryBefore)

	movedPath := stagedPath + ".moved"
	require.NoError(t, os.Rename(stagedPath, movedPath), "cancelled assembly must close the staged file")
	require.NoError(t, os.Rename(movedPath, stagedPath))

	require.NoError(t, volume.DownloadFilesystemVolume(
		context.Background(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		tarPath,
		stagingDir,
		srv.URL+"/files/",
		1,
		1<<20,
		newFSFetcher(srv),
		mustCodec(t, "none"),
		nil,
		nil,
	))
	require.Equal(t, int64(1), listingRequests.Load(), "retry must reuse the preserved valid inventory")

	retried, err := os.ReadFile(tarPath)
	require.NoError(t, err)

	freshNodeDir := t.TempDir()
	freshTarPath := filepath.Join(freshNodeDir, archive.FsTarName)
	require.NoError(t, volume.DownloadFilesystemVolume(
		context.Background(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		freshTarPath,
		filepath.Join(freshNodeDir, archive.FsTarStagingDirName),
		srv.URL+"/files/",
		1,
		1<<20,
		newFSFetcher(srv),
		mustCodec(t, "none"),
		nil,
		nil,
	))

	uninterrupted, err := os.ReadFile(freshTarPath)
	require.NoError(t, err)
	require.Equal(t, uninterrupted, retried, "retry must match an uninterrupted deterministic tar")
}

func newLargeLinkInventoryServer(t *testing.T, entries int, reverse bool) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/files/" {
			http.NotFound(w, r)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`)

		encoder := json.NewEncoder(w)
		for index := range entries {
			if index > 0 {
				_, _ = io.WriteString(w, ",")
			}

			itemIndex := index
			if reverse {
				itemIndex = entries - index - 1
			}

			if err := encoder.Encode(exporter.Item{
				Name:       fmt.Sprintf("link-%08d", itemIndex),
				Type:       "link",
				TargetPath: "target",
				Attributes: map[string]any{},
			}); err != nil {
				return
			}
		}

		_, _ = io.WriteString(w, `]}`)
	}))
	t.Cleanup(srv.Close)

	return srv
}

type repeatedByteReader byte

func (r repeatedByteReader) Read(p []byte) (int, error) {
	for index := range p {
		p[index] = byte(r)
	}

	return len(p), nil
}

func TestDownloadFilesystemVolume_DeepInventoryUsesDiskQueue(t *testing.T) {
	const depth = 300

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/files/") {
			http.NotFound(w, r)

			return
		}

		currentDepth := strings.Count(strings.TrimPrefix(r.URL.Path, "/files/"), "d/")
		w.Header().Set("Content-Type", "application/json")

		if currentDepth < depth {
			nextURI := strings.Repeat("d/", currentDepth+1)
			_, _ = fmt.Fprintf(w, `{"apiVersion":"v1","items":[{"name":"d","type":"dir","uri":%q,"attributes":{}}]}`, nextURI)

			return
		}

		_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[{"name":"link","type":"link","uri":"","targetPath":"target","attributes":{}}]}`)
	}))
	t.Cleanup(srv.Close)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	err := volume.DownloadFilesystemVolume(
		context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)),
		tarPath, filepath.Join(nodeDir, archive.FsTarStagingDirName), srv.URL+"/files/",
		1, 0, newFSFetcher(srv), mustCodec(t, "none"), nil, nil,
	)
	if err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	headers, _ := readTar(t, tarPath)
	if len(headers) != depth+1 {
		t.Fatalf("tar entries = %d, want %d", len(headers), depth+1)
	}
}

func TestDownloadFilesystemVolume_InventoryCancellationRebuildsCleanly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	interrupted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`)

		encoder := json.NewEncoder(w)
		for index := range 2_000 {
			if index > 0 {
				_, _ = io.WriteString(w, ",")
			}

			if err := encoder.Encode(exporter.Item{
				Name:       fmt.Sprintf("link-%06d", index),
				Type:       "link",
				TargetPath: "target",
				Attributes: map[string]any{},
			}); err != nil {
				return
			}

			if index == 500 {
				cancel()

				return
			}
		}
	}))
	t.Cleanup(interrupted.Close)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)

	err := volume.DownloadFilesystemVolume(
		ctx, slog.Default(), tarPath, stagingDir, interrupted.URL+"/files/",
		1, 0, newFSFetcher(interrupted), mustCodec(t, "none"), nil, nil,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted inventory error = %v, want context.Canceled", err)
	}

	if _, err := os.Stat(tarPath); !os.IsNotExist(err) {
		t.Fatalf("cancelled inventory published tar: %v", err)
	}

	if _, err := os.Stat(filepath.Join(stagingDir, volume.FSMetaDirName, "inventory.work")); !os.IsNotExist(err) {
		t.Fatalf("graceful cancellation left inventory work state: %v", err)
	}

	healthy := newLargeLinkInventoryServer(t, 2_000, true)
	err = volume.DownloadFilesystemVolume(
		context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)),
		tarPath, stagingDir, healthy.URL+"/files/",
		1, 0, newFSFetcher(healthy), mustCodec(t, "none"), nil, nil,
	)
	if err != nil {
		t.Fatalf("resume after cancelled inventory: %v", err)
	}
}

func TestDownloadFilesystemVolume_CorruptInventoryIsRebuilt(t *testing.T) {
	var listings atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/files/" {
			http.NotFound(w, r)

			return
		}

		listings.Add(1)
		_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[{"name":"link","type":"link","uri":"","targetPath":"target","attributes":{}}]}`)
	}))
	t.Cleanup(srv.Close)

	nodeDir := t.TempDir()
	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)
	metaDir := filepath.Join(stagingDir, volume.FSMetaDirName)
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(metaDir, "inventory.jsonl"), []byte("{truncated"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := volume.DownloadFilesystemVolume(
		context.Background(), slog.Default(), tarPath, stagingDir, srv.URL+"/files/",
		1, 0, newFSFetcher(srv), mustCodec(t, "none"), nil, nil,
	)
	if err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	if listings.Load() != 1 {
		t.Fatalf("listing requests = %d, want 1 rebuild", listings.Load())
	}
}

func TestDownloadFilesystemVolume_RejectsInventoryPathConflictsBeforeFetch(t *testing.T) {
	tests := []struct {
		name  string
		codec string
		items []exporter.Item
	}{
		{
			name:  "duplicate",
			codec: "none",
			items: []exporter.Item{
				{Name: "same", Type: "file", URI: "first", Attributes: map[string]any{"size": 1}},
				{Name: "same", Type: "file", URI: "second", Attributes: map[string]any{"size": 1}},
			},
		},
		{
			name:  "file-directory conflict",
			codec: "none",
			items: []exporter.Item{
				{Name: "a", Type: "file", URI: "a", Attributes: map[string]any{"size": 1}},
				{Name: "a", Type: "dir", URI: "a/", Attributes: map[string]any{}},
			},
		},
		{
			name:  "codec stored-path collision",
			codec: "zstd",
			items: []exporter.Item{
				{Name: "a", Type: "file", URI: "a", Attributes: map[string]any{"size": 1}},
				{Name: "a.zst", Type: "link", TargetPath: "target", Attributes: map[string]any{}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var bodyCalls atomic.Int64

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/files/" {
					_ = json.NewEncoder(w).Encode(struct {
						APIVersion string          `json:"apiVersion"`
						Items      []exporter.Item `json:"items"`
					}{
						APIVersion: "v1",
						Items:      test.items,
					})

					return
				}

				if r.URL.Path == "/files/a/" {
					_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[]}`)

					return
				}

				bodyCalls.Add(1)
				_, _ = io.WriteString(w, "x")
			}))
			t.Cleanup(srv.Close)

			nodeDir := t.TempDir()
			tarPath := filepath.Join(nodeDir, archive.FsTarName)
			err := volume.DownloadFilesystemVolume(
				context.Background(), slog.Default(), tarPath,
				filepath.Join(nodeDir, archive.FsTarStagingDirName), srv.URL+"/files/",
				2, 0, newFSFetcher(srv), mustCodec(t, test.codec), nil, nil,
			)
			if err == nil || !strings.Contains(err.Error(), "conflicting paths") {
				t.Fatalf("conflicting inventory error = %v", err)
			}

			if bodyCalls.Load() != 0 {
				t.Fatalf("file requests before conflict rejection = %d, want 0", bodyCalls.Load())
			}

			if _, statErr := os.Stat(tarPath); !os.IsNotExist(statErr) {
				t.Fatalf("conflicting inventory published tar: %v", statErr)
			}
		})
	}
}

func TestDownloadFilesystemVolume_DeterministicAcrossListingOrder(t *testing.T) {
	const entries = 2_500

	run := func(reverse bool) []byte {
		srv := newLargeLinkInventoryServer(t, entries, reverse)
		nodeDir := t.TempDir()
		tarPath := filepath.Join(nodeDir, archive.FsTarName)

		err := volume.DownloadFilesystemVolume(
			context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)),
			tarPath, filepath.Join(nodeDir, archive.FsTarStagingDirName), srv.URL+"/files/",
			2, 0, newFSFetcher(srv), mustCodec(t, "none"), nil, nil,
		)
		if err != nil {
			t.Fatalf("DownloadFilesystemVolume: %v", err)
		}

		data, err := os.ReadFile(tarPath)
		if err != nil {
			t.Fatal(err)
		}

		return data
	}

	forward := run(false)
	reverse := run(true)
	if !bytes.Equal(forward, reverse) {
		t.Fatal("data.tar differs for equivalent forward and reverse producer listings")
	}
}
