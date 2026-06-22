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
		newFSFetcher(srv),
		mustCodec(t, "none"),
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

	if err := volume.DownloadFilesystemVolume(context.Background(), slog.Default(), tarPath, stagingDir, rootURL, 1, newFSFetcher(srv), mustCodec(t, "none")); err != nil {
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

	if err := volume.DownloadFilesystemVolume(context.Background(), slog.Default(), tarPath, stagingDir, rootURL, 1, newFSFetcher(srv), mustCodec(t, "none")); err != nil {
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

	if err := volume.DownloadFilesystemVolume(context.Background(), slog.Default(), tarPath, stagingDir, rootURL, 1, newFSFetcher(srv), mustCodec(t, "none")); err != nil {
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
		newFSFetcher(srv),
		codec,
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

	if err := volume.DownloadFilesystemVolume(context.Background(), slog.Default(), tarPath, stagingDir, rootURL, 1, newFSFetcher(srv), codec); err != nil {
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

	if err := volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		tarPath,
		stagingDir,
		srv.URL+"/files/",
		1,
		newFSFetcher(srv),
		mustCodec(t, "none"),
	); err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
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
