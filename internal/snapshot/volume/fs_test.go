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
	"testing"
	"time"

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

func TestDownloadFilesystemVolume_DownloadsTree(t *testing.T) {
	srv, files := fsTestServer(t)

	enc, err := compress.NewEncoder(compress.LevelDefault)
	if err != nil {
		t.Fatalf("compress.NewEncoder: %v", err)
	}

	nodeDir := t.TempDir()
	dataDir := filepath.Join(nodeDir, archive.DataDirName)
	rootURL := srv.URL + "/files/"

	err = volume.DownloadFilesystemVolume(
		context.Background(),
		slog.Default(),
		dataDir,
		rootURL,
		2,
		newFSFetcher(srv),
		enc,
	)
	if err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	// Verify each expected file.
	for _, f := range files {
		destRel := filepath.FromSlash(archive.FsFileName(f.relPath))
		destPath := filepath.Join(dataDir, destRel)

		raw, err := os.ReadFile(destPath)
		if err != nil {
			t.Fatalf("read %s: %v", destPath, err)
		}

		decoded := decodeZstdStream(t, raw)

		if !bytes.Equal(decoded, f.content) {
			t.Errorf("file %s: got %q, want %q", f.relPath, decoded, f.content)
		}
	}

	// The link must NOT produce an output file.
	linkDest := filepath.Join(dataDir, "symlink.txt.zst")
	if _, err := os.Stat(linkDest); !os.IsNotExist(err) {
		t.Error("symlink should not produce an output file")
	}
}

func TestDownloadFilesystemVolume_SkipsExistingFiles(t *testing.T) {
	srv, files := fsTestServer(t)

	enc, err := compress.NewEncoder(compress.LevelDefault)
	if err != nil {
		t.Fatalf("compress.NewEncoder: %v", err)
	}

	nodeDir := t.TempDir()
	dataDir := filepath.Join(nodeDir, archive.DataDirName)
	rootURL := srv.URL + "/files/"

	// First run.
	if err := volume.DownloadFilesystemVolume(context.Background(), slog.Default(), dataDir, rootURL, 1, newFSFetcher(srv), enc); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// Capture modification times.
	type modEntry struct {
		path    string
		modTime time.Time
	}

	var modTimes []modEntry

	for _, f := range files {
		destRel := filepath.FromSlash(archive.FsFileName(f.relPath))
		destPath := filepath.Join(dataDir, destRel)

		fi, err := os.Stat(destPath)
		if err != nil {
			t.Fatalf("stat after first run: %v", err)
		}

		modTimes = append(modTimes, modEntry{path: destPath, modTime: fi.ModTime()})
	}

	// Second run (all files already present).
	if err := volume.DownloadFilesystemVolume(context.Background(), slog.Default(), dataDir, rootURL, 1, newFSFetcher(srv), enc); err != nil {
		t.Fatalf("second run: %v", err)
	}

	for _, m := range modTimes {
		fi, err := os.Stat(m.path)
		if err != nil {
			t.Fatalf("stat after second run: %v", err)
		}

		if !fi.ModTime().Equal(m.modTime) {
			t.Errorf("file %s was modified on second run (should be skipped)", m.path)
		}
	}
}

func TestDownloadFilesystemVolume_CleansStaleTmp(t *testing.T) {
	srv, _ := fsTestServer(t)

	enc, err := compress.NewEncoder(compress.LevelDefault)
	if err != nil {
		t.Fatalf("compress.NewEncoder: %v", err)
	}

	nodeDir := t.TempDir()
	dataDir := filepath.Join(nodeDir, archive.DataDirName)

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Plant a stale .tmp for root.txt.zst.
	staleTmp := filepath.Join(dataDir, "root.txt.zst.tmp")
	if err := os.WriteFile(staleTmp, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}

	rootURL := srv.URL + "/files/"

	if err := volume.DownloadFilesystemVolume(context.Background(), slog.Default(), dataDir, rootURL, 1, newFSFetcher(srv), enc); err != nil {
		t.Fatalf("DownloadFilesystemVolume: %v", err)
	}

	// Stale .tmp must be gone.
	if _, err := os.Stat(staleTmp); !os.IsNotExist(err) {
		t.Error("stale .tmp should have been removed")
	}

	// Final file must exist.
	dest := filepath.Join(dataDir, "root.txt.zst")
	if _, err := os.Stat(dest); err != nil {
		t.Errorf("root.txt.zst should exist after download: %v", err)
	}
}
