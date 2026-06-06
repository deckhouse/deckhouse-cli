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

package pipeline_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/pipeline"
)

// ---- helpers ----------------------------------------------------------------

// gunzipAll decompresses a possibly multi-member gzip stream and returns all bytes.
func gunzipAll(t *testing.T, data []byte) []byte {
	t.Helper()

	var out []byte

	r := bytes.NewReader(data)

	for r.Len() > 0 {
		gz, err := gzip.NewReader(r)
		if err != nil {
			t.Fatalf("gzip.NewReader: %v", err)
		}

		chunk, err := io.ReadAll(gz)
		if err != nil {
			t.Fatalf("decompress member: %v", err)
		}

		if err := gz.Close(); err != nil {
			t.Fatalf("close gzip reader: %v", err)
		}

		out = append(out, chunk...)
	}

	return out
}

// newVolumeMeta returns a minimal archive.Meta for volume compression tests.
func newVolumeMeta(id string) archive.Meta {
	return archive.Meta{
		Magic:         archive.Magic,
		SchemaVersion: archive.SchemaVersion,
		ArchiveID:     id,
		CreatedAt:     time.Now().UTC(),
		CreatedBy:     archive.Creator{Tool: "d8", Version: "test"},
		Source: archive.Source{
			Namespace:    "test",
			RootSnapshot: archive.SnapshotRef{Name: "s"},
		},
		Selection: archive.Selection{Mode: archive.SelectionFull, RootNodeID: "Snapshot--s"},
	}
}

// dirListing returns the JSON the data-exporter HTTP server returns for a directory.
func dirListing(items []map[string]string) []byte {
	type listing struct {
		Items []map[string]string `json:"items"`
	}

	data, _ := json.Marshal(listing{Items: items})

	return data
}

// writeGzipFile creates a gzip-compressed file at path containing data.
func writeGzipFile(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	defer f.Close()

	gz := gzip.NewWriter(f)

	if _, err := gz.Write(data); err != nil {
		return err
	}

	return gz.Close()
}

// downloadFilesystemInTest replicates the filesystem download logic inline
// (mirroring recursiveVolumeDownload) so we can test the gzip/raw paths and
// per-file resume behaviour without needing to export the internal functions.
func downloadFilesystemInTest(
	t *testing.T,
	serverURL string,
	dstRootDir string,
	useGzip bool,
) {
	t.Helper()

	client := &http.Client{}

	var rec func(srcPath, dstDir string) error
	rec = func(srcPath, dstDir string) error {
		isDir := strings.HasSuffix(srcPath, "/")

		// Per-file resume: skip the HTTP request entirely if the .gz already exists.
		if !isDir && useGzip {
			if _, err := os.Stat(strings.TrimSuffix(dstDir, "/") + ".gz"); err == nil {
				return nil
			}
		}

		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, serverURL+srcPath, nil)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			return fmt.Errorf("HTTP %d %s: %s", resp.StatusCode, srcPath, body)
		}

		if isDir {
			var listing struct {
				Items []struct {
					Name string `json:"name"`
					Type string `json:"type"`
				} `json:"items"`
			}

			if err := json.NewDecoder(resp.Body).Decode(&listing); err != nil {
				return fmt.Errorf("decode listing: %w", err)
			}

			for _, item := range listing.Items {
				childPath := srcPath + item.Name
				childDst := filepath.Join(dstDir, item.Name)

				if item.Type == "dir" {
					if err := os.MkdirAll(childDst, 0o755); err != nil {
						return err
					}

					childPath += "/"
					childDst += "/"
				}

				if err := rec(childPath, childDst); err != nil {
					return err
				}
			}

			return nil
		}

		// Leaf file.
		if useGzip {
			outPath := strings.TrimSuffix(dstDir, "/") + ".gz"

			// Per-file resume: skip if .gz already exists.
			if _, err := os.Stat(outPath); err == nil {
				return nil
			}

			tmpPath := outPath + ".tmp"

			f, err := os.Create(tmpPath)
			if err != nil {
				return err
			}

			gz := gzip.NewWriter(f)

			if _, copyErr := io.Copy(gz, resp.Body); copyErr != nil {
				_ = f.Close()
				_ = os.Remove(tmpPath)
				return copyErr
			}

			if err := gz.Close(); err != nil {
				_ = f.Close()
				_ = os.Remove(tmpPath)
				return err
			}

			_ = f.Sync()
			_ = f.Close()

			return os.Rename(tmpPath, outPath)
		}

		// Raw mode.
		outPath := strings.TrimSuffix(dstDir, "/")

		f, err := os.Create(outPath)
		if err != nil {
			return err
		}

		defer f.Close()

		_, err = io.Copy(f, resp.Body)

		return err
	}

	if err := rec("/", dstRootDir+"/"); err != nil {
		t.Fatalf("downloadFilesystemInTest: %v", err)
	}
}

// ---- Filesystem gzip tests --------------------------------------------------

// TestFilesystemVolumeGzip_PerFileResume verifies that already-downloaded .gz
// files are skipped on resume (per-file resume behaviour).
func TestFilesystemVolumeGzip_PerFileResume(t *testing.T) {
	files := map[string]string{
		"alpha.txt": "content of alpha",
		"beta.txt":  "content of beta",
		"gamma.txt": "content of gamma",
	}

	var fetchCount int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/" {
			listing := dirListing([]map[string]string{
				{"name": "alpha.txt", "type": "file"},
				{"name": "beta.txt", "type": "file"},
				{"name": "gamma.txt", "type": "file"},
			})
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(listing)
			return
		}

		name := strings.TrimPrefix(path, "/")

		content, ok := files[name]
		if !ok {
			http.NotFound(w, r)
			return
		}

		fetchCount++
		_, _ = w.Write([]byte(content))
	}))
	defer srv.Close()

	dataDir := t.TempDir()

	// Pre-create alpha.gz to simulate a previously downloaded file.
	preExistingGz := filepath.Join(dataDir, "alpha.txt.gz")
	if err := writeGzipFile(preExistingGz, []byte("content of alpha")); err != nil {
		t.Fatalf("pre-create alpha.gz: %v", err)
	}

	fetchCount = 0 // reset after pre-setup phase

	downloadFilesystemInTest(t, srv.URL, dataDir, true /*useGzip*/)

	// Only beta.txt and gamma.txt should have been fetched (alpha was pre-existing).
	if fetchCount != 2 {
		t.Errorf("fetchCount = %d, want 2 (alpha.txt.gz should be skipped)", fetchCount)
	}

	// All three files must exist as .gz.
	for _, name := range []string{"alpha.txt.gz", "beta.txt.gz", "gamma.txt.gz"} {
		p := filepath.Join(dataDir, name)
		if _, err := os.Stat(p); err != nil {
			t.Errorf("expected %s to exist: %v", name, err)
		}
	}

	// Verify that newly downloaded files decompress correctly.
	for _, tc := range []struct {
		gz   string
		want string
	}{
		{"beta.txt.gz", "content of beta"},
		{"gamma.txt.gz", "content of gamma"},
	} {
		data, err := os.ReadFile(filepath.Join(dataDir, tc.gz))
		if err != nil {
			t.Fatalf("read %s: %v", tc.gz, err)
		}

		got := gunzipAll(t, data)

		if string(got) != tc.want {
			t.Errorf("%s: decompressed = %q, want %q", tc.gz, string(got), tc.want)
		}
	}
}

// TestFilesystemVolumeGzip_AtomicWrite verifies that even when we simulate a
// crash (tmp file left behind), the next run replaces it atomically.
func TestFilesystemVolumeGzip_AtomicWrite(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			_, _ = w.Write(dirListing([]map[string]string{{"name": "data.bin", "type": "file"}}))
			return
		}

		_, _ = w.Write(bytes.Repeat([]byte{0xAB}, 128))
	}))
	defer srv.Close()

	dataDir := t.TempDir()

	// Leave a stale .tmp file to simulate an interrupted write.
	stale := filepath.Join(dataDir, "data.bin.gz.tmp")
	if err := os.WriteFile(stale, []byte("stale garbage"), 0o644); err != nil {
		t.Fatalf("write stale tmp: %v", err)
	}

	downloadFilesystemInTest(t, srv.URL, dataDir, true)

	// .gz must exist and be valid.
	data, err := os.ReadFile(filepath.Join(dataDir, "data.bin.gz"))
	if err != nil {
		t.Fatalf("read data.bin.gz: %v", err)
	}

	got := gunzipAll(t, data)

	if !bytes.Equal(got, bytes.Repeat([]byte{0xAB}, 128)) {
		t.Errorf("unexpected decompressed content (len=%d)", len(got))
	}
}

// TestFilesystemVolumeNone_RawFiles verifies that with useGzip=false,
// files are stored uncompressed without a .gz extension.
func TestFilesystemVolumeNone_RawFiles(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			_, _ = w.Write(dirListing([]map[string]string{{"name": "readme.md", "type": "file"}}))
			return
		}

		_, _ = w.Write([]byte("# Hello World"))
	}))
	defer srv.Close()

	dataDir := t.TempDir()

	downloadFilesystemInTest(t, srv.URL, dataDir, false /*useGzip*/)

	// File must exist without .gz extension.
	data, err := os.ReadFile(filepath.Join(dataDir, "readme.md"))
	if err != nil {
		t.Fatalf("read readme.md: %v", err)
	}

	if string(data) != "# Hello World" {
		t.Errorf("content = %q, want %q", string(data), "# Hello World")
	}

	// .gz must NOT exist.
	if _, err := os.Stat(filepath.Join(dataDir, "readme.md.gz")); err == nil {
		t.Error("unexpected .gz file when useGzip=false")
	}
}

// ---- pipeline.Run integration tests for VolumeCompression ------------------

// TestRun_VolumeCompression_Default verifies that when VolumeCompression is
// unset, it defaults to "gzip" before being passed to DownloadNodeVolumesFunc.
func TestRun_VolumeCompression_Default(t *testing.T) {
	var gotCompression string

	checkCompVol := func(_ context.Context, req pipeline.NodeVolumesRequest) error {
		gotCompression = req.Options.VolumeCompression

		for _, dr := range req.Node.DataRefs {
			if err := req.Writer.AppendVolumeProgress(archive.VolumeProgressRecord{
				NodeID:      req.Node.ID,
				VSCName:     dr.VSCName,
				VolumeMode:  "Block",
				Compression: req.Options.VolumeCompression,
				BytesDone:   1024,
				BytesTotal:  1024,
				Complete:    true,
			}); err != nil {
				return err
			}
		}

		return nil
	}

	setupVolumeSeams(t, stubBuildTreeWithData, stubFetchManifests, checkCompVol)

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:        "demo",
		SnapshotName:     "vol-snap",
		OutputDir:        dir,
		IncludeManifests: true,
		IncludeVolumes:   true,
		// VolumeCompression intentionally left empty to test default.
	}

	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if gotCompression != "gzip" {
		t.Errorf("VolumeCompression = %q, want %q (default)", gotCompression, "gzip")
	}
}

// TestRun_VolumeCompression_None verifies that Options.VolumeCompression="none"
// is passed through to DownloadNodeVolumesFunc unchanged.
func TestRun_VolumeCompression_None(t *testing.T) {
	var gotCompression string

	checkCompVol := func(_ context.Context, req pipeline.NodeVolumesRequest) error {
		gotCompression = req.Options.VolumeCompression

		for _, dr := range req.Node.DataRefs {
			if err := req.Writer.AppendVolumeProgress(archive.VolumeProgressRecord{
				NodeID:      req.Node.ID,
				VSCName:     dr.VSCName,
				VolumeMode:  "Block",
				Compression: req.Options.VolumeCompression,
				BytesDone:   1024,
				BytesTotal:  1024,
				Complete:    true,
			}); err != nil {
				return err
			}
		}

		return nil
	}

	setupVolumeSeams(t, stubBuildTreeWithData, stubFetchManifests, checkCompVol)

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:         "demo",
		SnapshotName:      "vol-snap",
		OutputDir:         dir,
		IncludeManifests:  true,
		IncludeVolumes:    true,
		VolumeCompression: "none",
	}

	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if gotCompression != "none" {
		t.Errorf("VolumeCompression = %q, want %q", gotCompression, "none")
	}
}

// TestRun_IndexVolumeModel_Gzip verifies that index.json contains the correct
// volumeModel when VolumeCompression=gzip (default).
func TestRun_IndexVolumeModel_Gzip(t *testing.T) {
	setupVolumeSeams(t, stubBuildTreeWithData, stubFetchManifests, noopDownloadVolumes)

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:         "demo",
		SnapshotName:      "vol-snap",
		OutputDir:         dir,
		IncludeManifests:  true,
		IncludeVolumes:    true,
		VolumeCompression: "gzip",
	}

	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	r, err := archive.OpenDir(dir)
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}

	idx, err := r.Index()
	if err != nil {
		t.Fatalf("Index: %v", err)
	}

	if idx.VolumeModel.Compression != "gzip" {
		t.Errorf("VolumeModel.Compression = %q, want %q", idx.VolumeModel.Compression, "gzip")
	}

	if idx.VolumeModel.Format != "per-file-gzip" {
		t.Errorf("VolumeModel.Format = %q, want %q", idx.VolumeModel.Format, "per-file-gzip")
	}
}

// TestRun_IndexVolumeModel_None verifies that index.json reflects compression=none.
func TestRun_IndexVolumeModel_None(t *testing.T) {
	setupVolumeSeams(t, stubBuildTreeWithData, stubFetchManifests, noopDownloadVolumes)

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:         "demo",
		SnapshotName:      "vol-snap",
		OutputDir:         dir,
		IncludeManifests:  true,
		IncludeVolumes:    true,
		VolumeCompression: "none",
	}

	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	r, err := archive.OpenDir(dir)
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}

	idx, err := r.Index()
	if err != nil {
		t.Fatalf("Index: %v", err)
	}

	if idx.VolumeModel.Compression != "none" {
		t.Errorf("VolumeModel.Compression = %q, want %q", idx.VolumeModel.Compression, "none")
	}

	if idx.VolumeModel.Format != "raw" {
		t.Errorf("VolumeModel.Format = %q, want %q", idx.VolumeModel.Format, "raw")
	}
}
