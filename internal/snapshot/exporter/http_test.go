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

package exporter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// blockData is the synthetic block volume used by the test server.
var blockData = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// fileData is the content of the synthetic file served by the test server.
const fileContent = "hello from the filesystem volume\n"

// fileURI is the URI returned in the listing for the synthetic file.
const fileURI = "data.txt"

// newTestServer builds an httptest.Server simulating the data-exporter HTTP API.
// It registers three endpoints:
//
//	/api/v1/block        — block volume (HEAD + GET with Range support)
//	/api/v1/files/       — directory listing (trailing slash)
//	/api/v1/files/data.txt — file download
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()

	// Block endpoint: served via http.ServeContent which handles Range natively.
	mux.HandleFunc("/api/v1/block", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Disposition", "attachment; filename=data.img")
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, "data.img", time.Time{}, bytes.NewReader(blockData))
	})

	// Filesystem listing endpoint (trailing slash).
	mux.HandleFunc("/api/v1/files/", func(w http.ResponseWriter, r *http.Request) {
		// Strip the /api/v1/files/ prefix to get the relative path.
		relPath := strings.TrimPrefix(r.URL.Path, "/api/v1/files/")
		if relPath == "" {
			// Root listing.
			w.Header().Set("Content-Type", "application/json")
			listing := fmt.Sprintf(
				`{"apiVersion":"v1","items":[{"name":"data.txt","type":"file","uri":%q,"attributes":{"size":%d}}]}`,
				fileURI, len(fileContent),
			)
			_, _ = io.WriteString(w, listing)

			return
		}

		// File download.
		if relPath == fileURI {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", fmt.Sprint(len(fileContent)))
			_, _ = io.WriteString(w, fileContent)

			return
		}

		http.NotFound(w, r)
	})

	return httptest.NewServer(mux)
}

func TestHeadVolume(t *testing.T) {
	t.Helper()

	srv := newTestServer(t)
	defer srv.Close()

	f := NewFetcher(http.DefaultClient)

	blockURL, err := BlockURL(srv.URL)
	if err != nil {
		t.Fatalf("BlockURL: %v", err)
	}

	size, err := f.HeadVolume(context.Background(), blockURL)
	if err != nil {
		t.Fatalf("HeadVolume: %v", err)
	}

	if size != int64(len(blockData)) {
		t.Errorf("HeadVolume size: got %d, want %d", size, len(blockData))
	}
}

func TestRangeGet(t *testing.T) {
	t.Helper()

	srv := newTestServer(t)
	defer srv.Close()

	f := NewFetcher(http.DefaultClient)

	blockURL, err := BlockURL(srv.URL)
	if err != nil {
		t.Fatalf("BlockURL: %v", err)
	}

	// Request bytes [4, 9] (inclusive) → "EFGHIJ" (6 bytes).
	rc, err := f.RangeGet(context.Background(), blockURL, 4, 9)
	if err != nil {
		t.Fatalf("RangeGet: %v", err)
	}

	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	want := blockData[4:10]
	if !bytes.Equal(got, want) {
		t.Errorf("RangeGet bytes: got %q, want %q", got, want)
	}
}

func TestRangeGet_FullRange(t *testing.T) {
	t.Helper()

	srv := newTestServer(t)
	defer srv.Close()

	f := NewFetcher(http.DefaultClient)

	blockURL, err := BlockURL(srv.URL)
	if err != nil {
		t.Fatalf("BlockURL: %v", err)
	}

	last := int64(len(blockData)) - 1
	rc, err := f.RangeGet(context.Background(), blockURL, 0, last)
	if err != nil {
		t.Fatalf("RangeGet full: %v", err)
	}

	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if !bytes.Equal(got, blockData) {
		t.Errorf("RangeGet full: got %q, want %q", got, blockData)
	}
}

func TestRangeGet_Non206Error(t *testing.T) {
	t.Helper()

	// A server that always returns 200 (no Range support).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "body")
	}))
	defer srv.Close()

	f := NewFetcher(http.DefaultClient)
	_, err := f.RangeGet(context.Background(), srv.URL, 0, 4)

	if err == nil {
		t.Fatal("expected error for non-206 response, got nil")
	}
}

func TestListDir(t *testing.T) {
	t.Helper()

	srv := newTestServer(t)
	defer srv.Close()

	f := NewFetcher(http.DefaultClient)

	filesURL, err := FilesURL(srv.URL)
	if err != nil {
		t.Fatalf("FilesURL: %v", err)
	}

	items, err := f.ListDir(context.Background(), filesURL)
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("ListDir items count: got %d, want 1", len(items))
	}

	it := items[0]

	if it.Name != "data.txt" {
		t.Errorf("item.Name: got %q, want %q", it.Name, "data.txt")
	}

	if it.Type != "file" {
		t.Errorf("item.Type: got %q, want %q", it.Type, "file")
	}

	if it.URI != fileURI {
		t.Errorf("item.URI: got %q, want %q", it.URI, fileURI)
	}
}

func TestListDir_RequestsStatAndHashMd5Attributes(t *testing.T) {
	t.Helper()

	var gotQuery url.Values

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[]}`)
	}))
	defer srv.Close()

	f := NewFetcher(http.DefaultClient)

	filesURL, err := FilesURL(srv.URL)
	if err != nil {
		t.Fatalf("FilesURL: %v", err)
	}

	if _, err := f.ListDir(context.Background(), filesURL); err != nil {
		t.Fatalf("ListDir: %v", err)
	}

	got := gotQuery["attribute"]

	want := []string{"stat", "hash.md5"}
	if len(got) != len(want) {
		t.Fatalf("attribute query params: got %v, want %v", got, want)
	}

	for i, w := range want {
		if got[i] != w {
			t.Errorf("attribute query param %d: got %q, want %q", i, got[i], w)
		}
	}
}

func TestGetFile(t *testing.T) {
	t.Helper()

	srv := newTestServer(t)
	defer srv.Close()

	f := NewFetcher(http.DefaultClient)

	fileURL, err := FilesURL(srv.URL)
	if err != nil {
		t.Fatalf("FilesURL: %v", err)
	}

	fileURL += fileURI

	rc, err := f.GetFile(context.Background(), fileURL)
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}

	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if string(got) != fileContent {
		t.Errorf("GetFile content: got %q, want %q", got, fileContent)
	}
}

func TestBlockURL(t *testing.T) {
	t.Helper()

	got, err := BlockURL("https://export.example.com")
	if err != nil {
		t.Fatalf("BlockURL: %v", err)
	}

	want := "https://export.example.com/api/v1/block"
	if got != want {
		t.Errorf("BlockURL: got %q, want %q", got, want)
	}
}

func TestFilesURL(t *testing.T) {
	t.Helper()

	got, err := FilesURL("https://export.example.com")
	if err != nil {
		t.Fatalf("FilesURL: %v", err)
	}

	want := "https://export.example.com/api/v1/files/"
	if got != want {
		t.Errorf("FilesURL: got %q, want %q", got, want)
	}
}
