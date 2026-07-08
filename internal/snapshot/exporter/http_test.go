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
	"errors"
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

func TestRangeGet_UnknownTotalAccepted(t *testing.T) {
	t.Helper()

	// A 206 with total "*" (RFC 9110 §14.4) is a valid, if unusual, complete-length-
	// unknown response and must still be verified against start/end.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Range", "bytes 4-9/*")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, "EFGHIJ")
	}))
	defer srv.Close()

	f := NewFetcher(http.DefaultClient)

	rc, err := f.RangeGet(context.Background(), srv.URL, 4, 9)
	if err != nil {
		t.Fatalf("RangeGet: %v", err)
	}

	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if string(got) != "EFGHIJ" {
		t.Errorf("RangeGet body: got %q, want %q", got, "EFGHIJ")
	}
}

// doerFunc adapts a plain function to the Doer interface, letting tests fabricate an
// *http.Response (and observe its Body.Close calls) without a real network round trip.
type doerFunc func(req *http.Request) (*http.Response, error)

func (f doerFunc) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

// closeTrackingBody wraps an io.Reader as an io.ReadCloser and records whether Close was
// called, so a test can assert a response body was released on a validation-failure path.
type closeTrackingBody struct {
	io.Reader
	closed bool
}

func (b *closeTrackingBody) Close() error {
	b.closed = true
	return nil
}

// fakeRangeResponse builds a fabricated 206 *http.Response carrying contentRange (empty
// to simulate an absent header) and body, for RangeGet validation tests that a real
// httptest.Server cannot easily misbehave enough to exercise.
func fakeRangeResponse(contentRange, body string) (*http.Response, *closeTrackingBody) {
	tracked := &closeTrackingBody{Reader: strings.NewReader(body)}

	header := http.Header{}
	if contentRange != "" {
		header.Set("Content-Range", contentRange)
	}

	resp := &http.Response{
		StatusCode: http.StatusPartialContent,
		Status:     "206 Partial Content",
		Header:     header,
		Body:       tracked,
	}

	return resp, tracked
}

func TestRangeGet_MissingContentRange(t *testing.T) {
	t.Helper()

	resp, body := fakeRangeResponse("", "EFGHIJ")
	f := NewFetcher(doerFunc(func(_ *http.Request) (*http.Response, error) { return resp, nil }))

	_, err := f.RangeGet(context.Background(), "http://example.invalid/api/v1/block", 4, 9)
	if err == nil {
		t.Fatal("expected error for 206 with no Content-Range header, got nil")
	}

	if !errors.Is(err, ErrContentRangeMismatch) {
		t.Errorf("expected errors.Is(err, ErrContentRangeMismatch), got: %v", err)
	}

	if !body.closed {
		t.Error("expected response body to be closed when Content-Range is missing")
	}
}

func TestRangeGet_MismatchedContentRangeStart(t *testing.T) {
	t.Helper()

	// A misbehaving server/proxy that returns 206 for a DIFFERENT range than requested;
	// the length (6 bytes) still matches, but the content is from the wrong offset.
	resp, body := fakeRangeResponse("bytes 10-15/37", "wrong!")
	f := NewFetcher(doerFunc(func(_ *http.Request) (*http.Response, error) { return resp, nil }))

	_, err := f.RangeGet(context.Background(), "http://example.invalid/api/v1/block", 4, 9)
	if err == nil {
		t.Fatal("expected error for mismatched Content-Range, got nil")
	}

	if !errors.Is(err, ErrContentRangeMismatch) {
		t.Errorf("expected errors.Is(err, ErrContentRangeMismatch), got: %v", err)
	}

	if !body.closed {
		t.Error("expected response body to be closed on Content-Range mismatch")
	}
}

func TestRangeGet_InconsistentTotal(t *testing.T) {
	t.Helper()

	// start/end match the request, but total (8) is not greater than end (9): the
	// server's own header is self-contradictory and must not be trusted.
	resp, body := fakeRangeResponse("bytes 4-9/8", "EFGHIJ")
	f := NewFetcher(doerFunc(func(_ *http.Request) (*http.Response, error) { return resp, nil }))

	_, err := f.RangeGet(context.Background(), "http://example.invalid/api/v1/block", 4, 9)
	if err == nil {
		t.Fatal("expected error for inconsistent total, got nil")
	}

	if !errors.Is(err, ErrContentRangeMismatch) {
		t.Errorf("expected errors.Is(err, ErrContentRangeMismatch), got: %v", err)
	}

	if !body.closed {
		t.Error("expected response body to be closed on inconsistent total")
	}
}

func TestRangeGet_MalformedContentRange(t *testing.T) {
	t.Helper()

	resp, body := fakeRangeResponse("not-a-content-range", "EFGHIJ")
	f := NewFetcher(doerFunc(func(_ *http.Request) (*http.Response, error) { return resp, nil }))

	_, err := f.RangeGet(context.Background(), "http://example.invalid/api/v1/block", 4, 9)
	if err == nil {
		t.Fatal("expected error for malformed Content-Range, got nil")
	}

	if !errors.Is(err, ErrContentRangeMismatch) {
		t.Errorf("expected errors.Is(err, ErrContentRangeMismatch), got: %v", err)
	}

	if !body.closed {
		t.Error("expected response body to be closed on malformed Content-Range")
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
