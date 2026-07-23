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
	"sync"
	"sync/atomic"
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

func TestListDir_RequestsStatAttributeOnly(t *testing.T) {
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

	want := []string{"stat"}
	if len(got) != len(want) {
		t.Fatalf("attribute query params: got %v, want %v", got, want)
	}

	for i, w := range want {
		if got[i] != w {
			t.Errorf("attribute query param %d: got %q, want %q", i, got[i], w)
		}
	}
}

func TestSourceMD5_ProducerHeaderContract(t *testing.T) {
	t.Parallel()

	const wantMD5 = "8c7dd922ad47494fc02c388e12c00eac"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("method = %s, want HEAD", r.Method)
		}

		if got := r.URL.Query()["attribute"]; len(got) != 1 || got[0] != "hash.md5" {
			t.Errorf("attribute query = %v, want [hash.md5]", got)
		}

		w.Header().Set("X-Attribute-Hash-Md5", wantMD5)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := NewFetcher(srv.Client())

	got, err := f.SourceMD5(context.Background(), srv.URL+"/api/v1/files/file", 4)
	if err != nil {
		t.Fatalf("SourceMD5: %v", err)
	}

	if got != wantMD5 {
		t.Errorf("SourceMD5 = %q, want %q", got, wantMD5)
	}
}

type readCountingBody struct {
	reads  atomic.Int32
	closes atomic.Int32
}

func (b *readCountingBody) Read(_ []byte) (int, error) {
	b.reads.Add(1)

	return 0, io.EOF
}

func (b *readCountingBody) Close() error {
	b.closes.Add(1)

	return nil
}

func TestSourceMD5_UsesHeaderWithoutBufferingBody(t *testing.T) {
	t.Parallel()

	const wantMD5 = "8c7dd922ad47494fc02c388e12c00eac"

	body := &readCountingBody{}
	f := NewFetcher(doerFunc(func(_ *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     http.Header{"X-Attribute-Hash-Md5": []string{wantMD5}},
			Body:       body,
		}, nil
	}))

	got, err := f.SourceMD5(context.Background(), "http://example.invalid/api/v1/files/file", 1<<40)
	if err != nil {
		t.Fatalf("SourceMD5: %v", err)
	}

	if got != wantMD5 {
		t.Errorf("SourceMD5 = %q, want %q", got, wantMD5)
	}

	if reads := body.reads.Load(); reads != 0 {
		t.Errorf("response body reads = %d, want 0", reads)
	}

	if closes := body.closes.Load(); closes != 1 {
		t.Errorf("response body closes = %d, want 1", closes)
	}
}

func TestSourceMD5_SizeDerivedDeadlineOutlivesOrdinaryHeaderTimeout(t *testing.T) {
	t.Parallel()

	const (
		ordinaryTimeout = 30 * time.Millisecond
		hashDelay       = 90 * time.Millisecond
		wantMD5         = "8c7dd922ad47494fc02c388e12c00eac"
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(hashDelay)
		w.Header().Set("X-Attribute-Hash-Md5", wantMD5)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ordinaryTransport := srv.Client().Transport.(*http.Transport).Clone()
	ordinaryTransport.ResponseHeaderTimeout = ordinaryTimeout

	hashTransport := srv.Client().Transport.(*http.Transport).Clone()
	hashTransport.ResponseHeaderTimeout = time.Second

	f := NewFetcher(
		&http.Client{Transport: ordinaryTransport},
		WithSourceHashDoer(&http.Client{Transport: hashTransport}),
	)
	f.sourceHashTimeout = func(int64) time.Duration { return time.Second }

	body, ordinaryErr := f.GetFile(context.Background(), srv.URL)
	if ordinaryErr == nil {
		_ = body.Close()
		t.Fatal("ordinary data-plane request unexpectedly outlived its response-header timeout")
	}

	got, err := f.SourceMD5(context.Background(), srv.URL, 4)
	if err != nil {
		t.Fatalf("SourceMD5: %v", err)
	}

	if got != wantMD5 {
		t.Errorf("SourceMD5 = %q, want %q", got, wantMD5)
	}
}

func TestSourceMD5_ExplicitDeadlineStopsWedgedProducer(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	f := NewFetcher(srv.Client())
	f.sourceHashTimeout = func(int64) time.Duration { return 50 * time.Millisecond }

	start := time.Now()
	_, err := f.SourceMD5(context.Background(), srv.URL, 4)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SourceMD5 error = %v, want context.DeadlineExceeded", err)
	}

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("SourceMD5 deadline returned after %v, want <= 1s", elapsed)
	}
}

func TestSourceMD5_ParentCancellationReturnsPromptly(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	f := NewFetcher(srv.Client())
	f.sourceHashTimeout = func(int64) time.Duration { return time.Minute }

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)

	start := time.Now()
	_, err := f.SourceMD5(ctx, srv.URL, 4)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SourceMD5 error = %v, want context.Canceled", err)
	}

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("SourceMD5 cancellation returned after %v, want <= 1s", elapsed)
	}
}

func TestSourceHashTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		size int64
		want time.Duration
	}{
		{name: "unknown size uses floor", size: -1, want: sourceHashTimeoutFloor},
		{name: "small file uses floor", size: 1, want: sourceHashTimeoutFloor},
		{
			name: "large file uses throughput plus slack",
			size: 10 * sourceHashMinimumThroughput * 60,
			want: 10*time.Minute + sourceHashTimeoutSlack,
		},
		{name: "untrusted size is capped", size: int64(^uint64(0) >> 1), want: sourceHashTimeoutCeiling},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := sourceHashTimeout(tc.size); got != tc.want {
				t.Errorf("sourceHashTimeout(%d) = %v, want %v", tc.size, got, tc.want)
			}
		})
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

// errBlockingBodyClosed is returned by blockingBody.Read once the body is
// closed, standing in for the "read on closed response body" error a real
// *http.Response body surfaces when the watchdog (or the caller) closes it.
var errBlockingBodyClosed = errors.New("blocking body closed")

// blockingBody is an io.ReadCloser whose Read blocks until a chunk is delivered
// on data (never, in these tests) or the body is closed, letting a test simulate
// a connection that stops delivering bytes without erroring. It records how many
// times Close was called so a test can assert the watchdog closed it exactly
// once.
type blockingBody struct {
	data     chan []byte
	closed   chan struct{}
	once     sync.Once
	closeCnt atomic.Int32
}

func newBlockingBody() *blockingBody {
	return &blockingBody{data: make(chan []byte), closed: make(chan struct{})}
}

func (b *blockingBody) Read(p []byte) (int, error) {
	select {
	case chunk, ok := <-b.data:
		if !ok {
			return 0, io.EOF
		}

		return copy(p, chunk), nil
	case <-b.closed:
		return 0, errBlockingBodyClosed
	}
}

func (b *blockingBody) Close() error {
	b.closeCnt.Add(1)
	b.once.Do(func() { close(b.closed) })

	return nil
}

// TestIdleReadCloser_TripsWhenNoBytes asserts the watchdog closes the underlying
// body and reports ErrDataPlaneIdle when a Read blocks for the whole window.
func TestIdleReadCloser_TripsWhenNoBytes(t *testing.T) {
	t.Parallel()

	body := newBlockingBody()
	r := newIdleReadCloser(context.Background(), body, 40*time.Millisecond)

	_, err := r.Read(make([]byte, 8))
	if !errors.Is(err, ErrDataPlaneIdle) {
		t.Fatalf("expected ErrDataPlaneIdle, got %v", err)
	}

	if body.closeCnt.Load() == 0 {
		t.Error("watchdog should have closed the underlying body on trip")
	}

	_ = r.Close()
}

// TestIdleReadCloser_CloseUnblocksReadWithoutTrip asserts that closing the
// watchdog unblocks an in-flight Read promptly WITHOUT reporting the idle
// sentinel (Close is the normal way a caller aborts a stream), and that the
// reader goroutine always returns — i.e. no goroutine leak.
func TestIdleReadCloser_CloseUnblocksReadWithoutTrip(t *testing.T) {
	t.Parallel()

	body := newBlockingBody()
	// A long window guarantees any prompt return is due to Close, not the timer.
	r := newIdleReadCloser(context.Background(), body, time.Minute)

	done := make(chan error, 1)
	go func() {
		_, e := r.Read(make([]byte, 8))
		done <- e
	}()

	// Let the Read arm the timer and block on the body before closing.
	time.Sleep(20 * time.Millisecond)

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case e := <-done:
		if errors.Is(e, ErrDataPlaneIdle) {
			t.Errorf("Close must not surface the idle sentinel, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read did not return after Close (goroutine leak)")
	}

	if _, e := r.Read(make([]byte, 1)); e == nil {
		t.Error("Read after Close should return an error")
	}
}

// TestRangeGet_IdleWatchdogTripsOnStall asserts a block-range body that stops
// delivering bytes mid-stream (server flushes a prefix, then goes silent without
// erroring) unblocks with ErrDataPlaneIdle within the idle window instead of
// hanging forever.
func TestRangeGet_IdleWatchdogTripsOnStall(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-9/100")
		w.Header().Set("Content-Length", "10")
		w.WriteHeader(http.StatusPartialContent)

		if fl, ok := w.(http.Flusher); ok {
			_, _ = w.Write([]byte("ABC"))
			fl.Flush()
		}

		<-r.Context().Done()
	}))
	defer srv.Close()

	f := NewFetcher(srv.Client(), WithIdleReadTimeout(50*time.Millisecond))

	rc, err := f.RangeGet(context.Background(), srv.URL, 0, 9)
	if err != nil {
		t.Fatalf("RangeGet: %v", err)
	}

	defer func() { _ = rc.Close() }()

	if _, err := io.ReadAll(rc); !errors.Is(err, ErrDataPlaneIdle) {
		t.Fatalf("expected ErrDataPlaneIdle on a mid-stream stall, got %v", err)
	}
}

// TestRangeGet_TrickleWithinWindowCompletes asserts a slow-but-flowing stream
// (bytes arriving one at a time, each gap well within the idle window) is never
// aborted by the watchdog and delivers all bytes.
func TestRangeGet_TrickleWithinWindowCompletes(t *testing.T) {
	t.Parallel()

	payload := []byte("ABCDEFGHIJ")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		w.WriteHeader(http.StatusPartialContent)

		fl, ok := w.(http.Flusher)

		for _, c := range payload {
			_, _ = w.Write([]byte{c})

			if ok {
				fl.Flush()
			}

			time.Sleep(15 * time.Millisecond)
		}
	}))
	defer srv.Close()

	f := NewFetcher(srv.Client(), WithIdleReadTimeout(300*time.Millisecond))

	rc, err := f.RangeGet(context.Background(), srv.URL, 0, int64(len(payload)-1))
	if err != nil {
		t.Fatalf("RangeGet: %v", err)
	}

	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if !bytes.Equal(got, payload) {
		t.Errorf("trickled body: got %q, want %q", got, payload)
	}
}

// TestGetFile_IdleWatchdogTripsOnStall asserts the file-download path (GetFile)
// carries the same watchdog as the block-range path.
func TestGetFile_IdleWatchdogTripsOnStall(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusOK)

		if fl, ok := w.(http.Flusher); ok {
			_, _ = io.WriteString(w, "partial")
			fl.Flush()
		}

		<-r.Context().Done()
	}))
	defer srv.Close()

	f := NewFetcher(srv.Client(), WithIdleReadTimeout(50*time.Millisecond))

	rc, err := f.GetFile(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}

	defer func() { _ = rc.Close() }()

	if _, err := io.ReadAll(rc); !errors.Is(err, ErrDataPlaneIdle) {
		t.Fatalf("expected ErrDataPlaneIdle on a mid-stream stall, got %v", err)
	}
}

// TestRangeGet_ContextCancelReturnsPromptly asserts ctx cancellation aborts an
// in-flight body read promptly (well before the large idle window) and is not
// masked by the watchdog's idle sentinel.
func TestRangeGet_ContextCancelReturnsPromptly(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-9/100")
		w.Header().Set("Content-Length", "10")
		w.WriteHeader(http.StatusPartialContent)

		if fl, ok := w.(http.Flusher); ok {
			_, _ = w.Write([]byte("AB"))
			fl.Flush()
		}

		<-r.Context().Done()
	}))
	defer srv.Close()

	// A large idle window ensures a prompt return can only be ctx cancellation.
	f := NewFetcher(srv.Client(), WithIdleReadTimeout(30*time.Second))

	ctx, cancel := context.WithCancel(context.Background())

	rc, err := f.RangeGet(ctx, srv.URL, 0, 9)
	if err != nil {
		t.Fatalf("RangeGet: %v", err)
	}

	defer func() { _ = rc.Close() }()

	time.AfterFunc(30*time.Millisecond, cancel)

	start := time.Now()

	_, err = io.ReadAll(rc)
	if err == nil {
		t.Fatal("expected an error after ctx cancellation")
	}

	if errors.Is(err, ErrDataPlaneIdle) {
		t.Errorf("ctx cancellation must not surface the idle sentinel, got %v", err)
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("read did not return promptly after cancel: %v", elapsed)
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
