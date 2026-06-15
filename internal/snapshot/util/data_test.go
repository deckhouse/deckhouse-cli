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

package util

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func payloadOf(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('A' + (i % 26))
	}
	return b
}

// serveContentServer serves payload with full HTTP Range support (HEAD Content-Length + 206).
func serveContentServer(payload []byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/block" {
			http.NotFound(w, r)
			return
		}
		http.ServeContent(w, r, "block", time.Time{}, bytes.NewReader(payload))
	}))
}

func TestDownloadBlock_Fresh(t *testing.T) {
	payload := payloadOf(20000)
	srv := serveContentServer(payload)
	defer srv.Close()
	out := filepath.Join(t.TempDir(), "out.img")

	if err := downloadBlock(context.Background(), &fakeDoer{srv.Client()}, srv.URL+"/api/v1/block", out); err != nil {
		t.Fatalf("downloadBlock: %v", err)
	}
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, payload) {
		t.Fatalf("downloaded content mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

func TestDownloadBlock_Resume(t *testing.T) {
	payload := payloadOf(20000)
	srv := serveContentServer(payload)
	defer srv.Close()
	out := filepath.Join(t.TempDir(), "out.img")
	if err := os.WriteFile(out, payload[:7777], 0o644); err != nil {
		t.Fatal(err)
	}

	if err := downloadBlock(context.Background(), &fakeDoer{srv.Client()}, srv.URL+"/api/v1/block", out); err != nil {
		t.Fatalf("downloadBlock: %v", err)
	}
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, payload) {
		t.Fatalf("resumed content mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

// TestDownloadBlock_RangeIgnored is the C1 regression: a server that ignores Range and answers 200 with
// the whole body while the client is resuming at offset>0 must not corrupt the file (the partial prefix
// must be discarded and the full body written from byte 0).
func TestDownloadBlock_RangeIgnored(t *testing.T) {
	payload := payloadOf(20000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK) // ignore Range
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	out := filepath.Join(t.TempDir(), "out.img")
	// Pre-seed a stale partial prefix that would corrupt the file if written at offset>0.
	if err := os.WriteFile(out, bytes.Repeat([]byte{'Z'}, 7777), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := downloadBlock(context.Background(), &fakeDoer{srv.Client()}, srv.URL+"/api/v1/block", out); err != nil {
		t.Fatalf("downloadBlock: %v", err)
	}
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, payload) {
		t.Fatalf("Range-ignored download corrupted the file: got %d bytes (want %d)", len(got), len(payload))
	}
}

// TestDownloadBlock_Incomplete is the H1 regression: a short stream that never delivers `total` bytes
// must surface an error, not silently report success.
func TestDownloadBlock_Incomplete(t *testing.T) {
	payload := payloadOf(20000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.WriteHeader(http.StatusOK)
			return
		}
		// Claim partial content but deliver an empty body, so the loop can never reach total.
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))
		w.WriteHeader(http.StatusPartialContent)
	}))
	defer srv.Close()

	out := filepath.Join(t.TempDir(), "out.img")
	err := downloadBlock(context.Background(), &fakeDoer{srv.Client()}, srv.URL+"/api/v1/block", out)
	if err == nil {
		t.Fatal("expected an incomplete-download error, got nil")
	}
	if !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("expected incomplete error, got: %v", err)
	}
}

// TestDownloadBlock_OversizedLocal is the H2 regression: a local file larger than the remote object is
// stale/corrupt and must be restarted, not accepted as complete.
func TestDownloadBlock_OversizedLocal(t *testing.T) {
	payload := payloadOf(20000)
	srv := serveContentServer(payload)
	defer srv.Close()

	out := filepath.Join(t.TempDir(), "out.img")
	if err := os.WriteFile(out, payloadOf(30000), 0o644); err != nil { // larger than payload
		t.Fatal(err)
	}

	if err := downloadBlock(context.Background(), &fakeDoer{srv.Client()}, srv.URL+"/api/v1/block", out); err != nil {
		t.Fatalf("downloadBlock: %v", err)
	}
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, payload) {
		t.Fatalf("oversized local file not restarted: got %d bytes, want %d", len(got), len(payload))
	}
}

func TestValidateContentRangeStart(t *testing.T) {
	cases := []struct {
		header  string
		want    int64
		wantErr bool
	}{
		{"bytes 0-9/10", 0, false},
		{"bytes 5-9/10", 5, false},
		{"bytes 5-9/10", 0, true},  // mismatch
		{"", 99, false},            // omitted: best-effort accept
		{"bytes x-9/10", 0, true},  // malformed start
		{"bytes 09/10", 0, true},   // no dash
	}
	for _, c := range cases {
		err := validateContentRangeStart(c.header, c.want)
		if (err != nil) != c.wantErr {
			t.Errorf("validateContentRangeStart(%q,%d): err=%v wantErr=%v", c.header, c.want, err, c.wantErr)
		}
	}
}

func TestUploadBlock_Fresh(t *testing.T) {
	payload := payloadOf(50000)
	st := newImportState()
	srv := httptest.NewServer(blockHandler(st))
	defer srv.Close()

	in := writeTempFile(t, payload)
	if err := uploadBlock(context.Background(), &fakeDoer{srv.Client()}, srv.URL, in); err != nil {
		t.Fatalf("uploadBlock: %v", err)
	}
	got, finished := st.snapshot()
	if !bytes.Equal(got, payload) {
		t.Fatalf("uploaded content mismatch: got %d bytes, want %d", len(got), len(payload))
	}
	if !finished {
		t.Fatal("expected the importer to be marked finished")
	}
}

func TestUploadBlock_Resume(t *testing.T) {
	payload := payloadOf(50000)
	st := newImportState()
	st.received = append([]byte(nil), payload[:12345]...) // server already has a prefix
	srv := httptest.NewServer(blockHandler(st))
	defer srv.Close()

	in := writeTempFile(t, payload)
	if err := uploadBlock(context.Background(), &fakeDoer{srv.Client()}, srv.URL, in); err != nil {
		t.Fatalf("uploadBlock: %v", err)
	}
	got, finished := st.snapshot()
	if !bytes.Equal(got, payload) || !finished {
		t.Fatalf("resume mismatch: got %d bytes, finished=%v", len(got), finished)
	}
}

// TestUploadBlock_409ForwardConverge: HEAD lies (returns 0) but the server already has a prefix, so the
// first PUT 409s with the authoritative offset; the client must converge forward.
func TestUploadBlock_409ForwardConverge(t *testing.T) {
	payload := payloadOf(50000)
	st := newImportState()
	st.received = append([]byte(nil), payload[:9000]...)
	st.headOverride = func(int) (int, bool) { return 0, true } // pretend nothing uploaded yet
	srv := httptest.NewServer(blockHandler(st))
	defer srv.Close()

	in := writeTempFile(t, payload)
	if err := uploadBlock(context.Background(), &fakeDoer{srv.Client()}, srv.URL, in); err != nil {
		t.Fatalf("uploadBlock: %v", err)
	}
	got, finished := st.snapshot()
	if !bytes.Equal(got, payload) || !finished {
		t.Fatalf("forward-converge mismatch: got %d bytes, finished=%v", len(got), finished)
	}
}

// TestUploadBlock_409BackwardConverge: the server requests a rewind to an earlier offset once; the
// client must follow it backward (the old `next <= offset` stall guard wrongly aborted this).
func TestUploadBlock_409BackwardConverge(t *testing.T) {
	payload := payloadOf(50000)
	st := newImportState()
	st.received = append([]byte(nil), payload[:30000]...)
	st.rewindOnceTo = 10000 // first PUT triggers a backward resync to byte 10000
	srv := httptest.NewServer(blockHandler(st))
	defer srv.Close()

	in := writeTempFile(t, payload)
	if err := uploadBlock(context.Background(), &fakeDoer{srv.Client()}, srv.URL, in); err != nil {
		t.Fatalf("uploadBlock: %v", err)
	}
	got, finished := st.snapshot()
	if !bytes.Equal(got, payload) || !finished {
		t.Fatalf("backward-converge mismatch: got %d bytes, finished=%v", len(got), finished)
	}
}

// TestUploadBlock_409NoHeader: a 409 without X-Next-Offset is unrecoverable and must be a hard error,
// not an optimistic advance past the rejected offset.
func TestUploadBlock_409NoHeader(t *testing.T) {
	payload := payloadOf(50000)
	st := newImportState()
	st.put409NoHeader = true
	srv := httptest.NewServer(blockHandler(st))
	defer srv.Close()

	in := writeTempFile(t, payload)
	err := uploadBlock(context.Background(), &fakeDoer{srv.Client()}, srv.URL, in)
	if err == nil {
		t.Fatal("expected an error on 409 without X-Next-Offset, got nil")
	}
	if !strings.Contains(err.Error(), "cannot converge") {
		t.Fatalf("expected a convergence error, got: %v", err)
	}
}
