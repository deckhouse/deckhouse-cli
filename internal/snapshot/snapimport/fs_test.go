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

package snapimport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// plainHTTPDoer satisfies httpDoer by delegating to http.DefaultClient so tests can
// reach an httptest.Server without pulling in SafeClient or TLS setup.
type plainHTTPDoer struct{}

func (plainHTTPDoer) HTTPDo(req *http.Request) (*http.Response, error) {
	return http.DefaultClient.Do(req)
}

func TestPutFile_SingleShotUpload_CorrectHeaders(t *testing.T) {
	payload := []byte("hello, filesystem import")

	dir := t.TempDir()
	localPath := filepath.Join(dir, "data.txt")

	if err := os.WriteFile(localPath, payload, 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}

	var capturedHeaders http.Header

	modTime := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)
	attrs := fileAttrs{Perm: 0o644, UID: 1000, GID: 2000, ModTime: modTime}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		capturedHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "data.txt", localPath, attrs); err != nil {
		t.Fatalf("putFile: %v", err)
	}

	if capturedHeaders == nil {
		t.Fatal("no PUT request received by the server")
	}

	// The FS importer's CheckRequiredHeaders middleware rejects PUTs missing any of these.
	required := []string{"X-Content-Length", "X-Offset", "X-Attribute-Permissions", "X-Attribute-Uid", "X-Attribute-Gid"}
	for _, h := range required {
		if capturedHeaders.Get(h) == "" {
			t.Errorf("missing required header %q on PUT", h)
		}
	}

	if got := capturedHeaders.Get("X-Content-Length"); got != strconv.Itoa(len(payload)) {
		t.Errorf("X-Content-Length = %q, want %d", got, len(payload))
	}

	if got := capturedHeaders.Get("X-Offset"); got != "0" {
		t.Errorf("X-Offset = %q, want 0 (fresh upload)", got)
	}

	if got := capturedHeaders.Get("X-Attribute-Permissions"); got != "0644" {
		t.Errorf("X-Attribute-Permissions = %q, want 0644", got)
	}

	if got := capturedHeaders.Get("X-Attribute-Uid"); got != "1000" {
		t.Errorf("X-Attribute-Uid = %q, want 1000", got)
	}

	if got := capturedHeaders.Get("X-Attribute-Gid"); got != "2000" {
		t.Errorf("X-Attribute-Gid = %q, want 2000", got)
	}

	if got := capturedHeaders.Get("X-Attribute-ModTime"); got != "2026-01-15T10:30:00Z" {
		t.Errorf("X-Attribute-ModTime = %q, want 2026-01-15T10:30:00Z", got)
	}
}

func TestPutFile_ResumeFromPartialOffset(t *testing.T) {
	payload := []byte("0123456789abcde") // 15 bytes; server has the first 8

	dir := t.TempDir()
	localPath := filepath.Join(dir, "data.bin")

	if err := os.WriteFile(localPath, payload, 0o600); err != nil {
		t.Fatalf("write local file: %v", err)
	}

	var putOffsets []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)

		if r.Method == http.MethodHead {
			// Temp file exists with 8 bytes already written.
			w.Header().Set("X-Next-Offset", "8")
			w.WriteHeader(http.StatusOK)

			return
		}

		putOffsets = append(putOffsets, r.Header.Get("X-Offset"))
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	attrs := fileAttrs{Perm: 0o600, UID: 0, GID: 0, ModTime: time.Now()}

	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "data.bin", localPath, attrs); err != nil {
		t.Fatalf("putFile: %v", err)
	}

	if len(putOffsets) != 1 {
		t.Fatalf("expected exactly 1 PUT request, got %d", len(putOffsets))
	}

	// The PUT must resume from the server-reported offset, not restart at 0.
	if putOffsets[0] != "8" {
		t.Errorf("X-Offset = %q, want 8 (resume from server temp-file size)", putOffsets[0])
	}
}

func TestPutFile_OffsetMismatchCorrection(t *testing.T) {
	payload := []byte("abcdefghij") // 10 bytes

	dir := t.TempDir()
	localPath := filepath.Join(dir, "data.bin")

	if err := os.WriteFile(localPath, payload, 0o600); err != nil {
		t.Fatalf("write local file: %v", err)
	}

	putCount := 0

	var putOffsets []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)

		if r.Method == http.MethodHead {
			// No temp or final file; fresh start.
			w.WriteHeader(http.StatusNotFound)

			return
		}

		putCount++
		putOffsets = append(putOffsets, r.Header.Get("X-Offset"))

		if putCount == 1 {
			// First PUT arrives at offset 0 but the server (race / concurrent write)
			// already has 4 bytes; correct the client.
			w.Header().Set("X-Expected-Offset", "4")
			w.WriteHeader(http.StatusConflict)

			return
		}

		// Second PUT at the corrected offset 4; accept and signal completion.
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	attrs := fileAttrs{Perm: 0o600, UID: 0, GID: 0, ModTime: time.Now()}

	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "data.bin", localPath, attrs); err != nil {
		t.Fatalf("putFile: %v", err)
	}

	if len(putOffsets) != 2 {
		t.Fatalf("expected 2 PUTs (initial + corrected), got %d", len(putOffsets))
	}

	if putOffsets[0] != "0" {
		t.Errorf("first PUT X-Offset = %q, want 0", putOffsets[0])
	}

	if putOffsets[1] != "4" {
		t.Errorf("second PUT X-Offset = %q, want 4 (corrected by server 409)", putOffsets[1])
	}
}

func TestPutFile_AlreadyComplete_NoPUT(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "data.bin")

	if err := os.WriteFile(localPath, []byte("done"), 0o600); err != nil {
		t.Fatalf("write local file: %v", err)
	}

	putCalled := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)

		if r.Method == http.MethodHead {
			// No X-Next-Offset on a 200 → final file already exists; upload is complete.
			w.Header().Set("Content-Length", "4")
			w.WriteHeader(http.StatusOK)

			return
		}

		putCalled = true
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	attrs := fileAttrs{Perm: 0o600, UID: 0, GID: 0, ModTime: time.Now()}

	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "data.bin", localPath, attrs); err != nil {
		t.Fatalf("putFile: %v", err)
	}

	if putCalled {
		t.Error("PUT must not be issued when HEAD indicates the final file already exists")
	}
}

func TestPutFile_FinishedPostUsesSharedEndpoint(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "file.txt")

	if err := os.WriteFile(localPath, []byte("content"), 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}

	var finishedPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)

		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			w.WriteHeader(http.StatusCreated)
		case http.MethodPost:
			finishedPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	attrs := fileAttrs{Perm: 0o644, UID: 0, GID: 0, ModTime: time.Now()}

	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "file.txt", localPath, attrs); err != nil {
		t.Fatalf("putFile: %v", err)
	}

	if err := postFinished(context.Background(), plainHTTPDoer{}, srv.URL); err != nil {
		t.Fatalf("postFinished: %v", err)
	}

	// The FS upload path reuses the same unified finished endpoint as the block path.
	if finishedPath != "/"+uploadFinishedSubpath {
		t.Errorf("finished POST path = %q, want /%s", finishedPath, uploadFinishedSubpath)
	}
}
