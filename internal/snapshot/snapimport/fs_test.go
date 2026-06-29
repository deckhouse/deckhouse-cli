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
	"archive/tar"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
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

// zstdCompress returns the zstd-compressed form of data.
func zstdCompress(t *testing.T, data []byte) []byte {
	t.Helper()

	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}

	return enc.EncodeAll(data, nil)
}

// addTarEntry writes a regular file entry to tw with the given attributes and body.
func addTarEntry(t *testing.T, tw *tar.Writer, name string, body []byte, mode int64, uid, gid int, modTime time.Time) {
	t.Helper()

	hdr := &tar.Header{
		Typeflag: tar.TypeReg,
		Name:     name,
		Mode:     mode,
		Uid:      uid,
		Gid:      gid,
		ModTime:  modTime,
		Size:     int64(len(body)),
	}

	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("write tar header for %s: %v", name, err)
	}

	if _, err := tw.Write(body); err != nil {
		t.Fatalf("write tar body for %s: %v", name, err)
	}
}

// fsCapture records per-file uploads received by the test FS importer server.
type fsCapture struct {
	mu      sync.Mutex
	uploads []fsUpload
}

type fsUpload struct {
	relPath string
	body    []byte
	headers http.Header
}

func (c *fsCapture) record(relPath string, body []byte, h http.Header) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.uploads = append(c.uploads, fsUpload{relPath: relPath, body: body, headers: h})
}

func (c *fsCapture) find(relPath string) (fsUpload, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, u := range c.uploads {
		if u.relPath == relPath {
			return u, true
		}
	}

	return fsUpload{}, false
}

func TestImportFSFromTar_DecompressesAndUploads(t *testing.T) {
	alphaContent := []byte("hello from alpha file")
	betaContent := []byte("hello from beta in subdir")
	plainContent := []byte("plain no compression verbatim")

	alphaZstd := zstdCompress(t, alphaContent)
	betaZstd := zstdCompress(t, betaContent)

	modTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	var tarBuf bytes.Buffer

	tw := tar.NewWriter(&tarBuf)
	addTarEntry(t, tw, "alpha.txt.zst", alphaZstd, 0o644, 100, 200, modTime)
	addTarEntry(t, tw, "sub/beta.txt.zst", betaZstd, 0o600, 101, 201, modTime)
	addTarEntry(t, tw, "plain.txt", plainContent, 0o755, 0, 0, modTime)
	_ = tw.Close()

	dir := t.TempDir()
	tarPath := filepath.Join(dir, "data.tar")

	if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
		t.Fatalf("write data.tar: %v", err)
	}

	cap := &fsCapture{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		body, _ := io.ReadAll(r.Body)
		relPath := strings.TrimPrefix(r.URL.Path, "/api/v1/files/")
		cap.record(relPath, body, r.Header.Clone())
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	if err := importFSFromTar(context.Background(), plainHTTPDoer{}, srv.URL, tarPath, discardLogger(), nil); err != nil {
		t.Fatalf("importFSFromTar: %v", err)
	}

	// --- alpha.txt (zstd decompressed) ---
	alpha, ok := cap.find("alpha.txt")
	if !ok {
		t.Fatal("alpha.txt not uploaded")
	}

	if !bytes.Equal(alpha.body, alphaContent) {
		t.Errorf("alpha.txt body = %q, want %q", alpha.body, alphaContent)
	}

	if got := alpha.headers.Get("X-Attribute-Permissions"); got != "0644" {
		t.Errorf("alpha.txt X-Attribute-Permissions = %q, want 0644", got)
	}

	if got := alpha.headers.Get("X-Attribute-Uid"); got != "100" {
		t.Errorf("alpha.txt X-Attribute-Uid = %q, want 100", got)
	}

	if got := alpha.headers.Get("X-Attribute-Gid"); got != "200" {
		t.Errorf("alpha.txt X-Attribute-Gid = %q, want 200", got)
	}

	// --- sub/beta.txt (zstd, in a subdirectory path) ---
	beta, ok := cap.find("sub/beta.txt")
	if !ok {
		t.Fatal("sub/beta.txt not uploaded")
	}

	if !bytes.Equal(beta.body, betaContent) {
		t.Errorf("sub/beta.txt body mismatch: got %q, want %q", beta.body, betaContent)
	}

	// --- plain.txt (no codec — verbatim bytes) ---
	plain, ok := cap.find("plain.txt")
	if !ok {
		t.Fatal("plain.txt not uploaded")
	}

	if !bytes.Equal(plain.body, plainContent) {
		t.Errorf("plain.txt body mismatch: got %q, want %q", plain.body, plainContent)
	}

	// Exactly three files must have been uploaded (no spurious entries).
	cap.mu.Lock()
	total := len(cap.uploads)
	cap.mu.Unlock()

	if total != 3 {
		t.Errorf("expected 3 uploads, got %d", total)
	}
}

func TestImportFSFromTar_SkipsNonRegularEntries(t *testing.T) {
	fileContent := []byte("only file")

	var tarBuf bytes.Buffer

	tw := tar.NewWriter(&tarBuf)

	// Directory entry — should be skipped.
	dirHdr := &tar.Header{Typeflag: tar.TypeDir, Name: "emptydir/", Mode: 0o755}
	if err := tw.WriteHeader(dirHdr); err != nil {
		t.Fatalf("write dir header: %v", err)
	}

	addTarEntry(t, tw, "file.txt", fileContent, 0o644, 0, 0, time.Now())
	_ = tw.Close()

	dir := t.TempDir()
	tarPath := filepath.Join(dir, "data.tar")

	if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
		t.Fatalf("write data.tar: %v", err)
	}

	cap := &fsCapture{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		body, _ := io.ReadAll(r.Body)
		relPath := strings.TrimPrefix(r.URL.Path, "/api/v1/files/")
		cap.record(relPath, body, r.Header.Clone())
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	if err := importFSFromTar(context.Background(), plainHTTPDoer{}, srv.URL, tarPath, discardLogger(), nil); err != nil {
		t.Fatalf("importFSFromTar: %v", err)
	}

	// Only the regular file must be uploaded; the directory entry must be skipped.
	cap.mu.Lock()
	total := len(cap.uploads)
	cap.mu.Unlock()

	if total != 1 {
		t.Errorf("expected 1 upload (dir entry skipped), got %d", total)
	}

	if _, ok := cap.find("file.txt"); !ok {
		t.Error("file.txt not found in uploads")
	}
}

func TestPutFile_EmptyFile_CreatesViaSinglePUT(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "empty.txt")

	if err := os.WriteFile(localPath, []byte{}, 0o644); err != nil {
		t.Fatalf("write empty local file: %v", err)
	}

	putCount := 0

	var capturedHeaders http.Header

	modTime := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	attrs := fileAttrs{Perm: 0o644, UID: 500, GID: 500, ModTime: modTime}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		putCount++
		capturedHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "empty.txt", localPath, attrs); err != nil {
		t.Fatalf("putFile: %v", err)
	}

	if putCount != 1 {
		t.Fatalf("expected exactly 1 PUT for a 0-byte file, got %d", putCount)
	}

	if got := capturedHeaders.Get("X-Content-Length"); got != "0" {
		t.Errorf("X-Content-Length = %q, want 0", got)
	}

	if got := capturedHeaders.Get("X-Offset"); got != "0" {
		t.Errorf("X-Offset = %q, want 0", got)
	}

	required := []string{"X-Attribute-Permissions", "X-Attribute-Uid", "X-Attribute-Gid"}
	for _, h := range required {
		if capturedHeaders.Get(h) == "" {
			t.Errorf("missing required header %q on empty-file PUT", h)
		}
	}
}

func TestPutFile_EmptyFile_AlreadyExists_NoPUT(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "empty.txt")

	if err := os.WriteFile(localPath, []byte{}, 0o644); err != nil {
		t.Fatalf("write empty local file: %v", err)
	}

	putCalled := false

	attrs := fileAttrs{Perm: 0o644, UID: 0, GID: 0, ModTime: time.Now()}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)

		if r.Method == http.MethodHead {
			// HEAD 200 with no X-Next-Offset → final file already exists.
			w.WriteHeader(http.StatusOK)

			return
		}

		putCalled = true
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "empty.txt", localPath, attrs); err != nil {
		t.Fatalf("putFile: %v", err)
	}

	if putCalled {
		t.Error("PUT must not be issued when HEAD indicates the final file already exists (0-byte file)")
	}
}

func TestImportFSFromTar_EmptyFileIsUploaded(t *testing.T) {
	modTime := time.Date(2026, 2, 10, 12, 0, 0, 0, time.UTC)

	var tarBuf bytes.Buffer

	tw := tar.NewWriter(&tarBuf)
	addTarEntry(t, tw, "empty.txt", []byte{}, 0o644, 10, 20, modTime)
	addTarEntry(t, tw, "nonempty.txt", []byte("data"), 0o644, 10, 20, modTime)
	_ = tw.Close()

	dir := t.TempDir()
	tarPath := filepath.Join(dir, "data.tar")

	if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
		t.Fatalf("write data.tar: %v", err)
	}

	cap := &fsCapture{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		body, _ := io.ReadAll(r.Body)
		relPath := strings.TrimPrefix(r.URL.Path, "/api/v1/files/")
		cap.record(relPath, body, r.Header.Clone())
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	if err := importFSFromTar(context.Background(), plainHTTPDoer{}, srv.URL, tarPath, discardLogger(), nil); err != nil {
		t.Fatalf("importFSFromTar: %v", err)
	}

	emptyUpload, ok := cap.find("empty.txt")
	if !ok {
		t.Fatal("empty.txt was not uploaded (zero-byte file silently dropped)")
	}

	if len(emptyUpload.body) != 0 {
		t.Errorf("empty.txt body = %d bytes, want 0", len(emptyUpload.body))
	}

	if got := emptyUpload.headers.Get("X-Content-Length"); got != "0" {
		t.Errorf("empty.txt X-Content-Length = %q, want 0", got)
	}

	if _, ok := cap.find("nonempty.txt"); !ok {
		t.Fatal("nonempty.txt not uploaded")
	}

	cap.mu.Lock()
	total := len(cap.uploads)
	cap.mu.Unlock()

	if total != 2 {
		t.Errorf("expected 2 uploads, got %d", total)
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
