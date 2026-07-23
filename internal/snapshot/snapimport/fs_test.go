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
	"errors"
	"fmt"
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

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
)

// plainHTTPDoer satisfies httpDoer by delegating to http.DefaultClient so tests can
// reach an httptest.Server without pulling in SafeClient or TLS setup.
type plainHTTPDoer struct{}

func (plainHTTPDoer) HTTPDo(req *http.Request) (*http.Response, error) {
	return http.DefaultClient.Do(req)
}

type failOnHTTPDoer struct {
	called bool
}

func (d *failOnHTTPDoer) HTTPDo(*http.Request) (*http.Response, error) {
	d.called = true

	return nil, errors.New("unexpected HTTP call")
}

func TestPutFile_SingleShotUpload_CorrectHeaders(t *testing.T) {
	payload := []byte("hello, filesystem import")

	var capturedHeaders http.Header

	modTime := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)
	attrs := fileAttrs{Perm: 0o644, UID: 1000, GID: 2000, ModTime: modTime}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()

		body, _ := io.ReadAll(r.Body)
		if !bytes.Equal(body, payload) {
			t.Errorf("PUT body = %q, want %q", body, payload)
		}

		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	body := bytes.NewReader(payload)

	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "data.txt", body, int64(len(payload)), 0, attrs); err != nil {
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

// TestPutFile_ResumeFromPartialOffset verifies that putFile, given a body reader already
// fast-forwarded to a nonzero offset by the caller (its new contract — putFile itself does
// no HEAD probing or fast-forwarding), sends exactly one PUT carrying only the remaining
// bytes, with X-Offset set to that offset.
func TestPutFile_ResumeFromPartialOffset(t *testing.T) {
	payload := []byte("0123456789abcde") // 15 bytes; caller already sent the first 8

	var putOffsets []string

	var receivedBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		putOffsets = append(putOffsets, r.Header.Get("X-Offset"))
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	attrs := fileAttrs{Perm: 0o600, UID: 0, GID: 0, ModTime: time.Now()}
	body := bytes.NewReader(payload[8:])

	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "data.bin", body, int64(len(payload)), 8, attrs); err != nil {
		t.Fatalf("putFile: %v", err)
	}

	if len(putOffsets) != 1 {
		t.Fatalf("expected exactly 1 PUT request, got %d", len(putOffsets))
	}

	if putOffsets[0] != "8" {
		t.Errorf("X-Offset = %q, want 8 (caller-supplied resume offset)", putOffsets[0])
	}

	if !bytes.Equal(receivedBody, payload[8:]) {
		t.Errorf("PUT body = %q, want %q (only the remaining bytes)", receivedBody, payload[8:])
	}
}

// TestPutFile_OffsetMismatchCorrection documents putFile's new, honest behavior around a
// mid-upload 409 offset conflict now that its body is a one-pass io.Reader rather than a
// seekable local file. net/http always drains the reader for the request it is currently
// writing regardless of the eventual response status, so a rejected PUT leaves nothing left
// in body to resend at a corrected offset within THE SAME call — attempt 1 below must
// surface an error. The real recovery path is the same one block streaming uses (see
// TestPutBlock_InterruptAndResume_AllCodecs / TestImportFSFromTar_InterruptAndResume_AllCodecs):
// a wholly independent, later call (a restarted process) that re-probes headFileOffset and
// builds a fresh, correctly-positioned reader — attempt 2 below simulates exactly that and
// must succeed. This deliberately replaces the pre-streaming version of this test (which
// relied on a seekable local file to resend the corrected tail from the SAME putFile call).
func TestPutFile_OffsetMismatchCorrection(t *testing.T) {
	payload := []byte("abcdefghij") // 10 bytes

	imp := newFakeFileImporter()
	// Seed the server with 4 bytes already durably written — simulating that the caller's
	// belief (offset 0) is stale relative to the server's true state.
	imp.seed("data.bin", payload[:4])

	srv := httptest.NewServer(imp)
	defer srv.Close()

	attrs := fileAttrs{Perm: 0o600, UID: 0, GID: 0, ModTime: time.Now()}

	// Attempt 1: putFile believes offset 0 (stale). Its single PUT sends the whole 10-byte
	// body, is rejected 409 (X-Expected-Offset: 4), and the loop's next iteration finds body
	// already fully drained — net/http itself detects the short body and errors out.
	err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "data.bin", bytes.NewReader(payload), int64(len(payload)), 0, attrs)
	if err == nil {
		t.Fatal("expected attempt 1 (stale offset, mid-call correction impossible with a one-pass reader) to return an error")
	}

	if got := imp.received("data.bin"); !bytes.Equal(got, payload[:4]) {
		t.Fatalf("server state must be unchanged by the rejected attempt 1: got %q, want %q", got, payload[:4])
	}

	// Attempt 2: a fresh, independent call using the server-reported offset and a freshly
	// positioned reader — exactly what a caller re-probing headFileOffset would build.
	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "data.bin", bytes.NewReader(payload[4:]), int64(len(payload)), 4, attrs); err != nil {
		t.Fatalf("putFile (attempt 2, corrected offset): %v", err)
	}

	if got := imp.received("data.bin"); !bytes.Equal(got, payload) {
		t.Errorf("after corrected attempt 2, server holds %q, want %q", got, payload)
	}
}

// TestPutFile_AlreadyComplete_NoPUT verifies the offset>=totalSize short-circuit: a resume
// offset already at (or past) the total means a prior call already durably transferred
// every byte, so putFile must not issue any PUT. This mirrors putBlock's analogous
// TestPutBlock_SkipsDecodeWhenOffsetEqualsTotal; the pre-streaming version of this test
// (which asserted putFile's own internal HEAD detected server-side completion) no longer
// applies, since that check now lives entirely in the caller (importFSFromTar) before
// putFile is ever invoked.
func TestPutFile_AlreadyComplete_NoPUT(t *testing.T) {
	putCalled := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		putCalled = true
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	attrs := fileAttrs{Perm: 0o600, UID: 0, GID: 0, ModTime: time.Now()}

	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "data.bin", strings.NewReader(""), 4, 4, attrs); err != nil {
		t.Fatalf("putFile: %v", err)
	}

	if putCalled {
		t.Error("PUT must not be issued when offset already equals totalSize")
	}
}

func TestPutFile_EmptyFile_CreatesViaSinglePUT(t *testing.T) {
	putCount := 0

	var capturedHeaders http.Header

	modTime := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	attrs := fileAttrs{Perm: 0o644, UID: 500, GID: 500, ModTime: modTime}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		putCount++
		capturedHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "empty.txt", strings.NewReader(""), 0, 0, attrs); err != nil {
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

func TestPutFile_FinishedPostUsesSharedEndpoint(t *testing.T) {
	var finishedPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			_, _ = io.Copy(io.Discard, r.Body)
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
	payload := []byte("content")

	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "file.txt", bytes.NewReader(payload), int64(len(payload)), 0, attrs); err != nil {
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

// encodeEntry compresses content with the named codec via EncodeStream — the same
// per-file compression the FS download path uses for data.tar entries — and returns the
// codec's file extension (matching codecExt's recognized set; "" for "none") alongside the
// encoded bytes.
func encodeEntry(t *testing.T, codecName string, content []byte) (string, []byte) {
	t.Helper()

	codec, err := compress.New(codecName, 0)
	if err != nil {
		t.Fatalf("compress.New(%q): %v", codecName, err)
	}

	var buf bytes.Buffer

	if err := codec.EncodeStream(&buf, bytes.NewReader(content)); err != nil {
		t.Fatalf("EncodeStream(%q): %v", codecName, err)
	}

	return codec.Ext(), buf.Bytes()
}

// addTarEntry writes a format-current regular entry. rawSizes can avoid decoding
// large or deliberately corrupt test payloads while constructing the fixture.
func addTarEntry(t *testing.T, tw *tar.Writer, name string, body []byte, mode int64, uid, gid int, modTime time.Time, rawSizes ...int64) {
	t.Helper()

	ext := fixtureCodecExt(name)
	rawSize := int64(0)

	if len(rawSizes) > 0 {
		rawSize = rawSizes[0]
	} else {
		reader, err := compress.NewReader(ext, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("open fixture decoder for %s: %v", name, err)
		}

		rawSize, err = io.Copy(io.Discard, reader)
		if err != nil {
			t.Fatalf("decode fixture %s: %v", name, err)
		}

		if err := reader.Close(); err != nil {
			t.Fatalf("close fixture decoder for %s: %v", name, err)
		}
	}

	originalPath := strings.TrimSuffix(name, ext)
	addTarEntryMetadata(t, tw, name, originalPath, codecName(ext), rawSize, body, mode, uid, gid, modTime)
}

func fixtureCodecExt(name string) string {
	switch {
	case strings.HasSuffix(name, ".zst"):
		return ".zst"
	case strings.HasSuffix(name, ".gz"):
		return ".gz"
	case strings.HasSuffix(name, ".lz4"):
		return ".lz4"
	default:
		return ""
	}
}

func addTarEntryMetadata(
	t *testing.T,
	tw *tar.Writer,
	name, originalPath, codec string,
	rawSize int64,
	body []byte,
	mode int64,
	uid, gid int,
	modTime time.Time,
) {
	t.Helper()

	metadata, err := archive.NewFSMetadata(codec, originalPath, rawSize)
	if err != nil {
		t.Fatalf("build tar metadata for %s: %v", name, err)
	}

	hdr := &tar.Header{
		Format:     tar.FormatPAX,
		Typeflag:   tar.TypeReg,
		Name:       name,
		Mode:       mode,
		Uid:        uid,
		Gid:        gid,
		ModTime:    modTime,
		Size:       int64(len(body)),
		PAXRecords: metadata.PAXRecords(),
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

	alphaExt, alphaZstd := encodeEntry(t, "zstd", alphaContent)
	_, betaZstd := encodeEntry(t, "zstd", betaContent)

	modTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	var tarBuf bytes.Buffer

	tw := tar.NewWriter(&tarBuf)
	addTarEntry(t, tw, "alpha.txt"+alphaExt, alphaZstd, 0o644, 100, 200, modTime)
	addTarEntry(t, tw, "sub/beta.txt"+alphaExt, betaZstd, 0o600, 101, 201, modTime)
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

	if err := importFSFromTar(context.Background(), plainHTTPDoer{}, srv.URL, tarPath, discardLogger(), nil, nil, nil); err != nil {
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

	if got := alpha.headers.Get("X-Content-Length"); got != strconv.Itoa(len(alphaContent)) {
		t.Errorf("alpha.txt X-Content-Length = %q, want %d (exact decompressed size)", got, len(alphaContent))
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

	// --- plain.txt (no codec — verbatim bytes, hdr.Size used directly) ---
	plain, ok := cap.find("plain.txt")
	if !ok {
		t.Fatal("plain.txt not uploaded")
	}

	if !bytes.Equal(plain.body, plainContent) {
		t.Errorf("plain.txt body mismatch: got %q, want %q", plain.body, plainContent)
	}

	if got := plain.headers.Get("X-Content-Length"); got != strconv.Itoa(len(plainContent)) {
		t.Errorf("plain.txt X-Content-Length = %q, want %d", got, len(plainContent))
	}

	// Exactly three files must have been uploaded (no spurious entries).
	cap.mu.Lock()
	total := len(cap.uploads)
	cap.mu.Unlock()

	if total != 3 {
		t.Errorf("expected 3 uploads, got %d", total)
	}
}

func TestImportFSFromTar_UsesPAXForCodecSuffixedSourceNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		codec        string
		originalPath string
		storedPath   string
	}{
		{name: "none keeps zstd-looking source name", codec: "none", originalPath: "report.zst", storedPath: "report.zst"},
		{name: "zstd keeps gzip-looking source name", codec: "zstd", originalPath: "report.gz", storedPath: "report.gz.zst"},
		{name: "gzip keeps lz4-looking source name", codec: "gzip", originalPath: "report.lz4", storedPath: "report.lz4.gz"},
		{name: "lz4 keeps gzip-looking source name", codec: "lz4", originalPath: "report.gz", storedPath: "report.gz.lz4"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			content := []byte("source name and codec are independent")
			_, encoded := encodeEntry(t, tc.codec, content)

			var tarBuf bytes.Buffer

			tw := tar.NewWriter(&tarBuf)
			addTarEntryMetadata(
				t,
				tw,
				tc.storedPath,
				tc.originalPath,
				tc.codec,
				int64(len(content)),
				encoded,
				0o640,
				12,
				34,
				time.Unix(10, 0).UTC(),
			)

			if err := tw.Close(); err != nil {
				t.Fatalf("close tar: %v", err)
			}

			tarPath := filepath.Join(t.TempDir(), "data.tar")
			if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
				t.Fatalf("write tar: %v", err)
			}

			importer := newFakeFileImporter()
			server := httptest.NewServer(importer)
			t.Cleanup(server.Close)

			if err := importFSFromTar(
				context.Background(),
				plainHTTPDoer{},
				server.URL,
				tarPath,
				discardLogger(),
				nil,
				nil,
				nil,
			); err != nil {
				t.Fatalf("importFSFromTar: %v", err)
			}

			if got := importer.received(tc.originalPath); !bytes.Equal(got, content) {
				t.Fatalf("uploaded %q = %q, want %q", tc.originalPath, got, content)
			}

			if got := importer.headers[tc.originalPath].Get("X-Content-Length"); got != strconv.Itoa(len(content)) {
				t.Fatalf("X-Content-Length = %q, want %d", got, len(content))
			}
		})
	}
}

func TestImportFSFromTar_MetadataPreflightBeforeHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		second *tar.Header
	}{
		{
			name: "missing metadata",
			second: &tar.Header{
				Typeflag: tar.TypeReg,
				Name:     "missing.txt",
				Mode:     0o600,
				Size:     1,
			},
		},
		{
			name: "malformed raw size",
			second: &tar.Header{
				Format:   tar.FormatPAX,
				Typeflag: tar.TypeReg,
				Name:     "bad.txt.zst",
				Mode:     0o600,
				Size:     1,
				PAXRecords: map[string]string{
					archive.PAXFSCodec:        "zstd",
					archive.PAXFSOriginalPath: "bad.txt",
					archive.PAXFSRawSize:      "not-a-size",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var tarBuf bytes.Buffer

			tw := tar.NewWriter(&tarBuf)
			addTarEntryMetadata(t, tw, "valid.txt", "valid.txt", "none", 1, []byte("v"), 0o600, 0, 0, time.Time{})

			if err := tw.WriteHeader(tc.second); err != nil {
				t.Fatalf("write malformed header: %v", err)
			}

			if _, err := tw.Write([]byte("x")); err != nil {
				t.Fatalf("write malformed body: %v", err)
			}

			if err := tw.Close(); err != nil {
				t.Fatalf("close tar: %v", err)
			}

			tarPath := filepath.Join(t.TempDir(), "data.tar")
			if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
				t.Fatalf("write tar: %v", err)
			}

			doer := &failOnHTTPDoer{}
			err := importFSFromTar(context.Background(), doer, "https://import.invalid", tarPath, discardLogger(), nil, nil, nil)
			if !errors.Is(err, archive.ErrInvalidFSMetadata) {
				t.Fatalf("importFSFromTar error = %v, want ErrInvalidFSMetadata", err)
			}

			if doer.called {
				t.Fatal("metadata preflight must reject the whole tar before the first HEAD or PUT")
			}
		})
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

	if err := importFSFromTar(context.Background(), plainHTTPDoer{}, srv.URL, tarPath, discardLogger(), nil, nil, nil); err != nil {
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

func TestImportFSFromTar_SkipsAlreadyUploadedEntryWithoutDecompressing(t *testing.T) {
	alphaPlain := []byte("alpha file the server already has fully, byte for byte")
	betaPlain := []byte("beta file still needs uploading")

	alphaExt, alphaZstd := encodeEntry(t, "zstd", alphaPlain)

	modTime := time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)

	var tarBuf bytes.Buffer

	tw := tar.NewWriter(&tarBuf)
	addTarEntry(t, tw, "alpha.txt"+alphaExt, alphaZstd, 0o644, 100, 200, modTime)
	addTarEntry(t, tw, "beta.txt", betaPlain, 0o644, 100, 200, modTime)

	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}

	dir := t.TempDir()
	tarPath := filepath.Join(dir, "data.tar")

	if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
		t.Fatalf("write data.tar: %v", err)
	}

	alphaPUTCalled := false
	cap := &fsCapture{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		relPath := strings.TrimPrefix(r.URL.Path, "/api/v1/files/")

		if r.Method == http.MethodHead {
			if relPath == "alpha.txt" {
				// Final file already exists server-side: 200, no X-Next-Offset, and
				// Content-Length set to the exact decompressed (plaintext) size.
				w.Header().Set("Content-Length", strconv.Itoa(len(alphaPlain)))
				w.WriteHeader(http.StatusOK)

				return
			}

			w.WriteHeader(http.StatusNotFound)

			return
		}

		if relPath == "alpha.txt" {
			alphaPUTCalled = true
		}

		body, _ := io.ReadAll(r.Body)
		cap.record(relPath, body, r.Header.Clone())
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	dirBefore, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir (before): %v", err)
	}

	progressed := 0
	onProgress := func(n int) { progressed += n }

	var totals []int64

	setTotal := func(n int64) { totals = append(totals, n) }

	if err := importFSFromTar(context.Background(), plainHTTPDoer{}, srv.URL, tarPath, discardLogger(), setTotal, onProgress, nil); err != nil {
		t.Fatalf("importFSFromTar: %v", err)
	}

	if alphaPUTCalled {
		t.Error("PUT must not be issued for an already-fully-uploaded entry (alpha.txt)")
	}

	if _, ok := cap.find("alpha.txt"); ok {
		t.Error("alpha.txt must not appear among uploads — it was already complete server-side")
	}

	// beta.txt was NOT already done, so it must be uploaded exactly as before this fix.
	beta, ok := cap.find("beta.txt")
	if !ok {
		t.Fatal("beta.txt (not yet uploaded) was not uploaded")
	}

	if !bytes.Equal(beta.body, betaPlain) {
		t.Errorf("beta.txt body = %q, want %q", beta.body, betaPlain)
	}

	// tar.Reader must have auto-skipped alpha's remaining unread compressed bytes
	// cleanly, or beta's entry (and its body) would be corrupted/misaligned above.

	if want := len(alphaPlain) + len(betaPlain); progressed != want {
		t.Errorf("onProgress total = %d, want %d (skipped alpha.txt must still be credited at its exact decompressed size, plus beta.txt)", progressed, want)
	}

	// setTotal must grow progressively: alpha.txt's exact size becomes known first (at
	// the done-skip credit point, from HEAD's Content-Length, entirely without
	// decompressing it), then beta.txt's (at the not-done measure point) adds its own
	// exact size on top — never a single upfront call with the grand total.
	wantTotals := []int64{int64(len(alphaPlain)), int64(len(alphaPlain) + len(betaPlain))}
	if len(totals) != len(wantTotals) {
		t.Fatalf("setTotal called %d times with %v, want %d calls with %v", len(totals), totals, len(wantTotals), wantTotals)
	}

	for i, want := range wantTotals {
		if totals[i] != want {
			t.Errorf("setTotal call #%d = %d, want %d", i, totals[i], want)
		}
	}

	dirAfter, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir (after): %v", err)
	}

	if len(dirAfter) != len(dirBefore) {
		t.Errorf("archive directory entry count changed during upload: before=%d after=%d (a temp file was left behind)", len(dirBefore), len(dirAfter))
	}
}

// TestImportFSFromTar_SkipsAlreadyUploadedEntry_NeverAttemptsDecompression closes a gap
// TestImportFSFromTar_SkipsAlreadyUploadedEntryWithoutDecompressing above leaves open: that
// test proves the done-skip branch never issues a PUT and leaves no temp file behind, but a
// PUT/directory-listing assertion cannot detect a regression that wastefully decodes an
// already-done entry's payload and simply discards the result before the (correctly skipped)
// PUT -- the two-pass streaming architecture has no other externally observable side effect
// such a regression would trip.
//
// This test closes that gap directly: each already-done entry's STORED (compressed) bytes
// are deliberately truncated by one byte, so they are not a valid codec stream. If
// importFSFromTar ever attempted to decode them (measureEntrySize or streamCompressedEntry),
// the decoder would surface a decode error (verified for zstd/gzip/lz4 truncation above the
// package) and importFSFromTar would return a non-nil error, which this test treats as a
// failure. The done-skip branch, correctly implemented, `continue`s before ever reading the
// entry's payload through tr, so the corruption is inert. "none" has no decode step to
// corrupt and is already covered by the PUT-call assertion above and by the "none" case in
// TestImportFSFromTar_InterruptAndResume_AllCodecs.
func TestImportFSFromTar_SkipsAlreadyUploadedEntry_NeverAttemptsDecompression(t *testing.T) {
	plain := []byte("already-fully-uploaded-content-that-must-never-be-decompressed")

	for _, tc := range codecCases {
		if tc.codec == "none" {
			continue
		}

		t.Run(tc.codec, func(t *testing.T) {
			_, encoded := encodeEntry(t, tc.codec, plain)
			corrupted := encoded[:len(encoded)-1]

			modTime := time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)

			var tarBuf bytes.Buffer

			tw := tar.NewWriter(&tarBuf)
			addTarEntry(t, tw, "done"+tc.ext, corrupted, 0o644, 10, 20, modTime, int64(len(plain)))

			if err := tw.Close(); err != nil {
				t.Fatalf("close tar writer: %v", err)
			}

			dir := t.TempDir()
			tarPath := filepath.Join(dir, "data.tar")

			if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
				t.Fatalf("write data.tar: %v", err)
			}

			putCalled := false

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodHead:
					// Final file already exists server-side: 200, no X-Next-Offset, and
					// Content-Length set to the exact decompressed (plaintext) size.
					w.Header().Set("Content-Length", strconv.Itoa(len(plain)))
					w.WriteHeader(http.StatusOK)
				case http.MethodPut:
					putCalled = true

					http.Error(w, "PUT must never be issued for an already-done entry", http.StatusInternalServerError)
				default:
					http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
				}
			}))
			defer srv.Close()

			progressed := 0
			activated := 0

			err := importFSFromTar(context.Background(), plainHTTPDoer{}, srv.URL, tarPath, discardLogger(),
				nil, func(n int) { progressed += n }, func() { activated++ })
			if err != nil {
				t.Fatalf("importFSFromTar returned an error for a corrupted-but-already-done %s entry -- "+
					"this means decompression WAS attempted on a done entry (the skip branch must "+
					"`continue` before ever reading the entry's payload): %v", tc.codec, err)
			}

			if putCalled {
				t.Error("PUT must not be issued for an already-fully-uploaded entry")
			}

			if progressed != len(plain) {
				t.Errorf("onProgress total = %d, want %d (done entry still credited from HEAD's Content-Length, without decompressing it)",
					progressed, len(plain))
			}

			// A fully server-side-skipped entry must never activate the caller's progress
			// stream: onProgress crediting (asserted above) is a bar-accounting concern
			// (invariant #7) independent of the "was anything really transferred" signal
			// activate exists to carry (backlog #21 Bug A).
			if activated != 0 {
				t.Errorf("activate call count = %d, want 0 (a fully server-side-skipped entry must never activate)", activated)
			}
		})
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

	if err := importFSFromTar(context.Background(), plainHTTPDoer{}, srv.URL, tarPath, discardLogger(), nil, nil, nil); err != nil {
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

// fakeFileImporter mimics enough of the import_files HEAD/PUT/POST contract to exercise
// importFSFromTar end-to-end, keyed per relPath: HEAD reports either "not found" (404), a
// partial upload's current size (200 + X-Next-Offset), or the final file's exact size
// (200, Content-Length, no X-Next-Offset); PUT rejects an offset that doesn't match the
// file's current durable size (409 + X-Expected-Offset, mirroring the real handler) and
// otherwise appends the body, finalizing once the running size reaches X-Content-Length.
type fakeFileImporter struct {
	mu      sync.Mutex
	files   map[string][]byte
	final   map[string]bool
	headers map[string]http.Header
}

func newFakeFileImporter() *fakeFileImporter {
	return &fakeFileImporter{
		files:   make(map[string][]byte),
		final:   make(map[string]bool),
		headers: make(map[string]http.Header),
	}
}

// seed pre-populates a file's durable buffer, simulating bytes a previous, interrupted run
// already delivered before a fresh upload resumes from that offset.
func (f *fakeFileImporter) seed(relPath string, prefix []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.files[relPath] = append([]byte(nil), prefix...)
}

// received returns a copy of every byte durably written so far for relPath.
func (f *fakeFileImporter) received(relPath string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]byte(nil), f.files[relPath]...)
}

func (f *fakeFileImporter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	relPath := strings.TrimPrefix(r.URL.Path, "/"+uploadFilesSubpath+"/")

	switch r.Method {
	case http.MethodHead:
		f.serveHead(w, relPath)
	case http.MethodPut:
		f.servePut(w, r, relPath)
	case http.MethodPost:
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (f *fakeFileImporter) serveHead(w http.ResponseWriter, relPath string) {
	f.mu.Lock()
	body, exists := f.files[relPath]
	final := f.final[relPath]
	f.mu.Unlock()

	if !exists {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	if final {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)

		return
	}

	w.Header().Set("X-Next-Offset", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
}

func (f *fakeFileImporter) servePut(w http.ResponseWriter, r *http.Request, relPath string) {
	offset, _ := strconv.ParseInt(r.Header.Get("X-Offset"), 10, 64)
	expectedTotal, _ := strconv.ParseInt(r.Header.Get("X-Content-Length"), 10, 64)

	f.mu.Lock()
	cur := int64(len(f.files[relPath]))
	f.mu.Unlock()

	if offset != cur {
		w.Header().Set("X-Expected-Offset", strconv.FormatInt(cur, 10))
		http.Error(w, "offset mismatch", http.StatusConflict)

		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	f.mu.Lock()
	f.files[relPath] = append(f.files[relPath], body...)
	next := int64(len(f.files[relPath]))
	f.headers[relPath] = r.Header.Clone()

	if next == expectedTotal {
		f.final[relPath] = true
	}
	f.mu.Unlock()

	if next == expectedTotal {
		w.WriteHeader(http.StatusCreated)

		return
	}

	w.Header().Set("X-Next-Offset", strconv.FormatInt(next, 10))
	w.WriteHeader(http.StatusNoContent)
}

// codecCases enumerates the codecs importFSFromTar must round-trip, pairing compress.New's
// codec name with the extension its entries carry in the tar (as codecExt would report).
var codecCases = []struct {
	codec string
	ext   string
}{
	{"zstd", ".zst"},
	{"gzip", ".gz"},
	{"lz4", ".lz4"},
	{"none", ""},
}

// TestImportFSFromTar_PerCodecRoundTrip verifies, for every codec, that a data.tar with two
// entries uploads both with byte-exact bodies and an X-Content-Length exactly equal to the
// true decompressed size, and that tar.Reader correctly enumerates the SECOND entry after
// the first entry's two-pass (measure + stream) read — proving no dual-tar-reader desync.
func TestImportFSFromTar_PerCodecRoundTrip(t *testing.T) {
	firstContent := []byte("hello from the first entry in the tar, used to verify tar.Reader alignment")
	secondContent := []byte("hello from the second entry, proving Next() still walks correctly afterwards")

	for _, tc := range codecCases {
		t.Run(tc.codec, func(t *testing.T) {
			ext, encodedFirst := encodeEntry(t, tc.codec, firstContent)
			if ext != tc.ext {
				t.Fatalf("codec %q Ext() = %q, want %q", tc.codec, ext, tc.ext)
			}

			_, encodedSecond := encodeEntry(t, tc.codec, secondContent)

			modTime := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

			var tarBuf bytes.Buffer

			tw := tar.NewWriter(&tarBuf)
			addTarEntry(t, tw, "first.dat"+ext, encodedFirst, 0o644, 1, 2, modTime)
			addTarEntry(t, tw, "second.dat"+ext, encodedSecond, 0o640, 3, 4, modTime)

			if err := tw.Close(); err != nil {
				t.Fatalf("close tar writer: %v", err)
			}

			dir := t.TempDir()
			tarPath := filepath.Join(dir, "data.tar")

			if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
				t.Fatalf("write data.tar: %v", err)
			}

			imp := newFakeFileImporter()
			srv := httptest.NewServer(imp)
			defer srv.Close()

			var reported int

			var totals []int64

			activated := 0

			if err := importFSFromTar(context.Background(), plainHTTPDoer{}, srv.URL, tarPath, discardLogger(),
				func(n int64) { totals = append(totals, n) }, func(n int) { reported += n }, func() { activated++ }); err != nil {
				t.Fatalf("importFSFromTar: %v", err)
			}

			// A fresh (non-resumed) upload where every file is genuinely PUT must activate
			// the caller's progress stream at least once (backlog #21 Bug A).
			if activated == 0 {
				t.Error("activate call count = 0, want >= 1 (a first-time upload with real transfers must activate)")
			}

			if got := imp.received("first.dat"); !bytes.Equal(got, firstContent) {
				t.Errorf("first.dat: got %q, want %q", got, firstContent)
			}

			if got := imp.received("second.dat"); !bytes.Equal(got, secondContent) {
				t.Errorf("second.dat: got %q, want %q (tar.Reader must still correctly enumerate "+
					"the second entry after the first's two-pass read)", got, secondContent)
			}

			if got := imp.headers["first.dat"].Get("X-Content-Length"); got != strconv.Itoa(len(firstContent)) {
				t.Errorf("first.dat X-Content-Length = %q, want %d (exact decompressed size)", got, len(firstContent))
			}

			if want := len(firstContent) + len(secondContent); reported != want {
				t.Errorf("onProgress total = %d, want %d", reported, want)
			}

			// setTotal must grow progressively across both not-done entries: first.dat's
			// exact size is measured (or read from hdr.Size for codec "none") before
			// second.dat is even reached, then second.dat's own exact size is added on
			// top — proving the running sum, not a single grand total known up front.
			wantTotals := []int64{int64(len(firstContent)), int64(len(firstContent) + len(secondContent))}
			if len(totals) != len(wantTotals) {
				t.Fatalf("setTotal called %d times with %v, want %d calls with %v", len(totals), totals, len(wantTotals), wantTotals)
			}

			for i, want := range wantTotals {
				if totals[i] != want {
					t.Errorf("setTotal call #%d = %d, want %d", i, totals[i], want)
				}
			}
		})
	}
}

// interruptingFileImporter wraps a fakeFileImporter but, on the FIRST PUT to crashPath
// only, durably persists just partialN bytes of the request body and then hijacks and
// closes the raw connection with no HTTP response — simulating a killed CLI process or a
// dropped network connection partway through a single file's transfer. Every later PUT
// (including later attempts at crashPath) behaves like the wrapped fakeFileImporter.
type interruptingFileImporter struct {
	inner     *fakeFileImporter
	crashPath string
	partialN  int64
	mu        sync.Mutex
	crashed   bool
}

func (f *interruptingFileImporter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	relPath := strings.TrimPrefix(r.URL.Path, "/"+uploadFilesSubpath+"/")

	if r.Method == http.MethodPut && relPath == f.crashPath {
		f.mu.Lock()
		shouldCrash := !f.crashed
		f.crashed = true
		f.mu.Unlock()

		if shouldCrash {
			f.crashMidTransfer(w, r, relPath)

			return
		}
	}

	f.inner.ServeHTTP(w, r)
}

// crashMidTransfer durably records exactly partialN bytes of the request body, then
// hijacks and closes the raw connection with no HTTP response, exactly as it would after
// the importer process (or the network path to it) died mid-transfer.
func (f *interruptingFileImporter) crashMidTransfer(w http.ResponseWriter, r *http.Request, relPath string) {
	chunk := make([]byte, f.partialN)
	if _, err := io.ReadFull(r.Body, chunk); err != nil {
		panic(fmt.Sprintf("test setup: reading %d-byte partial prefix: %v", f.partialN, err))
	}

	f.inner.mu.Lock()
	f.inner.files[relPath] = append(f.inner.files[relPath], chunk...)
	f.inner.mu.Unlock()

	hj, ok := w.(http.Hijacker)
	if !ok {
		panic("test setup: httptest ResponseWriter does not support Hijack")
	}

	conn, _, err := hj.Hijack()
	if err != nil {
		panic(fmt.Sprintf("test setup: hijack: %v", err))
	}

	_ = conn.Close()
}

// TestImportFSFromTar_InterruptAndResume_AllCodecs is the acceptance test for this task's
// core promise: a filesystem entry interrupted mid-upload can be resumed by a wholly
// independent later call (as a restarted CLI process would make) and still produce the
// exact original bytes on the server, for every codec. Attempt 1 is severed mid-transfer
// with no HTTP response; attempt 2 re-opens the same on-disk tar from scratch and re-derives
// the resume offset purely from a fresh headFileOffset HEAD probe, then (for a compressed
// entry) re-measures the exact size and discard-and-fast-forwards to the resume point before
// streaming the remainder. "none" exercises the direct-from-tr path as a regression guard.
func TestImportFSFromTar_InterruptAndResume_AllCodecs(t *testing.T) {
	content := bytes.Repeat([]byte("fs-interrupt-then-resume-bytes-"), 3000)

	for _, tc := range codecCases {
		t.Run(tc.codec, func(t *testing.T) {
			ext, encoded := encodeEntry(t, tc.codec, content)

			var tarBuf bytes.Buffer

			tw := tar.NewWriter(&tarBuf)
			addTarEntry(t, tw, "big.txt"+ext, encoded, 0o644, 10, 20, time.Now())

			if err := tw.Close(); err != nil {
				t.Fatalf("close tar writer: %v", err)
			}

			dir := t.TempDir()
			tarPath := filepath.Join(dir, "data.tar")

			if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
				t.Fatalf("write data.tar: %v", err)
			}

			// Deliberately not aligned to any codec frame boundary, proving the
			// crash-then-resume mechanism does not depend on frame geometry.
			partialN := int64(len(content)/3 + 11)

			inner := newFakeFileImporter()
			imp := &interruptingFileImporter{inner: inner, crashPath: "big.txt", partialN: partialN}
			srv := httptest.NewServer(imp)
			defer srv.Close()

			// Attempt 1: simulates the CLI process being killed mid-transfer.
			err := importFSFromTar(context.Background(), plainHTTPDoer{}, srv.URL, tarPath, discardLogger(), nil, nil, nil)
			if err == nil {
				t.Fatal("expected attempt 1 (simulated crash mid-transfer) to return an error")
			}

			if got := int64(len(inner.received("big.txt"))); got != partialN {
				t.Fatalf("after simulated crash, server durably holds %d bytes, want exactly %d", got, partialN)
			}

			// Attempt 2: a genuinely independent invocation — re-opens the same archive
			// file from scratch and re-derives everything from a fresh HEAD probe, exactly
			// as a restarted process would.
			var reported int

			var lastTotal int64

			activated := 0

			err = importFSFromTar(context.Background(), plainHTTPDoer{}, srv.URL, tarPath, discardLogger(),
				func(n int64) { lastTotal = n }, func(n int) { reported += n }, func() { activated++ })
			if err != nil {
				t.Fatalf("importFSFromTar (attempt 2, resume after simulated crash): %v", err)
			}

			// Attempt 2 genuinely PUTs the remaining bytes of a partially-resumed file, so
			// it must activate (backlog #21 Bug A) even though the file was not uploaded
			// from scratch.
			if activated == 0 {
				t.Error("activate call count = 0, want >= 1 (a partially-resumed upload with real remaining bytes must activate)")
			}

			got := inner.received("big.txt")
			if !bytes.Equal(got, content) {
				t.Fatalf("after crash-then-resume, server holds %d bytes not matching the original %d-byte content "+
					"(a regression here means either duplicated already-durable bytes or dropped bytes)", len(got), len(content))
			}

			if reported != len(content) {
				t.Errorf("attempt 2 reported %d progress bytes, want %d (full file size credited once on completion)", reported, len(content))
			}

			// A single-file tar has a running total equal to that one file's exact
			// (measured) decompressed size — the resume attempt re-measures from
			// scratch, so the total reflects the same value regardless of the
			// earlier interrupted attempt.
			if lastTotal != int64(len(content)) {
				t.Errorf("attempt 2 setTotal = %d, want %d (single file's exact decompressed size)", lastTotal, len(content))
			}
		})
	}
}

// newMemoryBoundedFSServer returns an httptest.Server mimicking just enough of the
// import_files HEAD/PUT contract for a single fresh (offset 0) upload: HEAD reports
// not-found (no prior partial or completed upload), and PUT discards the body without
// retaining it (the whole point of this test is to keep the TEST PROCESS's own memory
// bounded too, not just the code under test) and responds 201 Created.
func newMemoryBoundedFSServer(t *testing.T) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)

				return
			}

			w.WriteHeader(http.StatusCreated)
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
}

// TestImportFSFromTar_StreamingIsMemoryBounded is the regression test for
// import-fs-two-pass-streaming-put's core promise: uploading a large compressed
// filesystem tar entry must never materialize the whole (or a large fraction of the)
// decompressed payload in one in-process buffer, in either of the two passes
// (measureEntrySize / PASS 1, streamCompressedEntry / PASS 2). It mirrors
// TestPutBlockCompressed_StreamingIsMemoryBounded: build a >=200 MiB, highly-compressible
// synthetic zstd tar entry — the on-disk fixture itself stays tiny thanks to the
// repetition, keeping this test fast despite the large logical size — and upload it
// through importFSFromTar against a real httptest.Server, using the same
// requestBodyReadTracker / trackedRequestBody / trackingBodyDoer helpers the block-side
// test uses.
//
// The pass/fail signal is the live-heap growth sampled at the very first Read of the
// outgoing PUT body (armHeapBaseline/peakHeapDelta), not the PUT body's Read() chunk size:
// tracking only the chunk size does NOT detect a full-buffering regression, since net/http's
// own request-write copy loop chunks ANY io.Reader body into the same small (~32KiB) pieces
// whether the underlying data was disk-streamed or pre-materialized (empirically confirmed in
// the 2026-07-22 review — see cross-cutting invariant #11 in .agent/implementer-prompt.md).
// The chunk-size metric is still reported for diagnostics below, but no longer decides the
// outcome.
//
// PASS 1 and PASS 2 both construct their decode reader via the IDENTICAL
// compress.NewReader(ext, ...) call over the same codec and the same on-disk bytes — PASS
// 1 just discards the decoded output (io.Copy into io.Discard) instead of streaming it
// into an HTTP body. Because importFSFromTar opens its own file handle internally
// (tarPath is a path, not an injectable reader) and PASS 1's sink is a hardcoded
// io.Discard, there is no externally reachable Read/Write seam for a test to instrument
// PASS 1 directly without changing fs.go, which is outside this task's file scope. This
// test therefore instruments the one reachable seam — PASS 2's outgoing PUT body — and
// relies on PASS 1 sharing the identical decode-reader construction: a regression that
// made the zstd decode reader non-streaming would manifest identically in both passes,
// since both wrap the SAME compress.NewReader output type over the SAME bytes.
func TestImportFSFromTar_StreamingIsMemoryBounded(t *testing.T) {
	const payloadSize = 200 * 1024 * 1024 // >=200 MiB per this task's acceptance criteria

	pattern := []byte("fs-memory-bound-upload-regression-test-data. ")
	content := bytes.Repeat(pattern, payloadSize/len(pattern)+2)
	content = content[:payloadSize]

	ext, encoded := encodeEntry(t, "zstd", content)

	modTime := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	var tarBuf bytes.Buffer

	tw := tar.NewWriter(&tarBuf)
	addTarEntry(t, tw, "bigfile.bin"+ext, encoded, 0o644, 10, 20, modTime, int64(len(content)))

	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}

	dir := t.TempDir()
	tarPath := filepath.Join(dir, "data.tar")

	if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
		t.Fatalf("write data.tar: %v", err)
	}

	dirBefore, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir (before): %v", err)
	}

	srv := newMemoryBoundedFSServer(t)
	defer srv.Close()

	tracker := &requestBodyReadTracker{}
	doer := &trackingBodyDoer{client: srv.Client(), tracker: tracker}

	// Arm the baseline AFTER every fixture allocation (content, the tar buffer, the on-disk
	// file) so those stay folded into the baseline and only new allocations made by
	// importFSFromTar itself move the delta.
	tracker.armHeapBaseline()

	var reported int

	var lastTotal int64

	err = importFSFromTar(context.Background(), doer, srv.URL, tarPath, discardLogger(),
		func(n int64) { lastTotal = n }, func(n int) { reported += n }, nil)
	if err != nil {
		t.Fatalf("importFSFromTar: %v", err)
	}

	if reported != len(content) {
		t.Errorf("reported progress bytes = %d, want %d (full decompressed payload size)", reported, len(content))
	}

	if lastTotal != int64(len(content)) {
		t.Errorf("setTotal final value = %d, want %d (single file's exact decompressed size)", lastTotal, len(content))
	}

	// The heap must not have grown by anywhere near the payload size at the moment the
	// transport started consuming the request body: an order of magnitude below payloadSize
	// comfortably covers zstd's bounded decode window while still catching a regression that
	// materializes a large fraction of the payload in one buffer before handing it to the body.
	const heapCeiling = payloadSize / 10

	if delta := tracker.peakHeapDelta(); delta >= heapCeiling {
		t.Errorf("live heap grew by %d bytes (%.1f MiB) at the first Read of the outgoing PUT "+
			"body, want < %d bytes (%d MiB): the streaming FS upload path must never have the "+
			"whole (or a large fraction of the) decompressed %d-byte payload already resident in "+
			"memory when the transport starts reading the request body",
			delta, float64(delta)/(1024*1024), heapCeiling, heapCeiling/(1024*1024), len(content))
	}

	// Diagnostics only (see requestBodyReadTracker's doc comment): this number alone cannot
	// tell genuine streaming apart from full buffering, so it no longer gates the test.
	t.Logf("largest single Read() on the outgoing PUT body: %d bytes", tracker.max())

	dirAfter, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir (after): %v", err)
	}

	if len(dirAfter) != len(dirBefore) {
		t.Errorf("archive directory entry count changed during upload: before=%d after=%d (a temp file was left behind)", len(dirBefore), len(dirAfter))
	}
}
