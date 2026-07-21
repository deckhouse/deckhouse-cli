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
	gotar "archive/tar"
	"bytes"
	"context"
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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
)

// recordingDoer captures the requests putBlock/postFinished send and returns canned responses.
type recordingDoer struct {
	headers []http.Header
	methods []string
	// resumeOffset is what the importer reports as already written via the HEAD probe.
	resumeOffset int64
}

func (d *recordingDoer) HTTPDo(req *http.Request) (*http.Response, error) {
	d.headers = append(d.headers, req.Header.Clone())
	d.methods = append(d.methods, req.Method)

	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
	}

	respHeader := http.Header{}

	// The block importer answers a resume probe (HEAD) with the current write offset.
	if req.Method == http.MethodHead {
		respHeader.Set("X-Next-Offset", strconv.FormatInt(d.resumeOffset, 10))

		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     respHeader,
		}, nil
	}

	return &http.Response{
		StatusCode: http.StatusCreated,
		Status:     "201 Created",
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Header:     respHeader,
	}, nil
}

func TestPutBlock_SendsRequiredHeaders(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.raw")

	payload := []byte("rawblockbytes")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	doer := &recordingDoer{}

	if err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, "", int64(len(payload)), discardLogger(), nil); err != nil {
		t.Fatalf("putBlock: %v", err)
	}

	putIdx := -1
	for i, m := range doer.methods {
		if m == http.MethodPut {
			putIdx = i

			break
		}
	}

	if putIdx < 0 {
		t.Fatalf("expected a PUT request, methods=%v", doer.methods)
	}

	// The SVDM importer's CheckRequiredHeaders rejects PUTs missing any of these.
	required := []string{"X-Content-Length", "X-Offset", "X-Attribute-Permissions", "X-Attribute-Uid", "X-Attribute-Gid"}
	for _, h := range required {
		if doer.headers[putIdx].Get(h) == "" {
			t.Errorf("missing required header %q on block PUT", h)
		}
	}
}

func TestPutBlock_ResumesFromServerOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.raw")

	payload := []byte("0123456789abc") // 13 bytes
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	doer := &recordingDoer{resumeOffset: 5}

	if err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, "", int64(len(payload)), discardLogger(), nil); err != nil {
		t.Fatalf("putBlock: %v", err)
	}

	putIdx := -1
	for i, m := range doer.methods {
		if m == http.MethodPut {
			putIdx = i

			break
		}
	}

	if putIdx < 0 {
		t.Fatalf("expected a PUT request, methods=%v", doer.methods)
	}

	// The PUT must resume from the server-reported offset, not restart at 0.
	if got := doer.headers[putIdx].Get("X-Offset"); got != "5" {
		t.Errorf("X-Offset = %q, want 5 (resume from server offset)", got)
	}
}

// TestPutBlock_ReportsProgressBytes verifies that putBlock calls onProgress with the number
// of bytes written per PUT, summing to the total uploaded size over the whole transfer.
func TestPutBlock_ReportsProgressBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.raw")

	payload := []byte("rawblockdata0123456789") // 22 bytes
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	var reported int

	doer := &recordingDoer{}

	if err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, "", int64(len(payload)), discardLogger(), func(n int) { reported += n }); err != nil {
		t.Fatalf("putBlock: %v", err)
	}

	if reported != len(payload) {
		t.Errorf("reported bytes = %d, want %d (total bytes uploaded)", reported, len(payload))
	}
}

func TestPutBlock_RejectsOversizeServerOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.raw")

	payload := []byte("0123456789abc") // 13 bytes
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	// Importer reports more bytes than the archive has: a mismatched reused DataImport.
	doer := &recordingDoer{resumeOffset: 20}

	err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, "", int64(len(payload)), discardLogger(), nil)
	if err == nil {
		t.Fatal("expected error for oversize server offset, got nil")
	}

	for _, m := range doer.methods {
		if m == http.MethodPut {
			t.Error("no PUT should be issued when the server offset exceeds the archive size")
		}
	}
}

// fakeBlockImporter is a minimal httptest.Server handler mimicking the storage-volume-
// data-manager import_block handler's HEAD/PUT/POST contract closely enough to exercise
// putBlock end to end: HEAD reports the current durable write offset, PUT appends the
// request body at that offset (rejecting an offset mismatch, mirroring the real
// handler's 409) and reports the new offset, and POST .../finished is a no-op success.
// It deliberately has no on-disk device and no independent Content-Length bound check —
// net/http's own enforcement of req.ContentLength (see transfer.go) and putBlockCompressed's
// own post-loop safety-net read are what this file's size-mismatch tests exercise.
type fakeBlockImporter struct {
	mu      sync.Mutex
	written []byte
}

// seed pre-populates the durable buffer, simulating bytes a previous, interrupted run
// already delivered before a fresh putBlock call resumes from that offset.
func (f *fakeBlockImporter) seed(prefix []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.written = append([]byte(nil), prefix...)
}

// received returns a copy of every byte durably written so far.
func (f *fakeBlockImporter) received() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]byte(nil), f.written...)
}

func (f *fakeBlockImporter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodHead:
		f.mu.Lock()
		n := int64(len(f.written))
		f.mu.Unlock()

		w.Header().Set("X-Next-Offset", strconv.FormatInt(n, 10))
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		offset, _ := strconv.ParseInt(r.Header.Get("X-Offset"), 10, 64)
		expectedTotal, _ := strconv.ParseInt(r.Header.Get("X-Content-Length"), 10, 64)

		f.mu.Lock()
		cur := int64(len(f.written))
		f.mu.Unlock()

		if offset != cur {
			http.Error(w, "offset mismatch", http.StatusConflict)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		f.mu.Lock()
		f.written = append(f.written, body...)
		next := int64(len(f.written))
		f.mu.Unlock()

		w.Header().Set("X-Next-Offset", strconv.FormatInt(next, 10))

		if next == expectedTotal {
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	case http.MethodPost:
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// interruptingBlockImporter mimics the import_block HEAD/PUT/POST contract like
// fakeBlockImporter, but its FIRST PUT durably persists only a partial prefix of the
// request body and then severs the underlying TCP connection with no HTTP response at
// all -- simulating a killed CLI process or a dropped network connection partway through
// a transfer, rather than a clean error response. Every PUT after the first behaves like
// fakeBlockImporter (accepts the whole body, reports the new offset), so a second,
// independent putBlock call standing in for a restarted process can complete normally.
type interruptingBlockImporter struct {
	mu       sync.Mutex
	written  []byte
	putCount int
	// partialN is the number of body bytes durably persisted before the first PUT's
	// connection is severed. It is deliberately not aligned to any codec frame or chunk
	// boundary, mirroring TestPutBlockCompressed_ResumesViaFastForward's seedLen.
	partialN int64
}

// durablyWritten returns a copy of every byte the server has durably accepted so far.
func (f *interruptingBlockImporter) durablyWritten() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]byte(nil), f.written...)
}

func (f *interruptingBlockImporter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodHead:
		f.mu.Lock()
		n := int64(len(f.written))
		f.mu.Unlock()

		w.Header().Set("X-Next-Offset", strconv.FormatInt(n, 10))
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		f.handlePut(w, r)
	case http.MethodPost:
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePut services one PUT: the very first call across the server's lifetime accepts
// only partialN bytes and kills the connection before any response is written; every
// later call behaves exactly like fakeBlockImporter.
func (f *interruptingBlockImporter) handlePut(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.putCount++
	isFirstPut := f.putCount == 1
	cur := int64(len(f.written))
	f.mu.Unlock()

	offset, _ := strconv.ParseInt(r.Header.Get("X-Offset"), 10, 64)
	if offset != cur {
		http.Error(w, "offset mismatch", http.StatusConflict)
		return
	}

	if isFirstPut {
		f.crashMidTransfer(w, r)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	f.mu.Lock()
	f.written = append(f.written, body...)
	next := int64(len(f.written))
	f.mu.Unlock()

	w.Header().Set("X-Next-Offset", strconv.FormatInt(next, 10))
	w.WriteHeader(http.StatusCreated)
}

// crashMidTransfer durably records exactly partialN bytes of the request body, then
// hijacks and closes the raw connection with no HTTP response -- the client observes
// neither a success nor an error status, only a severed connection, exactly as it would
// after the importer process (or the network path to it) died mid-transfer.
func (f *interruptingBlockImporter) crashMidTransfer(w http.ResponseWriter, r *http.Request) {
	chunk := make([]byte, f.partialN)
	if _, err := io.ReadFull(r.Body, chunk); err != nil {
		panic(fmt.Sprintf("test setup: reading %d-byte partial prefix: %v", f.partialN, err))
	}

	f.mu.Lock()
	f.written = append(f.written, chunk...)
	f.mu.Unlock()

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

// TestPutBlock_InterruptAndResume_AllCodecs proves the streaming resume mechanism
// (import-block-streaming-decode-put) survives a simulated process restart, not just a
// clean server-reported partial offset: attempt 1 is severed mid-transfer with no HTTP
// response (interruptingBlockImporter.crashMidTransfer), so putBlock must return an
// error; attempt 2 is a wholly separate putBlock call -- as a restarted CLI process would
// make, carrying forward nothing but what HEAD reports -- and must complete the transfer
// so that the server's durably-received bytes equal the original plaintext exactly. Run
// across every codec putBlock supports: zstd/gzip/lz4 exercise the new discard-and-fast-
// forward decode path (putBlockCompressed); none exercises the pre-existing
// io.SectionReader-based resume path (putBlockRaw) as a regression guard.
func TestPutBlock_InterruptAndResume_AllCodecs(t *testing.T) {
	payload := bytes.Repeat([]byte("interrupt-then-resume-bytes-"), 3000)

	for _, tc := range blockCodecCases {
		t.Run(tc.codec, func(t *testing.T) {
			dir := t.TempDir()
			dataFile := filepath.Join(dir, "data.bin"+tc.ext)

			writeEncodedBlockFile(t, dataFile, tc.codec, payload)

			// Deliberately not aligned to either encoded frame's boundary (mirrors
			// TestPutBlockCompressed_ResumesViaFastForward), proving the crash-then-
			// resume mechanism does not depend on frame geometry.
			partialN := int64(len(payload)/3 + 11)

			imp := &interruptingBlockImporter{partialN: partialN}
			srv := httptest.NewServer(imp)
			defer srv.Close()

			totalSize := int64(len(payload))

			// Attempt 1: simulates the CLI process being killed (or the connection
			// dropping) mid-transfer. putBlock must surface an error -- the connection
			// was severed with no response -- and the server must have durably kept
			// exactly partialN bytes, no more and no less.
			err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, tc.ext, totalSize, discardLogger(), nil)
			if err == nil {
				t.Fatal("expected attempt 1 (simulated crash mid-transfer) to return an error")
			}

			if got := int64(len(imp.durablyWritten())); got != partialN {
				t.Fatalf("after simulated crash, server durably holds %d bytes, want exactly %d", got, partialN)
			}

			// Attempt 2: a genuinely independent invocation of putBlock -- a fresh call
			// with its own local variables, exactly as a restarted process would make.
			// Nothing from attempt 1 is passed in except the same on-disk archive file
			// (which a real restarted process would also re-open from disk) and the same
			// server URL; the resume offset itself is re-derived entirely from this
			// call's own HEAD probe, per headBlockOffset/putBlock.
			var reported int64

			err = putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, tc.ext, totalSize, discardLogger(), func(n int) { reported += int64(n) })
			if err != nil {
				t.Fatalf("putBlock (attempt 2, resume after simulated crash): %v", err)
			}

			got := imp.durablyWritten()
			if !bytes.Equal(got, payload) {
				t.Fatalf("after crash-then-resume, server holds %d bytes not matching the original %d-byte payload "+
					"(a regression here means either duplicated already-durable bytes or dropped bytes)", len(got), len(payload))
			}

			if want := totalSize - partialN; reported != want {
				t.Errorf("attempt 2 reported %d progress bytes, want %d (only the bytes it actually sent, not "+
					"re-crediting the pre-crash prefix attempt 1 already reported nothing for)", reported, want)
			}
		})
	}
}

// blockCodecCases enumerates the codecs putBlock must round-trip, pairing compress.New's
// codec name with the file extension putBlock/compress.NewReader dispatch on.
var blockCodecCases = []struct {
	codec string
	ext   string
}{
	{"zstd", ".zst"},
	{"gzip", ".gz"},
	{"lz4", ".lz4"},
	{"none", ""},
}

// writeEncodedBlockFile writes payload to path encoded as two concatenated codec frames
// (mirroring multi-chunk block-download output), or verbatim for "none" (a raw block
// file carries no framing).
func writeEncodedBlockFile(t *testing.T, path, codecName string, payload []byte) {
	t.Helper()

	if codecName == "none" {
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			t.Fatalf("write raw block file: %v", err)
		}

		return
	}

	codec, err := compress.New(codecName, 0)
	if err != nil {
		t.Fatalf("compress.New(%q): %v", codecName, err)
	}

	var buf bytes.Buffer

	mid := len(payload) / 2
	for _, part := range [][]byte{payload[:mid], payload[mid:]} {
		frame, encErr := codec.EncodeFrame(part)
		if encErr != nil {
			t.Fatalf("EncodeFrame: %v", encErr)
		}

		buf.Write(frame)
	}

	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("write encoded block file: %v", err)
	}
}

// TestPutBlock_FreshUploadRoundTrip verifies a from-scratch upload (HEAD offset 0) for
// every codec putBlock supports, including the raw ("none") fast path, against a real
// net/http round trip (httptest.Server), asserting the server receives exactly the
// original plaintext and onProgress sums to the full transferred size.
func TestPutBlock_FreshUploadRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("block-device-bytes-"), 4000)

	for _, tc := range blockCodecCases {
		t.Run(tc.codec, func(t *testing.T) {
			dir := t.TempDir()
			dataFile := filepath.Join(dir, "data.bin"+tc.ext)

			writeEncodedBlockFile(t, dataFile, tc.codec, payload)

			imp := &fakeBlockImporter{}
			srv := httptest.NewServer(imp)
			defer srv.Close()

			var reported int

			if err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, tc.ext, int64(len(payload)), discardLogger(), func(n int) { reported += n }); err != nil {
				t.Fatalf("putBlock: %v", err)
			}

			if got := imp.received(); !bytes.Equal(got, payload) {
				t.Fatalf("server received %d bytes not matching the original %d-byte payload", len(got), len(payload))
			}

			if reported != len(payload) {
				t.Errorf("reported progress bytes = %d, want %d (total payload size)", reported, len(payload))
			}
		})
	}
}

// TestPutBlockCompressed_ResumesViaFastForward verifies that resuming from a nonzero
// HEAD-reported offset re-derives the correct decompressed position via
// io.CopyN(io.Discard, ...) fast-forward and uploads only the remainder, so that the
// pre-seeded prefix (standing in for a prior, interrupted run's durable bytes) plus this
// call's PUT concatenate back to the exact original plaintext. seedLen is deliberately
// not aligned to either encoded frame's boundary, proving the mechanism does not depend
// on frame geometry.
func TestPutBlockCompressed_ResumesViaFastForward(t *testing.T) {
	payload := bytes.Repeat([]byte("resume-me-please-"), 3000)
	seedLen := len(payload)/2 + 7

	for _, tc := range blockCodecCases {
		if tc.ext == "" {
			continue // the raw path's resume behavior is covered by TestPutBlock_ResumesFromServerOffset.
		}

		t.Run(tc.codec, func(t *testing.T) {
			dir := t.TempDir()
			dataFile := filepath.Join(dir, "data.bin"+tc.ext)

			writeEncodedBlockFile(t, dataFile, tc.codec, payload)

			imp := &fakeBlockImporter{}
			imp.seed(payload[:seedLen])

			srv := httptest.NewServer(imp)
			defer srv.Close()

			if err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, tc.ext, int64(len(payload)), discardLogger(), nil); err != nil {
				t.Fatalf("putBlock (resume): %v", err)
			}

			if got := imp.received(); !bytes.Equal(got, payload) {
				t.Fatalf("resumed upload produced %d bytes not matching the original %d-byte payload", len(got), len(payload))
			}
		})
	}
}

// TestPutBlock_SkipsDecodeWhenOffsetEqualsTotal verifies that an offset already equal to
// totalSize (a fully-transferred-but-not-yet-finalized resume) never opens dataFile or
// builds a decode reader: pointing dataFile at a path that does not exist would fail
// immediately if putBlock tried to open it, so a nil return proves the short-circuit
// fired before any file I/O.
func TestPutBlock_SkipsDecodeWhenOffsetEqualsTotal(t *testing.T) {
	imp := &fakeBlockImporter{}
	imp.seed(bytes.Repeat([]byte("x"), 100))

	srv := httptest.NewServer(imp)
	defer srv.Close()

	missing := filepath.Join(t.TempDir(), "data.bin.zst")

	if err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, missing, ".zst", 100, discardLogger(), nil); err != nil {
		t.Fatalf("putBlock with offset==totalSize must skip decoding entirely, got error: %v", err)
	}
}

// TestPutBlockCompressed_TooSmallDeclaredSizeErrors verifies the under-declared-size
// safety net: when totalSize is smaller than the archive's actual decompressed content,
// putBlockCompressed's post-loop probe read must catch the leftover bytes and fail
// loudly instead of silently truncating a successful-looking upload.
func TestPutBlockCompressed_TooSmallDeclaredSizeErrors(t *testing.T) {
	payload := bytes.Repeat([]byte("extra-bytes-beyond-declared-total-"), 200)

	dir := t.TempDir()
	dataFile := filepath.Join(dir, "data.bin.zst")
	writeEncodedBlockFile(t, dataFile, "zstd", payload)

	srv := httptest.NewServer(&fakeBlockImporter{})
	defer srv.Close()

	shortTotal := int64(len(payload) - 500)

	err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, ".zst", shortTotal, discardLogger(), nil)
	if err == nil {
		t.Fatal("expected an error for an under-declared totalSize, got nil")
	}
}

// TestPutBlockCompressed_TooLargeDeclaredSizeErrors verifies the over-declared-size
// safety net: when totalSize is larger than the archive's actual decompressed content,
// the explicit req.ContentLength must make net/http refuse to send a short body, so
// putBlockCompressed surfaces a clear wrapped error instead of hanging or letting the
// server reject the request opaquely.
func TestPutBlockCompressed_TooLargeDeclaredSizeErrors(t *testing.T) {
	payload := bytes.Repeat([]byte("short-archive-"), 50)

	dir := t.TempDir()
	dataFile := filepath.Join(dir, "data.bin.zst")
	writeEncodedBlockFile(t, dataFile, "zstd", payload)

	imp := &fakeBlockImporter{}
	srv := httptest.NewServer(imp)
	defer srv.Close()

	longTotal := int64(len(payload) + 5000)

	err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, ".zst", longTotal, discardLogger(), nil)
	if err == nil {
		t.Fatal("expected an error for an over-declared totalSize, got nil")
	}

	// Guard against a false pass for an unrelated reason: the server must never have
	// been told the transfer completed (it would only see longTotal bytes if net/http's
	// own short-body detection failed to fire and the request were sent anyway).
	if got := int64(len(imp.received())); got >= longTotal {
		t.Fatalf("server received %d bytes, want fewer than the over-declared total %d (net/http should have refused to send a short body)", got, longTotal)
	}
}

// TestPutBlockCompressed_NoTempFilesCreated asserts the streaming decode path creates
// zero temporary files anywhere under the archive directory during the whole upload —
// the entire point of decoding directly into the PUT body instead of materializing a
// decompressed copy first.
func TestPutBlockCompressed_NoTempFilesCreated(t *testing.T) {
	payload := bytes.Repeat([]byte("no-temp-files-please-"), 5000)

	dir := t.TempDir()
	dataFile := filepath.Join(dir, "data.bin.zst")
	writeEncodedBlockFile(t, dataFile, "zstd", payload)

	before, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir before upload: %v", err)
	}

	srv := httptest.NewServer(&fakeBlockImporter{})
	defer srv.Close()

	if err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, ".zst", int64(len(payload)), discardLogger(), nil); err != nil {
		t.Fatalf("putBlock: %v", err)
	}

	after, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir after upload: %v", err)
	}

	if len(before) != len(after) {
		t.Fatalf("archive directory entry count changed during upload: before=%d after=%d (a temp file was left behind)", len(before), len(after))
	}

	for i := range before {
		if before[i].Name() != after[i].Name() {
			t.Fatalf("archive directory contents changed during upload: before=%q after=%q", before[i].Name(), after[i].Name())
		}
	}
}

// requestBodyReadTracker records the largest single byte count any Read call on an
// outgoing PUT request body ever returned, across every request a trackingBodyDoer
// forwards — used by TestPutBlockCompressed_StreamingIsMemoryBounded to prove the
// streaming block-upload path never hands net/http's transport a chunk anywhere near
// the whole decompressed payload size.
type requestBodyReadTracker struct {
	mu      sync.Mutex
	maxRead int
}

func (t *requestBodyReadTracker) record(n int) {
	if n <= 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if n > t.maxRead {
		t.maxRead = n
	}
}

func (t *requestBodyReadTracker) max() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.maxRead
}

// trackedRequestBody wraps an outgoing request body, recording the length of the buffer
// every Read call receives. It deliberately implements ONLY io.ReadCloser, not
// io.WriterTo: verified empirically against the pinned Go stdlib (io.LimitedReader has
// no WriteTo method, and io.NopCloser only preserves WriteTo when its wrapped reader
// already has one — see io/io.go and io/io.go's NopCloser doc), putBlockCompressed's
// io.NopCloser(io.LimitReader(decodeReader, remain)) body is never eligible for that
// fast path in the first place, so hiding it here costs nothing on the current
// implementation while guaranteeing a hypothetical regression to a fully-buffered
// *bytes.Reader body (which DOES implement WriteTo) gets forced through net/http's
// default bounded-buffer copy loop instead of silently bypassing this tracker.
type trackedRequestBody struct {
	rc      io.ReadCloser
	tracker *requestBodyReadTracker
}

func (b *trackedRequestBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	b.tracker.record(n)

	return n, err
}

func (b *trackedRequestBody) Close() error {
	return b.rc.Close()
}

// trackingBodyDoer wraps a real *http.Client, intercepting every outgoing request's body
// with a trackedRequestBody before handing the request to the real transport — so the
// recorded maximum reflects exactly what net/http itself reads off the body while
// writing the request to the wire, against a genuine httptest.Server round trip.
type trackingBodyDoer struct {
	client  *http.Client
	tracker *requestBodyReadTracker
}

func (d *trackingBodyDoer) HTTPDo(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		req.Body = &trackedRequestBody{rc: req.Body, tracker: d.tracker}
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tracking body doer: %w", err)
	}

	return resp, nil
}

// newMemoryBoundedBlockServer returns an httptest.Server mimicking just enough of the
// import_block HEAD/PUT/POST contract for a single fresh (offset 0) upload: HEAD
// reports no prior bytes, PUT discards the body without retaining it (the whole point of
// this test is to keep the TEST PROCESS's own memory bounded too, not just the code
// under test), and POST .../finished is a no-op success.
func newMemoryBoundedBlockServer(t *testing.T) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("X-Next-Offset", "0")
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			total := r.Header.Get("X-Content-Length")

			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.Header().Set("X-Next-Offset", total)
			w.WriteHeader(http.StatusCreated)
		case http.MethodPost:
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
}

// TestPutBlockCompressed_StreamingIsMemoryBounded is the regression test for
// import-block-streaming-decode-put's core promise: uploading a large compressed block
// volume must never materialize the whole (or a large fraction of the) decompressed
// payload in one in-process buffer. It builds a >=200 MiB, highly-compressible synthetic
// zstd block volume — the on-disk fixture itself stays tiny thanks to the repetition,
// via writeEncodedBlockFile, keeping this test fast despite the large logical size — and
// uploads it through putBlock against a real httptest.Server, tracking the largest
// single Read() the outgoing PUT body ever serves.
//
// zstd is used deliberately: it is both the default codec and, per
// compress/codec_test.go's TestCodec_EncodeFrameStream_MemoryBound, the one codec whose
// ENCODE-side EncodeFrameStream is documented to read a whole chunk in one shot (bounded
// by the chunk-size cap). That fact says nothing about DEcoding, which is what this
// upload path exercises (compress.NewReader(".zst", ...) wraps klauspost's streaming
// zstd.Decoder) — this test verifies decode streaming empirically rather than assuming
// it from the unrelated encode-side behavior, per code-style.md's "verified
// empirically, not from a doc comment" rule.
func TestPutBlockCompressed_StreamingIsMemoryBounded(t *testing.T) {
	const payloadSize = 200 * 1024 * 1024 // >=200 MiB per this task's acceptance criteria

	pattern := []byte("memory-bound-upload-regression-test-data. ")
	payload := bytes.Repeat(pattern, payloadSize/len(pattern)+2)
	payload = payload[:payloadSize]

	dir := t.TempDir()
	dataFile := filepath.Join(dir, "data.bin.zst")
	writeEncodedBlockFile(t, dataFile, "zstd", payload)

	srv := newMemoryBoundedBlockServer(t)
	defer srv.Close()

	tracker := &requestBodyReadTracker{}
	doer := &trackingBodyDoer{client: srv.Client(), tracker: tracker}

	var reported int64

	err := putBlock(context.Background(), doer, srv.URL, dataFile, ".zst", int64(len(payload)), discardLogger(), func(n int) { reported += int64(n) })
	if err != nil {
		t.Fatalf("putBlock: %v", err)
	}

	if reported != int64(len(payload)) {
		t.Errorf("reported progress bytes = %d, want %d (total payload size)", reported, len(payload))
	}

	// 4 MiB sits far below payloadSize yet comfortably above net/http's actual ~32 KiB
	// default copy buffer, giving ample margin while still catching any regression that
	// buffers a large fraction of the payload in one shot.
	const ceiling = 4 * 1024 * 1024

	if got := tracker.max(); got >= ceiling {
		t.Errorf("largest single Read() on the outgoing PUT body was %d bytes, want < %d (%d MiB): "+
			"the streaming upload path must never hand net/http a chunk anywhere near the full %d-byte payload",
			got, ceiling, ceiling/(1024*1024), len(payload))
	}
}

func completedDataImportObj(namespace, name string) *unstructured.Unstructured {
	obj := dataImportObj(namespace, name, false)
	_ = unstructured.SetNestedSlice(obj.Object, readyConditions(conditionCompleted), "status", "conditions")
	_ = unstructured.SetNestedMap(obj.Object, map[string]interface{}{"name": "vsc-1"}, "status", "data", "artifact")

	return obj
}

func TestUploadVolumeData_SkipsCompleted(t *testing.T) {
	// DataFile is set so the block-data preflight passes; the file is never opened because
	// the completed-import short-circuit returns before any upload.
	leaf := PlannedNode{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "pvc-1",
		DataFile:   filepath.Join(t.TempDir(), "data.bin"),
	}

	dyn := newFakeDataImportDyn(completedDataImportObj(targetNS, "pvc-1"))
	imp := newTestVolumeImporter(dyn) // sc is nil: reaching the HTTP upload would panic.

	if err := imp.UploadVolumeData(context.Background(), leaf, "pvc-1", targetNS, nil, nil); err != nil {
		t.Fatalf("UploadVolumeData on an already-completed import must be a no-op: %v", err)
	}
}

func newFakeDataImportDyn(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	gvrToListKind := map[schema.GroupVersionResource]string{
		dataImportGVR: "DataImportList",
	}

	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), gvrToListKind, objs...)
}

func dataImportObj(namespace, name string, expired bool) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": dataImportGVR.GroupVersion().String(),
		"kind":       dataImportKind,
		"metadata":   map[string]interface{}{"namespace": namespace, "name": name},
	}}

	if expired {
		_ = unstructured.SetNestedSlice(obj.Object, readyConditions(conditionExpired), "status", "conditions")
	}

	return obj
}

func countDataImportActions(dyn *dynamicfake.FakeDynamicClient, verb string) int {
	n := 0
	for _, a := range dyn.Actions() {
		if a.GetVerb() == verb && a.GetResource() == dataImportGVR {
			n++
		}
	}

	return n
}

func newTestVolumeImporter(dyn *dynamicfake.FakeDynamicClient) *clusterVolumeImporter {
	return &clusterVolumeImporter{
		dyn:  dyn,
		ttl:  "1h",
		poll: time.Millisecond,
		wait: 2 * time.Second,
		log:  discardLogger(),
	}
}

func TestEnsureDataImport_ReusesHealthy(t *testing.T) {
	leaf := volumeSnapshotLeaf("pvc-1")

	dyn := newFakeDataImportDyn(dataImportObj(targetNS, "pvc-1", false))
	imp := newTestVolumeImporter(dyn)

	name, err := imp.EnsureDataImport(context.Background(), leaf, targetNS)
	if err != nil {
		t.Fatalf("EnsureDataImport: %v", err)
	}

	if name != "pvc-1" {
		t.Errorf("name = %q, want pvc-1", name)
	}

	if c := countDataImportActions(dyn, "create"); c != 0 {
		t.Errorf("a healthy DataImport must be reused, not recreated (creates=%d)", c)
	}

	if d := countDataImportActions(dyn, "delete"); d != 0 {
		t.Errorf("a healthy DataImport must not be deleted (deletes=%d)", d)
	}
}

// volumeSnapshotLeaf builds a CSI VolumeSnapshot data leaf carrying the captured scratch-volume
// parameters EnsureDataImport sends as the PopulateData DataImport's spec.storageParams.
func volumeSnapshotLeaf(name string) PlannedNode {
	return PlannedNode{
		APIVersion:       "snapshot.storage.k8s.io/v1",
		Kind:             "VolumeSnapshot",
		Name:             name,
		StorageClassName: "sc-fast",
		Size:             "10Gi",
		VolumeMode:       "Block",
	}
}

func TestEnsureDataImport_BuildsPopulateDataSpec(t *testing.T) {
	leaf := volumeSnapshotLeaf("pvc-1")

	dyn := newFakeDataImportDyn()
	imp := newTestVolumeImporter(dyn)

	if _, err := imp.EnsureDataImport(context.Background(), leaf, targetNS); err != nil {
		t.Fatalf("EnsureDataImport: %v", err)
	}

	got, err := dyn.Resource(dataImportGVR).Namespace(targetNS).Get(context.Background(), "pvc-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("created DataImport not found: %v", err)
	}

	if v, _, _ := unstructured.NestedString(got.Object, "spec", "mode"); v != "PopulateData" {
		t.Errorf("spec.mode = %q, want PopulateData", v)
	}

	apiVersion, _, _ := unstructured.NestedString(got.Object, "spec", "snapshotRef", "apiVersion")
	kind, _, _ := unstructured.NestedString(got.Object, "spec", "snapshotRef", "kind")
	refName, _, _ := unstructured.NestedString(got.Object, "spec", "snapshotRef", "name")

	if apiVersion != "snapshot.storage.k8s.io/v1" || kind != "VolumeSnapshot" || refName != "pvc-1" {
		t.Errorf("snapshotRef = {apiVersion:%q, kind:%q, name:%q}, want {snapshot.storage.k8s.io/v1, VolumeSnapshot, pvc-1}", apiVersion, kind, refName)
	}

	sc, _, _ := unstructured.NestedString(got.Object, "spec", "storageParams", "storageClassName")
	size, _, _ := unstructured.NestedString(got.Object, "spec", "storageParams", "size")
	volumeMode, _, _ := unstructured.NestedString(got.Object, "spec", "storageParams", "volumeMode")

	if sc != "sc-fast" || size != "10Gi" || volumeMode != "Block" {
		t.Errorf("storageParams = {storageClassName:%q, size:%q, volumeMode:%q}, want {sc-fast, 10Gi, Block}", sc, size, volumeMode)
	}

	// The obsolete Mode A fields must not be sent (the DataImport CRD prunes unknown fields).
	if _, found, _ := unstructured.NestedString(got.Object, "spec", "dataArtifactType"); found {
		t.Error("spec.dataArtifactType must not be set (removed from the DataImport contract)")
	}

	if _, found, _ := unstructured.NestedMap(got.Object, "spec", "targetRef"); found {
		t.Error("spec.targetRef must not be set (replaced by spec.snapshotRef)")
	}
}

func TestEnsureDataImport_RejectsMissingStorageParams(t *testing.T) {
	// A data leaf without captured storage parameters means a malformed/stale archive;
	// EnsureDataImport must fail fast instead of creating an incomplete PopulateData DataImport.
	leaf := PlannedNode{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "pvc-1"}

	dyn := newFakeDataImportDyn()
	imp := newTestVolumeImporter(dyn)

	if _, err := imp.EnsureDataImport(context.Background(), leaf, targetNS); err == nil {
		t.Fatal("expected error for missing storage parameters, got nil")
	}

	if c := countDataImportActions(dyn, "create"); c != 0 {
		t.Errorf("no DataImport must be created when storage parameters are missing (creates=%d)", c)
	}
}

func TestEnsureDataImport_AlignsTTLOnReuse(t *testing.T) {
	leaf := volumeSnapshotLeaf("pvc-1")

	existing := dataImportObj(targetNS, "pvc-1", false)
	_ = unstructured.SetNestedField(existing.Object, "2m", "spec", "ttl")

	dyn := newFakeDataImportDyn(existing)
	imp := newTestVolumeImporter(dyn) // ttl: "1h"

	if _, err := imp.EnsureDataImport(context.Background(), leaf, targetNS); err != nil {
		t.Fatalf("EnsureDataImport: %v", err)
	}

	if c := countDataImportActions(dyn, "create"); c != 0 {
		t.Errorf("a healthy DataImport must be reused, not recreated (creates=%d)", c)
	}

	got, err := dyn.Resource(dataImportGVR).Namespace(targetNS).Get(context.Background(), "pvc-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get DataImport: %v", err)
	}

	ttl, _, _ := unstructured.NestedString(got.Object, "spec", "ttl")
	if ttl != "1h" {
		t.Errorf("spec.ttl = %q, want 1h (aligned to the current run's --ttl)", ttl)
	}
}

func TestEnsureDataImport_RecreatesExpired(t *testing.T) {
	leaf := volumeSnapshotLeaf("pvc-1")

	dyn := newFakeDataImportDyn(dataImportObj(targetNS, "pvc-1", true))
	imp := newTestVolumeImporter(dyn)

	name, err := imp.EnsureDataImport(context.Background(), leaf, targetNS)
	if err != nil {
		t.Fatalf("EnsureDataImport: %v", err)
	}

	if name != "pvc-1" {
		t.Errorf("name = %q, want pvc-1", name)
	}

	if d := countDataImportActions(dyn, "delete"); d != 1 {
		t.Errorf("expired DataImport must be deleted (deletes=%d)", d)
	}

	if c := countDataImportActions(dyn, "create"); c != 1 {
		t.Errorf("a fresh DataImport must be created after expiry (creates=%d)", c)
	}

	got, err := dyn.Resource(dataImportGVR).Namespace(targetNS).Get(context.Background(), "pvc-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("DataImport not present after recreate: %v", err)
	}

	if conditionTrue(got, conditionExpired) {
		t.Errorf("recreated DataImport must not carry the Expired condition")
	}
}

// TestSendVolumeData_FSLeaf_UsesTarFile is the regression test for the wiring bug
// introduced in import-fs-tar-source: UploadVolumeData's Filesystem branch was calling
// importFSFromTar with leaf.DataFile (the block-data glob result, always empty for FS
// leaves), instead of leaf.TarFile. With an empty DataFile, os.Open("") fails immediately.
// This test calls sendVolumeData directly with FilesystemData=true, DataFile="" and a real
// TarFile path; it would fail before the fix (os.Open("")) and must pass after.
func TestSendVolumeData_FSLeaf_UsesTarFile(t *testing.T) {
	// Build a real data.tar with one zstd-compressed file entry.
	content := []byte("hello filesystem import regression")
	ext, compressed := encodeEntry(t, "zstd", content)

	var tarBuf bytes.Buffer

	tw := gotar.NewWriter(&tarBuf)
	addTarEntry(t, tw, "file.txt"+ext, compressed, 0o644, 1000, 2000, time.Now())

	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}

	dir := t.TempDir()
	tarPath := filepath.Join(dir, archive.FsTarName)

	if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
		t.Fatalf("write data.tar: %v", err)
	}

	// PlannedNode carries FilesystemData=true and an empty DataFile (the wiring bug target).
	// Before the fix, sendVolumeData called importFSFromTar with leaf.DataFile (""),
	// causing os.Open("") to fail.
	leaf := PlannedNode{
		APIVersion:     "snapshot.storage.k8s.io/v1",
		Kind:           "VolumeSnapshot",
		Name:           "pvc-1",
		FilesystemData: true,
		DataFile:       "",
		TarFile:        tarPath,
	}

	var mu sync.Mutex

	var putPaths []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)

		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			mu.Lock()
			putPaths = append(putPaths, strings.TrimPrefix(r.URL.Path, "/api/v1/files/"))
			mu.Unlock()

			w.WriteHeader(http.StatusCreated)
		case http.MethodPost:
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	imp := &clusterVolumeImporter{log: discardLogger()}

	if err := imp.sendVolumeData(context.Background(), plainHTTPDoer{}, srv.URL, volumeModeFilesystem, leaf, targetNS, "pvc-1", nil, nil); err != nil {
		t.Fatalf("sendVolumeData with FS leaf and valid TarFile: %v", err)
	}

	mu.Lock()
	n := len(putPaths)
	mu.Unlock()

	// At least one PUT must have reached the server (the decompressed "file.txt" entry).
	if n == 0 {
		t.Error("expected at least one PUT (FS entry uploaded via TarFile), got none")
	}
}
