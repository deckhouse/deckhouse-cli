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
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
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
		_ = req.Body.Close()
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

	offset, _ := strconv.ParseInt(req.Header.Get("X-Offset"), 10, 64)
	respHeader.Set("X-Next-Offset", strconv.FormatInt(offset+req.ContentLength, 10))

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

	if err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, "", int64(len(payload)), discardLogger(), nil, nil); err != nil {
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
	progressCalls := make([]int, 0, 2)

	if err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, "", int64(len(payload)), discardLogger(), func(n int) {
		progressCalls = append(progressCalls, n)
	}, nil); err != nil {
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

	if !slices.Equal(progressCalls, []int{5, 8}) {
		t.Errorf("progress calls = %v, want [5 8] (HEAD prefix exactly once, then PUT suffix)", progressCalls)
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

	if err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, "", int64(len(payload)), discardLogger(), func(n int) { reported += n }, nil); err != nil {
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

	err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, "", int64(len(payload)), discardLogger(), nil, nil)
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
			activated1 := 0

			err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, tc.ext, totalSize, discardLogger(), nil, func() { activated1++ })
			if err == nil {
				t.Fatal("expected attempt 1 (simulated crash mid-transfer) to return an error")
			}

			if got := int64(len(imp.durablyWritten())); got != partialN {
				t.Fatalf("after simulated crash, server durably holds %d bytes, want exactly %d", got, partialN)
			}

			// Attempt 1 genuinely PUT partialN real bytes before the crash, so it must have
			// activated even though it ultimately errored (backlog #21 Bug A).
			if activated1 == 0 {
				t.Error("attempt 1 activate call count = 0, want >= 1 (real bytes were transferred before the crash)")
			}

			// Attempt 2: a genuinely independent invocation of putBlock -- a fresh call
			// with its own local variables, exactly as a restarted process would make.
			// Nothing from attempt 1 is passed in except the same on-disk archive file
			// (which a real restarted process would also re-open from disk) and the same
			// server URL; the resume offset itself is re-derived entirely from this
			// call's own HEAD probe, per headBlockOffset/putBlock.
			var reported int64

			activated2 := 0

			err = putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, tc.ext, totalSize, discardLogger(),
				func(n int) { reported += int64(n) }, func() { activated2++ })
			if err != nil {
				t.Fatalf("putBlock (attempt 2, resume after simulated crash): %v", err)
			}

			got := imp.durablyWritten()
			if !bytes.Equal(got, payload) {
				t.Fatalf("after crash-then-resume, server holds %d bytes not matching the original %d-byte payload "+
					"(a regression here means either duplicated already-durable bytes or dropped bytes)", len(got), len(payload))
			}

			if reported != totalSize {
				t.Errorf("attempt 2 reported %d progress bytes, want %d (validated HEAD prefix plus newly sent suffix)",
					reported, totalSize)
			}

			// Attempt 2 is a partial resume with real remaining bytes to PUT, so it must
			// activate exactly because genuine transfer happens (backlog #21 Bug A).
			if activated2 == 0 {
				t.Error("attempt 2 activate call count = 0, want >= 1 (a partially-resumed upload with real remaining bytes must activate)")
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

			activated := 0

			if err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, tc.ext, int64(len(payload)), discardLogger(),
				func(n int) { reported += n }, func() { activated++ }); err != nil {
				t.Fatalf("putBlock: %v", err)
			}

			if got := imp.received(); !bytes.Equal(got, payload) {
				t.Fatalf("server received %d bytes not matching the original %d-byte payload", len(got), len(payload))
			}

			if reported != len(payload) {
				t.Errorf("reported progress bytes = %d, want %d (total payload size)", reported, len(payload))
			}

			// A first-time (non-resumed, non-skipped) upload must activate the caller's
			// progress stream (backlog #21 Bug A).
			if activated == 0 {
				t.Error("activate call count = 0, want >= 1 (a fresh upload with real transfer must activate)")
			}
		})
	}
}

func TestPutBlock_RawRespectsProducerIngressBodyLimit(t *testing.T) {
	t.Parallel()

	const producerIngressBodyLimit = int64(64 * 1024 * 1024)

	totalSize := producerIngressBodyLimit + 1
	dataFile := filepath.Join(t.TempDir(), "data.bin")

	file, err := os.Create(dataFile)
	if err != nil {
		t.Fatalf("create sparse raw block fixture: %v", err)
	}

	if err := file.Truncate(totalSize); err != nil {
		_ = file.Close()

		t.Fatalf("truncate sparse raw block fixture: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("close sparse raw block fixture: %v", err)
	}

	var (
		offset   int64
		putCount int
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("X-Next-Offset", strconv.FormatInt(offset, 10))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			body := http.MaxBytesReader(w, r.Body, producerIngressBodyLimit)

			written, readErr := io.Copy(io.Discard, body)
			if readErr != nil {
				http.Error(w, "request exceeds producer ingress body limit", http.StatusRequestEntityTooLarge)

				return
			}

			if written != r.ContentLength {
				http.Error(w, "content length mismatch", http.StatusBadRequest)

				return
			}

			offset += written
			putCount++

			w.Header().Set("X-Next-Offset", strconv.FormatInt(offset, 10))
			if offset == totalSize {
				w.WriteHeader(http.StatusCreated)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(srv.Close)

	if err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, "", totalSize, discardLogger(), nil, nil); err != nil {
		t.Fatalf("putBlock: %v", err)
	}

	if putCount != 3 {
		t.Errorf("PUT count = %d, want 3 bounded requests for a 64 MiB + 1 byte payload", putCount)
	}
}

func TestPutBlock_RawAndZstdUseExactBoundedChunks(t *testing.T) {
	t.Parallel()

	payload := randomPayload(t, blockPutPayloadLimit+17)

	tests := []struct {
		name  string
		codec string
		ext   string
	}{
		{name: "raw", codec: "none"},
		{name: "zstd", codec: "zstd", ext: ".zst"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dataFile := filepath.Join(t.TempDir(), "data.bin"+tc.ext)
			writeEncodedBlockFile(t, dataFile, tc.codec, payload)

			var (
				received      []byte
				requestBodies [][]byte
				offsets       []int64
				contentLens   []int64
			)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodHead:
					w.Header().Set("X-Next-Offset", "0")
					w.WriteHeader(http.StatusOK)
				case http.MethodPut:
					offset, parseErr := strconv.ParseInt(r.Header.Get("X-Offset"), 10, 64)
					if parseErr != nil {
						http.Error(w, parseErr.Error(), http.StatusBadRequest)

						return
					}

					body, readErr := io.ReadAll(r.Body)
					if readErr != nil {
						http.Error(w, readErr.Error(), http.StatusInternalServerError)

						return
					}

					offsets = append(offsets, offset)
					contentLens = append(contentLens, r.ContentLength)
					requestBodies = append(requestBodies, body)
					received = append(received, body...)

					next := offset + int64(len(body))
					w.Header().Set("X-Next-Offset", strconv.FormatInt(next, 10))
					if next == int64(len(payload)) {
						w.WriteHeader(http.StatusCreated)
					} else {
						w.WriteHeader(http.StatusNoContent)
					}
				default:
					http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				}
			}))
			t.Cleanup(srv.Close)

			if err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, tc.ext, int64(len(payload)), discardLogger(), nil, nil); err != nil {
				t.Fatalf("putBlock: %v", err)
			}

			wantOffsets := []int64{0, blockPutPayloadLimit}
			if !slices.Equal(offsets, wantOffsets) {
				t.Errorf("X-Offset values = %v, want %v", offsets, wantOffsets)
			}

			wantLengths := []int64{blockPutPayloadLimit, int64(len(payload)) - blockPutPayloadLimit}
			if !slices.Equal(contentLens, wantLengths) {
				t.Errorf("Content-Length values = %v, want %v", contentLens, wantLengths)
			}

			for i, body := range requestBodies {
				if int64(len(body)) > blockPutPayloadLimit {
					t.Errorf("PUT %d body length = %d, want <= client cap %d", i, len(body), blockPutPayloadLimit)
				}

				start := wantOffsets[i]
				end := start + wantLengths[i]
				if !bytes.Equal(body, payload[start:end]) {
					t.Errorf("PUT %d body is not the exact raw suffix [%d,%d)", i, start, end)
				}
			}

			if !bytes.Equal(received, payload) {
				t.Errorf("concatenated PUT bodies differ from the original %d-byte payload", len(payload))
			}
		})
	}
}

func TestPutBlock_RawAndZstdRepositionAfterConflict(t *testing.T) {
	t.Parallel()

	payload := randomPayload(t, 1024*1024+31)
	forwardOffset := int64(len(payload)/3 + 7)

	tests := []struct {
		name             string
		codec            string
		ext              string
		headOffset       int64
		repositionOffset int64
	}{
		{name: "raw forward", codec: "none", repositionOffset: forwardOffset},
		{name: "zstd forward", codec: "zstd", ext: ".zst", repositionOffset: forwardOffset},
		{name: "zstd backward to zero", codec: "zstd", ext: ".zst", headOffset: 19},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dataFile := filepath.Join(t.TempDir(), "data.bin"+tc.ext)
			writeEncodedBlockFile(t, dataFile, tc.codec, payload)

			received := append([]byte(nil), payload[:tc.repositionOffset]...)
			offsets := make([]int64, 0, 2)
			putCount := 0

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodHead:
					// A stale HEAD forces the real producer's 409/X-Expected-Offset recovery path.
					w.Header().Set("X-Next-Offset", strconv.FormatInt(tc.headOffset, 10))
					w.WriteHeader(http.StatusOK)
				case http.MethodPut:
					offset, parseErr := strconv.ParseInt(r.Header.Get("X-Offset"), 10, 64)
					if parseErr != nil {
						http.Error(w, parseErr.Error(), http.StatusBadRequest)

						return
					}

					offsets = append(offsets, offset)
					putCount++

					body, readErr := io.ReadAll(r.Body)
					if readErr != nil {
						http.Error(w, readErr.Error(), http.StatusInternalServerError)

						return
					}

					if putCount == 1 {
						// Consuming the whole rejected body proves compressed retry must rebuild
						// its decoder instead of reusing the now-exhausted stream.
						w.Header().Set("X-Expected-Offset", strconv.FormatInt(tc.repositionOffset, 10))
						http.Error(w, http.StatusText(http.StatusConflict), http.StatusConflict)

						return
					}

					received = append(received, body...)
					next := offset + int64(len(body))
					w.Header().Set("X-Next-Offset", strconv.FormatInt(next, 10))
					w.WriteHeader(http.StatusCreated)
				default:
					http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				}
			}))
			t.Cleanup(srv.Close)

			reported := 0

			err := putBlock(
				context.Background(),
				plainHTTPDoer{},
				srv.URL,
				dataFile,
				tc.ext,
				int64(len(payload)),
				discardLogger(),
				func(n int) { reported += n },
				nil,
			)
			if err != nil {
				t.Fatalf("putBlock: %v", err)
			}

			if !slices.Equal(offsets, []int64{tc.headOffset, tc.repositionOffset}) {
				t.Errorf("X-Offset values = %v, want [%d %d]", offsets, tc.headOffset, tc.repositionOffset)
			}

			if !bytes.Equal(received, payload) {
				t.Errorf("repositioned upload differs from the original %d-byte payload", len(payload))
			}

			if reported != len(payload) {
				t.Errorf("reported progress = %d, want %d durable bytes", reported, len(payload))
			}
		})
	}
}

func TestPutBlock_ConflictRepositionHonorsCancellation(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("cancel-reposition-"), 256)
	dataFile := filepath.Join(t.TempDir(), "data.bin.zst")
	writeEncodedBlockFile(t, dataFile, "zstd", payload)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	putCount := 0
	doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
		header := http.Header{}

		if req.Method == http.MethodHead {
			header.Set("X-Next-Offset", "0")

			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     header,
				Body:       io.NopCloser(bytes.NewReader(nil)),
			}, nil
		}

		putCount++
		cancel()
		header.Set("X-Expected-Offset", "7")

		return &http.Response{
			StatusCode: http.StatusConflict,
			Status:     "409 Conflict",
			Header:     header,
			Body:       io.NopCloser(bytes.NewReader(nil)),
		}, nil
	})

	reported := 0
	err := putBlock(ctx, doer, "https://importer.local/block", dataFile, ".zst", int64(len(payload)), discardLogger(), func(n int) {
		reported += n
	}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want errors.Is(context.Canceled)", err)
	}

	if putCount != 1 {
		t.Errorf("PUT count = %d, want 1 before cancelled decoder rebuild", putCount)
	}

	if reported != 7 {
		t.Errorf("reported progress = %d, want validated durable conflict offset 7", reported)
	}
}

func TestPutBlock_RejectsConflictOffsetLoopWithoutExtraProgress(t *testing.T) {
	t.Parallel()

	payload := []byte("0123456789")
	dataFile := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(dataFile, payload, 0o600); err != nil {
		t.Fatalf("write raw block fixture: %v", err)
	}

	putCount := 0
	doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
		header := http.Header{}

		if req.Method == http.MethodHead {
			header.Set("X-Next-Offset", "0")

			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     header,
				Body:       io.NopCloser(bytes.NewReader(nil)),
			}, nil
		}

		putCount++
		if putCount == 1 {
			header.Set("X-Expected-Offset", "5")
		} else {
			header.Set("X-Expected-Offset", "0")
		}

		return &http.Response{
			StatusCode: http.StatusConflict,
			Status:     "409 Conflict",
			Header:     header,
			Body:       io.NopCloser(bytes.NewReader(nil)),
		}, nil
	})

	reported := 0
	err := putBlock(context.Background(), doer, "https://importer.local/block", dataFile, "", int64(len(payload)), discardLogger(), func(n int) {
		reported += n
	}, nil)
	if err == nil {
		t.Fatal("expected offset-loop error, got nil")
	}

	if putCount != 2 {
		t.Errorf("PUT count = %d, want 2 before detecting 0 -> 5 -> 0 loop", putCount)
	}

	if reported != 5 {
		t.Errorf("reported progress = %d, want only first validated durable offset 5", reported)
	}
}

func TestPutBlock_ZstdAllowsRollbackAfterSuccessfulChunk(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("restart-rollback-"), blockPutPayloadLimit/len("restart-rollback-")+2)
	payload = payload[:blockPutPayloadLimit+17]
	dataFile := filepath.Join(t.TempDir(), "data.bin.zst")
	writeCompressedProofFixture(t, dataFile, "zstd", payload, false, nil)

	var (
		received []byte
		offsets  []int64
		putCount int
	)

	doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
		header := http.Header{}

		if req.Method == http.MethodHead {
			header.Set("X-Next-Offset", "0")

			return newTestHTTPResponse(http.StatusOK, header), nil
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}

		offset, err := strconv.ParseInt(req.Header.Get("X-Offset"), 10, 64)
		if err != nil {
			return nil, err
		}

		offsets = append(offsets, offset)
		putCount++

		if putCount == 2 {
			// The producer restarted after the first successful chunk and lost its
			// in-memory written offset. Its 409 legitimately rolls back to zero.
			received = nil
			header.Set("X-Expected-Offset", "0")

			return newTestHTTPResponse(http.StatusConflict, header), nil
		}

		received = append(received, body...)
		next := offset + int64(len(body))
		header.Set("X-Next-Offset", strconv.FormatInt(next, 10))

		statusCode := http.StatusNoContent
		if next == int64(len(payload)) {
			statusCode = http.StatusCreated
		}

		return newTestHTTPResponse(statusCode, header), nil
	})

	if err := putBlock(
		context.Background(),
		doer,
		"https://importer.local/block",
		dataFile,
		".zst",
		int64(len(payload)),
		discardLogger(),
		nil,
		nil,
	); err != nil {
		t.Fatalf("putBlock: %v", err)
	}

	wantOffsets := []int64{0, blockPutPayloadLimit, 0, blockPutPayloadLimit}
	if !slices.Equal(offsets, wantOffsets) {
		t.Errorf("PUT offsets = %v, want %v", offsets, wantOffsets)
	}

	if !bytes.Equal(received, payload) {
		t.Errorf("re-uploaded suffix differs after producer rollback: got %d bytes, want %d", len(received), len(payload))
	}
}

func TestBlockConflictTracker_BoundsOnlyConsecutiveConflicts(t *testing.T) {
	t.Parallel()

	t.Run("error: consecutive cycle", func(t *testing.T) {
		t.Parallel()

		var tracker blockConflictTracker

		if err := tracker.observe(0, 1); err != nil {
			t.Fatalf("first conflict: %v", err)
		}

		if err := tracker.observe(1, 0); err == nil {
			t.Fatal("expected consecutive conflict cycle error, got nil")
		}
	})

	t.Run("error: bounded acyclic conflicts", func(t *testing.T) {
		t.Parallel()

		var tracker blockConflictTracker

		for offset := int64(0); offset < maxConsecutiveBlockConflicts; offset++ {
			if err := tracker.observe(offset, offset+1); err != nil {
				t.Fatalf("conflict %d: %v", offset, err)
			}
		}

		if err := tracker.observe(maxConsecutiveBlockConflicts, maxConsecutiveBlockConflicts+1); err == nil {
			t.Fatal("expected bounded consecutive-conflict error, got nil")
		}
	})

	t.Run("success resets prior offsets", func(t *testing.T) {
		t.Parallel()

		var tracker blockConflictTracker

		for i := 0; i < 10_000; i++ {
			if err := tracker.observe(32, 0); err != nil {
				t.Fatalf("iteration %d conflict: %v", i, err)
			}

			tracker.reset()
		}

		if tracker.count != 0 {
			t.Errorf("tracker count = %d after successes, want 0", tracker.count)
		}

		if len(tracker.offsets) != maxConsecutiveBlockConflicts+1 {
			t.Errorf("tracker capacity = %d, want fixed %d", len(tracker.offsets), maxConsecutiveBlockConflicts+1)
		}
	})
}

// TestPutBlockCompressed_ResumesViaFastForward verifies that resuming from a nonzero
// HEAD-reported offset re-derives the correct decompressed position via
// io.CopyN(io.Discard, ...) fast-forward and uploads only the remainder, so that the
// pre-seeded prefix (standing in for a prior, interrupted run's durable bytes) plus this
// call's PUT concatenate back to the exact original plaintext. seedLen is deliberately
// not aligned to either encoded frame's boundary, proving the mechanism does not depend
// on frame geometry. Every codec here (zstd included) goes through the same discard
// fallback path (see resolveBlockDecodeReader / discardFromStart): there is currently no
// native chunk-skipping fast path for any codec.
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

			if err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, tc.ext, int64(len(payload)), discardLogger(), nil, nil); err != nil {
				t.Fatalf("putBlock (resume): %v", err)
			}

			if got := imp.received(); !bytes.Equal(got, payload) {
				t.Fatalf("resumed upload produced %d bytes not matching the original %d-byte payload", len(got), len(payload))
			}
		})
	}
}

// randomPayload returns n deterministic pseudo-random bytes. Multi-chunk resume tests use
// this instead of a repeated string pattern so a codec cannot compress each chunk down to
// a handful of bytes, which would make byte-count assertions trivially true regardless of
// which code path actually ran. The seed is fixed purely for test determinism, not for any
// security property.
func randomPayload(t *testing.T, n int) []byte {
	t.Helper()

	buf := make([]byte, n)

	rng := rand.New(rand.NewSource(20260722))
	if _, err := rng.Read(buf); err != nil {
		t.Fatalf("generate deterministic payload: %v", err)
	}

	return buf
}

// TestResolveBlockDecodeReader_ResumedSuffixMatches verifies every compressed codec
// returns a reader at the exact raw resume offset. This small fixture stays inside frame
// zero, so zstd's bounded intra-frame discard equals offset while gzip/lz4 use their
// byte-zero fallback.
func TestResolveBlockDecodeReader_ResumedSuffixMatches(t *testing.T) {
	t.Parallel()

	const chunkSize = 100_000

	const numChunks = 5

	const targetChunk = 3

	payload := randomPayload(t, chunkSize*numChunks)
	offset := int64(targetChunk)*int64(chunkSize) + chunkSize/2

	for _, tc := range blockCodecCases {
		if tc.ext == "" {
			continue // "none" never reaches putBlockCompressed.
		}

		t.Run(tc.codec, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			dataFile := filepath.Join(dir, "data.bin"+tc.ext)
			writeEncodedBlockFile(t, dataFile, tc.codec, payload)

			file, err := os.Open(dataFile)
			if err != nil {
				t.Fatalf("open data file: %v", err)
			}
			defer file.Close()

			decodeReader, discarded, err := resolveBlockDecodeReader(context.Background(), file, dataFile, tc.ext, offset, discardLogger())
			if err != nil {
				t.Fatalf("resolveBlockDecodeReader: %v", err)
			}
			defer decodeReader.Close()

			if discarded != offset {
				t.Fatalf("discarded = %d, want %d", discarded, offset)
			}

			rest, err := io.ReadAll(decodeReader)
			if err != nil {
				t.Fatalf("read remaining decoded bytes: %v", err)
			}

			if !bytes.Equal(rest, payload[offset:]) {
				t.Fatalf("decode reader positioned at the wrong offset: got %d remaining bytes, want %d matching payload[offset:]",
					len(rest), len(payload)-int(offset))
			}
		})
	}
}

func TestUploadControlEndpoints_PropagateResponseDrainDeadline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(httpDoer) error
	}{
		{
			name: "block HEAD",
			run: func(doer httpDoer) error {
				_, err := headBlockOffset(context.Background(), doer, "https://upload.test/api/v1/block", 1)

				return err
			},
		},
		{
			name: "filesystem HEAD",
			run: func(doer httpDoer) error {
				_, _, _, err := headFileOffset(
					context.Background(),
					doer,
					"https://upload.test/api/v1/files/data",
					1,
				)

				return err
			},
		},
		{
			name: "finished POST",
			run: func(doer httpDoer) error {
				return postFinished(context.Background(), doer, "https://upload.test")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			doer := testHTTPDoer(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusNoContent,
					Status:     http.StatusText(http.StatusNoContent),
					Header: http.Header{
						"X-Next-Offset": []string{"0"},
					},
					Body: deadlineResponseBody{},
				}, nil
			})

			err := tc.run(doer)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("error = %v, want context.DeadlineExceeded", err)
			}
		})
	}
}

func TestUploadControlEndpoints_PropagateResponseByteLimit(t *testing.T) {
	t.Parallel()

	newPUTRequest := func(t *testing.T) *http.Request {
		t.Helper()

		req, err := http.NewRequestWithContext(
			context.Background(),
			http.MethodPut,
			"https://upload.test/data",
			io.NopCloser(strings.NewReader("x")),
		)
		if err != nil {
			t.Fatalf("build PUT request: %v", err)
		}

		req.ContentLength = 1

		return req
	}

	tests := []struct {
		name   string
		status int
		next   string
		run    func(*testing.T, httpDoer) error
	}{
		{
			name:   "block HEAD",
			status: http.StatusOK,
			next:   "0",
			run: func(_ *testing.T, doer httpDoer) error {
				_, err := headBlockOffset(context.Background(), doer, "https://upload.test/api/v1/block", 1)

				return err
			},
		},
		{
			name:   "filesystem HEAD",
			status: http.StatusOK,
			next:   "0",
			run: func(_ *testing.T, doer httpDoer) error {
				_, _, _, err := headFileOffset(
					context.Background(),
					doer,
					"https://upload.test/api/v1/files/data",
					1,
				)

				return err
			},
		},
		{
			name:   "block successful PUT",
			status: http.StatusCreated,
			next:   "1",
			run: func(t *testing.T, doer httpDoer) error {
				_, _, err := doBlockChunk(doer, newPUTRequest(t), 0, 1, 1)

				return err
			},
		},
		{
			name:   "block conflict PUT",
			status: http.StatusConflict,
			next:   "0",
			run: func(t *testing.T, doer httpDoer) error {
				_, _, err := doBlockChunk(doer, newPUTRequest(t), 0, 1, 1)

				return err
			},
		},
		{
			name:   "block error response",
			status: http.StatusInternalServerError,
			run: func(t *testing.T, doer httpDoer) error {
				_, _, err := doBlockChunk(doer, newPUTRequest(t), 0, 1, 1)

				return err
			},
		},
		{
			name:   "filesystem successful PUT",
			status: http.StatusCreated,
			next:   "1",
			run: func(t *testing.T, doer httpDoer) error {
				_, _, err := doFileChunk(doer, newPUTRequest(t), 0, 1, 1)

				return err
			},
		},
		{
			name:   "filesystem conflict PUT",
			status: http.StatusConflict,
			next:   "0",
			run: func(t *testing.T, doer httpDoer) error {
				_, _, err := doFileChunk(doer, newPUTRequest(t), 0, 1, 1)

				return err
			},
		},
		{
			name:   "finished POST",
			status: http.StatusOK,
			run: func(_ *testing.T, doer httpDoer) error {
				return postFinished(context.Background(), doer, "https://upload.test")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			doer := testHTTPDoer(func(*http.Request) (*http.Response, error) {
				header := make(http.Header)
				if tc.next != "" {
					header.Set("X-Next-Offset", tc.next)
				}

				return &http.Response{
					StatusCode: tc.status,
					Status:     fmt.Sprintf("%d %s", tc.status, http.StatusText(tc.status)),
					Header:     header,
					Body: causalResponseBody{
						err: safeClient.ErrResponseBodyLimitExceeded,
					},
				}, nil
			})

			err := tc.run(t, doer)
			if !errors.Is(err, safeClient.ErrResponseBodyLimitExceeded) {
				t.Fatalf("error = %v, want ErrResponseBodyLimitExceeded", err)
			}
		})
	}
}

func TestSendVolumeData_WriteDeadlineLeavesResumeOffsetAndSkipsFinished(t *testing.T) {
	t.Parallel()

	const (
		totalSize    = int64(10)
		resumeOffset = int64(3)
	)

	dataFile := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(dataFile, []byte("0123456789"), 0o600); err != nil {
		t.Fatalf("write data file: %v", err)
	}

	var (
		finishedCalls int
		progress      int
		activated     int
	)

	doer := fileHTTPDoer(func(req *http.Request) (*http.Response, error) {
		switch req.Method {
		case http.MethodHead:
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     http.StatusText(http.StatusOK),
				Header: http.Header{
					"X-Next-Offset": []string{strconv.FormatInt(resumeOffset, 10)},
				},
				Body: http.NoBody,
			}, nil
		case http.MethodPut:
			if req.Header.Get("X-Offset") != strconv.FormatInt(resumeOffset, 10) {
				t.Fatalf("PUT offset = %q, want %d", req.Header.Get("X-Offset"), resumeOffset)
			}

			if _, err := io.CopyN(io.Discard, req.Body, 2); err != nil {
				t.Fatalf("read partial PUT body: %v", err)
			}
			if err := req.Body.Close(); err != nil {
				t.Fatalf("close partial PUT body: %v", err)
			}

			return nil, context.DeadlineExceeded
		case http.MethodPost:
			finishedCalls++

			return &http.Response{
				StatusCode: http.StatusNoContent,
				Status:     http.StatusText(http.StatusNoContent),
				Header:     make(http.Header),
				Body:       http.NoBody,
			}, nil
		default:
			return nil, fmt.Errorf("unexpected method %s", req.Method)
		}
	})

	importer := &clusterVolumeImporter{log: discardLogger()}
	leaf := PlannedNode{
		DataFile: dataFile,
		Size:     strconv.FormatInt(totalSize, 10),
	}

	err := importer.sendVolumeData(
		context.Background(),
		doer,
		"https://upload.test",
		volumeModeBlock,
		leaf,
		"target",
		"dataimport",
		nil,
		func(count int) { progress += count },
		func() { activated++ },
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("sendVolumeData error = %v, want context.DeadlineExceeded", err)
	}
	if progress != int(resumeOffset) {
		t.Fatalf("durable progress = %d, want resume offset %d only", progress, resumeOffset)
	}
	if activated != 1 {
		t.Fatalf("activation count = %d, want 1", activated)
	}
	if finishedCalls != 0 {
		t.Fatalf("finished POST calls = %d, want 0 after stalled PUT", finishedCalls)
	}
}

func TestSendVolumeData_ResponseLimitLeavesResumeOffsetAndSkipsFinished(t *testing.T) {
	t.Parallel()

	const (
		totalSize    = int64(10)
		resumeOffset = int64(3)
	)

	dataFile := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(dataFile, []byte("0123456789"), 0o600); err != nil {
		t.Fatalf("write data file: %v", err)
	}

	var (
		finishedCalls int
		progress      int
	)

	doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)

		switch req.Method {
		case http.MethodHead:
			header.Set("X-Next-Offset", strconv.FormatInt(resumeOffset, 10))

			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     http.StatusText(http.StatusOK),
				Header:     header,
				Body:       http.NoBody,
			}, nil
		case http.MethodPut:
			header.Set("X-Next-Offset", strconv.FormatInt(totalSize, 10))

			return &http.Response{
				StatusCode: http.StatusCreated,
				Status:     http.StatusText(http.StatusCreated),
				Header:     header,
				Body: causalResponseBody{
					err: safeClient.ErrResponseBodyLimitExceeded,
				},
			}, nil
		case http.MethodPost:
			finishedCalls++

			return &http.Response{
				StatusCode: http.StatusNoContent,
				Status:     http.StatusText(http.StatusNoContent),
				Header:     header,
				Body:       http.NoBody,
			}, nil
		default:
			return nil, fmt.Errorf("unexpected method %s", req.Method)
		}
	})

	importer := &clusterVolumeImporter{log: discardLogger()}
	leaf := PlannedNode{
		DataFile: dataFile,
		Size:     strconv.FormatInt(totalSize, 10),
	}

	err := importer.sendVolumeData(
		context.Background(),
		doer,
		"https://upload.test",
		volumeModeBlock,
		leaf,
		"target",
		"dataimport",
		nil,
		func(count int) { progress += count },
		nil,
	)
	if !errors.Is(err, safeClient.ErrResponseBodyLimitExceeded) {
		t.Fatalf("sendVolumeData error = %v, want ErrResponseBodyLimitExceeded", err)
	}
	if progress != int(resumeOffset) {
		t.Fatalf("durable progress = %d, want resume offset %d only", progress, resumeOffset)
	}
	if finishedCalls != 0 {
		t.Fatalf("finished POST calls = %d, want 0 after response limit", finishedCalls)
	}
}

type deadlineResponseBody struct{}

func (deadlineResponseBody) Read([]byte) (int, error) {
	return 0, context.DeadlineExceeded
}

func (deadlineResponseBody) Close() error {
	return nil
}

type causalResponseBody struct {
	err error
}

func (b causalResponseBody) Read([]byte) (int, error) {
	return 0, b.err
}

func (causalResponseBody) Close() error {
	return nil
}

type testHTTPDoer func(*http.Request) (*http.Response, error)

func (f testHTTPDoer) HTTPDo(req *http.Request) (*http.Response, error) {
	resp, requestErr := f(req)
	if req.Body == nil {
		return resp, requestErr
	}

	_, drainErr := io.Copy(io.Discard, req.Body)
	if errors.Is(drainErr, http.ErrBodyReadAfterClose) {
		drainErr = nil
	}

	closeErr := req.Body.Close()

	return resp, errors.Join(requestErr, drainErr, closeErr)
}

type testRoundTripper func(*http.Request) (*http.Response, error)

func (f testRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type clientHTTPDoer struct {
	client *http.Client
}

func (d clientHTTPDoer) HTTPDo(req *http.Request) (*http.Response, error) {
	return d.client.Do(req)
}

type testUploadHTTPClient struct {
	do    func(*http.Request) (*http.Response, error)
	close func()
}

func (c *testUploadHTTPClient) HTTPDo(req *http.Request) (*http.Response, error) {
	return c.do(req)
}

func (c *testUploadHTTPClient) CloseIdleConnections() {
	c.close()
}

func newRoundTripDoer(roundTrip testRoundTripper) clientHTTPDoer {
	return clientHTTPDoer{client: &http.Client{Transport: roundTrip}}
}

type testReadCloser struct {
	reader   io.Reader
	closeErr error
	closed   bool
}

func (r *testReadCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *testReadCloser) Close() error {
	r.closed = true

	return r.closeErr
}

type cancelAfterRead struct {
	cancel  context.CancelFunc
	maxRead int
	reads   int
}

func (r *cancelAfterRead) Read(p []byte) (int, error) {
	if len(p) > r.maxRead {
		r.maxRead = len(p)
	}

	r.reads++
	for i := range p {
		p[i] = byte(i)
	}

	if r.reads == 1 {
		r.cancel()
	}

	return len(p), nil
}

type resetFailSeeker struct {
	err error
}

func (s *resetFailSeeker) Read(_ []byte) (int, error) {
	return 0, io.EOF
}

func (s *resetFailSeeker) Seek(_ int64, _ int) (int64, error) {
	return 0, s.err
}

type boundaryFailSeeker struct {
	reader      *bytes.Reader
	boundary    int64
	boundaryErr error
	failed      bool
}

func (s *boundaryFailSeeker) Read(p []byte) (int, error) {
	return s.reader.Read(p)
}

func (s *boundaryFailSeeker) Seek(offset int64, whence int) (int64, error) {
	if !s.failed && whence == io.SeekStart && offset == s.boundary {
		s.failed = true

		return 0, s.boundaryErr
	}

	return s.reader.Seek(offset, whence)
}

func TestResolveBlockDecodeReader_ZstdSkipsWholeFrames(t *testing.T) {
	t.Parallel()

	const (
		chunkSize = int64(100)
		intra     = int64(73)
	)

	offset := chunkSize + intra
	source := bytes.NewReader([]byte("compressed-prefix-frame-suffix"))
	decoded := append(bytes.Repeat([]byte("d"), int(intra)), []byte("wanted-suffix")...)

	var gotFrame int
	var gotCompressedOffset int64

	deps := blockDecodeDependencies{
		skipZstdFrames: func(_ io.ReadSeeker, frame int) (int64, error) {
			gotFrame = frame

			return 7, nil
		},
		newReader: func(ext string, src io.Reader) (io.ReadCloser, error) {
			if ext != ".zst" {
				t.Fatalf("decoder extension = %q, want .zst", ext)
			}

			seeker, ok := src.(io.Seeker)
			if !ok {
				t.Fatal("decoder source does not implement io.Seeker")
			}

			pos, err := seeker.Seek(0, io.SeekCurrent)
			if err != nil {
				t.Fatalf("query decoder source offset: %v", err)
			}

			gotCompressedOffset = pos

			return &testReadCloser{reader: bytes.NewReader(decoded)}, nil
		},
	}

	reader, discarded, err := resolveBlockDecodeReaderWith(
		context.Background(),
		source,
		"data.bin.zst",
		".zst",
		offset,
		chunkSize,
		discardLogger(),
		deps,
	)
	if err != nil {
		t.Fatalf("resolveBlockDecodeReaderWith: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := reader.Close(); closeErr != nil {
			t.Errorf("close resolved reader: %v", closeErr)
		}
	})

	if gotFrame != 1 {
		t.Errorf("walker frame = %d, want 1", gotFrame)
	}

	if gotCompressedOffset != 7 {
		t.Errorf("decoder compressed offset = %d, want 7", gotCompressedOffset)
	}

	if discarded != intra {
		t.Errorf("discarded = %d, want intra-frame %d", discarded, intra)
	}

	if discarded >= chunkSize {
		t.Errorf("discarded = %d, want less than fixed frame size %d", discarded, chunkSize)
	}

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read resolved suffix: %v", err)
	}

	if string(got) != "wanted-suffix" {
		t.Errorf("resolved suffix = %q, want %q", got, "wanted-suffix")
	}
}

func TestResolveBlockDecodeReader_ZstdCorruptionDoesNotYieldSuffix(t *testing.T) {
	t.Parallel()

	const chunkSize = int64(100)

	codec, err := compress.New(compress.DefaultCodecName, 0)
	if err != nil {
		t.Fatalf("create zstd codec: %v", err)
	}

	var encoded bytes.Buffer
	for _, frame := range [][]byte{bytes.Repeat([]byte("a"), int(chunkSize)), bytes.Repeat([]byte("b"), int(chunkSize))} {
		if err := codec.EncodeFrameStream(&encoded, bytes.NewReader(frame), int64(len(frame))); err != nil {
			t.Fatalf("encode zstd frame: %v", err)
		}
	}

	stream := encoded.Bytes()
	tests := []struct {
		name string
		data []byte
	}{
		{name: "corrupt first frame magic", data: append([]byte{stream[0] ^ 0xff}, stream[1:]...)},
		{name: "truncated second frame", data: append([]byte(nil), stream[:len(stream)-1]...)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "data.bin.zst")
			if err := os.WriteFile(path, tc.data, 0o600); err != nil {
				t.Fatalf("write malformed archive: %v", err)
			}

			file, err := os.Open(path)
			if err != nil {
				t.Fatalf("open malformed archive: %v", err)
			}
			t.Cleanup(func() {
				if closeErr := file.Close(); closeErr != nil {
					t.Errorf("close malformed archive: %v", closeErr)
				}
			})

			reader, _, resolveErr := resolveBlockDecodeReaderWith(
				context.Background(), file, path, ".zst", chunkSize+1, chunkSize, discardLogger(), blockDecodeDependencies{
					skipZstdFrames: compress.SkipZstdFrames,
					newReader:      compress.NewReader,
				},
			)
			if resolveErr != nil {
				return
			}

			t.Cleanup(func() {
				if closeErr := reader.Close(); closeErr != nil {
					t.Errorf("close malformed decoder: %v", closeErr)
				}
			})

			if _, err := io.ReadAll(reader); err == nil {
				t.Fatal("malformed zstd archive produced a suffix without an error")
			}
		})
	}
}

func TestResolveBlockDecodeReader_FallbacksResetToByteZero(t *testing.T) {
	t.Parallel()

	walkerErr := errors.New("walker failed")
	decodeErr := errors.New("decoder failed")

	tests := []struct {
		name      string
		ext       string
		failFirst bool
	}{
		{name: "zstd: walker failure", ext: ".zst"},
		{name: "zstd: decoder failure after source consumption", ext: ".zst", failFirst: true},
		{name: "gzip: byte-zero fallback", ext: ".gz"},
		{name: "lz4: byte-zero fallback", ext: ".lz4"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const offset = int64(5)

			source := bytes.NewReader([]byte("encoded-source"))
			decoderCalls := 0
			deps := blockDecodeDependencies{
				skipZstdFrames: func(_ io.ReadSeeker, _ int) (int64, error) {
					if tc.failFirst {
						return 3, nil
					}

					return 0, walkerErr
				},
				newReader: func(_ string, src io.Reader) (io.ReadCloser, error) {
					decoderCalls++

					seeker := src.(io.Seeker)
					pos, err := seeker.Seek(0, io.SeekCurrent)
					if err != nil {
						t.Fatalf("query source offset: %v", err)
					}

					if tc.failFirst && decoderCalls == 1 {
						if pos != 3 {
							t.Fatalf("fast decoder source offset = %d, want 3", pos)
						}

						if _, err := seeker.Seek(2, io.SeekCurrent); err != nil {
							t.Fatalf("consume source before injected decoder failure: %v", err)
						}

						return nil, decodeErr
					}

					if pos != 0 {
						t.Fatalf("fallback decoder source offset = %d, want byte zero", pos)
					}

					return &testReadCloser{reader: strings.NewReader("01234wanted")}, nil
				},
			}

			reader, discarded, err := resolveBlockDecodeReaderWith(
				context.Background(),
				source,
				"data.bin"+tc.ext,
				tc.ext,
				offset,
				volume.DefaultChunkSize,
				discardLogger(),
				deps,
			)
			if err != nil {
				t.Fatalf("resolveBlockDecodeReaderWith: %v", err)
			}
			t.Cleanup(func() {
				if closeErr := reader.Close(); closeErr != nil {
					t.Errorf("close resolved reader: %v", closeErr)
				}
			})

			if discarded != offset {
				t.Errorf("discarded = %d, want full fallback offset %d", discarded, offset)
			}

			got, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("read fallback suffix: %v", err)
			}

			if string(got) != "wanted" {
				t.Errorf("fallback suffix = %q, want %q", got, "wanted")
			}
		})
	}
}

func TestResolveBlockDecodeReader_ClosesFailedDecoders(t *testing.T) {
	t.Parallel()

	readErr := errors.New("discard failed")
	closeErr := errors.New("close failed")

	tests := []struct {
		name           string
		ext            string
		closeErr       error
		wantFallback   bool
		wantCloseError bool
	}{
		{name: "zstd fast discard failure falls back after close", ext: ".zst", wantFallback: true},
		{name: "zstd fast discard and close failures are preserved", ext: ".zst", closeErr: closeErr, wantCloseError: true},
		{name: "gzip fallback discard and close failures are preserved", ext: ".gz", closeErr: closeErr, wantCloseError: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			failedDecoder := &testReadCloser{reader: errorReader{err: readErr}, closeErr: tc.closeErr}
			fallbackDecoder := &testReadCloser{reader: strings.NewReader("01234wanted")}
			decoderCalls := 0

			deps := blockDecodeDependencies{
				skipZstdFrames: func(_ io.ReadSeeker, _ int) (int64, error) {
					return 0, nil
				},
				newReader: func(_ string, _ io.Reader) (io.ReadCloser, error) {
					decoderCalls++
					if decoderCalls == 1 {
						return failedDecoder, nil
					}

					return fallbackDecoder, nil
				},
			}

			reader, _, err := resolveBlockDecodeReaderWith(
				context.Background(),
				bytes.NewReader([]byte("source")),
				"data.bin"+tc.ext,
				tc.ext,
				5,
				volume.DefaultChunkSize,
				discardLogger(),
				deps,
			)

			if !failedDecoder.closed {
				t.Fatal("failed decoder was not closed")
			}

			if tc.wantCloseError {
				if !errors.Is(err, readErr) {
					t.Errorf("error %v does not preserve discard failure", err)
				}

				if !errors.Is(err, closeErr) {
					t.Errorf("error %v does not preserve close failure", err)
				}

				if reader != nil {
					t.Fatal("reader returned together with decoder close error")
				}

				return
			}

			if err != nil {
				t.Fatalf("resolveBlockDecodeReaderWith: %v", err)
			}

			if !tc.wantFallback || decoderCalls != 2 {
				t.Fatalf("decoder calls = %d, want fast attempt plus fallback", decoderCalls)
			}

			t.Cleanup(func() {
				if closeErr := reader.Close(); closeErr != nil {
					t.Errorf("close fallback reader: %v", closeErr)
				}
			})
		})
	}
}

type errorReader struct {
	err error
}

func (r errorReader) Read(_ []byte) (int, error) {
	return 0, r.err
}

func TestResolveBlockDecodeReader_ResetFailureIsReturned(t *testing.T) {
	t.Parallel()

	walkerErr := errors.New("walker failed")
	resetErr := errors.New("reset failed")
	deps := blockDecodeDependencies{
		skipZstdFrames: func(_ io.ReadSeeker, _ int) (int64, error) {
			return 0, walkerErr
		},
		newReader: func(_ string, _ io.Reader) (io.ReadCloser, error) {
			t.Fatal("decoder must not open when reset fails")

			return nil, nil
		},
	}

	reader, _, err := resolveBlockDecodeReaderWith(
		context.Background(),
		&resetFailSeeker{err: resetErr},
		"data.bin.zst",
		".zst",
		1,
		volume.DefaultChunkSize,
		discardLogger(),
		deps,
	)
	if reader != nil {
		t.Fatal("reader returned when reset failed")
	}

	if !errors.Is(err, walkerErr) {
		t.Errorf("error %v does not preserve walker failure", err)
	}

	if !errors.Is(err, resetErr) {
		t.Errorf("error %v does not preserve reset failure", err)
	}
}

func TestResolveBlockDecodeReader_BoundarySeekFailureFallsBackFromZero(t *testing.T) {
	t.Parallel()

	seekErr := errors.New("boundary seek failed")
	source := &boundaryFailSeeker{
		reader:      bytes.NewReader([]byte("encoded")),
		boundary:    7,
		boundaryErr: seekErr,
	}
	deps := blockDecodeDependencies{
		skipZstdFrames: func(_ io.ReadSeeker, _ int) (int64, error) {
			return 7, nil
		},
		newReader: func(_ string, src io.Reader) (io.ReadCloser, error) {
			pos, err := src.(io.Seeker).Seek(0, io.SeekCurrent)
			if err != nil {
				t.Fatalf("query fallback source offset: %v", err)
			}

			if pos != 0 {
				t.Fatalf("fallback source offset = %d, want byte zero", pos)
			}

			return &testReadCloser{reader: strings.NewReader("01234wanted")}, nil
		},
	}

	reader, discarded, err := resolveBlockDecodeReaderWith(
		context.Background(),
		source,
		"data.bin.zst",
		".zst",
		5,
		volume.DefaultChunkSize,
		discardLogger(),
		deps,
	)
	if err != nil {
		t.Fatalf("resolveBlockDecodeReaderWith: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := reader.Close(); closeErr != nil {
			t.Errorf("close fallback reader: %v", closeErr)
		}
	})

	if !source.failed {
		t.Fatal("frame-boundary Seek failure was not injected")
	}

	if discarded != 5 {
		t.Errorf("discarded = %d, want fallback offset 5", discarded)
	}

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read fallback suffix: %v", err)
	}

	if string(got) != "wanted" {
		t.Errorf("fallback suffix = %q, want %q", got, "wanted")
	}
}

func TestResolveBlockDecodeReader_DiscardHonorsContextAndBound(t *testing.T) {
	t.Parallel()

	const maxDiscardRead = 32 * 1024

	tests := []struct {
		name string
		ext  string
	}{
		{name: "zstd intra-frame discard", ext: ".zst"},
		{name: "gzip byte-zero fallback discard", ext: ".gz"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			cancelReader := &cancelAfterRead{cancel: cancel}
			failedDecoder := &testReadCloser{reader: cancelReader}
			deps := blockDecodeDependencies{
				skipZstdFrames: func(_ io.ReadSeeker, _ int) (int64, error) {
					return 0, nil
				},
				newReader: func(_ string, _ io.Reader) (io.ReadCloser, error) {
					return failedDecoder, nil
				},
			}

			reader, _, err := resolveBlockDecodeReaderWith(
				ctx,
				bytes.NewReader([]byte("source")),
				"data.bin"+tc.ext,
				tc.ext,
				int64(blockDiscardBufferSize*2),
				volume.DefaultChunkSize,
				discardLogger(),
				deps,
			)
			if reader != nil {
				t.Fatal("reader returned after cancellation")
			}

			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want errors.Is(context.Canceled)", err)
			}

			if !failedDecoder.closed {
				t.Error("decoder was not closed after cancellation")
			}

			if cancelReader.maxRead > maxDiscardRead {
				t.Errorf("largest discard Read buffer = %d, want <= %d", cancelReader.maxRead, maxDiscardRead)
			}
		})
	}
}

func TestHeadBlockOffset_ValidatesServerOffset(t *testing.T) {
	t.Parallel()

	const totalSize = int64(10)

	tests := []struct {
		name    string
		header  string
		want    int64
		wantErr bool
	}{
		{name: "error: absent header", wantErr: true},
		{name: "success: zero", header: "0", want: 0},
		{name: "success: total", header: "10", want: totalSize},
		{name: "error: malformed", header: "wat", wantErr: true},
		{name: "error: negative", header: "-1", wantErr: true},
		{name: "error: overshoot", header: "11", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doer := testHTTPDoer(func(_ *http.Request) (*http.Response, error) {
				header := http.Header{}
				if tc.header != "" {
					header.Set("X-Next-Offset", tc.header)
				}

				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Header:     header,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			})

			got, err := headBlockOffset(context.Background(), doer, "https://importer.local/block", totalSize)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("headBlockOffset: %v", err)
			}

			if got != tc.want {
				t.Errorf("offset = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestDoBlockChunk_ValidatesAdvancingServerOffset(t *testing.T) {
	t.Parallel()

	const (
		offset     = int64(4)
		requestEnd = int64(7)
		totalSize  = int64(10)
	)

	tests := []struct {
		name    string
		header  string
		want    int64
		wantErr bool
	}{
		{name: "error: absent header", wantErr: true},
		{name: "success: advancing partial", header: "7", want: 7},
		{name: "error: mismatched request end", header: "10", wantErr: true},
		{name: "error: malformed", header: "wat", wantErr: true},
		{name: "error: negative", header: "-1", wantErr: true},
		{name: "error: overshoot", header: "11", wantErr: true},
		{name: "error: equal", header: "4", wantErr: true},
		{name: "error: decreasing", header: "3", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
				header := http.Header{}
				if tc.header != "" {
					header.Set("X-Next-Offset", tc.header)
				}

				_, _ = io.Copy(io.Discard, req.Body)

				return &http.Response{
					StatusCode: http.StatusNoContent,
					Status:     "204 No Content",
					Header:     header,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			})

			body := bytes.Repeat([]byte("x"), int(requestEnd-offset))
			req, err := http.NewRequestWithContext(
				context.Background(),
				http.MethodPut,
				"https://importer.local/block",
				bytes.NewReader(body),
			)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}

			got, reposition, err := doBlockChunk(doer, req, offset, requestEnd, totalSize)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("doBlockChunk: %v", err)
			}

			if got != tc.want {
				t.Errorf("next offset = %d, want %d", got, tc.want)
			}

			if reposition {
				t.Error("successful PUT unexpectedly requested reposition")
			}
		})
	}
}

func TestDoBlockChunk_EnforcesProducerSuccessStatusByPosition(t *testing.T) {
	t.Parallel()

	const totalSize = int64(10)

	tests := []struct {
		name       string
		requestEnd int64
		statusCode int
		wantErr    bool
	}{
		{name: "success: 204 intermediate", requestEnd: 7, statusCode: http.StatusNoContent},
		{name: "error: 201 intermediate", requestEnd: 7, statusCode: http.StatusCreated, wantErr: true},
		{name: "success: 201 final", requestEnd: totalSize, statusCode: http.StatusCreated},
		{name: "error: 204 final", requestEnd: totalSize, statusCode: http.StatusNoContent, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
				header := http.Header{}
				header.Set("X-Next-Offset", strconv.FormatInt(tc.requestEnd, 10))

				_, _ = io.Copy(io.Discard, req.Body)

				return &http.Response{
					StatusCode: tc.statusCode,
					Status:     strconv.Itoa(tc.statusCode) + " " + http.StatusText(tc.statusCode),
					Header:     header,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			})

			body := bytes.Repeat([]byte("x"), int(tc.requestEnd-4))
			req, err := http.NewRequestWithContext(
				context.Background(),
				http.MethodPut,
				"https://importer.local/block",
				bytes.NewReader(body),
			)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}

			_, reposition, err := doBlockChunk(doer, req, 4, tc.requestEnd, totalSize)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected status-position error, got nil")
				}

				if reposition {
					t.Error("impossible success status requested reposition")
				}

				return
			}

			if err != nil {
				t.Fatalf("doBlockChunk: %v", err)
			}

			if reposition {
				t.Error("successful PUT unexpectedly requested reposition")
			}
		})
	}
}

func TestDoBlockChunk_ValidatesConflictOffset(t *testing.T) {
	t.Parallel()

	const (
		offset     = int64(4)
		requestEnd = int64(7)
		totalSize  = int64(10)
	)

	tests := []struct {
		name    string
		header  string
		want    int64
		wantErr bool
	}{
		{name: "success: reposition forward", header: "8", want: 8},
		{name: "success: reposition backward", header: "2", want: 2},
		{name: "error: absent", wantErr: true},
		{name: "error: malformed", header: "wat", wantErr: true},
		{name: "error: negative", header: "-1", wantErr: true},
		{name: "error: overshoot", header: "11", wantErr: true},
		{name: "error: non-progressing", header: "4", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doer := testHTTPDoer(func(_ *http.Request) (*http.Response, error) {
				header := http.Header{}
				if tc.header != "" {
					header.Set("X-Expected-Offset", tc.header)
				}

				return &http.Response{
					StatusCode: http.StatusConflict,
					Status:     "409 Conflict",
					Header:     header,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			})

			req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, "https://importer.local/block", nil)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}

			got, reposition, err := doBlockChunk(doer, req, offset, requestEnd, totalSize)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("doBlockChunk: %v", err)
			}

			if got != tc.want {
				t.Errorf("reposition offset = %d, want %d", got, tc.want)
			}

			if !reposition {
				t.Error("409 response did not request reposition")
			}
		})
	}
}

func TestSendVolumeData_BlockOffsetsGateProgressAndFinalize(t *testing.T) {
	t.Parallel()

	const totalSize = int64(10)

	tests := []struct {
		name         string
		head         string
		put          string
		wantError    bool
		wantPUT      bool
		wantPOST     bool
		wantProgress int
	}{
		{name: "error: missing HEAD", wantError: true},
		{name: "error: malformed HEAD", head: "wat", wantError: true},
		{name: "error: negative HEAD", head: "-1", wantError: true},
		{name: "error: overshoot HEAD", head: "11", wantError: true},
		{name: "success: HEAD equal total finalizes without PUT", head: "10", wantPOST: true, wantProgress: 10},
		{name: "error: missing PUT", head: "0", wantError: true, wantPUT: true},
		{name: "error: malformed PUT", head: "0", put: "wat", wantError: true, wantPUT: true},
		{name: "error: negative PUT", head: "0", put: "-1", wantError: true, wantPUT: true},
		{name: "error: overshoot PUT", head: "0", put: "11", wantError: true, wantPUT: true},
		{name: "error: equal PUT", head: "0", put: "0", wantError: true, wantPUT: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dataFile := filepath.Join(t.TempDir(), "data.bin")
			if err := os.WriteFile(dataFile, bytes.Repeat([]byte("x"), int(totalSize)), 0o600); err != nil {
				t.Fatalf("write raw block fixture: %v", err)
			}

			methods := make([]string, 0, 3)
			doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
				methods = append(methods, req.Method)

				header := http.Header{}
				statusCode := http.StatusOK
				status := "200 OK"

				switch req.Method {
				case http.MethodHead:
					header.Set("X-Next-Offset", tc.head)
				case http.MethodPut:
					header.Set("X-Next-Offset", tc.put)
					statusCode = http.StatusCreated
					status = "201 Created"
				case http.MethodPost:
				}

				return &http.Response{
					StatusCode: statusCode,
					Status:     status,
					Header:     header,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			})

			leaf := PlannedNode{DataFile: dataFile, Ext: "", Size: strconv.FormatInt(totalSize, 10)}
			importer := &clusterVolumeImporter{log: discardLogger()}
			reported := 0

			err := importer.sendVolumeData(
				context.Background(),
				doer,
				"https://importer.local",
				volumeModeBlock,
				leaf,
				"namespace",
				"data-import",
				nil,
				func(n int) { reported += n },
				nil,
			)
			if tc.wantError && err == nil {
				t.Fatal("expected error, got nil")
			}

			if !tc.wantError && err != nil {
				t.Fatalf("sendVolumeData: %v", err)
			}

			if reported != tc.wantProgress {
				t.Errorf("reported progress = %d, want %d", reported, tc.wantProgress)
			}

			gotPUT := slices.Contains(methods, http.MethodPut)
			if gotPUT != tc.wantPUT {
				t.Errorf("PUT observed = %t, want %t; methods=%v", gotPUT, tc.wantPUT, methods)
			}

			gotPOST := slices.Contains(methods, http.MethodPost)
			if gotPOST != tc.wantPOST {
				t.Errorf("POST finalize observed = %t, want %t; methods=%v", gotPOST, tc.wantPOST, methods)
			}
		})
	}
}

// TestPutBlock_SkipsRawFileWhenOffsetEqualsTotal verifies that a raw payload needs no
// second local read once HEAD proves the whole file durable. Compressed payloads are
// deliberately different: they must reopen the archive to prove exact decoded size.
func TestPutBlock_SkipsRawFileWhenOffsetEqualsTotal(t *testing.T) {
	t.Parallel()

	imp := &fakeBlockImporter{}
	imp.seed(bytes.Repeat([]byte("x"), 100))

	srv := httptest.NewServer(imp)
	t.Cleanup(srv.Close)

	missing := filepath.Join(t.TempDir(), "data.bin")

	activated := 0
	reported := 0

	if err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, missing, "", 100, discardLogger(), func(n int) { reported += n }, func() { activated++ }); err != nil {
		t.Fatalf("putBlock raw full skip: %v", err)
	}

	// A fully server-side-skipped block volume (offset==totalSize on HEAD) must never
	// activate the caller's progress stream (backlog #21 Bug A).
	if activated != 0 {
		t.Errorf("activate call count = %d, want 0 (a fully server-side-skipped upload must never activate)", activated)
	}

	if reported != 100 {
		t.Errorf("reported progress = %d, want 100 durable bytes from HEAD", reported)
	}
}

func TestSendVolumeData_CompressedFullSkipRequiresExactDecodedSize(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("full-skip-proof-"), 200)

	tests := []struct {
		name      string
		codec     string
		ext       string
		totalSize int64
		mutate    func([]byte) []byte
		noFCS     bool
		wantErr   bool
	}{
		{name: "success: zstd exact", codec: "zstd", ext: ".zst", totalSize: int64(len(payload))},
		{name: "error: zstd short", codec: "zstd", ext: ".zst", totalSize: int64(len(payload) + 1), wantErr: true},
		{name: "error: zstd extra", codec: "zstd", ext: ".zst", totalSize: int64(len(payload) - 1), wantErr: true},
		{name: "error: zstd truncated", codec: "zstd", ext: ".zst", totalSize: int64(len(payload)), mutate: truncateLastByte, wantErr: true},
		{name: "error: zstd corrupt", codec: "zstd", ext: ".zst", totalSize: int64(len(payload)), mutate: mutateFirstByte, wantErr: true},
		{name: "error: zstd content size less", codec: "zstd", ext: ".zst", totalSize: int64(len(payload)), noFCS: true, wantErr: true},
		{name: "success: gzip exact", codec: "gzip", ext: ".gz", totalSize: int64(len(payload))},
		{name: "error: gzip short", codec: "gzip", ext: ".gz", totalSize: int64(len(payload) + 1), wantErr: true},
		{name: "error: gzip extra", codec: "gzip", ext: ".gz", totalSize: int64(len(payload) - 1), wantErr: true},
		{name: "error: gzip truncated", codec: "gzip", ext: ".gz", totalSize: int64(len(payload)), mutate: truncateLastByte, wantErr: true},
		{name: "success: lz4 exact", codec: "lz4", ext: ".lz4", totalSize: int64(len(payload))},
		{name: "error: lz4 short", codec: "lz4", ext: ".lz4", totalSize: int64(len(payload) + 1), wantErr: true},
		{name: "error: lz4 extra", codec: "lz4", ext: ".lz4", totalSize: int64(len(payload) - 1), wantErr: true},
		{name: "error: lz4 truncated", codec: "lz4", ext: ".lz4", totalSize: int64(len(payload)), mutate: truncateLastByte, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dataFile := filepath.Join(t.TempDir(), "data.bin"+tc.ext)
			writeCompressedProofFixture(t, dataFile, tc.codec, payload, tc.noFCS, tc.mutate)

			finished := 0
			doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
				header := http.Header{}

				switch req.Method {
				case http.MethodHead:
					header.Set("X-Next-Offset", strconv.FormatInt(tc.totalSize, 10))
				case http.MethodPut:
					t.Fatal("full compressed skip must not send PUT")
				case http.MethodPost:
					finished++
				}

				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Header:     header,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			})

			leaf := PlannedNode{DataFile: dataFile, Ext: tc.ext, Size: strconv.FormatInt(tc.totalSize, 10)}
			importer := &clusterVolumeImporter{log: discardLogger()}

			err := importer.sendVolumeData(
				context.Background(),
				doer,
				"https://importer.local",
				volumeModeBlock,
				leaf,
				"namespace",
				"data-import",
				nil,
				nil,
				nil,
			)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected decoded-size proof error, got nil")
				}

				if finished != 0 {
					t.Errorf("finished POST count = %d, want 0", finished)
				}

				return
			}

			if err != nil {
				t.Fatalf("sendVolumeData: %v", err)
			}

			if finished != 1 {
				t.Errorf("finished POST count = %d, want 1", finished)
			}
		})
	}
}

func truncateLastByte(data []byte) []byte {
	return append([]byte(nil), data[:len(data)-1]...)
}

func mutateFirstByte(data []byte) []byte {
	corrupt := append([]byte(nil), data...)
	corrupt[0] ^= 0xFF

	return corrupt
}

func writeCompressedProofFixture(
	t *testing.T,
	path, codecName string,
	payload []byte,
	noFCS bool,
	mutate func([]byte) []byte,
) {
	t.Helper()

	codec, err := compress.New(codecName, 0)
	if err != nil {
		t.Fatalf("compress.New(%q): %v", codecName, err)
	}

	var encoded []byte

	if codecName == "zstd" {
		var buf bytes.Buffer

		if err := codec.EncodeFrameStream(&buf, bytes.NewReader(payload), int64(len(payload))); err != nil {
			t.Fatalf("EncodeFrameStream(%q): %v", codecName, err)
		}

		encoded = buf.Bytes()
	} else {
		encoded, err = codec.EncodeFrame(payload)
		if err != nil {
			t.Fatalf("EncodeFrame(%q): %v", codecName, err)
		}
	}

	if noFCS {
		encoded = removeZstdContentSize(t, encoded)
	}

	if mutate != nil {
		encoded = mutate(encoded)
	}

	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write compressed proof fixture: %v", err)
	}
}

func removeZstdContentSize(t *testing.T, frame []byte) []byte {
	t.Helper()

	if len(frame) < 6 {
		t.Fatalf("zstd fixture is too short: %d", len(frame))
	}

	fhd := frame[4]
	if fhd&0x03 != 0 {
		t.Fatalf("zstd fixture unexpectedly carries a dictionary ID: descriptor 0x%02x", fhd)
	}

	fcsFlag := fhd >> 6
	singleSegment := fhd&(1<<5) != 0

	fcsSize := 0
	switch fcsFlag {
	case 1:
		fcsSize = 2
	case 2:
		fcsSize = 4
	case 3:
		fcsSize = 8
	default:
		if singleSegment {
			fcsSize = 1
		}
	}

	if fcsSize == 0 || len(frame) < 5+fcsSize {
		t.Fatalf("zstd fixture has no removable content size: descriptor 0x%02x", fhd)
	}

	contentSizeLess := make([]byte, 0, len(frame)-fcsSize+1)
	contentSizeLess = append(contentSizeLess, frame[:4]...)
	contentSizeLess = append(contentSizeLess, fhd&0x1F)
	contentSizeLess = append(contentSizeLess, 0x00) // Minimal Window_Descriptor.
	contentSizeLess = append(contentSizeLess, frame[5+fcsSize:]...)

	return contentSizeLess
}

func TestSendVolumeData_TwoRunCompressedUndercountNeverFinalizes(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("two-run-undercount-"), 300)
	totalSize := int64(len(payload) - 7)
	dataFile := filepath.Join(t.TempDir(), "data.bin.zst")
	writeCompressedProofFixture(t, dataFile, "zstd", payload, false, nil)

	var (
		written       []byte
		putCount      int
		finishedCount int
	)

	doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
		header := http.Header{}

		switch req.Method {
		case http.MethodHead:
			header.Set("X-Next-Offset", strconv.Itoa(len(written)))

			return newTestHTTPResponse(http.StatusOK, header), nil
		case http.MethodPut:
			body, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}

			written = append(written, body...)
			putCount++
			header.Set("X-Next-Offset", strconv.Itoa(len(written)))

			return newTestHTTPResponse(http.StatusCreated, header), nil
		case http.MethodPost:
			finishedCount++

			return newTestHTTPResponse(http.StatusNoContent, header), nil
		default:
			return newTestHTTPResponse(http.StatusMethodNotAllowed, header), nil
		}
	})

	leaf := PlannedNode{DataFile: dataFile, Ext: ".zst", Size: strconv.FormatInt(totalSize, 10)}
	importer := &clusterVolumeImporter{log: discardLogger()}

	for run := 1; run <= 2; run++ {
		err := importer.sendVolumeData(
			context.Background(),
			doer,
			"https://importer.local",
			volumeModeBlock,
			leaf,
			"namespace",
			"data-import",
			nil,
			nil,
			nil,
		)
		if err == nil {
			t.Fatalf("run %d: expected undercount error, got nil", run)
		}
	}

	if int64(len(written)) != totalSize {
		t.Errorf("durably written bytes = %d, want declared total %d", len(written), totalSize)
	}

	if putCount != 1 {
		t.Errorf("PUT count = %d, want 1 from the first run only", putCount)
	}

	if finishedCount != 0 {
		t.Errorf("finished POST count = %d, want 0 across both runs", finishedCount)
	}
}

func TestSendVolumeData_ConflictToTotalRequiresExactDecodedSize(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("conflict-to-total-"), 200)

	tests := []struct {
		name      string
		totalSize int64
		wantErr   bool
	}{
		{name: "success: exact", totalSize: int64(len(payload))},
		{name: "error: extra decoded byte", totalSize: int64(len(payload) - 1), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dataFile := filepath.Join(t.TempDir(), "data.bin.zst")
			writeCompressedProofFixture(t, dataFile, "zstd", payload, false, nil)

			putCount := 0
			finishedCount := 0
			doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
				header := http.Header{}

				switch req.Method {
				case http.MethodHead:
					header.Set("X-Next-Offset", "0")

					return newTestHTTPResponse(http.StatusOK, header), nil
				case http.MethodPut:
					putCount++
					header.Set("X-Expected-Offset", strconv.FormatInt(tc.totalSize, 10))

					return newTestHTTPResponse(http.StatusConflict, header), nil
				case http.MethodPost:
					finishedCount++

					return newTestHTTPResponse(http.StatusNoContent, header), nil
				default:
					return newTestHTTPResponse(http.StatusMethodNotAllowed, header), nil
				}
			})

			leaf := PlannedNode{DataFile: dataFile, Ext: ".zst", Size: strconv.FormatInt(tc.totalSize, 10)}
			importer := &clusterVolumeImporter{log: discardLogger()}

			err := importer.sendVolumeData(
				context.Background(),
				doer,
				"https://importer.local",
				volumeModeBlock,
				leaf,
				"namespace",
				"data-import",
				nil,
				nil,
				nil,
			)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected decoded-size proof error, got nil")
				}

				if finishedCount != 0 {
					t.Errorf("finished POST count = %d, want 0", finishedCount)
				}
			} else {
				if err != nil {
					t.Fatalf("sendVolumeData: %v", err)
				}

				if finishedCount != 1 {
					t.Errorf("finished POST count = %d, want 1", finishedCount)
				}
			}

			if putCount != 1 {
				t.Errorf("PUT count = %d, want one producer conflict", putCount)
			}
		})
	}
}

func TestCountDecodedBlock_HonorsContextAndBoundsReads(t *testing.T) {
	t.Parallel()

	const maxProofRead = 32 * 1024

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	reader := &cancelAfterRead{cancel: cancel}
	decoded, err := countDecodedBlock(ctx, reader, maxProofRead*2)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}

	if decoded != maxProofRead {
		t.Errorf("decoded bytes = %d, want one bounded buffer %d", decoded, maxProofRead)
	}

	if reader.maxRead > maxProofRead {
		t.Errorf("largest decode-count Read buffer = %d, want <= %d", reader.maxRead, maxProofRead)
	}
}

func newTestHTTPResponse(statusCode int, header http.Header) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Status:     strconv.Itoa(statusCode) + " " + http.StatusText(statusCode),
		Header:     header,
		Body:       io.NopCloser(bytes.NewReader(nil)),
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

	err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, ".zst", shortTotal, discardLogger(), nil, nil)
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

	err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, ".zst", longTotal, discardLogger(), nil, nil)
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

	if err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, ".zst", int64(len(payload)), discardLogger(), nil, nil); err != nil {
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

// requestBodyReadTracker records two signals about every outgoing PUT request body a
// trackingBodyDoer forwards.
//
// maxRead is the largest single byte count any Read call ever returned. It is REPORTED FOR
// DIAGNOSTICS ONLY and no longer decides pass/fail: net/http's own request-write copy loop
// chunks ANY io.Reader body -- a disk-streamed decode reader or a pre-filled in-memory
// buffer alike -- into the same small (~32KiB) pieces, so this number cannot by itself
// distinguish genuine incremental streaming from full in-memory buffering followed by a
// bytes.Reader-backed body. This was confirmed empirically in the 2026-07-22 whole-batch
// review: a throwaway io.ReadAll-then-bytes.Reader regression in putBlockCompressed still
// produced a maxRead of exactly 32768 here, sailing under any chunk-size ceiling. See
// cross-cutting invariant #11 in .agent/implementer-prompt.md.
//
// heapDelta is the actual discriminating signal: the process live-heap growth sampled ONCE,
// at the very first byte pulled off the tracked body, relative to a baseline armHeapBaseline
// records immediately before the code under test runs. A regression that decodes the whole
// payload into one buffer before ever constructing the request body has that buffer already
// live -- referenced by whatever wraps it as the body -- at the moment of that first Read, so
// the delta spikes by roughly the payload size; genuine incremental streaming never holds more
// than a small decode/copy buffer at once, so the delta stays near zero.
type requestBodyReadTracker struct {
	mu        sync.Mutex
	maxRead   int
	baseline  uint64
	sampled   bool
	heapDelta int64
}

// armHeapBaseline forces a GC pass -- settling unrelated garbage so the reading reflects
// genuinely live objects, not yet-uncollected trash -- and records the current live-heap
// size as the reference point the first tracked Read will compare against. Call this
// immediately before invoking the code under test, after all test-fixture setup: fixtures
// allocated earlier (e.g. the synthetic payload buffer) stay live for the whole test
// regardless of what the code under test does, so they are folded into the baseline and
// cancel out of the delta -- only NEW allocations made by the code under test move it.
func (t *requestBodyReadTracker) armHeapBaseline() {
	runtime.GC()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	t.mu.Lock()
	defer t.mu.Unlock()

	t.baseline = ms.HeapAlloc
}

// record is invoked by trackedRequestBody on every Read of the outgoing PUT body. It always
// tracks maxRead; on the very first call it also samples heapDelta against the armed
// baseline, capturing the live heap at the moment the transport starts consuming the body --
// exactly when a fully-materialized regression buffer would still be resident.
func (t *requestBodyReadTracker) record(n int) {
	if n <= 0 {
		return
	}

	t.mu.Lock()

	if n > t.maxRead {
		t.maxRead = n
	}

	firstRead := !t.sampled
	t.sampled = true
	baseline := t.baseline

	t.mu.Unlock()

	if !firstRead {
		return
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	t.mu.Lock()
	t.heapDelta = int64(ms.HeapAlloc) - int64(baseline)
	t.mu.Unlock()
}

func (t *requestBodyReadTracker) max() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.maxRead
}

// peakHeapDelta returns the live-heap growth sampled at the first Read of the tracked
// request body, relative to the baseline armHeapBaseline recorded. This is what
// TestPutBlockCompressed_StreamingIsMemoryBounded and
// TestImportFSFromTar_StreamingIsMemoryBounded assert against.
func (t *requestBodyReadTracker) peakHeapDelta() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.heapDelta
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
			offset, err := strconv.ParseInt(r.Header.Get("X-Offset"), 10, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			next := offset + r.ContentLength
			w.Header().Set("X-Next-Offset", strconv.FormatInt(next, 10))

			total, err := strconv.ParseInt(r.Header.Get("X-Content-Length"), 10, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if next == total {
				w.WriteHeader(http.StatusCreated)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
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
// uploads it through putBlock against a real httptest.Server.
//
// The pass/fail signal is the live-heap growth requestBodyReadTracker samples at the very
// first Read of the outgoing PUT body (see armHeapBaseline/peakHeapDelta): tracking only the
// PUT body's Read() chunk size, as this test originally did, does NOT detect a full-
// buffering regression — net/http's own request-write copy loop chunks ANY io.Reader body
// into the same small (~32KiB) pieces whether the underlying data was disk-streamed or
// pre-materialized, so that metric was empirically confirmed (2026-07-22 review) to still
// pass under a throwaway io.ReadAll-then-bytes.Reader regression. See cross-cutting
// invariant #11 in .agent/implementer-prompt.md. The chunk-size metric is still reported for
// diagnostics below, but no longer decides the outcome.
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

	// Arm the baseline AFTER every fixture allocation (payload, the on-disk file) so those
	// stay folded into the baseline and only new allocations made by putBlock itself move
	// the delta.
	tracker.armHeapBaseline()

	var reported int64

	err := putBlock(context.Background(), doer, srv.URL, dataFile, ".zst", int64(len(payload)), discardLogger(), func(n int) { reported += int64(n) }, nil)
	if err != nil {
		t.Fatalf("putBlock: %v", err)
	}

	if reported != int64(len(payload)) {
		t.Errorf("reported progress bytes = %d, want %d (total payload size)", reported, len(payload))
	}

	// The heap must not have grown by anywhere near the payload size at the moment the
	// transport started consuming the request body: an order of magnitude below payloadSize
	// comfortably covers zstd's bounded decode window while still catching a regression that
	// materializes a large fraction of the payload in one buffer before handing it to the body.
	const heapCeiling = payloadSize / 10

	if delta := tracker.peakHeapDelta(); delta >= heapCeiling {
		t.Errorf("live heap grew by %d bytes (%.1f MiB) at the first Read of the outgoing PUT "+
			"body, want < %d bytes (%d MiB): the streaming upload path must never have the whole "+
			"(or a large fraction of the) decompressed %d-byte payload already resident in memory "+
			"when the transport starts reading the request body",
			delta, float64(delta)/(1024*1024), heapCeiling, heapCeiling/(1024*1024), len(payload))
	}

	// Diagnostics only (see requestBodyReadTracker's doc comment): this number alone cannot
	// tell genuine streaming apart from full buffering, so it no longer gates the test.
	t.Logf("largest single Read() on the outgoing PUT body: %d bytes", tracker.max())
}

func completedDataImportObj(namespace, name string) *unstructured.Unstructured {
	obj := dataImportObj(namespace, name, false)
	_ = unstructured.SetNestedSlice(obj.Object, readyConditions(conditionCompleted), "status", "conditions")
	_ = unstructured.SetNestedMap(obj.Object, map[string]interface{}{"name": "vsc-1"}, "status", "data", "artifactRef")

	return obj
}

func readyDataImportObj(leaf PlannedNode, rawURL, volumeMode, ca string) *unstructured.Unstructured {
	obj := dataImportObjForLeaf(targetNS, leaf, false)
	_ = unstructured.SetNestedSlice(obj.Object, readyConditions(conditionReady), "status", "conditions")
	_ = unstructured.SetNestedField(obj.Object, rawURL, "status", "url")
	_ = unstructured.SetNestedField(obj.Object, volumeMode, "status", "volumeMode")
	_ = unstructured.SetNestedField(obj.Object, ca, "status", "ca")

	return obj
}

func TestUploadVolumeData_SkipsCompleted(t *testing.T) {
	// DataFile is set so the block-data preflight passes; the file is never opened because
	// the completed-import short-circuit returns before any upload.
	leaf := volumeSnapshotLeaf("pvc-1")
	leaf.DataFile = filepath.Join(t.TempDir(), "data.bin")
	diName := (&clusterVolumeImporter{}).DataImportName(leaf)

	dyn := newFakeDataImportDyn(completedDataImportObj(targetNS, "pvc-1"))
	imp := newTestVolumeImporter(dyn) // sc is nil: reaching the HTTP upload would panic.

	if err := imp.UploadVolumeData(context.Background(), leaf, diName, targetNS, nil, nil, nil); err != nil {
		t.Fatalf("UploadVolumeData on an already-completed import must be a no-op: %v", err)
	}
}

func TestUploadVolumeData_ClosesClientAfterRequestError(t *testing.T) {
	payload := []byte("upload request error")
	dataFile := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(dataFile, payload, 0o600); err != nil {
		t.Fatalf("write block payload: %v", err)
	}

	leaf := volumeSnapshotLeaf("pvc-request-error")
	leaf.DataFile = dataFile
	leaf.Size = strconv.Itoa(len(payload))
	leaf.SizeBytes = int64(len(payload))
	leaf.DataImportIdentity = dataImportIdentity(leaf)

	ca := testUploadCA(t)
	di := readyDataImportObj(leaf, "https://importer.test", volumeModeBlock, base64.StdEncoding.EncodeToString(ca))
	importer := newTestVolumeImporter(newFakeDataImportDyn(di))

	requestErr := errors.New("injected upload request error")

	var (
		builds atomic.Int64
		closes atomic.Int64
	)

	importer.newUploadClient = func(gotCA []byte, rawURL string) (uploadHTTPClient, error) {
		builds.Add(1)

		if !bytes.Equal(gotCA, ca) {
			t.Fatalf("decoded CA = %q, want %q", gotCA, ca)
		}
		if rawURL != "https://importer.test" {
			t.Fatalf("upload client origin = %q, want https://importer.test", rawURL)
		}

		return &testUploadHTTPClient{
			do: func(*http.Request) (*http.Response, error) {
				return nil, requestErr
			},
			close: func() {
				closes.Add(1)
			},
		}, nil
	}

	err := importer.UploadVolumeData(
		context.Background(),
		leaf,
		importer.DataImportName(leaf),
		targetNS,
		nil,
		nil,
		nil,
	)
	if !errors.Is(err, requestErr) {
		t.Fatalf("UploadVolumeData error = %v, want request error", err)
	}
	if builds.Load() != 1 {
		t.Fatalf("upload client builds = %d, want 1", builds.Load())
	}
	if closes.Load() != 1 {
		t.Fatalf("upload client closes = %d, want 1", closes.Load())
	}
}

func TestUploadVolumeData_CancellationClosesOnlyAfterInFlightRequestReturns(t *testing.T) {
	payload := []byte("cancel upload")
	dataFile := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(dataFile, payload, 0o600); err != nil {
		t.Fatalf("write block payload: %v", err)
	}

	leaf := volumeSnapshotLeaf("pvc-cancel")
	leaf.DataFile = dataFile
	leaf.Size = strconv.Itoa(len(payload))
	leaf.SizeBytes = int64(len(payload))
	leaf.DataImportIdentity = dataImportIdentity(leaf)

	ca := testUploadCA(t)
	di := readyDataImportObj(leaf, "https://importer.test", volumeModeBlock, base64.StdEncoding.EncodeToString(ca))
	importer := newTestVolumeImporter(newFakeDataImportDyn(di))

	requestStarted := make(chan struct{})

	var closes atomic.Int64

	importer.newUploadClient = func([]byte, string) (uploadHTTPClient, error) {
		return &testUploadHTTPClient{
			do: func(req *http.Request) (*http.Response, error) {
				close(requestStarted)
				<-req.Context().Done()

				return nil, req.Context().Err()
			},
			close: func() {
				closes.Add(1)
			},
		}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)

	go func() {
		result <- importer.UploadVolumeData(
			ctx,
			leaf,
			importer.DataImportName(leaf),
			targetNS,
			nil,
			nil,
			nil,
		)
	}()

	<-requestStarted

	if closes.Load() != 0 {
		t.Fatalf("upload client closed while request was in flight: %d", closes.Load())
	}

	cancel()

	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("UploadVolumeData error = %v, want context cancellation", err)
	}
	if closes.Load() != 1 {
		t.Fatalf("upload client closes after cancellation = %d, want 1", closes.Load())
	}
}

func TestUploadClientRejectsInvalidIdentityBeforeFactory(t *testing.T) {
	validCA := base64.StdEncoding.EncodeToString(testUploadCA(t))
	tests := []struct {
		name   string
		rawURL string
		ca     string
	}{
		{
			name:   "plaintext URL",
			rawURL: "http://127.0.0.1:8443",
			ca:     validCA,
		},
		{
			name:   "missing CA",
			rawURL: "https://127.0.0.1:8443",
		},
		{
			name:   "invalid base64 CA",
			rawURL: "https://127.0.0.1:8443",
			ca:     "%%%",
		},
		{
			name:   "malformed CA",
			rawURL: "https://127.0.0.1:8443",
			ca:     base64.StdEncoding.EncodeToString([]byte("not PEM")),
		},
		{
			name:   "certificate-less CA",
			rawURL: "https://127.0.0.1:8443",
			ca: base64.StdEncoding.EncodeToString(
				pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("key")}),
			),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var factoryCalls atomic.Int64

			importer := &clusterVolumeImporter{
				newUploadClient: func([]byte, string) (uploadHTTPClient, error) {
					factoryCalls.Add(1)

					return nil, errors.New("factory must not run")
				},
			}

			if _, err := importer.uploadClient(tc.ca, tc.rawURL); err == nil {
				t.Fatal("uploadClient unexpectedly accepted invalid identity")
			}
			if factoryCalls.Load() != 0 {
				t.Fatalf("upload client factory calls = %d, want 0", factoryCalls.Load())
			}
		})
	}
}

func testUploadCA(t *testing.T) []byte {
	t.Helper()

	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	certificate := server.Certificate()
	if certificate == nil {
		t.Fatal("TLS test server has no certificate")
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
}

func TestUploadVolumeData_CompletedReuseRejectsChangedVerifiedPayload(t *testing.T) {
	payload := []byte("verified completed payload")
	nodeSpec := archiveNode{
		apiVersion: "snapshot.storage.k8s.io/v1",
		kind:       "VolumeSnapshot",
		name:       "pvc-1",
		namespace:  "src",
		blockData:  payload,
	}
	nodeSpec.volumes = synthVolumeInfo(nodeSpec)
	nodeSpec.volumes[0].Size = strconv.Itoa(len(payload))

	root := t.TempDir()
	writeArchiveNode(t, root, nodeSpec)

	view, err := archive.OpenVerifiedArchive(root)
	if err != nil {
		t.Fatalf("open verified archive: %v", err)
	}
	defer func() { _ = view.Close() }()

	plan, err := buildPlanFromVerifiedArchive(view)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	if err := verifyArchiveIntegrity(context.Background(), view, plan); err != nil {
		t.Fatalf("verify archive: %v", err)
	}

	leaf := plan[0]
	completed := dataImportObjForLeaf(targetNS, leaf, false)
	_ = unstructured.SetNestedSlice(completed.Object, readyConditions(conditionCompleted), "status", "conditions")
	_ = unstructured.SetNestedMap(completed.Object, map[string]interface{}{"name": "vsc-1"}, "status", "data", "artifactRef")

	dataPath := filepath.Join(root, archive.DataBlockName(""))
	if err := os.Rename(dataPath, dataPath+".verified"); err != nil {
		t.Fatalf("move verified payload: %v", err)
	}

	if err := os.WriteFile(dataPath, []byte("changed completed payload!"), 0o600); err != nil {
		t.Fatalf("write changed payload: %v", err)
	}

	importer := newTestVolumeImporter(newFakeDataImportDyn(completed))
	diName := importer.DataImportName(leaf)

	err = importer.UploadVolumeData(context.Background(), leaf, diName, targetNS, nil, nil, nil)
	if !errors.Is(err, archive.ErrVerifiedArchiveChanged) {
		t.Fatalf("UploadVolumeData error = %v, want ErrVerifiedArchiveChanged", err)
	}
}

func newFakeDataImportDyn(objs ...k8sruntime.Object) *dynamicfake.FakeDynamicClient {
	gvrToListKind := map[schema.GroupVersionResource]string{
		dataImportGVR: "DataImportList",
	}

	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(k8sruntime.NewScheme(), gvrToListKind, objs...)
}

func dataImportObj(namespace, name string, expired bool) *unstructured.Unstructured {
	leaf := volumeSnapshotLeaf(name)
	return dataImportObjForLeaf(namespace, leaf, expired)
}

func dataImportObjForLeaf(namespace string, leaf PlannedNode, expired bool) *unstructured.Unstructured {
	diName := (&clusterVolumeImporter{}).DataImportName(leaf)
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": dataImportGVR.GroupVersion().String(),
		"kind":       dataImportKind,
		"metadata": map[string]interface{}{
			"namespace":   namespace,
			"name":        diName,
			"labels":      stringMapToInterfaceMap(map[string]string{dataImportIdentityLabel: dataImportShortID(leaf)}),
			"annotations": stringMapToInterfaceMap(dataImportAnnotations(leaf)),
		},
		"spec": map[string]interface{}{
			"ttl":  "1h",
			"mode": dataImportModePopulateData,
			"snapshotRef": map[string]interface{}{
				"apiVersion": leaf.APIVersion,
				"kind":       leaf.Kind,
				"name":       leaf.Name,
			},
			"storageParams": map[string]interface{}{
				"storageClassName": leaf.StorageClassName,
				"size":             strconv.FormatInt(leaf.SizeBytes, 10),
				"volumeMode":       leaf.VolumeMode,
			},
		},
	}}

	if expired {
		// The terminal Expired state is signalled as Ready=False with reason "Expired"
		// (the standalone "Expired" condition type was removed from the catalog).
		expiredCond := []interface{}{map[string]interface{}{
			"type":   conditionReady,
			"status": "False",
			"reason": reasonExpired,
		}}
		_ = unstructured.SetNestedSlice(obj.Object, expiredCond, "status", "conditions")
	}

	return obj
}

func stringMapToInterfaceMap(values map[string]string) map[string]interface{} {
	result := make(map[string]interface{}, len(values))
	for key, value := range values {
		result[key] = value
	}

	return result
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

	existing := dataImportObj(targetNS, "pvc-1", false)
	spec := existing.Object["spec"].(map[string]interface{})
	spec["storageParams"].(map[string]interface{})["size"] = "10Gi"

	dyn := newFakeDataImportDyn(existing)
	imp := newTestVolumeImporter(dyn)

	name, err := imp.EnsureDataImport(context.Background(), leaf, targetNS)
	if err != nil {
		t.Fatalf("EnsureDataImport: %v", err)
	}

	if want := imp.DataImportName(leaf); name != want {
		t.Errorf("name = %q, want %q", name, want)
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
	leaf := PlannedNode{
		APIVersion:       "snapshot.storage.k8s.io/v1",
		Kind:             "VolumeSnapshot",
		Name:             name,
		StorageClassName: "sc-fast",
		Size:             "10Gi",
		SizeBytes:        10 * 1024 * 1024 * 1024,
		VolumeMode:       "Block",
		NodeChecksum:     strings.Repeat("a", sha256HexLength),
		PayloadKind:      dataImportPayloadBlock,
		Codec:            "none",
	}
	leaf.DataImportIdentity = dataImportIdentity(leaf)

	return leaf
}

func TestEnsureDataImport_BuildsPopulateDataSpec(t *testing.T) {
	leaf := volumeSnapshotLeaf("pvc-1")

	dyn := newFakeDataImportDyn()
	imp := newTestVolumeImporter(dyn)

	if _, err := imp.EnsureDataImport(context.Background(), leaf, targetNS); err != nil {
		t.Fatalf("EnsureDataImport: %v", err)
	}

	diName := imp.DataImportName(leaf)

	got, err := dyn.Resource(dataImportGVR).Namespace(targetNS).Get(context.Background(), diName, metav1.GetOptions{})
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

	if sc != "sc-fast" || size != "10737418240" || volumeMode != "Block" {
		t.Errorf("storageParams = {storageClassName:%q, size:%q, volumeMode:%q}, want {sc-fast, 10737418240, Block}", sc, size, volumeMode)
	}

	if got.GetLabels()[dataImportIdentityLabel] != dataImportShortID(leaf) {
		t.Errorf("identity label = %q, want %q",
			got.GetLabels()[dataImportIdentityLabel], dataImportShortID(leaf))
	}

	for key, want := range dataImportAnnotations(leaf) {
		if value := got.GetAnnotations()[key]; value != want {
			t.Errorf("annotation %q = %q, want %q", key, value, want)
		}
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

	diName := imp.DataImportName(leaf)

	got, err := dyn.Resource(dataImportGVR).Namespace(targetNS).Get(context.Background(), diName, metav1.GetOptions{})
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

	if want := imp.DataImportName(leaf); name != want {
		t.Errorf("name = %q, want %q", name, want)
	}

	if d := countDataImportActions(dyn, "delete"); d != 1 {
		t.Errorf("expired DataImport must be deleted (deletes=%d)", d)
	}

	if c := countDataImportActions(dyn, "create"); c != 1 {
		t.Errorf("a fresh DataImport must be created after expiry (creates=%d)", c)
	}

	got, err := dyn.Resource(dataImportGVR).Namespace(targetNS).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("DataImport not present after recreate: %v", err)
	}

	if conditionFalseWithReason(got, conditionReady, reasonExpired) {
		t.Errorf("recreated DataImport must not be in the Ready=False/Expired state")
	}
}

func TestEnsureDataImport_RejectsForeignIdentityAndSpec(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*unstructured.Unstructured)
	}{
		{
			name: "missing identity label",
			mutate: func(obj *unstructured.Unstructured) {
				labels := obj.GetLabels()
				delete(labels, dataImportIdentityLabel)
				obj.SetLabels(labels)
			},
		},
		{
			name: "wrong full identity",
			mutate: func(obj *unstructured.Unstructured) {
				annotations := obj.GetAnnotations()
				annotations[dataImportIdentityAnnotation] = strings.Repeat("f", sha256HexLength)
				obj.SetAnnotations(annotations)
			},
		},
		{
			name: "wrong identity version",
			mutate: func(obj *unstructured.Unstructured) {
				annotations := obj.GetAnnotations()
				annotations[dataImportIdentityVersionAnnotation] = "v2"
				obj.SetAnnotations(annotations)
			},
		},
		{
			name: "wrong checksum",
			mutate: func(obj *unstructured.Unstructured) {
				annotations := obj.GetAnnotations()
				annotations[dataImportNodeChecksumAnnotation] = strings.Repeat("b", sha256HexLength)
				obj.SetAnnotations(annotations)
			},
		},
		{
			name: "wrong annotated volume mode",
			mutate: func(obj *unstructured.Unstructured) {
				annotations := obj.GetAnnotations()
				annotations[dataImportVolumeModeAnnotation] = archive.VolumeModeFilesystem
				obj.SetAnnotations(annotations)
			},
		},
		{
			name: "wrong annotated storage class",
			mutate: func(obj *unstructured.Unstructured) {
				annotations := obj.GetAnnotations()
				annotations[dataImportStorageClassAnnotation] = "sc-slow"
				obj.SetAnnotations(annotations)
			},
		},
		{
			name: "wrong annotated size",
			mutate: func(obj *unstructured.Unstructured) {
				annotations := obj.GetAnnotations()
				annotations[dataImportSizeBytesAnnotation] = "1"
				obj.SetAnnotations(annotations)
			},
		},
		{
			name: "wrong annotated payload kind",
			mutate: func(obj *unstructured.Unstructured) {
				annotations := obj.GetAnnotations()
				annotations[dataImportPayloadKindAnnotation] = dataImportPayloadFilesystem
				obj.SetAnnotations(annotations)
			},
		},
		{
			name: "wrong annotated codec",
			mutate: func(obj *unstructured.Unstructured) {
				annotations := obj.GetAnnotations()
				annotations[dataImportCodecAnnotation] = "zstd"
				obj.SetAnnotations(annotations)
			},
		},
		{
			name: "wrong mode",
			mutate: func(obj *unstructured.Unstructured) {
				obj.Object["spec"].(map[string]interface{})["mode"] = "Clone"
			},
		},
		{
			name: "wrong snapshotRef",
			mutate: func(obj *unstructured.Unstructured) {
				spec := obj.Object["spec"].(map[string]interface{})
				spec["snapshotRef"].(map[string]interface{})["name"] = "other"
			},
		},
		{
			name: "missing storage class",
			mutate: func(obj *unstructured.Unstructured) {
				unstructured.RemoveNestedField(obj.Object, "spec", "storageParams", "storageClassName")
			},
		},
		{
			name: "wrong volume mode",
			mutate: func(obj *unstructured.Unstructured) {
				spec := obj.Object["spec"].(map[string]interface{})
				spec["storageParams"].(map[string]interface{})["volumeMode"] = archive.VolumeModeFilesystem
			},
		},
		{
			name: "wrong size",
			mutate: func(obj *unstructured.Unstructured) {
				spec := obj.Object["spec"].(map[string]interface{})
				spec["storageParams"].(map[string]interface{})["size"] = "1Gi"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			leaf := volumeSnapshotLeaf("pvc-1")
			existing := dataImportObjForLeaf(targetNS, leaf, false)
			test.mutate(existing)

			dyn := newFakeDataImportDyn(existing)
			imp := newTestVolumeImporter(dyn)

			_, err := imp.EnsureDataImport(context.Background(), leaf, targetNS)
			if !errors.Is(err, ErrForeignDataImport) {
				t.Fatalf("EnsureDataImport error = %v, want ErrForeignDataImport", err)
			}

			for _, verb := range []string{"create", "update", "patch", "delete"} {
				if count := countDataImportActions(dyn, verb); count != 0 {
					t.Errorf("foreign DataImport triggered %s (%d action(s))", verb, count)
				}
			}
		})
	}
}

func TestEnsureDataImport_ForeignExpiredIsNeverDeleted(t *testing.T) {
	leaf := volumeSnapshotLeaf("pvc-1")
	existing := dataImportObjForLeaf(targetNS, leaf, true)
	annotations := existing.GetAnnotations()
	annotations[dataImportCodecAnnotation] = "zstd"
	existing.SetAnnotations(annotations)

	dyn := newFakeDataImportDyn(existing)
	imp := newTestVolumeImporter(dyn)

	_, err := imp.EnsureDataImport(context.Background(), leaf, targetNS)
	if !errors.Is(err, ErrForeignDataImport) {
		t.Fatalf("EnsureDataImport error = %v, want ErrForeignDataImport", err)
	}

	if count := countDataImportActions(dyn, "delete"); count != 0 {
		t.Errorf("foreign expired DataImport was deleted (%d delete action(s))", count)
	}
}

func TestEnsureDataImport_ForcedTruncatedIdentityCollision(t *testing.T) {
	owner := volumeSnapshotLeaf("pvc-1")
	collider := owner
	collider.DataImportIdentity = owner.DataImportIdentity[:dataImportIdentityIDLength] +
		strings.Repeat("f", sha256HexLength-dataImportIdentityIDLength)

	imp := &clusterVolumeImporter{}
	if imp.DataImportName(owner) != imp.DataImportName(collider) {
		t.Fatal("test setup did not force a truncated identity collision")
	}

	dyn := newFakeDataImportDyn(dataImportObjForLeaf(targetNS, owner, false))
	imp = newTestVolumeImporter(dyn)

	_, err := imp.EnsureDataImport(context.Background(), collider, targetNS)
	if !errors.Is(err, ErrForeignDataImport) {
		t.Fatalf("EnsureDataImport error = %v, want ErrForeignDataImport", err)
	}
}

func TestEnsureDataImport_AlreadyExistsRaceConverges(t *testing.T) {
	leaf := volumeSnapshotLeaf("pvc-1")
	dyn := newFakeDataImportDyn()
	imp := newTestVolumeImporter(dyn)

	var once sync.Once
	dyn.PrependReactor("create", dataImportGVR.Resource, func(action clienttesting.Action) (bool, k8sruntime.Object, error) {
		var addErr error

		once.Do(func() {
			create := action.(clienttesting.CreateAction)
			addErr = dyn.Tracker().Add(create.GetObject().DeepCopyObject())
		})

		if addErr != nil {
			return true, nil, fmt.Errorf("seed raced DataImport: %w", addErr)
		}

		return true, nil, kubeerrors.NewAlreadyExists(dataImportGVR.GroupResource(), imp.DataImportName(leaf))
	})

	name, err := imp.EnsureDataImport(context.Background(), leaf, targetNS)
	if err != nil {
		t.Fatalf("EnsureDataImport: %v", err)
	}

	if want := imp.DataImportName(leaf); name != want {
		t.Errorf("name = %q, want %q", name, want)
	}
}

func TestEnsureDataImport_ConcurrentArchivesSeparateByIdentity(t *testing.T) {
	same := volumeSnapshotLeaf("pvc-1")
	different := same
	different.NodeChecksum = strings.Repeat("b", sha256HexLength)
	different.DataImportIdentity = dataImportIdentity(different)

	dyn := newFakeDataImportDyn()
	imp := newTestVolumeImporter(dyn)
	leaves := []PlannedNode{same, same, different}

	errs := make(chan error, len(leaves))

	var group sync.WaitGroup
	group.Add(len(leaves))

	for i := range leaves {
		leaf := leaves[i]

		go func() {
			defer group.Done()

			_, err := imp.EnsureDataImport(context.Background(), leaf, targetNS)
			errs <- err
		}()
	}

	group.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("concurrent EnsureDataImport: %v", err)
		}
	}

	list, err := dyn.Resource(dataImportGVR).Namespace(targetNS).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list DataImports: %v", err)
	}

	if len(list.Items) != 2 {
		t.Errorf("DataImport count = %d, want 2 (same content converges, different content separates)", len(list.Items))
	}
}

func TestUploadVolumeData_ForeignCompletedCannotSkip(t *testing.T) {
	leaf := volumeSnapshotLeaf("pvc-1")
	leaf.DataFile = filepath.Join(t.TempDir(), "data.bin")
	existing := completedDataImportObj(targetNS, leaf.Name)
	annotations := existing.GetAnnotations()
	annotations[dataImportStorageClassAnnotation] = "foreign-sc"
	existing.SetAnnotations(annotations)

	dyn := newFakeDataImportDyn(existing)
	imp := newTestVolumeImporter(dyn)

	err := imp.UploadVolumeData(
		context.Background(),
		leaf,
		imp.DataImportName(leaf),
		targetNS,
		nil,
		nil,
		nil,
	)
	if !errors.Is(err, ErrForeignDataImport) {
		t.Fatalf("UploadVolumeData error = %v, want ErrForeignDataImport", err)
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

	var totals []int64

	setTotal := func(n int64) { totals = append(totals, n) }

	if err := imp.sendVolumeData(context.Background(), plainHTTPDoer{}, srv.URL, volumeModeFilesystem, leaf, targetNS, "pvc-1", setTotal, nil, nil); err != nil {
		t.Fatalf("sendVolumeData with FS leaf and valid TarFile: %v", err)
	}

	mu.Lock()
	n := len(putPaths)
	mu.Unlock()

	// At least one PUT must have reached the server (the decompressed "file.txt" entry).
	if n == 0 {
		t.Error("expected at least one PUT (FS entry uploaded via TarFile), got none")
	}

	// sendVolumeData's FS branch must thread setTotal through to importFSFromTar instead
	// of discarding it: with a single not-done entry, setTotal is called exactly once,
	// with that entry's exact decompressed size.
	if want := []int64{int64(len(content))}; len(totals) != len(want) || totals[0] != want[0] {
		t.Errorf("setTotal calls = %v, want %v (FS branch must thread setTotal through to importFSFromTar)", totals, want)
	}
}

// TestBlockTotalSize covers every codec and every invalid-size shape
// blockTotalSize must handle: the raw (ext=="") on-disk size is cross-checked
// against the captured VolumeInfo.Size for BOTH a short and a long mismatch,
// while a compressed file's on-disk (compressed) size is never compared to
// the captured (decompressed) size at all. A missing or unparsable captured
// size fails regardless of codec.
func TestBlockTotalSize(t *testing.T) {
	tests := []struct {
		name        string
		ext         string
		size        string
		fileContent []byte // nil => no on-disk file at all
		wantTotal   int64
		wantErr     error // nil => any non-nil error is acceptable
		wantErrNil  bool
	}{
		{
			name:        "raw exact match",
			ext:         "",
			size:        "10",
			fileContent: []byte("0123456789"),
			wantTotal:   10,
			wantErrNil:  true,
		},
		{
			name:        "raw short mismatch (on-disk smaller than captured)",
			ext:         "",
			size:        "10",
			fileContent: []byte("12345"),
			wantErr:     ErrRawBlockSizeMismatch,
		},
		{
			name:        "raw long mismatch (on-disk larger than captured)",
			ext:         "",
			size:        "10",
			fileContent: []byte("012345678901234567890123456789"),
			wantErr:     ErrRawBlockSizeMismatch,
		},
		{
			name:        "zstd: on-disk (compressed) size never compared to captured size",
			ext:         ".zst",
			size:        "10Gi",
			fileContent: []byte("short-compressed-stand-in"),
			wantTotal:   10 * 1024 * 1024 * 1024,
			wantErrNil:  true,
		},
		{
			name:        "gzip: captured size is authoritative",
			ext:         ".gz",
			size:        "5Mi",
			fileContent: []byte("x"),
			wantTotal:   5 * 1024 * 1024,
			wantErrNil:  true,
		},
		{
			name:        "lz4: captured size is authoritative",
			ext:         ".lz4",
			size:        "1Ki",
			fileContent: []byte("x"),
			wantTotal:   1024,
			wantErrNil:  true,
		},
		{
			name:        "missing captured size",
			ext:         "",
			size:        "",
			fileContent: []byte("12345"),
			wantErrNil:  false,
		},
		{
			name:        "invalid captured size",
			ext:         "",
			size:        "not-a-quantity",
			fileContent: []byte("12345"),
			wantErrNil:  false,
		},
		{
			name:       "raw file missing on disk",
			ext:        "",
			size:       "10",
			wantErrNil: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			dataFile := filepath.Join(dir, "data.bin"+tc.ext)

			if tc.fileContent != nil {
				if err := os.WriteFile(dataFile, tc.fileContent, 0o600); err != nil {
					t.Fatalf("write %s: %v", dataFile, err)
				}
			}

			got, err := blockTotalSize(dataFile, tc.size, tc.ext)

			if tc.wantErrNil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if got != tc.wantTotal {
					t.Errorf("total = %d, want %d", got, tc.wantTotal)
				}

				return
			}

			if err == nil {
				t.Fatal("expected error, got nil")
			}

			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("expected error wrapping %v, got: %v", tc.wantErr, err)
			}
		})
	}
}

// noHTTPDoer fails the test immediately if HTTPDo is ever called. It is used
// to prove that a preflight failure (invalid/mismatched captured size) sends
// zero HTTP requests -- the check must run strictly before any HEAD/PUT.
type noHTTPDoer struct{ t *testing.T }

func (d noHTTPDoer) HTTPDo(_ *http.Request) (*http.Response, error) {
	d.t.Helper()
	d.t.Fatal("unexpected HTTP call: the size preflight must fail before any HEAD/PUT is attempted")

	return nil, nil
}

// TestSendVolumeData_Block_RawSizeMismatch_SendsNoHTTP verifies that a raw
// (codec none) block leaf whose on-disk data.bin size disagrees with its
// captured VolumeInfo.Size fails deterministically via blockTotalSize and
// never issues a single HTTP request (no HEAD, no PUT).
func TestSendVolumeData_Block_RawSizeMismatch_SendsNoHTTP(t *testing.T) {
	dir := t.TempDir()
	dataFile := filepath.Join(dir, "data.bin")

	if err := os.WriteFile(dataFile, []byte("12345"), 0o600); err != nil {
		t.Fatalf("write data.bin: %v", err)
	}

	leaf := PlannedNode{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "pvc-1",
		DataFile:   dataFile,
		Ext:        "",
		Size:       "10", // disagrees with the 5-byte file actually on disk
	}

	imp := &clusterVolumeImporter{log: discardLogger()}

	err := imp.sendVolumeData(context.Background(), noHTTPDoer{t: t}, "https://importer.local", volumeModeBlock, leaf, targetNS, "pvc-1", nil, nil, nil)
	if err == nil {
		t.Fatal("expected error for raw size mismatch, got nil")
	}

	if !errors.Is(err, ErrRawBlockSizeMismatch) {
		t.Errorf("expected ErrRawBlockSizeMismatch, got: %v", err)
	}
}

// TestSendVolumeData_Block_InvalidSize_SendsNoHTTP verifies that a block leaf
// with a missing/unparsable captured size fails before any HTTP request,
// for every codec (raw and compressed alike).
func TestSendVolumeData_Block_InvalidSize_SendsNoHTTP(t *testing.T) {
	for _, tc := range blockCodecCases {
		t.Run(tc.codec, func(t *testing.T) {
			dir := t.TempDir()
			dataFile := filepath.Join(dir, "data.bin"+tc.ext)

			if err := os.WriteFile(dataFile, []byte("irrelevant"), 0o600); err != nil {
				t.Fatalf("write %s: %v", dataFile, err)
			}

			leaf := PlannedNode{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshot",
				Name:       "pvc-1",
				DataFile:   dataFile,
				Ext:        tc.ext,
				Size:       "", // missing captured size
			}

			imp := &clusterVolumeImporter{log: discardLogger()}

			err := imp.sendVolumeData(context.Background(), noHTTPDoer{t: t}, "https://importer.local", volumeModeBlock, leaf, targetNS, "pvc-1", nil, nil, nil)
			if err == nil {
				t.Fatal("expected error for missing captured size, got nil")
			}
		})
	}
}

func TestSendVolumeData_MidPUTReplacementUsesPinnedBytesAndDoesNotFinish(t *testing.T) {
	payload := bytes.Repeat([]byte("verified-block-"), 4096)
	nodeSpec := archiveNode{
		apiVersion: "snapshot.storage.k8s.io/v1",
		kind:       "VolumeSnapshot",
		name:       "pvc-1",
		namespace:  "src",
		blockData:  payload,
	}
	nodeSpec.volumes = synthVolumeInfo(nodeSpec)
	nodeSpec.volumes[0].Size = strconv.Itoa(len(payload))

	root := t.TempDir()
	writeArchiveNode(t, root, nodeSpec)

	view, err := archive.OpenVerifiedArchive(root)
	if err != nil {
		t.Fatalf("open verified archive: %v", err)
	}
	defer func() { _ = view.Close() }()

	plan, err := buildPlanFromVerifiedArchive(view)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	if err := verifyArchiveIntegrity(context.Background(), view, plan); err != nil {
		t.Fatalf("verify archive: %v", err)
	}

	dataPath := filepath.Join(root, archive.DataBlockName(""))
	replacement := bytes.Repeat([]byte("changed-block--"), 4096)

	var (
		received []byte
		finished int
		replaced bool
	)

	const resumeOffset = 1024

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodHead:
			writer.Header().Set("X-Next-Offset", strconv.Itoa(resumeOffset))
			writer.WriteHeader(http.StatusOK)
		case http.MethodPut:
			if !replaced {
				replaced = true
				if err := os.Rename(dataPath, dataPath+".verified"); err != nil {
					t.Fatalf("move verified payload: %v", err)
				}

				if err := os.WriteFile(dataPath, replacement, 0o600); err != nil {
					t.Fatalf("write replacement payload: %v", err)
				}
			}

			var err error
			received, err = io.ReadAll(request.Body)
			if err != nil {
				t.Fatalf("read PUT body: %v", err)
			}

			writer.Header().Set("X-Next-Offset", strconv.Itoa(len(payload)))
			writer.WriteHeader(http.StatusCreated)
		case http.MethodPost:
			finished++
			writer.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()

	importer := &clusterVolumeImporter{log: discardLogger()}
	err = importer.sendVolumeData(
		context.Background(),
		plainHTTPDoer{},
		server.URL,
		volumeModeBlock,
		plan[0],
		targetNS,
		"di-pvc-1",
		nil,
		nil,
		nil,
	)
	if !errors.Is(err, archive.ErrVerifiedArchiveChanged) {
		t.Fatalf("sendVolumeData error = %v, want ErrVerifiedArchiveChanged", err)
	}

	if !bytes.Equal(received, payload[resumeOffset:]) {
		t.Fatal("PUT consumed replacement bytes instead of the pinned verified descriptor")
	}

	if finished != 0 {
		t.Fatalf("finished POSTs = %d, want 0", finished)
	}
}

func TestSendVolumeData_BlockMutateUseRestoreDuringPUTIsRejected(t *testing.T) {
	payload := randomPayload(t, 2*1024*1024+4096)

	tests := []struct {
		name         string
		codec        string
		ext          string
		resumeOffset int64
		conflictTo   int64
		hardlink     bool
		swallowError bool
	}{
		{name: "raw same inode with transport swallowing body error", codec: "none", swallowError: true},
		{name: "raw external hardlink partial resume", codec: "none", resumeOffset: 1024*1024 + 17, hardlink: true},
		{name: "raw conflict reposition rereads authenticated range", codec: "none", conflictTo: 2*1024*1024 + 17},
		{name: "zstd same inode", codec: "zstd", ext: ".zst"},
		{name: "zstd external hardlink", codec: "zstd", ext: ".zst", hardlink: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encodedPath := filepath.Join(t.TempDir(), "encoded"+tc.ext)
			writeEncodedBlockFile(t, encodedPath, tc.codec, payload)

			encoded, err := os.ReadFile(encodedPath)
			if err != nil {
				t.Fatalf("read encoded payload: %v", err)
			}

			nodeSpec := archiveNode{
				apiVersion: "snapshot.storage.k8s.io/v1",
				kind:       "VolumeSnapshot",
				name:       "pvc-1",
				namespace:  "src",
				blockData:  encoded,
				blockExt:   tc.ext,
			}
			nodeSpec.volumes = synthVolumeInfo(nodeSpec)
			nodeSpec.volumes[0].Size = strconv.Itoa(len(payload))

			root := t.TempDir()
			writeArchiveNode(t, root, nodeSpec)

			view, err := archive.OpenVerifiedArchive(root)
			if err != nil {
				t.Fatalf("open verified archive: %v", err)
			}
			defer func() { _ = view.Close() }()

			plan, err := buildPlanFromVerifiedArchive(view)
			if err != nil {
				t.Fatalf("build plan: %v", err)
			}

			if err := verifyArchiveIntegrity(context.Background(), view, plan); err != nil {
				t.Fatalf("verify archive: %v", err)
			}

			payloadPath := filepath.Join(root, archive.DataBlockName(tc.ext))
			writerPath := payloadPath
			if tc.hardlink {
				writerPath = filepath.Join(t.TempDir(), "external-hardlink")
				if err := os.Link(payloadPath, writerPath); err != nil {
					t.Fatalf("create external hardlink: %v", err)
				}
			}

			mutationOffset := tc.resumeOffset
			if tc.ext != "" {
				mutationOffset = 1024*1024 + 17
			}

			var (
				finished int
				received []byte
				methods  []string
				putCount int
			)

			doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
				methods = append(methods, req.Method)

				switch req.Method {
				case http.MethodHead:
					header := http.Header{}
					header.Set("X-Next-Offset", strconv.FormatInt(tc.resumeOffset, 10))

					return newTestHTTPResponse(http.StatusOK, header), nil
				case http.MethodPut:
					putCount++
					if tc.conflictTo > 0 && putCount == 1 {
						firstBody, readErr := io.ReadAll(req.Body)
						if readErr != nil {
							t.Fatalf("read pre-conflict PUT body: %v", readErr)
						}

						if !bytes.Equal(firstBody, payload[tc.resumeOffset:]) {
							t.Fatal("pre-conflict PUT did not consume the authenticated original payload")
						}

						header := http.Header{}
						header.Set("X-Expected-Offset", strconv.FormatInt(tc.conflictTo, 10))

						return newTestHTTPResponse(http.StatusConflict, header), nil
					}

					if tc.conflictTo > 0 {
						mutationOffset = tc.conflictTo
					}

					readData, readErr := readBodyDuringRestoredMutation(
						t,
						req.Body,
						payloadPath,
						writerPath,
						encoded,
						mutationOffset,
					)
					received = append(received, readData...)

					if readErr != nil && !tc.swallowError {
						return nil, readErr
					}

					header := http.Header{}
					header.Set("X-Next-Offset", strconv.Itoa(len(payload)))

					return newTestHTTPResponse(http.StatusCreated, header), nil
				case http.MethodPost:
					finished++

					return newTestHTTPResponse(http.StatusOK, nil), nil
				default:
					t.Fatalf("unexpected request method %s", req.Method)

					return nil, nil
				}
			})

			importer := &clusterVolumeImporter{log: discardLogger()}
			err = importer.sendVolumeData(
				context.Background(),
				doer,
				"https://importer.local",
				volumeModeBlock,
				plan[0],
				targetNS,
				"di-pvc-1",
				nil,
				nil,
				nil,
			)
			if !errors.Is(err, archive.ErrVerifiedArchiveChanged) {
				t.Fatalf("sendVolumeData error = %v, want ErrVerifiedArchiveChanged", err)
			}

			expectedStart := int(tc.resumeOffset)
			if tc.conflictTo > 0 {
				expectedStart = int(tc.conflictTo)
			}

			expectedEnd := expectedStart + len(received)
			if expectedEnd > len(payload) || !bytes.Equal(received, payload[expectedStart:expectedEnd]) {
				t.Fatalf("PUT received %d bytes that are not an authenticated original payload prefix", len(received))
			}

			if finished != 0 {
				t.Fatalf("finished POSTs = %d, want 0", finished)
			}

			expectedMethods := []string{http.MethodHead, http.MethodPut}
			if tc.conflictTo > 0 {
				expectedMethods = append(expectedMethods, http.MethodPut)
			}

			if !slices.Equal(methods, expectedMethods) {
				t.Fatalf("HTTP methods = %v, want %v", methods, expectedMethods)
			}
		})
	}
}

func readBodyDuringRestoredMutation(
	t *testing.T,
	body io.Reader,
	payloadPath, writerPath string,
	original []byte,
	offset int64,
) ([]byte, error) {
	t.Helper()

	if offset < 0 || offset+32 > int64(len(original)) {
		t.Fatalf("mutation range [%d,%d) is outside payload size %d", offset, offset+32, len(original))
	}

	info, err := os.Stat(payloadPath)
	if err != nil {
		t.Fatalf("inspect payload before mutation: %v", err)
	}

	writer, err := os.OpenFile(writerPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open mutation writer: %v", err)
	}

	changed := bytes.Repeat([]byte{0xA5}, 32)
	if _, err := writer.WriteAt(changed, offset); err != nil {
		t.Fatalf("mutate payload: %v", err)
	}

	data, readErr := io.ReadAll(body)

	if _, err := writer.WriteAt(original[offset:offset+32], offset); err != nil {
		t.Fatalf("restore payload: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close mutation writer: %v", err)
	}

	if err := os.Chtimes(payloadPath, info.ModTime(), info.ModTime()); err != nil {
		t.Fatalf("restore payload timestamp: %v", err)
	}

	return data, readErr
}

func TestSendVolumeData_BlockWaitsForExactBodyCompletion(t *testing.T) {
	payload := bytes.Repeat([]byte("attested-block-payload-"), 4096)

	tests := []struct {
		name  string
		codec string
		ext   string
	}{
		{name: "raw", codec: "none"},
		{name: "zstd", codec: "zstd", ext: ".zst"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encodedPath := filepath.Join(t.TempDir(), "encoded"+tc.ext)
			writeEncodedBlockFile(t, encodedPath, tc.codec, payload)

			encoded, err := os.ReadFile(encodedPath)
			if err != nil {
				t.Fatalf("read encoded block: %v", err)
			}

			nodeSpec := archiveNode{
				apiVersion: "snapshot.storage.k8s.io/v1",
				kind:       "VolumeSnapshot",
				name:       "pvc-1",
				namespace:  "src",
				blockData:  encoded,
				blockExt:   tc.ext,
			}
			nodeSpec.volumes = synthVolumeInfo(nodeSpec)
			nodeSpec.volumes[0].Size = strconv.Itoa(len(payload))

			root := t.TempDir()
			writeArchiveNode(t, root, nodeSpec)

			view, err := archive.OpenVerifiedArchive(root)
			if err != nil {
				t.Fatalf("open verified archive: %v", err)
			}
			defer func() { _ = view.Close() }()

			plan, err := buildPlanFromVerifiedArchive(view)
			if err != nil {
				t.Fatalf("build plan: %v", err)
			}

			if err := verifyArchiveIntegrity(context.Background(), view, plan); err != nil {
				t.Fatalf("verify archive: %v", err)
			}

			putBody := make(chan io.ReadCloser, 1)

			var (
				progressed atomic.Int64
				finished   atomic.Int64
			)

			doer := newRoundTripDoer(func(req *http.Request) (*http.Response, error) {
				switch req.Method {
				case http.MethodHead:
					header := http.Header{}
					header.Set("X-Next-Offset", "0")

					return newTestHTTPResponse(http.StatusOK, header), nil
				case http.MethodPut:
					putBody <- req.Body

					header := http.Header{}
					header.Set("X-Next-Offset", strconv.Itoa(len(payload)))

					return newTestHTTPResponse(http.StatusCreated, header), nil
				case http.MethodPost:
					finished.Add(1)

					return newTestHTTPResponse(http.StatusOK, nil), nil
				default:
					return nil, fmt.Errorf("unexpected method %s", req.Method)
				}
			})

			result := make(chan error, 1)
			go func() {
				importer := &clusterVolumeImporter{log: discardLogger()}
				result <- importer.sendVolumeData(
					context.Background(),
					doer,
					"https://importer.local",
					volumeModeBlock,
					plan[0],
					targetNS,
					"di-pvc-1",
					nil,
					func(count int) { progressed.Add(int64(count)) },
					nil,
				)
			}()

			body := <-putBody

			select {
			case err := <-result:
				t.Fatalf("upload returned before body completion: %v", err)
			default:
			}

			if got := progressed.Load(); got != 0 {
				t.Fatalf("progress before body completion = %d, want 0", got)
			}
			if got := finished.Load(); got != 0 {
				t.Fatalf("finished POSTs before body completion = %d, want 0", got)
			}

			received, err := io.ReadAll(body)
			if err != nil {
				t.Fatalf("finish asynchronous body read: %v", err)
			}

			select {
			case err := <-result:
				t.Fatalf("upload returned before delayed body close: %v", err)
			default:
			}

			if err := body.Close(); err != nil {
				t.Fatalf("finish asynchronous body close: %v", err)
			}
			if !bytes.Equal(received, payload) {
				t.Fatalf("received %d bytes, want exact %d-byte payload", len(received), len(payload))
			}

			if err := <-result; err != nil {
				t.Fatalf("sendVolumeData: %v", err)
			}
			if got := progressed.Load(); got != int64(len(payload)) {
				t.Fatalf("progress after attestation = %d, want %d", got, len(payload))
			}
			if got := finished.Load(); got != 1 {
				t.Fatalf("finished POSTs after attestation = %d, want 1", got)
			}
		})
	}
}

func TestSendVolumeData_BlockRejectsEarlySuccessShortBody(t *testing.T) {
	payload := bytes.Repeat([]byte("short-body"), 1024)
	dataFile := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(dataFile, payload, 0o600); err != nil {
		t.Fatalf("write block payload: %v", err)
	}

	putBody := make(chan io.ReadCloser, 1)

	var (
		progressed atomic.Int64
		finished   atomic.Int64
	)

	doer := newRoundTripDoer(func(req *http.Request) (*http.Response, error) {
		switch req.Method {
		case http.MethodHead:
			header := http.Header{}
			header.Set("X-Next-Offset", "0")

			return newTestHTTPResponse(http.StatusOK, header), nil
		case http.MethodPut:
			putBody <- req.Body

			header := http.Header{}
			header.Set("X-Next-Offset", strconv.Itoa(len(payload)))

			return newTestHTTPResponse(http.StatusCreated, header), nil
		case http.MethodPost:
			finished.Add(1)

			return newTestHTTPResponse(http.StatusOK, nil), nil
		default:
			return nil, fmt.Errorf("unexpected method %s", req.Method)
		}
	})

	result := make(chan error, 1)
	go func() {
		importer := &clusterVolumeImporter{log: discardLogger()}
		result <- importer.sendVolumeData(
			context.Background(),
			doer,
			"https://importer.local",
			volumeModeBlock,
			PlannedNode{DataFile: dataFile, Size: strconv.Itoa(len(payload))},
			targetNS,
			"di-pvc-1",
			nil,
			func(count int) { progressed.Add(int64(count)) },
			nil,
		)
	}()

	body := <-putBody
	prefix := make([]byte, len(payload)/2)
	if _, err := io.ReadFull(body, prefix); err != nil {
		t.Fatalf("read body prefix: %v", err)
	}
	if err := body.Close(); err != nil {
		t.Fatalf("close body after prefix: %v", err)
	}

	err := <-result
	if err == nil || !strings.Contains(err.Error(), "consumed") {
		t.Fatalf("sendVolumeData error = %v, want short body attestation failure", err)
	}
	if got := progressed.Load(); got != 0 {
		t.Fatalf("progress after short body = %d, want 0", got)
	}
	if got := finished.Load(); got != 0 {
		t.Fatalf("finished POSTs after short body = %d, want 0", got)
	}
}

func TestDoBlockChunk_CancellationClosesUnreadEarlySuccessBody(t *testing.T) {
	payload := []byte("body must never be read")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bodySeen := make(chan io.ReadCloser, 1)
	doer := newRoundTripDoer(func(req *http.Request) (*http.Response, error) {
		bodySeen <- req.Body

		header := http.Header{}
		header.Set("X-Next-Offset", strconv.Itoa(len(payload)))

		return newTestHTTPResponse(http.StatusCreated, header), nil
	})

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		"https://importer.local/block",
		bytes.NewReader(payload),
	)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, _, requestErr := doBlockChunk(doer, req, 0, int64(len(payload)), int64(len(payload)))
		result <- requestErr
	}()

	body := <-bodySeen
	cancel()

	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("doBlockChunk error = %v, want context cancellation", err)
	}

	buffer := make([]byte, 1)
	if _, err := body.Read(buffer); !errors.Is(err, http.ErrBodyReadAfterClose) {
		t.Fatalf("read after cancellation = %v, want closed request body", err)
	}
}

func TestSendVolumeData_BlockRejectsDelayedAuthenticatedReadFailure(t *testing.T) {
	payload := randomPayload(t, 2*1024*1024+4096)
	root := t.TempDir()

	nodeSpec := archiveNode{
		apiVersion: "snapshot.storage.k8s.io/v1",
		kind:       "VolumeSnapshot",
		name:       "pvc-1",
		namespace:  "src",
		blockData:  payload,
	}
	nodeSpec.volumes = synthVolumeInfo(nodeSpec)
	nodeSpec.volumes[0].Size = strconv.Itoa(len(payload))
	writeArchiveNode(t, root, nodeSpec)

	view, err := archive.OpenVerifiedArchive(root)
	if err != nil {
		t.Fatalf("open verified archive: %v", err)
	}
	defer func() { _ = view.Close() }()

	plan, err := buildPlanFromVerifiedArchive(view)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	if err := verifyArchiveIntegrity(context.Background(), view, plan); err != nil {
		t.Fatalf("verify archive: %v", err)
	}

	bodySeen := make(chan io.ReadCloser, 1)

	var (
		progressed atomic.Int64
		finished   atomic.Int64
	)

	doer := newRoundTripDoer(func(req *http.Request) (*http.Response, error) {
		switch req.Method {
		case http.MethodHead:
			header := http.Header{}
			header.Set("X-Next-Offset", "0")

			return newTestHTTPResponse(http.StatusOK, header), nil
		case http.MethodPut:
			bodySeen <- req.Body

			header := http.Header{}
			header.Set("X-Next-Offset", strconv.Itoa(len(payload)))

			return newTestHTTPResponse(http.StatusCreated, header), nil
		case http.MethodPost:
			finished.Add(1)

			return newTestHTTPResponse(http.StatusOK, nil), nil
		default:
			return nil, fmt.Errorf("unexpected method %s", req.Method)
		}
	})

	result := make(chan error, 1)
	go func() {
		importer := &clusterVolumeImporter{log: discardLogger()}
		result <- importer.sendVolumeData(
			context.Background(),
			doer,
			"https://importer.local",
			volumeModeBlock,
			plan[0],
			targetNS,
			"di-pvc-1",
			nil,
			func(count int) { progressed.Add(int64(count)) },
			nil,
		)
	}()

	body := <-bodySeen
	payloadPath := filepath.Join(root, archive.DataBlockName(""))
	received, readErr := readBodyDuringRestoredMutation(
		t,
		body,
		payloadPath,
		payloadPath,
		payload,
		1024*1024+17,
	)
	if !errors.Is(readErr, archive.ErrVerifiedArchiveChanged) {
		t.Fatalf("delayed body read error = %v, want ErrVerifiedArchiveChanged", readErr)
	}
	if err := body.Close(); err != nil && !errors.Is(err, archive.ErrVerifiedArchiveChanged) {
		t.Fatalf("close delayed body: %v", err)
	}
	if !bytes.Equal(received, payload[:len(received)]) {
		t.Fatalf("delayed body exposed bytes outside the authenticated original prefix")
	}

	err = <-result
	if !errors.Is(err, archive.ErrVerifiedArchiveChanged) {
		t.Fatalf("sendVolumeData error = %v, want ErrVerifiedArchiveChanged", err)
	}
	if got := progressed.Load(); got != 0 {
		t.Fatalf("progress after authenticated body failure = %d, want 0", got)
	}
	if got := finished.Load(); got != 0 {
		t.Fatalf("finished POSTs after authenticated body failure = %d, want 0", got)
	}
}

type causalBody struct {
	reader   io.Reader
	closeErr error
}

func (b *causalBody) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

func (b *causalBody) Close() error {
	return b.closeErr
}

type terminalErrorReader struct {
	err error
}

func (r terminalErrorReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestDoBlockChunk_JoinsTransportReadAndCloseCauses(t *testing.T) {
	networkErr := errors.New("independent network failure")
	closeErr := errors.New("independent body close failure")

	body := &causalBody{
		reader:   terminalErrorReader{err: archive.ErrVerifiedArchiveChanged},
		closeErr: closeErr,
	}
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPut,
		"https://importer.local/block",
		body,
	)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.ContentLength = 1

	doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
		buffer := make([]byte, 1)
		_, _ = req.Body.Read(buffer)

		return nil, networkErr
	})

	_, _, err = doBlockChunk(doer, req, 0, 1, 1)
	for _, want := range []error{archive.ErrVerifiedArchiveChanged, networkErr, closeErr} {
		if !errors.Is(err, want) {
			t.Fatalf("doBlockChunk error = %v, want errors.Is(_, %v)", err, want)
		}
	}
}

func TestDoBlockChunk_RejectsDelayedCloseErrorAfterExactRead(t *testing.T) {
	payload := []byte("exact bytes before delayed close failure")
	closeErr := errors.New("delayed request body close failure")
	bodySeen := make(chan io.ReadCloser, 1)

	doer := newRoundTripDoer(func(req *http.Request) (*http.Response, error) {
		bodySeen <- req.Body

		header := http.Header{}
		header.Set("X-Next-Offset", strconv.Itoa(len(payload)))

		return newTestHTTPResponse(http.StatusCreated, header), nil
	})

	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPut,
		"https://importer.local/block",
		&causalBody{reader: bytes.NewReader(payload), closeErr: closeErr},
	)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.ContentLength = int64(len(payload))

	result := make(chan error, 1)
	go func() {
		_, _, requestErr := doBlockChunk(doer, req, 0, int64(len(payload)), int64(len(payload)))
		result <- requestErr
	}()

	body := <-bodySeen
	received, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read exact delayed-close body: %v", err)
	}
	if !bytes.Equal(received, payload) {
		t.Fatalf("received %q, want %q", received, payload)
	}

	select {
	case err := <-result:
		t.Fatalf("request returned before delayed close: %v", err)
	default:
	}

	if err := body.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("body close error = %v, want %v", err, closeErr)
	}
	if err := <-result; !errors.Is(err, closeErr) {
		t.Fatalf("doBlockChunk error = %v, want delayed close cause", err)
	}
}

func TestAttestedRequestBody_ReportsExactCompletionAndRange(t *testing.T) {
	payload := []byte("authenticated request payload")
	body := newAttestedRequestBody(
		io.NopCloser(bytes.NewReader(payload)),
		requestBodyRange{start: 17, end: 17 + int64(len(payload))},
		int64(len(payload)),
	)

	received, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read attested body: %v", err)
	}
	if !bytes.Equal(received, payload) {
		t.Fatalf("received %q, want %q", received, payload)
	}
	if err := body.Close(); err != nil {
		t.Fatalf("close attested body: %v", err)
	}

	report, err := body.wait(context.Background())
	if err != nil {
		t.Fatalf("wait for body report: %v", err)
	}
	if err := report.validateExact(); err != nil {
		t.Fatalf("validate exact report: %v", err)
	}

	if report.bodyRange != (requestBodyRange{start: 17, end: 17 + int64(len(payload))}) {
		t.Fatalf("body range = %+v, want [17,%d)", report.bodyRange, 17+len(payload))
	}
}

func TestAttestedRequestBody_NetworkStallQuiescesWithDeadlineCause(t *testing.T) {
	t.Parallel()

	body := newAttestedRequestBody(
		io.NopCloser(bytes.NewReader([]byte("unread payload"))),
		requestBodyRange{start: 4, end: 18},
		14,
	)

	body.NetworkStall(context.DeadlineExceeded)

	report, err := body.wait(context.Background())
	if err != nil {
		t.Fatalf("wait for body report: %v", err)
	}

	err = report.validateExact()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("body report error = %v, want context.DeadlineExceeded", err)
	}
	if !report.closed {
		t.Fatal("network stall did not close the attested request body")
	}
}

func BenchmarkAttestedRequestBodyCompletion(b *testing.B) {
	const payloadSize = 8 << 20

	payload := bytes.Repeat([]byte{'a'}, payloadSize)
	bodyRange := requestBodyRange{start: 17, end: 17 + payloadSize}

	b.SetBytes(payloadSize)
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		body := newAttestedRequestBody(
			io.NopCloser(bytes.NewReader(payload)),
			bodyRange,
			payloadSize,
		)

		if _, err := io.Copy(io.Discard, body); err != nil {
			b.Fatalf("read attested body: %v", err)
		}
		if err := body.Close(); err != nil {
			b.Fatalf("close attested body: %v", err)
		}

		report, err := body.wait(context.Background())
		if err != nil {
			b.Fatalf("wait for body report: %v", err)
		}
		if err := report.validateExact(); err != nil {
			b.Fatalf("validate exact report: %v", err)
		}
	}
}

func TestDoBlockChunk_AttestationPreservesConnectionReuse(t *testing.T) {
	const totalSize = int64(12)

	var newConnections atomic.Int64

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		offset, err := strconv.ParseInt(request.Header.Get("X-Offset"), 10, 64)
		if err != nil {
			http.Error(writer, "invalid offset", http.StatusBadRequest)

			return
		}

		body, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(writer, "read body", http.StatusInternalServerError)

			return
		}

		next := offset + int64(len(body))
		writer.Header().Set("X-Next-Offset", strconv.FormatInt(next, 10))
		if next == totalSize {
			writer.WriteHeader(http.StatusCreated)

			return
		}

		writer.WriteHeader(http.StatusNoContent)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			newConnections.Add(1)
		}
	}
	server.Start()
	defer server.Close()

	client := clientHTTPDoer{client: server.Client()}

	for offset := int64(0); offset < totalSize; offset += 6 {
		requestEnd := offset + 6
		req, err := http.NewRequestWithContext(
			context.Background(),
			http.MethodPut,
			server.URL,
			bytes.NewReader([]byte("123456")),
		)
		if err != nil {
			t.Fatalf("build PUT: %v", err)
		}
		req.Header.Set("X-Offset", strconv.FormatInt(offset, 10))

		next, reposition, err := doBlockChunk(client, req, offset, requestEnd, totalSize)
		if err != nil {
			t.Fatalf("doBlockChunk at %d: %v", offset, err)
		}
		if reposition || next != requestEnd {
			t.Fatalf("doBlockChunk at %d = (%d,%v), want (%d,false)", offset, next, reposition, requestEnd)
		}
	}

	if got := newConnections.Load(); got != 1 {
		t.Fatalf("new TCP connections = %d, want one reused connection", got)
	}
}

func TestRequestBodyAttestation_DiscriminatesResponseOnlyAcceptance(t *testing.T) {
	payload := []byte("response arrival is not body completion")
	bodySeen := make(chan io.ReadCloser, 1)
	responseAccepted := make(chan struct{}, 1)

	doer := newRoundTripDoer(func(req *http.Request) (*http.Response, error) {
		bodySeen <- req.Body

		header := http.Header{}
		header.Set("X-Next-Offset", strconv.Itoa(len(payload)))

		return newTestHTTPResponse(http.StatusCreated, header), nil
	})

	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPut,
		"https://importer.local/block",
		bytes.NewReader(payload),
	)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	go func() {
		resp, requestErr := doer.HTTPDo(req)
		if resp != nil && resp.Body != nil {
			defer func() { _ = resp.Body.Close() }()
		}

		if requestErr == nil && resp.StatusCode == http.StatusCreated {
			responseAccepted <- struct{}{}
		}
	}()

	body := <-bodySeen
	<-responseAccepted

	if err := body.Close(); err != nil {
		t.Fatalf("close unread baseline body: %v", err)
	}
}
