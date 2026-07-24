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
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
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

type fileHTTPDoer func(*http.Request) (*http.Response, error)

func (d fileHTTPDoer) HTTPDo(req *http.Request) (*http.Response, error) {
	resp, requestErr := d(req)
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

func fileHTTPResponse(status int, header http.Header) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     header,
		Body:       io.NopCloser(strings.NewReader("")),
	}
}

func bytesBodyFactory(payload []byte) fileBodyFactory {
	return func(_ context.Context, offset, size int64) (io.ReadCloser, error) {
		if offset < 0 || size < 0 || offset > int64(len(payload)) || size > int64(len(payload))-offset {
			return nil, fmt.Errorf("requested byte range [%d,%d) outside payload size %d", offset, offset+size, len(payload))
		}

		return io.NopCloser(bytes.NewReader(payload[offset : offset+size])), nil
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)

	return len(p), nil
}

func TestPutFile_409ReopensExactServerSelectedBodies(t *testing.T) {
	t.Parallel()

	payload := []byte("abcdefghij")

	var gotOffsets []int64

	var gotBodies [][]byte

	responses := []*http.Response{
		fileHTTPResponse(http.StatusConflict, http.Header{"X-Expected-Offset": []string{"6"}}),
		fileHTTPResponse(http.StatusConflict, http.Header{"X-Expected-Offset": []string{"3"}}),
		fileHTTPResponse(http.StatusCreated, http.Header{"X-Next-Offset": []string{"10"}}),
	}

	doer := fileHTTPDoer(func(req *http.Request) (*http.Response, error) {
		offset, err := strconv.ParseInt(req.Header.Get("X-Offset"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse X-Offset: %w", err)
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("read request body: %w", err)
		}

		gotOffsets = append(gotOffsets, offset)
		gotBodies = append(gotBodies, body)

		response := responses[0]
		responses = responses[1:]

		return response, nil
	})

	var openedOffsets []int64

	newBody := func(_ context.Context, offset, size int64) (io.ReadCloser, error) {
		openedOffsets = append(openedOffsets, offset)

		return bytesBodyFactory(payload)(context.Background(), offset, size)
	}

	progressed := 0
	activated := 0
	progress := &fileUploadProgress{onProgress: func(n int) { progressed += n }}
	attrs := fileAttrs{Perm: 0o600, UID: 1, GID: 2}

	err := putFile(
		context.Background(),
		doer,
		"https://import.example",
		"file.bin",
		int64(len(payload)),
		0,
		attrs,
		newBody,
		progress,
		func() { activated++ },
	)
	if err != nil {
		t.Fatalf("putFile: %v", err)
	}

	if want := []int64{0, 6, 3}; !slices.Equal(gotOffsets, want) {
		t.Errorf("request offsets = %v, want %v", gotOffsets, want)
	}

	if want := []int64{0, 6, 3}; !slices.Equal(openedOffsets, want) {
		t.Errorf("opened body offsets = %v, want %v", openedOffsets, want)
	}

	wantBodies := [][]byte{payload, payload[6:], payload[3:]}
	if !reflect.DeepEqual(gotBodies, wantBodies) {
		t.Errorf("request bodies = %q, want %q", gotBodies, wantBodies)
	}

	if progressed != len(payload) {
		t.Errorf("high-water progress = %d, want %d", progressed, len(payload))
	}

	if activated != 3 {
		t.Errorf("activate calls = %d, want 3", activated)
	}
}

func TestPutFile_ConflictSequencesAreBounded(t *testing.T) {
	payload := []byte("0123456789abcdef")
	maximumConflicts := make([]int64, maxConsecutiveFileConflicts)
	for i := range maximumConflicts {
		maximumConflicts[i] = int64(i + 1)
	}

	excessConflicts := append(slices.Clone(maximumConflicts), int64(maxConsecutiveFileConflicts+1))

	tests := []struct {
		name         string
		conflicts    []int64
		recover      bool
		wantErr      string
		wantProgress int
	}{
		{
			name:         "maximum unique sequence recovers",
			conflicts:    maximumConflicts,
			recover:      true,
			wantProgress: len(payload),
		},
		{
			name:         "one excess unique transition stops",
			conflicts:    excessConflicts,
			wantErr:      "too many consecutive file upload conflicts (8)",
			wantProgress: maxConsecutiveFileConflicts,
		},
		{
			name:         "offset cycle stops",
			conflicts:    []int64{6, 0},
			wantErr:      "server-directed file upload offset cycle from 6 to 0",
			wantProgress: 6,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var requestOffsets []int64

			var openedOffsets []int64

			doer := fileHTTPDoer(func(req *http.Request) (*http.Response, error) {
				offset, err := strconv.ParseInt(req.Header.Get("X-Offset"), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("parse X-Offset: %w", err)
				}

				body, err := io.ReadAll(req.Body)
				if err != nil {
					return nil, fmt.Errorf("read request body: %w", err)
				}
				if !bytes.Equal(body, payload[offset:]) {
					return nil, fmt.Errorf("body at offset %d = %q, want %q", offset, body, payload[offset:])
				}

				requestOffsets = append(requestOffsets, offset)
				step := len(requestOffsets) - 1
				if step < len(tc.conflicts) {
					return fileHTTPResponse(http.StatusConflict, http.Header{
						"X-Expected-Offset": []string{strconv.FormatInt(tc.conflicts[step], 10)},
					}), nil
				}
				if !tc.recover {
					return nil, errors.New("opened an extra request body after rejecting conflict history")
				}

				return fileHTTPResponse(http.StatusCreated, http.Header{
					"X-Next-Offset": []string{strconv.Itoa(len(payload))},
				}), nil
			})

			newBody := func(ctx context.Context, offset, size int64) (io.ReadCloser, error) {
				openedOffsets = append(openedOffsets, offset)

				return bytesBodyFactory(payload)(ctx, offset, size)
			}

			progressed := 0
			activated := 0
			progress := &fileUploadProgress{onProgress: func(n int) { progressed += n }}

			err := putFile(
				context.Background(),
				doer,
				"https://import.example",
				"file.bin",
				int64(len(payload)),
				0,
				fileAttrs{Perm: 0o600},
				newBody,
				progress,
				func() { activated++ },
			)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("putFile: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("putFile error = %v, want containing %q", err, tc.wantErr)
			}

			wantRequests := len(tc.conflicts)
			if tc.recover {
				wantRequests++
			}

			if len(requestOffsets) != wantRequests {
				t.Errorf("request count = %d, want %d", len(requestOffsets), wantRequests)
			}
			if !slices.Equal(openedOffsets, requestOffsets) {
				t.Errorf("opened offsets = %v, request offsets %v", openedOffsets, requestOffsets)
			}
			if progressed != tc.wantProgress {
				t.Errorf("high-water progress = %d, want %d", progressed, tc.wantProgress)
			}
			if activated != wantRequests {
				t.Errorf("activate calls = %d, want %d", activated, wantRequests)
			}
		})
	}
}

func TestFileConflictTracker_ResetCompactsHistoryAndBoundsLifetimeReplay(t *testing.T) {
	var tracker fileConflictTracker

	for i := range maxFileConflictReplays {
		if err := tracker.observe(0, 1); err != nil {
			t.Fatalf("observe replay %d: %v", i+1, err)
		}

		tracker.reset()
	}

	if tracker.count != 0 {
		t.Fatalf("consecutive history count = %d, want compacted to zero", tracker.count)
	}
	if tracker.total != maxFileConflictReplays {
		t.Fatalf("lifetime replay count = %d, want %d", tracker.total, maxFileConflictReplays)
	}

	err := tracker.observe(0, 1)
	if err == nil || !strings.Contains(err.Error(), "too many file upload conflict replays (32)") {
		t.Fatalf("observe excess lifetime replay error = %v, want bounded replay failure", err)
	}
	if tracker.count != 0 || tracker.total != maxFileConflictReplays {
		t.Fatalf("tracker changed after rejected replay: count=%d total=%d", tracker.count, tracker.total)
	}
}

func TestPutFile_SuccessResetsConflictHistory(t *testing.T) {
	totalSize := int64(blockPutPayloadLimit) + 2
	responses := []*http.Response{
		fileHTTPResponse(http.StatusConflict, http.Header{"X-Expected-Offset": []string{"1"}}),
		fileHTTPResponse(http.StatusNoContent, http.Header{
			"X-Next-Offset": []string{strconv.FormatInt(int64(blockPutPayloadLimit)+1, 10)},
		}),
		fileHTTPResponse(http.StatusConflict, http.Header{"X-Expected-Offset": []string{"0"}}),
		fileHTTPResponse(http.StatusConflict, http.Header{"X-Expected-Offset": []string{"1"}}),
		fileHTTPResponse(http.StatusNoContent, http.Header{
			"X-Next-Offset": []string{strconv.FormatInt(int64(blockPutPayloadLimit)+1, 10)},
		}),
		fileHTTPResponse(http.StatusCreated, http.Header{
			"X-Next-Offset": []string{strconv.FormatInt(totalSize, 10)},
		}),
	}

	var requestOffsets []int64

	var requestSizes []int64

	doer := fileHTTPDoer(func(req *http.Request) (*http.Response, error) {
		offset, err := strconv.ParseInt(req.Header.Get("X-Offset"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse X-Offset: %w", err)
		}

		requestOffsets = append(requestOffsets, offset)
		requestSizes = append(requestSizes, req.ContentLength)

		step := len(requestOffsets) - 1
		if step >= len(responses) {
			return nil, errors.New("unexpected request after successful recovery")
		}

		return responses[step], nil
	})

	var openedOffsets []int64

	newBody := func(_ context.Context, offset, size int64) (io.ReadCloser, error) {
		openedOffsets = append(openedOffsets, offset)

		return io.NopCloser(io.LimitReader(zeroReader{}, size)), nil
	}

	progressed := 0
	activated := 0
	progress := &fileUploadProgress{onProgress: func(n int) { progressed += n }}

	err := putFile(
		context.Background(),
		doer,
		"https://import.example",
		"file.bin",
		totalSize,
		0,
		fileAttrs{Perm: 0o600},
		newBody,
		progress,
		func() { activated++ },
	)
	if err != nil {
		t.Fatalf("putFile: %v", err)
	}

	wantOffsets := []int64{0, 1, int64(blockPutPayloadLimit) + 1, 0, 1, int64(blockPutPayloadLimit) + 1}
	if !slices.Equal(requestOffsets, wantOffsets) {
		t.Errorf("request offsets = %v, want %v", requestOffsets, wantOffsets)
	}
	if !slices.Equal(openedOffsets, wantOffsets) {
		t.Errorf("opened offsets = %v, want %v", openedOffsets, wantOffsets)
	}

	wantSizes := []int64{
		blockPutPayloadLimit,
		blockPutPayloadLimit,
		1,
		blockPutPayloadLimit,
		blockPutPayloadLimit,
		1,
	}
	if !slices.Equal(requestSizes, wantSizes) {
		t.Errorf("request sizes = %v, want %v", requestSizes, wantSizes)
	}
	if progressed != int(totalSize) {
		t.Errorf("high-water progress = %d, want %d", progressed, totalSize)
	}
	if activated != len(responses) {
		t.Errorf("activate calls = %d, want %d", activated, len(responses))
	}
}

func TestDoFileChunk_StrictStatusesAndOffsets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		status         int
		header         http.Header
		offset         int64
		requestEnd     int64
		total          int64
		wantNext       int64
		wantReposition bool
		wantErr        bool
	}{
		{
			name:       "success: 204 acknowledges exact intermediate end",
			status:     http.StatusNoContent,
			header:     http.Header{"X-Next-Offset": []string{"5"}},
			requestEnd: 5,
			total:      10,
			wantNext:   5,
		},
		{
			name:       "success: 201 final may omit next header per producer",
			status:     http.StatusCreated,
			header:     http.Header{},
			offset:     5,
			requestEnd: 10,
			total:      10,
			wantNext:   10,
		},
		{
			name:           "success: 409 selects different forward offset",
			status:         http.StatusConflict,
			header:         http.Header{"X-Expected-Offset": []string{"7"}},
			requestEnd:     5,
			total:          10,
			wantNext:       7,
			wantReposition: true,
		},
		{
			name:       "error: 201 is not intermediate",
			status:     http.StatusCreated,
			header:     http.Header{"X-Next-Offset": []string{"5"}},
			requestEnd: 5,
			total:      10,
			wantErr:    true,
		},
		{
			name:       "error: 204 is not final",
			status:     http.StatusNoContent,
			header:     http.Header{"X-Next-Offset": []string{"10"}},
			offset:     5,
			requestEnd: 10,
			total:      10,
			wantErr:    true,
		},
		{
			name:       "error: 204 requires next header",
			status:     http.StatusNoContent,
			header:     http.Header{},
			requestEnd: 5,
			total:      10,
			wantErr:    true,
		},
		{
			name:       "error: malformed next header",
			status:     http.StatusNoContent,
			header:     http.Header{"X-Next-Offset": []string{"bad"}},
			requestEnd: 5,
			total:      10,
			wantErr:    true,
		},
		{
			name:       "error: next header must equal request end",
			status:     http.StatusNoContent,
			header:     http.Header{"X-Next-Offset": []string{"4"}},
			requestEnd: 5,
			total:      10,
			wantErr:    true,
		},
		{
			name:       "error: next header is in range",
			status:     http.StatusNoContent,
			header:     http.Header{"X-Next-Offset": []string{"11"}},
			requestEnd: 5,
			total:      10,
			wantErr:    true,
		},
		{
			name:       "error: 409 requires expected header",
			status:     http.StatusConflict,
			header:     http.Header{},
			requestEnd: 5,
			total:      10,
			wantErr:    true,
		},
		{
			name:       "error: malformed expected header",
			status:     http.StatusConflict,
			header:     http.Header{"X-Expected-Offset": []string{"bad"}},
			requestEnd: 5,
			total:      10,
			wantErr:    true,
		},
		{
			name:       "error: expected header is in range",
			status:     http.StatusConflict,
			header:     http.Header{"X-Expected-Offset": []string{"11"}},
			requestEnd: 5,
			total:      10,
			wantErr:    true,
		},
		{
			name:       "error: expected offset differs",
			status:     http.StatusConflict,
			header:     http.Header{"X-Expected-Offset": []string{"0"}},
			requestEnd: 5,
			total:      10,
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doer := fileHTTPDoer(func(*http.Request) (*http.Response, error) {
				return fileHTTPResponse(tc.status, tc.header), nil
			})
			body := bytes.Repeat([]byte("x"), int(tc.requestEnd-tc.offset))
			req, err := http.NewRequest(http.MethodPut, "https://import.example/file", bytes.NewReader(body))
			if err != nil {
				t.Fatalf("build request: %v", err)
			}

			next, reposition, err := doFileChunk(doer, req, tc.offset, tc.requestEnd, tc.total)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("doFileChunk error = nil, want failure (next=%d reposition=%v)", next, reposition)
				}

				return
			}

			if err != nil {
				t.Fatalf("doFileChunk: %v", err)
			}

			if next != tc.wantNext || reposition != tc.wantReposition {
				t.Errorf("doFileChunk = (%d,%v), want (%d,%v)",
					next, reposition, tc.wantNext, tc.wantReposition)
			}
		})
	}
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

		w.Header().Set("X-Next-Offset", strconv.Itoa(len(payload)))
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	progress := &fileUploadProgress{}
	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "data.txt", int64(len(payload)), 0,
		attrs, bytesBodyFactory(payload), progress, nil); err != nil {
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

// TestPutFile_ResumeFromPartialOffset verifies that putFile opens a fresh bounded body
// at the caller's validated durable offset.
func TestPutFile_ResumeFromPartialOffset(t *testing.T) {
	payload := []byte("0123456789abcde") // 15 bytes; caller already sent the first 8

	var putOffsets []string

	var receivedBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		putOffsets = append(putOffsets, r.Header.Get("X-Offset"))
		receivedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("X-Next-Offset", strconv.Itoa(len(payload)))
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	attrs := fileAttrs{Perm: 0o600, UID: 0, GID: 0, ModTime: time.Now()}
	progress := &fileUploadProgress{}

	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "data.bin", int64(len(payload)), 8,
		attrs, bytesBodyFactory(payload), progress, nil); err != nil {
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

// TestPutFile_OffsetMismatchCorrection proves a consumed rejected body is never reused:
// the 409-selected offset gets a newly opened exact suffix in the same call.
func TestPutFile_OffsetMismatchCorrection(t *testing.T) {
	payload := []byte("abcdefghij") // 10 bytes

	imp := newFakeFileImporter()
	// Seed the server with 4 bytes already durably written — simulating that the caller's
	// belief (offset 0) is stale relative to the server's true state.
	imp.seed("data.bin", payload[:4])

	srv := httptest.NewServer(imp)
	defer srv.Close()

	attrs := fileAttrs{Perm: 0o600, UID: 0, GID: 0, ModTime: time.Now()}

	progress := &fileUploadProgress{}
	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "data.bin", int64(len(payload)), 0,
		attrs, bytesBodyFactory(payload), progress, nil); err != nil {
		t.Fatalf("putFile: %v", err)
	}

	if got := imp.received("data.bin"); !bytes.Equal(got, payload) {
		t.Errorf("after corrected retry, server holds %q, want %q", got, payload)
	}
}

func TestPutFile_PartialOffsetAtTotalFinalizesViaEmptyPUT(t *testing.T) {
	putCount := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		putCount++

		if r.ContentLength != 0 {
			t.Errorf("Content-Length = %d, want 0", r.ContentLength)
		}

		if got := r.Header.Get("X-Offset"); got != "4" {
			t.Errorf("X-Offset = %q, want 4", got)
		}

		w.Header().Set("X-Next-Offset", "4")
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	attrs := fileAttrs{Perm: 0o600, UID: 0, GID: 0, ModTime: time.Now()}
	progress := &fileUploadProgress{}

	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "data.bin", 4, 4,
		attrs, bytesBodyFactory([]byte("data")), progress, nil); err != nil {
		t.Fatalf("putFile: %v", err)
	}

	if putCount != 1 {
		t.Errorf("PUT count = %d, want 1 finalization request", putCount)
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
		w.Header().Set("X-Next-Offset", "0")
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	progress := &fileUploadProgress{}
	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "empty.txt", 0, 0,
		attrs, bytesBodyFactory(nil), progress, nil); err != nil {
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

	payload := []byte("content")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("X-Next-Offset", strconv.Itoa(len(payload)))
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
	progress := &fileUploadProgress{}

	if err := putFile(context.Background(), plainHTTPDoer{}, srv.URL, "file.txt", int64(len(payload)), 0,
		attrs, bytesBodyFactory(payload), progress, nil); err != nil {
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

func writeSingleEntryFSTar(t *testing.T, codec string, content []byte) string {
	t.Helper()

	tarData := buildSingleEntryFSTar(t, codec, content)
	tarPath := filepath.Join(t.TempDir(), "data.tar")
	if err := os.WriteFile(tarPath, tarData, 0o600); err != nil {
		t.Fatalf("write data.tar: %v", err)
	}

	return tarPath
}

func buildSingleEntryFSTar(t *testing.T, codec string, content []byte) []byte {
	t.Helper()

	ext, stored := encodeEntry(t, codec, content)

	var tarBuf bytes.Buffer

	tw := tar.NewWriter(&tarBuf)
	addTarEntryMetadata(
		t,
		tw,
		"file.bin"+ext,
		"file.bin",
		codec,
		int64(len(content)),
		stored,
		0o600,
		1,
		2,
		time.Unix(100, 0).UTC(),
	)

	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}

	return tarBuf.Bytes()
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

func addTarEntryRawPAX(
	t *testing.T,
	tw *tar.Writer,
	storedPath, originalPath, codec string,
	rawSize int64,
	body []byte,
) {
	t.Helper()

	hdr := &tar.Header{
		Format:   tar.FormatPAX,
		Typeflag: tar.TypeReg,
		Name:     storedPath,
		Mode:     0o600,
		Size:     int64(len(body)),
		PAXRecords: map[string]string{
			archive.PAXFSCodec:        codec,
			archive.PAXFSOriginalPath: originalPath,
			archive.PAXFSRawSize:      strconv.FormatInt(rawSize, 10),
		},
	}

	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("write raw-PAX tar header for %s: %v", storedPath, err)
	}

	if _, err := tw.Write(body); err != nil {
		t.Fatalf("write raw-PAX tar body for %s: %v", storedPath, err)
	}
}

func TestValidateFSUploadPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "empty", path: ""},
		{name: "dot", path: "."},
		{name: "absolute", path: "/etc/passwd"},
		{name: "backslash", path: `dir\file`},
		{name: "nul", path: "dir/\x00file"},
		{name: "control", path: "dir/\nfile"},
		{name: "drive", path: "C:/file"},
		{name: "parent", path: "dir/../file"},
		{name: "double slash clean drift", path: "dir//file"},
		{name: "dot element clean drift", path: "dir/./file"},
		{name: "trailing slash clean drift", path: "dir/file/"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if err := validateFSUploadPath(tc.path); !errors.Is(err, archive.ErrInvalidFSMetadata) {
				t.Fatalf("validateFSUploadPath(%q) error = %v, want ErrInvalidFSMetadata", tc.path, err)
			}
		})
	}
}

func TestImportFSFromTar_UnsafeOrDuplicatePAXBeforeHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		build func(*testing.T, *tar.Writer)
	}{
		{
			name: "drive-like PAX wins over benign header",
			build: func(t *testing.T, tw *tar.Writer) {
				addTarEntryRawPAX(t, tw, "benign.txt", "C:/escape.txt", "none", 1, []byte("x"))
			},
		},
		{
			name: "path-clean drift PAX wins over conflicting header",
			build: func(t *testing.T, tw *tar.Writer) {
				addTarEntryRawPAX(t, tw, "different.txt", "dir//escape.txt", "none", 1, []byte("x"))
			},
		},
		{
			name: "duplicate normalized regular path",
			build: func(t *testing.T, tw *tar.Writer) {
				addTarEntryMetadata(t, tw, "dup.txt", "dup.txt", "none", 1, []byte("a"), 0o600, 0, 0, time.Time{})
				addTarEntryMetadata(t, tw, "dup.txt", "dup.txt", "none", 1, []byte("b"), 0o600, 0, 0, time.Time{})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var tarBuf bytes.Buffer

			tw := tar.NewWriter(&tarBuf)
			tc.build(t, tw)

			if err := tw.Close(); err != nil {
				t.Fatalf("close tar: %v", err)
			}

			tarPath := filepath.Join(t.TempDir(), "data.tar")
			if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
				t.Fatalf("write data.tar: %v", err)
			}

			doer := &failOnHTTPDoer{}
			err := importFSFromTar(
				context.Background(),
				doer,
				":// invalid base URL that must never be joined",
				tarPath,
				discardLogger(),
				nil,
				nil,
				nil,
			)
			if !errors.Is(err, archive.ErrInvalidFSMetadata) {
				t.Fatalf("importFSFromTar error = %v, want ErrInvalidFSMetadata", err)
			}

			if doer.called {
				t.Fatal("unsafe/duplicate full-tar preflight must run before HTTP")
			}
		})
	}
}

func TestScanFSTar_AcceptsStructuralDirectoryChain(t *testing.T) {
	t.Parallel()

	var tarBuf bytes.Buffer

	tw := tar.NewWriter(&tarBuf)
	for _, hdr := range []tar.Header{
		{Typeflag: tar.TypeDir, Name: "dir/", Mode: 0o755},
		{Typeflag: tar.TypeDir, Name: "dir/nested/", Mode: 0o700},
	} {
		if err := tw.WriteHeader(&hdr); err != nil {
			t.Fatalf("write non-regular header %q: %v", hdr.Name, err)
		}
	}

	addTarEntryMetadata(t, tw, "dir/nested/file.txt", "dir/nested/file.txt", "none", 1, []byte("x"), 0o600, 0, 0, time.Time{})

	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}

	tarPath := filepath.Join(t.TempDir(), "data.tar")
	if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
		t.Fatalf("write data.tar: %v", err)
	}

	scan, err := scanFSTar(tarPath)
	if err != nil {
		t.Fatalf("scanFSTar: %v", err)
	}

	if got, want := scan.RegularPaths, []string{"dir/nested/file.txt"}; !slices.Equal(got, want) {
		t.Errorf("regular paths = %v, want %v", got, want)
	}

	if scan.StructuralDirectoryCount != 2 {
		t.Errorf("structural directory count = %d, want 2", scan.StructuralDirectoryCount)
	}
}

func TestImportFSFromTar_RejectsUnsupportedEntriesBeforeHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		headers       []tar.Header
		regularPath   string
		wantFragments []string
	}{
		{
			name:        "empty directory beside unrelated regular file",
			headers:     []tar.Header{{Typeflag: tar.TypeDir, Name: "empty/"}},
			regularPath: "other/file.txt",
			wantFragments: []string{
				`entry "empty" (directory)`,
				"unsupported empty directory",
			},
		},
		{
			name: "directory chain with branch lacking regular descendant",
			headers: []tar.Header{
				{Typeflag: tar.TypeDir, Name: "tree/"},
				{Typeflag: tar.TypeDir, Name: "tree/full/"},
				{Typeflag: tar.TypeDir, Name: "tree/empty/"},
			},
			regularPath: "tree/full/file.txt",
			wantFragments: []string{
				`entry "tree/empty" (directory)`,
				"unsupported empty directory",
			},
		},
		{
			name: "all lossy types including late member aggregate",
			headers: []tar.Header{
				{Typeflag: tar.TypeSymlink, Name: "sym", Linkname: "file.txt"},
				{Typeflag: tar.TypeLink, Name: "hard", Linkname: "file.txt"},
				{Typeflag: tar.TypeChar, Name: "char"},
				{Typeflag: tar.TypeBlock, Name: "block"},
				{Typeflag: tar.TypeFifo, Name: "pipe"},
				{Typeflag: 'Z', Name: "late-unknown"},
			},
			regularPath: "file.txt",
			wantFragments: []string{
				`entry "sym" (symlink)`,
				`entry "hard" (hardlink)`,
				`entry "char" (character device)`,
				`entry "block" (block device)`,
				`entry "pipe" (FIFO)`,
				`entry "late-unknown" (unknown typeflag 0x5a)`,
				"unsupported filesystem tar entries (6)",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var tarBuf bytes.Buffer

			tw := tar.NewWriter(&tarBuf)
			for i := range tc.headers {
				if err := tw.WriteHeader(&tc.headers[i]); err != nil {
					t.Fatalf("write header %q: %v", tc.headers[i].Name, err)
				}
			}

			addTarEntryMetadata(
				t,
				tw,
				tc.regularPath,
				tc.regularPath,
				"none",
				1,
				[]byte("x"),
				0o600,
				0,
				0,
				time.Time{},
			)

			if err := tw.Close(); err != nil {
				t.Fatalf("close tar: %v", err)
			}

			tarPath := filepath.Join(t.TempDir(), "data.tar")
			if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
				t.Fatalf("write data.tar: %v", err)
			}

			doer := &failOnHTTPDoer{}
			err := importFSFromTar(
				context.Background(),
				doer,
				"https://import.invalid",
				tarPath,
				discardLogger(),
				nil,
				nil,
				nil,
			)
			if err == nil {
				t.Fatal("importFSFromTar error = nil, want unsupported-entry failure")
			}

			for _, fragment := range tc.wantFragments {
				if !strings.Contains(err.Error(), fragment) {
					t.Errorf("importFSFromTar error %q does not contain %q", err, fragment)
				}
			}

			if doer.called {
				t.Fatal("unsupported full-tar preflight must run before HTTP")
			}
		})
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
		w.Header().Set("X-Next-Offset", strconv.FormatInt(r.ContentLength, 10))
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

func TestImportFSFromTar_StructuralDirectoriesWarnOnceAndOnlyUploadRegularFiles(t *testing.T) {
	fileContent := []byte("only file")

	var tarBuf bytes.Buffer

	tw := tar.NewWriter(&tarBuf)

	for _, hdr := range []tar.Header{
		{
			Typeflag: tar.TypeDir,
			Name:     "parent/",
			Mode:     0o751,
			Uid:      10,
			Gid:      20,
			ModTime:  time.Unix(100, 0).UTC(),
		},
		{
			Typeflag: tar.TypeDir,
			Name:     "parent/nested/",
			Mode:     0o700,
			Uid:      30,
			Gid:      40,
			ModTime:  time.Unix(200, 0).UTC(),
		},
	} {
		if err := tw.WriteHeader(&hdr); err != nil {
			t.Fatalf("write dir header %q: %v", hdr.Name, err)
		}
	}

	addTarEntry(t, tw, "parent/nested/file.txt", fileContent, 0o644, 0, 0, time.Now())
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}

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
		w.Header().Set("X-Next-Offset", strconv.FormatInt(r.ContentLength, 10))
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	var logs bytes.Buffer

	log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if err := importFSFromTar(context.Background(), plainHTTPDoer{}, srv.URL, tarPath, log, nil, nil, nil); err != nil {
		t.Fatalf("importFSFromTar: %v", err)
	}

	cap.mu.Lock()
	total := len(cap.uploads)
	cap.mu.Unlock()

	if total != 1 {
		t.Errorf("upload count = %d, want 1 regular file only", total)
	}

	if _, ok := cap.find("parent/nested/file.txt"); !ok {
		t.Error("nested regular file not found in uploads")
	}

	logOutput := logs.String()
	if got := strings.Count(logOutput, "filesystem import creates structural parent directories implicitly"); got != 1 {
		t.Errorf("metadata warning count = %d, want 1; logs=%q", got, logOutput)
	}

	for _, fragment := range []string{
		"directory_count=2",
		"directory mode, uid, gid, and mtime cannot be restored",
	} {
		if !strings.Contains(logOutput, fragment) {
			t.Errorf("warning %q does not contain %q", logOutput, fragment)
		}
	}

	for _, archivePath := range []string{"parent/", "parent/nested/"} {
		if strings.Contains(logOutput, archivePath) {
			t.Errorf("bounded warning must not list archive path %q: %q", archivePath, logOutput)
		}
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
		w.Header().Set("X-Next-Offset", strconv.FormatInt(r.ContentLength, 10))
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
		w.Header().Set("X-Next-Offset", strconv.FormatInt(r.ContentLength, 10))
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

func TestUploadVolumeData_FilesystemReusesOneClientAndClosesItsConnections(t *testing.T) {
	const fileCount = 200

	var tarBuffer bytes.Buffer

	tarWriter := tar.NewWriter(&tarBuffer)
	for index := range fileCount {
		name := fmt.Sprintf("many/file-%03d.txt", index)
		addTarEntryMetadata(
			t,
			tarWriter,
			name,
			name,
			"none",
			1,
			[]byte{'x'},
			0o600,
			1000,
			1000,
			time.Time{},
		)
	}

	if err := tarWriter.Close(); err != nil {
		t.Fatalf("close many-file tar: %v", err)
	}

	tarPath := filepath.Join(t.TempDir(), "data.tar")
	if err := os.WriteFile(tarPath, tarBuffer.Bytes(), 0o600); err != nil {
		t.Fatalf("write many-file tar: %v", err)
	}

	leaf := volumeSnapshotLeaf("pvc-many-files")
	leaf.FilesystemData = true
	leaf.TarFile = tarPath
	leaf.DataFile = ""
	leaf.VolumeMode = volumeModeFilesystem
	leaf.PayloadKind = dataImportPayloadFilesystem
	leaf.DataImportIdentity = dataImportIdentity(leaf)

	importerHandler := newFakeFileImporter()

	var (
		requests          atomic.Int64
		newConnections    atomic.Int64
		closedConnections atomic.Int64
		wrongAuth         atomic.Int64
	)

	markCompleted := func() error {
		return errors.New("completion updater is not initialized")
	}

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)

		if request.Header.Get("Authorization") != "Bearer upload-token" {
			wrongAuth.Add(1)
			http.Error(writer, "wrong authorization", http.StatusUnauthorized)

			return
		}

		if request.Method == http.MethodPost {
			if err := markCompleted(); err != nil {
				http.Error(writer, err.Error(), http.StatusInternalServerError)

				return
			}
		}

		importerHandler.ServeHTTP(writer, request)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			newConnections.Add(1)
		case http.StateClosed:
			closedConnections.Add(1)
		}
	}
	server.StartTLS()
	t.Cleanup(server.Close)

	serverCertificate := server.Certificate()
	if serverCertificate == nil {
		t.Fatal("TLS upload server has no certificate")
	}

	caData := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertificate.Raw})
	di := readyDataImportObj(
		leaf,
		server.URL,
		volumeModeFilesystem,
		base64.StdEncoding.EncodeToString(caData),
	)
	dyn := newFakeDataImportDyn(di)
	importer := newTestVolumeImporter(dyn)

	markCompleted = func() error {
		current, err := dyn.Resource(dataImportGVR).Namespace(targetNS).Get(
			context.Background(),
			importer.DataImportName(leaf),
			metav1.GetOptions{},
		)
		if err != nil {
			return fmt.Errorf("get DataImport for completion: %w", err)
		}

		if err := unstructured.SetNestedSlice(current.Object, readyConditions(conditionCompleted), "status", "conditions"); err != nil {
			return fmt.Errorf("set completed condition: %w", err)
		}
		if err := unstructured.SetNestedMap(
			current.Object,
			map[string]interface{}{"name": "vsc-many-files"},
			"status",
			"data",
			"artifactRef",
		); err != nil {
			return fmt.Errorf("set completed artifact: %w", err)
		}

		if _, err := dyn.Resource(dataImportGVR).Namespace(targetNS).Update(
			context.Background(),
			current,
			metav1.UpdateOptions{},
		); err != nil {
			return fmt.Errorf("update completed DataImport: %w", err)
		}

		return nil
	}

	kubeconfigPath := filepath.Join(t.TempDir(), "config")
	kubeconfig := []byte(`apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://kubernetes.invalid
users:
- name: test
  user:
    token: upload-token
contexts:
- name: test
  context:
    cluster: test
    user: test
current-context: test
`)
	if err := os.WriteFile(kubeconfigPath, kubeconfig, 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}

	t.Setenv("KUBECONFIG", kubeconfigPath)

	sc, err := safeClient.NewSafeClient(pflag.NewFlagSet("upload-reuse-test", pflag.ContinueOnError))
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}
	importer.sc = sc

	if err := importer.UploadVolumeData(
		context.Background(),
		leaf,
		importer.DataImportName(leaf),
		targetNS,
		nil,
		nil,
		nil,
	); err != nil {
		t.Fatalf("UploadVolumeData: %v", err)
	}

	wantRequests := int64(2*fileCount + 1)
	if requests.Load() != wantRequests {
		t.Fatalf("HTTP requests = %d, want %d", requests.Load(), wantRequests)
	}
	if wrongAuth.Load() != 0 {
		t.Fatalf("requests with wrong authorization = %d, want 0", wrongAuth.Load())
	}
	if newConnections.Load() > 2 {
		t.Fatalf("new TCP connections = %d for %d requests, want at most 2", newConnections.Load(), wantRequests)
	}

	deadline := time.Now().Add(time.Second)
	for closedConnections.Load() != newConnections.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if closedConnections.Load() != newConnections.Load() {
		t.Fatalf(
			"closed TCP connections = %d, want all %d upload connections closed",
			closedConnections.Load(),
			newConnections.Load(),
		)
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

	w.Header().Set("X-Next-Offset", strconv.FormatInt(next, 10))

	if next == expectedTotal {
		w.WriteHeader(http.StatusCreated)

		return
	}

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

func TestImportFSFromTar_LargeRawAndCompressedUseBoundedExactPUTs(t *testing.T) {
	t.Parallel()

	const ingressLimit = 64 * 1024 * 1024

	tests := []struct {
		name  string
		codec string
	}{
		{name: "raw", codec: "none"},
		{name: "compressed", codec: "zstd"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			payloadSize := 2*blockPutPayloadLimit + 123
			pattern := []byte("producer-shaped-filesystem-upload.")
			content := bytes.Repeat(pattern, int(payloadSize)/len(pattern)+1)
			content = content[:payloadSize]

			ext, stored := encodeEntry(t, tc.codec, content)

			var tarBuf bytes.Buffer

			tw := tar.NewWriter(&tarBuf)
			addTarEntryMetadata(
				t,
				tw,
				"large.bin"+ext,
				"large.bin",
				tc.codec,
				int64(len(content)),
				stored,
				0o640,
				12,
				34,
				time.Unix(100, 0).UTC(),
			)

			if err := tw.Close(); err != nil {
				t.Fatalf("close tar: %v", err)
			}

			tarPath := filepath.Join(t.TempDir(), "data.tar")
			if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
				t.Fatalf("write data.tar: %v", err)
			}

			var mu sync.Mutex

			var offsets []int64

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.WriteHeader(http.StatusNotFound)

					return
				}

				if r.Method != http.MethodPut {
					http.Error(w, "unexpected method", http.StatusMethodNotAllowed)

					return
				}

				if r.ContentLength > ingressLimit {
					http.Error(w, "ingress body too large", http.StatusRequestEntityTooLarge)

					return
				}

				if r.ContentLength <= 0 || r.ContentLength > blockPutPayloadLimit {
					http.Error(w, "client chunk cap violated", http.StatusBadRequest)

					return
				}

				for name, want := range map[string]string{
					"X-Content-Length":        strconv.Itoa(len(content)),
					"X-Attribute-Permissions": "0640",
					"X-Attribute-Uid":         "12",
					"X-Attribute-Gid":         "34",
				} {
					if got := r.Header.Get(name); got != want {
						http.Error(w, fmt.Sprintf("%s=%q want %q", name, got, want), http.StatusBadRequest)

						return
					}
				}

				offset, err := strconv.ParseInt(r.Header.Get("X-Offset"), 10, 64)
				if err != nil {
					http.Error(w, "invalid offset", http.StatusBadRequest)

					return
				}

				requestEnd := offset + r.ContentLength
				if offset < 0 || requestEnd > int64(len(content)) {
					http.Error(w, "invalid request range", http.StatusBadRequest)

					return
				}

				body, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)

					return
				}

				if !bytes.Equal(body, content[offset:requestEnd]) {
					http.Error(w, "body is not the exact raw suffix", http.StatusBadRequest)

					return
				}

				mu.Lock()
				offsets = append(offsets, offset)
				mu.Unlock()

				w.Header().Set("X-Next-Offset", strconv.FormatInt(requestEnd, 10))
				if requestEnd == int64(len(content)) {
					w.WriteHeader(http.StatusCreated)

					return
				}

				w.WriteHeader(http.StatusNoContent)
			}))
			t.Cleanup(server.Close)

			progressed := 0
			err := importFSFromTar(
				context.Background(),
				plainHTTPDoer{},
				server.URL,
				tarPath,
				discardLogger(),
				nil,
				func(n int) { progressed += n },
				nil,
			)
			if err != nil {
				t.Fatalf("importFSFromTar: %v", err)
			}

			wantOffsets := []int64{0, blockPutPayloadLimit, 2 * blockPutPayloadLimit}

			mu.Lock()
			gotOffsets := append([]int64(nil), offsets...)
			mu.Unlock()

			if !slices.Equal(gotOffsets, wantOffsets) {
				t.Errorf("PUT offsets = %v, want %v", gotOffsets, wantOffsets)
			}

			if progressed != len(content) {
				t.Errorf("progress = %d, want %d", progressed, len(content))
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

func TestSendVolumeData_FSProtocolErrorDoesNotFinalizeOrCreditProgress(t *testing.T) {
	t.Parallel()

	content := []byte("protocol failures must not finalize")

	tests := []struct {
		name   string
		status int
		header http.Header
	}{
		{
			name:   "204 at final request end",
			status: http.StatusNoContent,
			header: http.Header{"X-Next-Offset": []string{strconv.Itoa(len(content))}},
		},
		{
			name:   "409 missing expected offset",
			status: http.StatusConflict,
			header: http.Header{},
		},
		{
			name:   "409 malformed expected offset",
			status: http.StatusConflict,
			header: http.Header{"X-Expected-Offset": []string{"bad"}},
		},
		{
			name:   "409 out-of-range expected offset",
			status: http.StatusConflict,
			header: http.Header{"X-Expected-Offset": []string{strconv.Itoa(len(content) + 1)}},
		},
		{
			name:   "409 non-progress expected offset",
			status: http.StatusConflict,
			header: http.Header{"X-Expected-Offset": []string{"0"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tarPath := writeSingleEntryFSTar(t, "none", content)

			var methods []string

			doer := fileHTTPDoer(func(req *http.Request) (*http.Response, error) {
				methods = append(methods, req.Method)

				switch req.Method {
				case http.MethodHead:
					return fileHTTPResponse(http.StatusNotFound, http.Header{}), nil
				case http.MethodPut:
					if _, err := io.Copy(io.Discard, req.Body); err != nil {
						return nil, fmt.Errorf("consume PUT: %w", err)
					}

					return fileHTTPResponse(tc.status, tc.header), nil
				case http.MethodPost:
					return fileHTTPResponse(http.StatusOK, http.Header{}), nil
				default:
					return nil, fmt.Errorf("unexpected method %s", req.Method)
				}
			})

			progressed := 0
			importer := &clusterVolumeImporter{log: discardLogger()}
			leaf := PlannedNode{FilesystemData: true, TarFile: tarPath}

			err := importer.sendVolumeData(
				context.Background(),
				doer,
				"https://import.example",
				volumeModeFilesystem,
				leaf,
				"target",
				"data-import",
				nil,
				func(n int) { progressed += n },
				nil,
			)
			if err == nil {
				t.Fatal("sendVolumeData error = nil, want protocol failure")
			}

			if !slices.Equal(methods, []string{http.MethodHead, http.MethodPut}) {
				t.Errorf("HTTP methods = %v, want HEAD+PUT only (no finished POST)", methods)
			}

			if progressed != 0 {
				t.Errorf("progress = %d, want 0 after invalid response", progressed)
			}
		})
	}
}

func TestSendVolumeData_FSExcessConflictsDoNotFinalize(t *testing.T) {
	content := []byte("0123456789abcdef")

	for _, codec := range []string{"none", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			tarPath := writeSingleEntryFSTar(t, codec, content)

			var methods []string

			putCount := 0
			doer := fileHTTPDoer(func(req *http.Request) (*http.Response, error) {
				methods = append(methods, req.Method)

				switch req.Method {
				case http.MethodHead:
					return fileHTTPResponse(http.StatusNotFound, nil), nil
				case http.MethodPut:
					putCount++
					if putCount > maxConsecutiveFileConflicts+1 {
						return nil, errors.New("opened an extra request body after rejecting conflict history")
					}

					offset, err := strconv.ParseInt(req.Header.Get("X-Offset"), 10, 64)
					if err != nil {
						return nil, fmt.Errorf("parse X-Offset: %w", err)
					}

					body, err := io.ReadAll(req.Body)
					if err != nil {
						return nil, fmt.Errorf("read request body: %w", err)
					}
					if !bytes.Equal(body, content[offset:]) {
						return nil, fmt.Errorf("body at offset %d = %q, want %q", offset, body, content[offset:])
					}

					return fileHTTPResponse(http.StatusConflict, http.Header{
						"X-Expected-Offset": []string{strconv.Itoa(putCount)},
					}), nil
				case http.MethodPost:
					return nil, errors.New("finished POST issued after rejecting conflict history")
				default:
					return nil, fmt.Errorf("unexpected method %s", req.Method)
				}
			})

			progressed := 0
			importer := &clusterVolumeImporter{log: discardLogger()}
			leaf := PlannedNode{FilesystemData: true, TarFile: tarPath}

			err := importer.sendVolumeData(
				context.Background(),
				doer,
				"https://import.example",
				volumeModeFilesystem,
				leaf,
				"target",
				"data-import",
				nil,
				func(n int) { progressed += n },
				nil,
			)
			if err == nil || !strings.Contains(err.Error(), "too many consecutive file upload conflicts (8)") {
				t.Fatalf("sendVolumeData error = %v, want bounded conflict failure", err)
			}

			wantMethods := make([]string, 1, maxConsecutiveFileConflicts+2)
			wantMethods[0] = http.MethodHead
			for range maxConsecutiveFileConflicts + 1 {
				wantMethods = append(wantMethods, http.MethodPut)
			}

			if !slices.Equal(methods, wantMethods) {
				t.Errorf("HTTP methods = %v, want %v (no extra PUT or finished POST)", methods, wantMethods)
			}
			if progressed != maxConsecutiveFileConflicts {
				t.Errorf("high-water progress = %d, want %d", progressed, maxConsecutiveFileConflicts)
			}
		})
	}
}

func TestSendVolumeData_FSCompressedCancelDuringRepositionDoesNotPUTOrFinalize(t *testing.T) {
	t.Parallel()

	content := bytes.Repeat([]byte("cancel-aware-discard"), 1024)
	tarPath := writeSingleEntryFSTar(t, "zstd", content)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var methods []string

	doer := fileHTTPDoer(func(req *http.Request) (*http.Response, error) {
		methods = append(methods, req.Method)

		if req.Method != http.MethodHead {
			return nil, fmt.Errorf("unexpected method %s after cancellation", req.Method)
		}

		cancel()

		return fileHTTPResponse(http.StatusOK, http.Header{"X-Next-Offset": []string{"1"}}), nil
	})

	progressed := 0
	importer := &clusterVolumeImporter{log: discardLogger()}
	leaf := PlannedNode{FilesystemData: true, TarFile: tarPath}

	err := importer.sendVolumeData(
		ctx,
		doer,
		"https://import.example",
		volumeModeFilesystem,
		leaf,
		"target",
		"data-import",
		nil,
		func(n int) { progressed += n },
		nil,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sendVolumeData error = %v, want context.Canceled", err)
	}

	if !slices.Equal(methods, []string{http.MethodHead}) {
		t.Errorf("HTTP methods = %v, want HEAD only", methods)
	}

	if progressed != 1 {
		t.Errorf("progress = %d, want validated durable HEAD high-water 1", progressed)
	}
}

const highCardinalityFSEntryCount = 2048

func buildHighCardinalityFSTar(t *testing.T, codec string, content []byte) []byte {
	t.Helper()

	ext, stored := encodeEntry(t, codec, content)

	var tarBuffer bytes.Buffer

	tarWriter := tar.NewWriter(&tarBuffer)
	for i := range highCardinalityFSEntryCount {
		originalPath := fmt.Sprintf("tiny/file-%05d.bin", i)
		addTarEntryMetadata(
			t,
			tarWriter,
			originalPath+ext,
			originalPath,
			codec,
			int64(len(content)),
			stored,
			0o600,
			0,
			0,
			time.Time{},
		)
	}

	if err := tarWriter.Close(); err != nil {
		t.Fatalf("close high-cardinality tar: %v", err)
	}

	return tarBuffer.Bytes()
}

func openVerifiedFSTarHandle(
	t *testing.T,
	ctx context.Context,
	tarData []byte,
) (string, *archive.VerifiedHandle) {
	t.Helper()

	root := t.TempDir()
	writeArchiveNode(t, root, archiveNode{
		apiVersion: "snapshot.storage.k8s.io/v1",
		kind:       "VolumeSnapshot",
		name:       "pvc",
		namespace:  "source",
		tarData:    tarData,
	})

	view, err := archive.OpenVerifiedArchive(root)
	if err != nil {
		t.Fatalf("open verified archive: %v", err)
	}
	t.Cleanup(func() {
		if err := view.Close(); err != nil {
			t.Errorf("close verified archive: %v", err)
		}
	})

	node, err := view.VerifyNode(context.Background(), root)
	if err != nil {
		t.Fatalf("verify archive node: %v", err)
	}

	payload, ok := node.File(archive.FsTarName)
	if !ok {
		t.Fatal("verified filesystem payload is absent")
	}

	handle, err := view.OpenVerifiedFile(ctx, payload)
	if err != nil {
		t.Fatalf("open verified filesystem payload: %v", err)
	}
	t.Cleanup(func() {
		if err := handle.Close(); err != nil {
			t.Errorf("close verified filesystem payload: %v", err)
		}
	})

	return filepath.Join(root, archive.FsTarName), handle
}

func assertHighCardinalityAuthenticationBound(
	t *testing.T,
	stats archive.AuthenticatedReadStats,
	archiveSize int64,
) {
	t.Helper()

	// Preflight and upload each traverse the tar once. Entry bodies, compressed proofs, and
	// boundary overlap have four additional archive-sized passes of headroom. The allowance is
	// independent of entry count and intentionally far below one 1 MiB reload per tiny file.
	const maxTraversalFactor = int64(6)

	maxBytes := maxTraversalFactor*archiveSize + 2*stats.ChunkSize
	if stats.SourceBytes > maxBytes || stats.HashedBytes > maxBytes {
		t.Fatalf("authenticated work = %+v for %d encoded bytes, want at most %d bytes",
			stats, archiveSize, maxBytes)
	}

	if stats.SourceBytes != stats.HashedBytes {
		t.Fatalf("authenticated source/hash bytes differ: %+v", stats)
	}

	chunks := (archiveSize + stats.ChunkSize - 1) / stats.ChunkSize
	maxLoads := maxTraversalFactor*chunks + 2
	if stats.ChunkLoads > maxLoads {
		t.Fatalf("authenticated chunk loads = %d, want at most %d for %d chunks",
			stats.ChunkLoads, maxLoads, chunks)
	}

	if stats.Resets != 2 {
		t.Fatalf("authenticated cache resets = %d, want exactly preflight and upload pass resets", stats.Resets)
	}
}

func TestImportFSFromTarSource_ConflictReplayWorkIsBounded(t *testing.T) {
	content := []byte("0123456789abcdef")

	tests := []struct {
		name          string
		codec         string
		conflictCount int
		wantErr       string
	}{
		{
			name:          "raw maximum conflicts recover",
			codec:         "none",
			conflictCount: maxConsecutiveFileConflicts,
		},
		{
			name:          "zstd maximum conflicts recover",
			codec:         "zstd",
			conflictCount: maxConsecutiveFileConflicts,
		},
		{
			name:          "raw excess conflict stops",
			codec:         "none",
			conflictCount: maxConsecutiveFileConflicts + 1,
			wantErr:       "too many consecutive file upload conflicts (8)",
		},
		{
			name:          "zstd excess conflict stops",
			codec:         "zstd",
			conflictCount: maxConsecutiveFileConflicts + 1,
			wantErr:       "too many consecutive file upload conflicts (8)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tarData := buildSingleEntryFSTar(t, tc.codec, content)
			tarPath, handle := openVerifiedFSTarHandle(t, context.Background(), tarData)

			putCount := 0
			var traversedPlaintext int64

			doer := fileHTTPDoer(func(req *http.Request) (*http.Response, error) {
				switch req.Method {
				case http.MethodHead:
					return fileHTTPResponse(http.StatusNotFound, nil), nil
				case http.MethodPut:
					putCount++
					maxPUTs := tc.conflictCount
					if tc.wantErr == "" {
						maxPUTs++
					}
					if putCount > maxPUTs {
						return nil, errors.New("opened an extra request body after rejecting conflict history")
					}

					offset, err := strconv.ParseInt(req.Header.Get("X-Offset"), 10, 64)
					if err != nil {
						return nil, fmt.Errorf("parse X-Offset: %w", err)
					}

					body, err := io.ReadAll(req.Body)
					if err != nil {
						return nil, fmt.Errorf("read request body: %w", err)
					}
					if !bytes.Equal(body, content[offset:]) {
						return nil, fmt.Errorf("body at offset %d = %q, want %q", offset, body, content[offset:])
					}

					// A compressed reopen decodes and discards offset bytes before yielding body.
					traversedPlaintext += offset + int64(len(body))

					if putCount <= tc.conflictCount {
						return fileHTTPResponse(http.StatusConflict, http.Header{
							"X-Expected-Offset": []string{strconv.Itoa(putCount)},
						}), nil
					}

					return fileHTTPResponse(http.StatusCreated, http.Header{
						"X-Next-Offset": []string{strconv.Itoa(len(content))},
					}), nil
				default:
					return nil, fmt.Errorf("unexpected HTTP method %s", req.Method)
				}
			})

			err := importFSFromTarSource(
				context.Background(),
				doer,
				"https://import.example",
				tarPath,
				handle,
				discardLogger(),
				nil,
				nil,
				nil,
			)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("import filesystem tar: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("import filesystem tar error = %v, want containing %q", err, tc.wantErr)
			}

			wantPUTs := tc.conflictCount
			if tc.wantErr == "" {
				wantPUTs++
			}

			if putCount != wantPUTs {
				t.Errorf("PUT count = %d, want %d", putCount, wantPUTs)
			}

			maxPlaintextWork := int64(maxConsecutiveFileConflicts+1) * int64(len(content))
			if traversedPlaintext > maxPlaintextWork {
				t.Errorf("plaintext replay work = %d, want at most %d", traversedPlaintext, maxPlaintextWork)
			}

			stats := handle.AuthenticatedReadStats()
			maxAuthenticatedBytes := 2 * int64(len(tarData))
			if stats.SourceBytes > maxAuthenticatedBytes || stats.HashedBytes > maxAuthenticatedBytes {
				t.Errorf("authenticated replay work = %+v, want at most two encoded traversals (%d bytes)",
					stats, maxAuthenticatedBytes)
			}
			if stats.ChunkLoads > 2 {
				t.Errorf("authenticated chunk loads = %d, want at most 2", stats.ChunkLoads)
			}

			assertHighCardinalityAuthenticationBound(
				t,
				stats,
				int64(len(tarData)),
			)
		})
	}
}

func TestImportFSFromTarSource_HighCardinalityAuthenticationWorkIsBounded(t *testing.T) {
	tests := []struct {
		name            string
		codec           string
		resumeConflict  bool
		concurrentStats bool
	}{
		{name: "raw adjacent entries with concurrent stats", codec: "none", concurrentStats: true},
		{name: "zstd adjacent entries", codec: "zstd"},
		{name: "raw resume and backward conflict", codec: "none", resumeConflict: true},
		{name: "zstd resume and backward conflict", codec: "zstd", resumeConflict: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			content := []byte("x")
			if tc.resumeConflict {
				content = []byte("abc")
			}

			tarData := buildHighCardinalityFSTar(t, tc.codec, content)
			tarPath, handle := openVerifiedFSTarHandle(t, context.Background(), tarData)

			doer := fileHTTPDoer(func(req *http.Request) (*http.Response, error) {
				switch req.Method {
				case http.MethodHead:
					if tc.resumeConflict {
						return fileHTTPResponse(http.StatusOK, http.Header{
							"X-Next-Offset": []string{"1"},
						}), nil
					}

					return fileHTTPResponse(http.StatusNotFound, nil), nil
				case http.MethodPut:
					offset, err := strconv.ParseInt(req.Header.Get("X-Offset"), 10, 64)
					if err != nil {
						return nil, fmt.Errorf("parse X-Offset: %w", err)
					}

					if tc.concurrentStats {
						readDone := make(chan error, 1)
						go func() {
							_, copyErr := io.Copy(io.Discard, req.Body)
							readDone <- copyErr
						}()

						for range 4 {
							_ = handle.AuthenticatedReadStats()
						}

						if err := <-readDone; err != nil {
							return nil, fmt.Errorf("consume concurrent request body: %w", err)
						}
					}

					if tc.resumeConflict && offset == 1 {
						return fileHTTPResponse(http.StatusConflict, http.Header{
							"X-Expected-Offset": []string{"0"},
						}), nil
					}

					next := offset + req.ContentLength

					return fileHTTPResponse(http.StatusCreated, http.Header{
						"X-Next-Offset": []string{strconv.FormatInt(next, 10)},
					}), nil
				default:
					return nil, fmt.Errorf("unexpected HTTP method %s", req.Method)
				}
			})

			err := importFSFromTarSource(
				context.Background(),
				doer,
				"https://import.example",
				tarPath,
				handle,
				discardLogger(),
				nil,
				nil,
				nil,
			)
			if err != nil {
				t.Fatalf("import high-cardinality filesystem tar: %v", err)
			}

			assertHighCardinalityAuthenticationBound(
				t,
				handle.AuthenticatedReadStats(),
				int64(len(tarData)),
			)
		})
	}
}

func TestImportFSFromTarSource_HighCardinalityCancellationIsStickyAndBounded(t *testing.T) {
	tarData := buildHighCardinalityFSTar(t, "zstd", []byte("cancel"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tarPath, handle := openVerifiedFSTarHandle(t, ctx, tarData)

	putCount := 0
	doer := fileHTTPDoer(func(req *http.Request) (*http.Response, error) {
		switch req.Method {
		case http.MethodHead:
			return fileHTTPResponse(http.StatusNotFound, nil), nil
		case http.MethodPut:
			putCount++
			if putCount == highCardinalityFSEntryCount/4 {
				cancel()

				return nil, context.Canceled
			}

			return fileHTTPResponse(http.StatusCreated, http.Header{
				"X-Next-Offset": []string{strconv.FormatInt(req.ContentLength, 10)},
			}), nil
		default:
			return nil, fmt.Errorf("unexpected HTTP method %s", req.Method)
		}
	})

	err := importFSFromTarSource(
		ctx,
		doer,
		"https://import.example",
		tarPath,
		handle,
		discardLogger(),
		nil,
		nil,
		nil,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("import high-cardinality filesystem tar error = %v, want context.Canceled", err)
	}

	var probe [1]byte
	if _, err := handle.ReadAt(probe[:], 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("read after cancellation = %v, want sticky context.Canceled", err)
	}

	assertHighCardinalityAuthenticationBound(
		t,
		handle.AuthenticatedReadStats(),
		int64(len(tarData)),
	)
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

			offset, _ := strconv.ParseInt(r.Header.Get("X-Offset"), 10, 64)
			next := offset + r.ContentLength
			w.Header().Set("X-Next-Offset", strconv.FormatInt(next, 10))

			total, _ := strconv.ParseInt(r.Header.Get("X-Content-Length"), 10, 64)
			if next == total {
				w.WriteHeader(http.StatusCreated)

				return
			}

			w.WriteHeader(http.StatusNoContent)
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

func TestSendVolumeData_FSTarReplacementUsesPinnedBytesAndDoesNotFinish(t *testing.T) {
	content := []byte("verified filesystem bytes")

	var tarBuffer bytes.Buffer
	tarWriter := tar.NewWriter(&tarBuffer)
	addTarEntryRawPAX(t, tarWriter, "file.txt", "file.txt", "none", int64(len(content)), content)
	if err := tarWriter.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}

	nodeSpec := archiveNode{
		apiVersion: "snapshot.storage.k8s.io/v1",
		kind:       "VolumeSnapshot",
		name:       "pvc-1",
		namespace:  "src",
		tarData:    tarBuffer.Bytes(),
	}
	nodeSpec.volumes = synthVolumeInfo(nodeSpec)
	nodeSpec.volumes[0].Size = strconv.Itoa(len(content))

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

	tarPath := filepath.Join(root, archive.FsTarName)

	var (
		received []byte
		finished int
		replaced bool
	)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodHead:
			if !replaced {
				replaced = true
				if err := os.Rename(tarPath, tarPath+".verified"); err != nil {
					t.Fatalf("move verified tar: %v", err)
				}

				if err := os.WriteFile(tarPath, []byte("replacement tar bytes"), 0o600); err != nil {
					t.Fatalf("write replacement tar: %v", err)
				}
			}

			writer.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			var err error
			received, err = io.ReadAll(request.Body)
			if err != nil {
				t.Fatalf("read PUT body: %v", err)
			}

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
		volumeModeFilesystem,
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

	if !bytes.Equal(received, content) {
		t.Fatalf("PUT body = %q, want verified tar entry %q", received, content)
	}

	if finished != 0 {
		t.Fatalf("finished POSTs = %d, want 0", finished)
	}
}

func TestSendVolumeData_AllServerSkippedFSStillRejectsReplacement(t *testing.T) {
	content := []byte("already durable filesystem bytes")

	var tarBuffer bytes.Buffer
	tarWriter := tar.NewWriter(&tarBuffer)
	addTarEntryRawPAX(t, tarWriter, "file.txt", "file.txt", "none", int64(len(content)), content)
	if err := tarWriter.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}

	nodeSpec := archiveNode{
		apiVersion: "snapshot.storage.k8s.io/v1",
		kind:       "VolumeSnapshot",
		name:       "pvc-1",
		namespace:  "src",
		tarData:    tarBuffer.Bytes(),
	}
	nodeSpec.volumes = synthVolumeInfo(nodeSpec)
	nodeSpec.volumes[0].Size = strconv.Itoa(len(content))

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

	tarPath := filepath.Join(root, archive.FsTarName)

	var putCount, finished int

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodHead:
			if err := os.Rename(tarPath, tarPath+".verified"); err != nil {
				t.Fatalf("move verified tar: %v", err)
			}

			if err := os.WriteFile(tarPath, []byte("replacement tar bytes"), 0o600); err != nil {
				t.Fatalf("write replacement tar: %v", err)
			}

			writer.Header().Set("Content-Length", strconv.Itoa(len(content)))
			writer.WriteHeader(http.StatusOK)
		case http.MethodPut:
			putCount++
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
		volumeModeFilesystem,
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

	if putCount != 0 || finished != 0 {
		t.Fatalf("requests after all-server skip: PUT=%d finished=%d, want zero", putCount, finished)
	}
}

func TestSendVolumeData_FSMutateUseRestoreDuringPUTIsRejected(t *testing.T) {
	content := randomPayload(t, 2*1024*1024+4096)

	tests := []struct {
		name         string
		codec        string
		resumeOffset int64
		hardlink     bool
	}{
		{name: "raw same inode", codec: "none"},
		{name: "raw external hardlink partial resume", codec: "none", resumeOffset: 512*1024 + 17, hardlink: true},
		{name: "zstd same inode", codec: "zstd"},
		{name: "zstd external hardlink", codec: "zstd", hardlink: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tarFixture := writeSingleEntryFSTar(t, tc.codec, content)
			tarData, err := os.ReadFile(tarFixture)
			if err != nil {
				t.Fatalf("read tar fixture: %v", err)
			}

			nodeSpec := archiveNode{
				apiVersion: "snapshot.storage.k8s.io/v1",
				kind:       "VolumeSnapshot",
				name:       "pvc-1",
				namespace:  "src",
				tarData:    tarData,
			}
			nodeSpec.volumes = synthVolumeInfo(nodeSpec)
			nodeSpec.volumes[0].Size = strconv.Itoa(len(content))

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

			payloadPath := filepath.Join(root, archive.FsTarName)
			writerPath := payloadPath
			if tc.hardlink {
				writerPath = filepath.Join(t.TempDir(), "external-hardlink")
				if err := os.Link(payloadPath, writerPath); err != nil {
					t.Fatalf("create external hardlink: %v", err)
				}
			}

			const mutationOffset = 1024*1024 + 17

			var (
				finished int
				received []byte
				methods  []string
			)

			doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
				methods = append(methods, req.Method)

				switch req.Method {
				case http.MethodHead:
					if tc.resumeOffset > 0 {
						header := http.Header{}
						header.Set("X-Next-Offset", strconv.FormatInt(tc.resumeOffset, 10))

						return newTestHTTPResponse(http.StatusOK, header), nil
					}

					return newTestHTTPResponse(http.StatusNotFound, nil), nil
				case http.MethodPut:
					readData, readErr := readBodyDuringRestoredMutation(
						t,
						req.Body,
						payloadPath,
						writerPath,
						tarData,
						mutationOffset,
					)
					received = append(received, readData...)

					if readErr != nil {
						return nil, readErr
					}

					header := http.Header{}
					header.Set("X-Next-Offset", strconv.Itoa(len(content)))

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
				volumeModeFilesystem,
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
			expectedEnd := expectedStart + len(received)
			if expectedEnd > len(content) || !bytes.Equal(received, content[expectedStart:expectedEnd]) {
				t.Fatalf("PUT received %d bytes that are not an authenticated original payload prefix", len(received))
			}

			if finished != 0 {
				t.Fatalf("finished POSTs = %d, want 0", finished)
			}

			if len(methods) != 2 || methods[0] != http.MethodHead || methods[1] != http.MethodPut {
				t.Fatalf("HTTP methods = %v, want HEAD then PUT only", methods)
			}
		})
	}
}

func TestSendVolumeData_FSWaitsForExactBodyCompletion(t *testing.T) {
	content := bytes.Repeat([]byte("attested-filesystem-payload-"), 4096)

	for _, codec := range []string{"none", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			tarFixture := writeSingleEntryFSTar(t, codec, content)
			tarData, err := os.ReadFile(tarFixture)
			if err != nil {
				t.Fatalf("read tar fixture: %v", err)
			}

			nodeSpec := archiveNode{
				apiVersion: "snapshot.storage.k8s.io/v1",
				kind:       "VolumeSnapshot",
				name:       "pvc-1",
				namespace:  "src",
				tarData:    tarData,
			}
			nodeSpec.volumes = synthVolumeInfo(nodeSpec)
			nodeSpec.volumes[0].Size = strconv.Itoa(len(content))

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
					return newTestHTTPResponse(http.StatusNotFound, nil), nil
				case http.MethodPut:
					putBody <- req.Body

					header := http.Header{}
					header.Set("X-Next-Offset", strconv.Itoa(len(content)))

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
					volumeModeFilesystem,
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
			if !bytes.Equal(received, content) {
				t.Fatalf("received %d bytes, want exact %d-byte content", len(received), len(content))
			}

			if err := <-result; err != nil {
				t.Fatalf("sendVolumeData: %v", err)
			}
			if got := progressed.Load(); got != int64(len(content)) {
				t.Fatalf("progress after attestation = %d, want %d", got, len(content))
			}
			if got := finished.Load(); got != 1 {
				t.Fatalf("finished POSTs after attestation = %d, want 1", got)
			}
		})
	}
}
