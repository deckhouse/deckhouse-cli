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
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestAPIClient(srv *httptest.Server) *APIClient {
	return &APIClient{httpClient: srv.Client(), baseURL: srv.URL}
}

// TestUploadBlob_FreshMultiChunkFinalize exercises the multi-chunk path plus a single finalize on the
// last chunk.
func TestUploadBlob_FreshMultiChunkFinalize(t *testing.T) {
	data := payloadOf(apiUploadChunkBytes*2 + 17) // forces 3 chunks
	st := newImportState()
	srv := httptest.NewServer(blobHandler(st))
	defer srv.Close()

	if err := newTestAPIClient(srv).UploadBlob(context.Background(), "/upload", data, true); err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}
	got, finished := st.snapshot()
	if !bytes.Equal(got, data) {
		t.Fatalf("uploaded content mismatch: got %d bytes, want %d", len(got), len(data))
	}
	if !finished {
		t.Fatal("expected the blob to be finalized")
	}
}

func TestUploadBlob_Resume(t *testing.T) {
	data := payloadOf(apiUploadChunkBytes + 5000)
	st := newImportState()
	st.received = append([]byte(nil), data[:100000]...)
	srv := httptest.NewServer(blobHandler(st))
	defer srv.Close()

	if err := newTestAPIClient(srv).UploadBlob(context.Background(), "/upload", data, true); err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}
	got, finished := st.snapshot()
	if !bytes.Equal(got, data) || !finished {
		t.Fatalf("resume mismatch: got %d bytes, finished=%v", len(got), finished)
	}
}

// TestUploadBlob_FinalizeOnlyAfterFullBody covers the "body fully persisted, client crashed before
// commit" resume: a single finalize-only request must commit the blob.
func TestUploadBlob_FinalizeOnlyAfterFullBody(t *testing.T) {
	data := payloadOf(40000)
	st := newImportState()
	st.received = append([]byte(nil), data...) // whole body already persisted, not finalized
	srv := httptest.NewServer(blobHandler(st))
	defer srv.Close()

	if err := newTestAPIClient(srv).UploadBlob(context.Background(), "/upload", data, true); err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}
	got, finished := st.snapshot()
	if !bytes.Equal(got, data) || !finished {
		t.Fatalf("finalize-only mismatch: got %d bytes, finished=%v", len(got), finished)
	}
}

// TestUploadBlob_FinalizeOnly409Resumes is the M1 regression: HEAD lies that the body is complete, so
// the client tries a finalize-only commit; the server 409s wanting more bytes. The client must resume
// the missing data instead of silently reporting success.
func TestUploadBlob_FinalizeOnly409Resumes(t *testing.T) {
	data := payloadOf(40000)
	st := newImportState()
	st.received = append([]byte(nil), data[:25000]...)
	st.headOverride = func(int) (int, bool) { return len(data), true } // lie: looks complete
	srv := httptest.NewServer(blobHandler(st))
	defer srv.Close()

	if err := newTestAPIClient(srv).UploadBlob(context.Background(), "/upload", data, true); err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}
	got, finished := st.snapshot()
	if !bytes.Equal(got, data) || !finished {
		t.Fatalf("finalize-only 409 resume mismatch: got %d bytes, finished=%v", len(got), finished)
	}
}

func TestUploadBlob_409ForwardConverge(t *testing.T) {
	data := payloadOf(apiUploadChunkBytes + 1234)
	st := newImportState()
	st.received = append([]byte(nil), data[:50000]...)
	st.headOverride = func(int) (int, bool) { return 0, true } // pretend nothing uploaded
	srv := httptest.NewServer(blobHandler(st))
	defer srv.Close()

	if err := newTestAPIClient(srv).UploadBlob(context.Background(), "/upload", data, true); err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}
	got, finished := st.snapshot()
	if !bytes.Equal(got, data) || !finished {
		t.Fatalf("forward-converge mismatch: got %d bytes, finished=%v", len(got), finished)
	}
}

func TestUploadBlob_409NoHeaderErrors(t *testing.T) {
	data := payloadOf(40000)
	st := newImportState()
	st.put409NoHeader = true
	srv := httptest.NewServer(blobHandler(st))
	defer srv.Close()

	err := newTestAPIClient(srv).UploadBlob(context.Background(), "/upload", data, true)
	if err == nil {
		t.Fatal("expected an error on 409 without X-Next-Offset, got nil")
	}
	if !strings.Contains(err.Error(), "cannot converge") {
		t.Fatalf("expected a convergence error, got: %v", err)
	}
}

// TestUploadBlob_NodeQueryPreserved verifies a pre-existing query (?node=<id>) is preserved and the
// finalize selector is appended with & rather than ?.
func TestUploadBlob_NodeQueryPreserved(t *testing.T) {
	data := payloadOf(1000)
	var sawFinalizeWithNode bool
	st := newImportState()
	base := blobHandler(st)
	srv := httptest.NewServer(testHandlerFunc(func(path, rawQuery string) {
		if strings.Contains(rawQuery, "node=n1") && strings.Contains(rawQuery, "finalize=true") {
			sawFinalizeWithNode = true
		}
	}, base))
	defer srv.Close()

	if err := newTestAPIClient(srv).UploadBlob(context.Background(), ManifestsNodePath("/manifests", "n1"), data, true); err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}
	if !sawFinalizeWithNode {
		t.Fatal("expected the finalize request to preserve the ?node=n1 selector")
	}
}
