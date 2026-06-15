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
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

// fakeDoer adapts a plain *http.Client to the httpDoer interface so the data-pod transfer cores can be
// exercised against an httptest server without a real kubeconfig-backed SafeClient.
type fakeDoer struct{ c *http.Client }

func (f *fakeDoer) HTTPDo(req *http.Request) (*http.Response, error) { return f.c.Do(req) }

// importState models a stateful resumable importer (X-Offset / X-Next-Offset protocol) shared by the
// block (data-pod) and blob (aggregated subresource) upload tests.
type importState struct {
	mu       sync.Mutex
	received []byte
	finished bool

	// headOverride, when set, controls the HEAD X-Next-Offset response: it returns the value to
	// advertise and whether to send the header at all.
	headOverride func(have int) (int, bool)
	// put409NoHeader makes every PUT answer 409 with no X-Next-Offset (a non-conforming server).
	put409NoHeader bool
	// rewindOnceTo, when >= 0, makes the first matching PUT issue a single backward 409 resync,
	// truncating received to this offset (simulates a server that lost its tail).
	rewindOnceTo int
	rewound      bool
}

func newImportState() *importState { return &importState{rewindOnceTo: -1} }

func (s *importState) head(w http.ResponseWriter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, send := len(s.received), true
	if s.headOverride != nil {
		val, send = s.headOverride(len(s.received))
	}
	if send {
		w.Header().Set("X-Next-Offset", strconv.Itoa(val))
	}
	w.WriteHeader(http.StatusOK)
}

func (s *importState) put(w http.ResponseWriter, off int, body []byte, finalize bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.put409NoHeader {
		w.WriteHeader(http.StatusConflict)
		return
	}
	if s.rewindOnceTo >= 0 && !s.rewound && off == len(s.received) {
		s.rewound = true
		s.received = s.received[:s.rewindOnceTo]
		w.Header().Set("X-Next-Offset", strconv.Itoa(len(s.received)))
		w.WriteHeader(http.StatusConflict)
		return
	}
	if off != len(s.received) {
		w.Header().Set("X-Next-Offset", strconv.Itoa(len(s.received)))
		w.WriteHeader(http.StatusConflict)
		return
	}
	s.received = append(s.received, body...)
	if finalize {
		s.finished = true
	}
	w.Header().Set("X-Next-Offset", strconv.Itoa(len(s.received)))
	w.WriteHeader(http.StatusOK)
}

func (s *importState) snapshot() (received []byte, finished bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.received...), s.finished
}

// blockHandler serves the data-pod block protocol: PUT /api/v1/block (X-Offset) + POST /api/v1/finished.
func blockHandler(s *importState) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/block", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			s.head(w)
		case http.MethodPut:
			off, _ := strconv.Atoi(r.Header.Get("X-Offset"))
			body, _ := io.ReadAll(r.Body)
			s.put(w, off, body, false)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/v1/finished", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		s.finished = true
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// blobHandler serves the aggregated subresource protocol: PUT <path>?finalize=true (X-Offset).
func blobHandler(s *importState) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			s.head(w)
		case http.MethodPut:
			off, _ := strconv.Atoi(r.Header.Get("X-Offset"))
			body, _ := io.ReadAll(r.Body)
			s.put(w, off, body, r.URL.Query().Get("finalize") == "true")
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

// testHandlerFunc wraps a handler to observe each request's path + raw query before delegating.
func testHandlerFunc(observe func(path, rawQuery string), next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observe(r.URL.Path, r.URL.RawQuery)
		next.ServeHTTP(w, r)
	})
}

func writeTempFile(t *testing.T, data []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "img")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return p
}
