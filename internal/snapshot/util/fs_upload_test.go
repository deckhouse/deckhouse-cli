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
)

// fsImportServer emulates the data-importer Filesystem protocol: per-relpath resumable PUT under
// /api/v1/files/<rel> (HEAD X-Next-Offset, PUT X-Offset) plus a global POST /api/v1/finished.
type fsImportServer struct {
	mu       sync.Mutex
	files    map[string]*importState
	perms    map[string]string
	finished bool
}

func newFSImportServer() *fsImportServer {
	return &fsImportServer{files: map[string]*importState{}, perms: map[string]string{}}
}

func (s *fsImportServer) state(rel string) *importState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.files[rel]
	if !ok {
		st = newImportState()
		s.files[rel] = st
	}
	return st
}

func (s *fsImportServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/finished", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		s.finished = true
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/api/v1/files/", func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/api/v1/files/")
		st := s.state(rel)
		switch r.Method {
		case http.MethodHead:
			st.head(w)
		case http.MethodPut:
			off, _ := strconv.Atoi(r.Header.Get("X-Offset"))
			body, _ := io.ReadAll(r.Body)
			if p := r.Header.Get("X-Attribute-Permissions"); p != "" {
				s.mu.Lock()
				s.perms[rel] = p
				s.mu.Unlock()
			}
			st.put(w, off, body, false)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	return mux
}

func (s *fsImportServer) received(rel string) ([]byte, bool) {
	s.mu.Lock()
	st, ok := s.files[rel]
	s.mu.Unlock()
	if !ok {
		return nil, false
	}
	data, _ := st.snapshot()
	return data, true
}

func TestUploadFilesystem_Tree(t *testing.T) {
	fsSrv := newFSImportServer()
	srv := httptest.NewServer(fsSrv.handler())
	defer srv.Close()

	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "a.txt"), "alpha")
	mustWrite(t, filepath.Join(root, "sub", "b.txt"), "bravo")
	mustWrite(t, filepath.Join(root, "empty.txt"), "")

	if err := uploadFilesystem(context.Background(), &fakeDoer{srv.Client()}, srv.URL, root); err != nil {
		t.Fatalf("uploadFilesystem: %v", err)
	}

	want := map[string]string{
		"a.txt":     "alpha",
		"sub/b.txt": "bravo",
		"empty.txt": "",
	}
	for rel, content := range want {
		got, ok := fsSrv.received(rel)
		if !ok {
			t.Errorf("server never received %q", rel)
			continue
		}
		if string(got) != content {
			t.Errorf("%s = %q, want %q", rel, got, content)
		}
		if fsSrv.perms[rel] == "" {
			t.Errorf("%s uploaded without X-Attribute-Permissions", rel)
		}
	}
	if !fsSrv.finished {
		t.Fatal("expected the importer to be POSTed /api/v1/finished")
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
