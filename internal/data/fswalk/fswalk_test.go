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

package fswalk

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fakeDoer struct{ c *http.Client }

func (f *fakeDoer) HTTPDo(req *http.Request) (*http.Response, error) { return f.c.Do(req) }

// fsTreeServer emulates the data-exporter filesystem listing protocol: a directory path (ending in
// "/") returns {"items":[{name,type}...]}, a file path returns its bytes. Tree:
//
//	/api/v1/files/        -> dir "sub", file "a.txt"
//	/api/v1/files/sub/    -> file "b.txt"
func fsTreeServer() *httptest.Server {
	files := map[string]string{
		"/api/v1/files/a.txt":     "alpha",
		"/api/v1/files/sub/b.txt": "bravo",
	}
	listings := map[string]string{
		"/api/v1/files/":     `{"items":[{"name":"sub","type":"dir"},{"name":"a.txt","type":"file"}]}`,
		"/api/v1/files/sub/": `{"items":[{"name":"b.txt","type":"file"}]}`,
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if body, ok := listings[r.URL.Path]; ok {
			_, _ = io.WriteString(w, body)
			return
		}
		if body, ok := files[r.URL.Path]; ok {
			_, _ = io.WriteString(w, body)
			return
		}
		http.NotFound(w, r)
	}))
}

func TestRecursiveDownload_Tree(t *testing.T) {
	srv := fsTreeServer()
	defer srv.Close()

	dst := t.TempDir()
	sem := make(chan struct{}, DefaultConcurrency)
	err := RecursiveDownload(context.Background(), &fakeDoer{srv.Client()}, slog.Default(), sem, srv.URL, "api/v1/files/", dst)
	if err != nil {
		t.Fatalf("RecursiveDownload: %v", err)
	}

	want := map[string]string{
		filepath.Join(dst, "a.txt"):        "alpha",
		filepath.Join(dst, "sub", "b.txt"): "bravo",
	}
	for path, content := range want {
		got, rerr := os.ReadFile(path)
		if rerr != nil {
			t.Fatalf("read %s: %v", path, rerr)
		}
		if string(got) != content {
			t.Errorf("%s = %q, want %q", path, got, content)
		}
	}
}

func TestRecursiveDownload_SingleFile(t *testing.T) {
	srv := fsTreeServer()
	defer srv.Close()

	out := filepath.Join(t.TempDir(), "only.txt")
	sem := make(chan struct{}, DefaultConcurrency)
	// A non-slash srcPath is treated as a single file.
	if err := RecursiveDownload(context.Background(), &fakeDoer{srv.Client()}, slog.Default(), sem, srv.URL, "api/v1/files/a.txt", out); err != nil {
		t.Fatalf("RecursiveDownload single file: %v", err)
	}
	got, _ := os.ReadFile(out)
	if strings.TrimSpace(string(got)) != "alpha" {
		t.Fatalf("single-file download = %q, want alpha", got)
	}
}
