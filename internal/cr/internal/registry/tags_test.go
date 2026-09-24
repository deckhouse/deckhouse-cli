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

package registry

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// listRegistry serves /v2/ plus a pluggable /v2/<repo>/tags/list, and counts
// how many times the tag endpoint was hit so tests can assert on round trips.
type listRegistry struct {
	host string
	hits *int32
}

func newListRegistry(t *testing.T, tagsList func(w http.ResponseWriter, r *http.Request, hit int)) *listRegistry {
	t.Helper()

	var hits int32

	reg := &listRegistry{hits: &hits}

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/tags/list") && !strings.HasSuffix(r.URL.Path, "/_catalog") {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("{}"))

			return
		}

		tagsList(w, r, int(atomic.AddInt32(&hits, 1)))
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	reg.host = strings.TrimPrefix(srv.URL, "http://")

	return reg
}

func insecureOpts() *Options {
	return New().WithInsecure()
}

// A registry that serves the tag list across three Link-chained pages must
// yield one complete list - the wire pagination is an implementation detail
// with no console equivalent, so nothing may be dropped or deferred.
func TestListTags_ConcatenatesAllPages(t *testing.T) {
	pages := [][]string{{"v1", "v2"}, {"v3", "v4"}, {"v5"}}

	reg := newListRegistry(t, func(w http.ResponseWriter, r *http.Request, hit int) {
		page := pages[hit-1]
		if hit < len(pages) {
			w.Header().Set("Link", fmt.Sprintf(`</v2/repo/tags/list?n=1000&last=%s>; rel="next"`, page[len(page)-1]))
		}

		writeTags(w, page)
	})

	got, err := ListTags(context.Background(), reg.host+"/repo", insecureOpts())
	if err != nil {
		t.Fatalf("ListTags: %v", err)
	}

	want := []string{"v1", "v2", "v3", "v4", "v5"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	if int(*reg.hits) != len(pages) {
		t.Errorf("expected %d page requests, got %d", len(pages), *reg.hits)
	}
}

// A registry that ignores ?n= and answers with everything in one response,
// no Link header, is the common case and must cost exactly one round trip.
func TestListTags_SinglePageNoLink(t *testing.T) {
	reg := newListRegistry(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		writeTags(w, []string{"v1", "v2", "v3"})
	})

	got, err := ListTags(context.Background(), reg.host+"/repo", insecureOpts())
	if err != nil {
		t.Fatalf("ListTags: %v", err)
	}

	if want := []string{"v1", "v2", "v3"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	if *reg.hits != 1 {
		t.Errorf("expected 1 request, got %d", *reg.hits)
	}
}

// go-containerregistry asks for n=1000 by default. A registry that rejects an
// ?n= it does not implement used to make `d8 cr ls` fail outright with an
// empty list; the retry without the parameter must recover the full list.
func TestListTags_RegistryRejectsPageSizeParam(t *testing.T) {
	reg := newListRegistry(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		if r.URL.Query().Get("n") != "" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"errors":[{"code":"UNSUPPORTED","message":"query param n is not supported"}]}`))

			return
		}

		writeTags(w, []string{"v1", "v2"})
	})

	got, err := ListTags(context.Background(), reg.host+"/repo", insecureOpts())
	if err != nil {
		t.Fatalf("ListTags must fall back to a request without ?n=: %v", err)
	}

	if want := []string{"v1", "v2"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// A genuine failure (bad credentials) must still surface, and must report the
// original error rather than the compatibility retry's.
func TestListTags_PropagatesRealError(t *testing.T) {
	reg := newListRegistry(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
		w.WriteHeader(http.StatusUnauthorized)
	})

	_, err := ListTags(context.Background(), reg.host+"/repo", insecureOpts())
	if err == nil {
		t.Fatalf("expected an error for a 401 registry")
	}

	if !strings.Contains(err.Error(), "read tags for") {
		t.Errorf("error should name the operation, got: %v", err)
	}
}

// A registry that ignores last= and echoes the same Link cursor forever used
// to spin the walk indefinitely, printing duplicates until the user hit
// Ctrl+C. It must now fail fast instead of looping.
func TestListTags_RefusesRepeatedCursor(t *testing.T) {
	reg := newListRegistry(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("Link", `</v2/repo/tags/list?n=1000&last=v2>; rel="next"`)
		writeTags(w, []string{"v1", "v2"})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := ListTags(ctx, reg.host+"/repo", insecureOpts())
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected an error, got a successful listing")
		}

		if !strings.Contains(err.Error(), "same pagination cursor") {
			t.Errorf("unexpected error: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("ListTags looped instead of refusing the repeated cursor")
	}

	// Two requests: the first page, then the one that repeats the cursor.
	if *reg.hits > 3 {
		t.Errorf("expected the walk to stop almost immediately, got %d requests", *reg.hits)
	}
}

// ListCatalog shares the walk and the guards; cover the two that matter.
func TestListCatalog_ConcatenatesAllPagesAndRefusesRepeatedCursor(t *testing.T) {
	t.Run("all pages", func(t *testing.T) {
		reg := newListRegistry(t, func(w http.ResponseWriter, _ *http.Request, hit int) {
			if hit == 1 {
				w.Header().Set("Link", `</v2/_catalog?n=1000&last=b>; rel="next"`)
				_, _ = w.Write([]byte(`{"repositories":["a","b"]}`))

				return
			}

			_, _ = w.Write([]byte(`{"repositories":["c"]}`))
		})

		got, err := ListCatalog(context.Background(), reg.host, insecureOpts())
		if err != nil {
			t.Fatalf("ListCatalog: %v", err)
		}

		if want := []string{"a", "b", "c"}; !slices.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("repeated cursor", func(t *testing.T) {
		reg := newListRegistry(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
			w.Header().Set("Link", `</v2/_catalog?n=1000&last=b>; rel="next"`)
			_, _ = w.Write([]byte(`{"repositories":["a","b"]}`))
		})

		if _, err := ListCatalog(context.Background(), reg.host, insecureOpts()); err == nil ||
			!strings.Contains(err.Error(), "same pagination cursor") {
			t.Errorf("expected a repeated-cursor refusal, got: %v", err)
		}
	})
}

func writeTags(w http.ResponseWriter, tags []string) {
	_, _ = fmt.Fprintf(w, `{"name":"repo","tags":["%s"]}`, strings.Join(tags, `","`))
}
