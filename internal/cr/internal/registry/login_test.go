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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
)

const (
	goodUser = "robot"
	goodPass = "s3cret"
)

// basicAuthRegistry mimics an htpasswd registry: /v2/ answers 200 only when the
// request carries the correct Basic credentials, otherwise 401 with a Basic
// challenge (the same shape `transport.NewWithContext` sees on its anonymous
// ping). This is the case the old Login silently accepted with any password.
func basicAuthRegistry() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if u, p, ok := r.BasicAuth(); ok && u == goodUser && p == goodPass {
			w.WriteHeader(http.StatusOK)
			return
		}

		w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
		w.WriteHeader(http.StatusUnauthorized)
	}))
}

// bearerAuthRegistry mimics a token registry (Docker Hub style): /v2/ requires a
// Bearer token, and /token exchanges correct Basic credentials for one. Wrong
// credentials are rejected at the token endpoint - the path the old Login
// already handled - so this guards against the fix regressing the bearer flow.
func bearerAuthRegistry() *httptest.Server {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)

	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer good-token" {
			w.WriteHeader(http.StatusOK)
			return
		}

		w.Header().Set("WWW-Authenticate", `Bearer realm="`+srv.URL+`/token",service="test"`)
		w.WriteHeader(http.StatusUnauthorized)
	})

	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if u, p, ok := r.BasicAuth(); !ok || u != goodUser || p != goodPass {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"token": "good-token"})
	})

	return srv
}

func TestLoginVerifiesCredentials(t *testing.T) {
	cases := []struct {
		name     string
		registry func() *httptest.Server
	}{
		{"basic-auth (htpasswd)", basicAuthRegistry},
		{"bearer-token", bearerAuthRegistry},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := tc.registry()
			defer srv.Close()

			host := strings.TrimPrefix(srv.URL, "http://")

			t.Run("correct credentials succeed", func(t *testing.T) {
				// Login persists to the Docker config; keep it inside the test.
				t.Setenv("DOCKER_CONFIG", t.TempDir())

				res, err := Login(context.Background(), host, goodUser, goodPass, insecureOptions())
				if err != nil {
					t.Fatalf("expected login to succeed, got error: %v", err)
				}
				if res.ServerAddress != host {
					t.Errorf("ServerAddress = %q, want %q", res.ServerAddress, host)
				}
			})

			t.Run("wrong password fails and persists nothing", func(t *testing.T) {
				dir := t.TempDir()
				t.Setenv("DOCKER_CONFIG", dir)

				if _, err := Login(context.Background(), host, goodUser, "wrong-password", insecureOptions()); err == nil {
					t.Fatal("expected login with wrong password to fail, got nil error")
				}

				// A failed login must not write a (broken) config.json.
				if _, err := os.Stat(filepath.Join(dir, "config.json")); !os.IsNotExist(err) {
					t.Errorf("wrong-password login wrote a config.json; expected none (stat err=%v)", err)
				}
			})
		})
	}
}

// insecureOptions parses the httptest host as plain HTTP (it serves no TLS),
// matching how `--insecure` / a localhost target is handled in real use.
func insecureOptions() *Options {
	o := New()
	o.Name = append(o.Name, name.Insecure)
	return o
}
