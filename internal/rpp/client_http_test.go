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

package rpp

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
)

func testClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return NewWithHTTPClient(srv.URL, srv.Client(), dkplog.NewNop())
}

func TestClientListTags(t *testing.T) {
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/v1/images/deckhouse-cli/tags", r.URL.Path)
		assert.Equal(t, mediaTypeJSON, r.Header.Get(headerAccept))

		w.Header().Set("Content-Type", mediaTypeJSON)
		_, _ = io.WriteString(w, `{"name":"deckhouse-cli","tags":["v0.13.0","v0.13.1"]}`)
	})

	tags, err := client.ListTags(context.Background(), CLIImage())
	require.NoError(t, err)
	assert.Equal(t, []string{"v0.13.0", "v0.13.1"}, tags)
}

func TestClientListTagsForPlugin(t *testing.T) {
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/images/deckhouse-cli/plugins/stronghold/tags", r.URL.Path)
		_, _ = io.WriteString(w, `{"name":"deckhouse-cli/plugins/stronghold","tags":["v1.0.0"]}`)
	})

	ref, err := PluginImage("stronghold")
	require.NoError(t, err)

	tags, err := client.ListTags(context.Background(), ref)
	require.NoError(t, err)
	assert.Equal(t, []string{"v1.0.0"}, tags)
}

func TestClientPullImage(t *testing.T) {
	const payload = "fake-tar-gz-bytes"

	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/v1/images/deckhouse-cli/images/v0.13.1", r.URL.Path)
		assert.Equal(t, runtime.GOOS+"-"+runtime.GOARCH, r.URL.Query().Get("platform"))

		w.Header().Set("Content-Type", "application/x-gzip")
		_, _ = io.WriteString(w, payload)
	})

	body, err := client.PullImage(context.Background(), CLIImage(), "v0.13.1")
	require.NoError(t, err)
	defer func() { _ = body.Close() }()

	got, err := io.ReadAll(body)
	require.NoError(t, err)
	assert.Equal(t, payload, string(got))
}

func TestClientGetManifest(t *testing.T) {
	const manifestJSON = `{"annotations":{"contract":"e30="}}`

	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/v1/images/deckhouse-cli/plugins/foo/manifests/v1.0.0", r.URL.Path)
		assert.Empty(t, r.URL.Query().Get("platform"), "manifest is platform-independent")
		assert.Equal(t, acceptManifest, r.Header.Get(headerAccept))

		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		_, _ = io.WriteString(w, manifestJSON)
	})

	ref, err := PluginImage("foo")
	require.NoError(t, err)

	raw, err := client.GetManifest(context.Background(), ref, "v1.0.0")
	require.NoError(t, err)
	assert.Equal(t, manifestJSON, string(raw))
}

func TestClientStatusErrorMapping(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		wantErr error
	}{
		{name: "not found", status: http.StatusNotFound, wantErr: ErrNotFound},
		{name: "unauthorized", status: http.StatusUnauthorized, wantErr: ErrUnauthorized},
		{name: "forbidden", status: http.StatusForbidden, wantErr: ErrForbidden},
		{name: "bad gateway", status: http.StatusBadGateway, wantErr: ErrUpstream},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := testClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
			})

			_, err := client.ListTags(context.Background(), CLIImage())
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// authInjectingTransport mimics client-go's bearer round tripper: it stamps the
// credential on EVERY request that passes through the transport - redirect hops
// included, which is exactly why the client must not follow redirects.
type authInjectingTransport struct{ base http.RoundTripper }

func (t authInjectingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer secret")

	return t.base.RoundTrip(req)
}

func TestClientRefusesRedirectsSoCredentialsCannotLeak(t *testing.T) {
	var leaked bool

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			leaked = true
		}

		_, _ = io.WriteString(w, `{"name":"deckhouse-cli","tags":["v1.0.0"]}`)
	}))
	t.Cleanup(target.Close)

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusFound)
	}))
	t.Cleanup(redirector.Close)

	httpClient := &http.Client{Transport: authInjectingTransport{base: http.DefaultTransport}}
	client := NewWithHTTPClient(redirector.URL, httpClient, dkplog.NewNop())

	_, err := client.ListTags(context.Background(), CLIImage())
	require.Error(t, err, "a redirect from the proxy is a protocol violation, not something to follow")
	assert.Contains(t, err.Error(), "302")
	assert.False(t, leaked, "the kubeconfig bearer must never be replayed to a redirect target")
}

func TestClientInvalidTagDoesNotCallServer(t *testing.T) {
	client := testClient(t, func(_ http.ResponseWriter, _ *http.Request) {
		t.Fatal("server must not be called for an invalid tag")
	})

	_, err := client.PullImage(context.Background(), CLIImage(), "with/slash")
	require.ErrorIs(t, err, ErrInvalidImage)
}

func TestClientListTagsRejectsOversizedResponse(t *testing.T) {
	client := testClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", mediaTypeJSON)
		_, _ = io.WriteString(w, `{"name":"deckhouse-cli","tags":[`)

		filler := `"` + strings.Repeat("x", 1024) + `",`
		for written := int64(0); written <= maxTagsResponseBytes; written += int64(len(filler)) {
			_, _ = io.WriteString(w, filler)
		}

		_, _ = io.WriteString(w, `"end"]}`)
	})

	_, err := client.ListTags(context.Background(), CLIImage())
	require.Error(t, err, "a response beyond the cap is a decode error, not an unbounded buffer")
}
