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

package selfupdate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/rpp"
)

// newTestRPPSource serves the given handler over TLS and returns a source wired to it.
func newTestRPPSource(t *testing.T, handler http.HandlerFunc) Source {
	t.Helper()

	srv := httptest.NewTLSServer(handler)
	t.Cleanup(srv.Close)

	client, err := rpp.New(
		srv.URL,
		&rest.Config{Host: srv.URL, BearerToken: "test-token"},
		dkplog.NewNop(),
		rpp.WithInsecureSkipTLSVerify(),
	)
	require.NoError(t, err)

	return NewRPPSource(client)
}

func TestVersionsSortsNewestFirstAndSkipsGarbage(t *testing.T) {
	source := newTestRPPSource(t, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name": "deckhouse-cli",
			"tags": []string{"v0.13.0", "not-a-version", "v0.14.0", "v0.13.1"},
		})
	})

	versions, err := NewUpdater(source, nil, dkplog.NewNop()).Versions(context.Background())
	require.NoError(t, err)

	got := make([]string, 0, len(versions))
	for _, v := range versions {
		got = append(got, v.Original())
	}

	assert.Equal(t, []string{"v0.14.0", "v0.13.1", "v0.13.0"}, got)
}

// TestRPPSourceListTagsNormalizesPlatformTags checks that this platform's
// "-<os>-<arch>" suffix is stripped (so the Updater can select the version),
// while foreign-platform and bare tags pass through untouched.
func TestRPPSourceListTagsNormalizesPlatformTags(t *testing.T) {
	source := newTestRPPSource(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/images/deckhouse-cli/tags", r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name": "deckhouse-cli",
			"tags": []string{
				"v0.13.1",                        // legacy bare tag
				"v0.14.0" + rpp.PlatformSuffix(), // ours -> normalized
				"v0.14.0-windows-amd64",          // foreign -> raw passthrough
				"v0.14.1" + rpp.PlatformSuffix(), // ours -> normalized
				"v0.14.1",                        // bare duplicate of ours -> deduped
			},
		})
	})

	tags, err := source.ListTags(context.Background())
	require.NoError(t, err)
	assert.ElementsMatch(t,
		[]string{"v0.13.1", "v0.14.0", "v0.14.0-windows-amd64", "v0.14.1"},
		tags,
	)

	// End to end through the Updater: the platform tag must win the selection,
	// and the foreign platform tag must never be picked (parses as pre-release).
	updater := NewUpdater(source, nil, dkplog.NewNop())
	latest, newer, err := updater.LatestVersion(context.Background(), "v0.13.1")
	require.NoError(t, err)
	assert.True(t, newer)
	assert.Equal(t, "v0.14.1", latest)
}

// TestRPPSourceExtractPrefersPlatformTag checks that the bare version selected by
// the Updater is resolved back to this platform's tag on download.
func TestRPPSourceExtractPrefersPlatformTag(t *testing.T) {
	tarball := gzipTarWithD8(t, "PLATFORM-BINARY")

	var requested []string

	source := newTestRPPSource(t, func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.Path)
		require.Equal(t, "/v1/images/deckhouse-cli/tags/v0.14.0"+rpp.PlatformSuffix(), r.URL.Path)
		_, _ = w.Write(tarball)
	})

	destination := filepath.Join(t.TempDir(), "d8.new")
	require.NoError(t, source.ExtractBinary(context.Background(), "v0.14.0", destination))

	got, err := os.ReadFile(destination)
	require.NoError(t, err)
	assert.Equal(t, "PLATFORM-BINARY", string(got))
	assert.Len(t, requested, 1, "the platform tag must be fetched directly, no extra round-trips")
}

// TestRPPSourceExtractFallsBackToBareTag checks legacy/platform-neutral publishing:
// when the platform tag is absent (404), the bare tag is downloaded instead.
func TestRPPSourceExtractFallsBackToBareTag(t *testing.T) {
	tarball := gzipTarWithD8(t, "BARE-BINARY")

	var requested []string

	source := newTestRPPSource(t, func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.Path)

		if r.URL.Path == "/v1/images/deckhouse-cli/tags/v0.13.1" {
			_, _ = w.Write(tarball)

			return
		}

		http.Error(w, "not found", http.StatusNotFound)
	})

	destination := filepath.Join(t.TempDir(), "d8.new")
	require.NoError(t, source.ExtractBinary(context.Background(), "v0.13.1", destination))

	got, err := os.ReadFile(destination)
	require.NoError(t, err)
	assert.Equal(t, "BARE-BINARY", string(got))
	assert.Equal(t, []string{
		"/v1/images/deckhouse-cli/tags/v0.13.1" + rpp.PlatformSuffix(),
		"/v1/images/deckhouse-cli/tags/v0.13.1",
	}, requested, "platform tag tried first, bare tag second")
}

// TestRPPSourceExtractPropagatesNonNotFoundErrors checks that the fallback fires
// only on 404: a 403 on the platform tag must surface as-is, not mask itself with
// a second request.
func TestRPPSourceExtractPropagatesNonNotFoundErrors(t *testing.T) {
	var requests int

	source := newTestRPPSource(t, func(w http.ResponseWriter, _ *http.Request) {
		requests++

		http.Error(w, "forbidden", http.StatusForbidden)
	})

	err := source.ExtractBinary(context.Background(), "v0.14.0", filepath.Join(t.TempDir(), "d8.new"))
	require.ErrorIs(t, err, rpp.ErrForbidden)
	assert.Equal(t, 1, requests)
}
