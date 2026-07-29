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

package plugins

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/rpp"
)

type tarFile struct {
	content string
	mode    int64
}

func gzipTar(t *testing.T, files map[string]tarFile) []byte {
	t.Helper()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	for name, f := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name:     name,
			Mode:     f.mode,
			Size:     int64(len(f.content)),
			Typeflag: tar.TypeReg,
		}))
		_, err := tw.Write([]byte(f.content))
		require.NoError(t, err)
	}

	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())

	return buf.Bytes()
}

func newTestRppSource(t *testing.T, body []byte) *rppPluginSource {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/images/deckhouse-cli/plugins/stronghold/images/v1.0.0", r.URL.Path)

		w.Header().Set("Content-Type", "application/x-gzip")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	client := rpp.NewWithHTTPClient(srv.URL, srv.Client(), dkplog.NewNop())

	return newRppPluginSource(client, dkplog.NewNop())
}

func TestRppSourceExtractPlugin(t *testing.T) {
	// A non-executable mode in the image must still yield an executable binary.
	src := newTestRppSource(t, gzipTar(t, map[string]tarFile{"plugin": {content: "BINARY", mode: 0o644}}))
	dest := filepath.Join(t.TempDir(), "stronghold")

	require.NoError(t, src.ExtractPlugin(context.Background(), "stronghold", "v1.0.0", dest))

	got, err := os.ReadFile(dest)
	require.NoError(t, err)
	assert.Equal(t, "BINARY", string(got))

	info, err := os.Stat(dest)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&0o100, "extracted binary must be executable")
}

// newTestRppManifestSource serves the /manifests route, returning the given status
// and (for 200) a raw manifest body. The contract tests read the contract from the
// manifest annotation rather than pulling the image.
func newTestRppManifestSource(t *testing.T, status int, manifestJSON string) *rppPluginSource {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/images/deckhouse-cli/plugins/stronghold/manifests/v1.0.0", r.URL.Path)

		if status != http.StatusOK {
			w.WriteHeader(status)

			return
		}

		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		_, _ = io.WriteString(w, manifestJSON)
	}))
	t.Cleanup(srv.Close)

	client := rpp.NewWithHTTPClient(srv.URL, srv.Client(), dkplog.NewNop())

	return newRppPluginSource(client, dkplog.NewNop())
}

// manifestWithContract renders a minimal manifest carrying contractJSON as the
// base64-encoded "contract" annotation - the exact shape the CLI decodes.
func manifestWithContract(t *testing.T, contractJSON string) string {
	t.Helper()

	encoded := base64.StdEncoding.EncodeToString([]byte(contractJSON))
	raw, err := json.Marshal(imageManifest{Annotations: map[string]string{contractAnnotation: encoded}})
	require.NoError(t, err)

	return string(raw)
}

func TestRppSourceGetPluginContractBackfillsIdentity(t *testing.T) {
	src := newTestRppManifestSource(t, http.StatusOK, manifestWithContract(t, `{}`))

	plugin, err := src.GetPluginContract(context.Background(), "stronghold", "v1.0.0")
	require.NoError(t, err)
	assert.Equal(t, "stronghold", plugin.Name)
	assert.Equal(t, "v1.0.0", plugin.Version)
}

func TestRppSourceGetPluginContractTolerantWhenAbsent(t *testing.T) {
	// A manifest with no contract annotation is tolerated as name+version only.
	src := newTestRppManifestSource(t, http.StatusOK, `{"annotations":{}}`)

	plugin, err := src.GetPluginContract(context.Background(), "stronghold", "v1.0.0")
	require.NoError(t, err)
	assert.Equal(t, "stronghold", plugin.Name)
	assert.Equal(t, "v1.0.0", plugin.Version)
}

func TestRppSourceGetPluginContractParsesContract(t *testing.T) {
	src := newTestRppManifestSource(t, http.StatusOK,
		manifestWithContract(t, `{"name":"stronghold","version":"v1.0.0","description":"d"}`))

	plugin, err := src.GetPluginContract(context.Background(), "stronghold", "v1.0.0")
	require.NoError(t, err)
	assert.Equal(t, "stronghold", plugin.Name)
	assert.Equal(t, "d", plugin.Description)
}

func TestRppSourceGetPluginContractFollowsIndexToChild(t *testing.T) {
	// Multi-platform plugin: the tag points at an index with no top-level contract;
	// the contract lives on the child manifest. The CLI follows the first child by
	// digest and reads it there.
	const childDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	const indexJSON = `{"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"digest":"` + childDigest + `"}]}`
	child := manifestWithContract(t, `{"name":"stronghold","version":"v1.0.0","description":"from-child"}`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/images/deckhouse-cli/plugins/stronghold/manifests/v1.0.0":
			_, _ = io.WriteString(w, indexJSON)
		case "/v1/images/deckhouse-cli/plugins/stronghold/manifests/" + childDigest:
			_, _ = io.WriteString(w, child)
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	src := newRppPluginSource(rpp.NewWithHTTPClient(srv.URL, srv.Client(), dkplog.NewNop()), dkplog.NewNop())

	plugin, err := src.GetPluginContract(context.Background(), "stronghold", "v1.0.0")
	require.NoError(t, err)
	assert.Equal(t, "stronghold", plugin.Name)
	assert.Equal(t, "from-child", plugin.Description)
}

func TestRppSourceGetPluginContractRejectsMalformedAnnotation(t *testing.T) {
	// A present but non-base64 contract annotation is a publishing bug, not a
	// contract-less plugin - it must surface as an error.
	src := newTestRppManifestSource(t, http.StatusOK, `{"annotations":{"contract":"!!not-base64!!"}}`)

	_, err := src.GetPluginContract(context.Background(), "stronghold", "v1.0.0")
	require.Error(t, err)
}

func TestRppSourceGetPluginContractPropagatesError(t *testing.T) {
	// An operational failure (5xx) must surface as an error, not a tolerated absence.
	src := newTestRppManifestSource(t, http.StatusInternalServerError, "")

	_, err := src.GetPluginContract(context.Background(), "stronghold", "v1.0.0")
	require.Error(t, err)
}
