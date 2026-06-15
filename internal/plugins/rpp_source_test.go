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
		assert.Equal(t, "/v1/images/deckhouse-cli/plugins/stronghold/tags/v1.0.0", r.URL.Path)

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

func TestRppSourceGetPluginContractBackfillsIdentity(t *testing.T) {
	src := newTestRppSource(t, gzipTar(t, map[string]tarFile{
		pluginContractEntryName: {content: `{}`, mode: 0o644},
	}))

	plugin, err := src.GetPluginContract(context.Background(), "stronghold", "v1.0.0")
	require.NoError(t, err)
	assert.Equal(t, "stronghold", plugin.Name)
	assert.Equal(t, "v1.0.0", plugin.Version)
}

func TestRppSourceGetPluginContractTolerantWhenAbsent(t *testing.T) {
	src := newTestRppSource(t, gzipTar(t, map[string]tarFile{"plugin": {content: "BINARY", mode: 0o755}}))

	plugin, err := src.GetPluginContract(context.Background(), "stronghold", "v1.0.0")
	require.NoError(t, err)
	assert.Equal(t, "stronghold", plugin.Name)
	assert.Equal(t, "v1.0.0", plugin.Version)
}

func TestRppSourceGetPluginContractParsesFile(t *testing.T) {
	// The real contract ships as YAML (contract.yaml), as produced by the plugin CI.
	contract := "name: stronghold\nversion: v1.0.0\ndescription: d\n"
	src := newTestRppSource(t, gzipTar(t, map[string]tarFile{
		pluginContractEntryName: {content: contract, mode: 0o644},
	}))

	plugin, err := src.GetPluginContract(context.Background(), "stronghold", "v1.0.0")
	require.NoError(t, err)
	assert.Equal(t, "stronghold", plugin.Name)
	assert.Equal(t, "d", plugin.Description)
}
